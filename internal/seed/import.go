package seed

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Importer puts a seeding disk's objects into the borrower's bucket on
// the lender, through the lender's running gateway (Dial reaches its
// socket), so they pass the policy every borrower's put passes.
type Importer struct {
	Self *identity.Identity
	// DB is the lender's state: the newest export imported per bucket
	// (state.SeedImport) and the borrower's tombstones here.
	DB   *state.DB
	Dial func(ctx context.Context) (net.Conn, error)
	Now  func() time.Time
	// Progress, if set, is called after each object, stored or refused.
	Progress func(done, total int, bytes int64)
}

// ImportResult is what an import did.
type ImportResult struct {
	Borrower identity.NodeID
	Bucket   string // the sealed name
	Objects  int64  // stored, or already held with the same hash
	Bytes    int64
	Refused  []Refusal
	// Deferred counts the metadata objects not put because a data object
	// was refused: metadata names data, and must not land before it
	// (REPL-12, #95). The borrower's node sends them, after the data.
	Deferred int64
}

// Refusal is an object the lender refused, and why.
type Refusal struct {
	Key, Reason string
}

// ErrStopped is in the chain of an import that stopped before its end:
// the friendship's quota or the floor, the borrower no longer a friend,
// or the gateway failing.
var ErrStopped = errors.New("import stopped")

// Import reads m's objects from dir and puts each: every data object,
// in manifest order, then the metadata and snapshots, only if no data
// object was refused (REPL-12, #95). A refusal of one object (bytes
// that do not match the hash, a key held with other bytes, a key the
// borrower deleted here, a file missing, of another size, or no regular
// file: a symbolic link or a device is never followed, openPlain) is
// recorded and the import goes on; a refusal no later object can pass
// stops it, with ErrStopped and the result so far. A disk older than
// one imported before from the same bucket is refused whole
// (state.ErrOlderSeed).
func (im *Importer) Import(ctx context.Context, dir string, m Manifest) (ImportResult, error) {
	r := ImportResult{Bucket: m.Bucket}
	borrower, err := identity.ParseNodeID(m.Borrower)
	if err != nil {
		return r, err
	}
	r.Borrower = borrower
	secret, err := im.Self.S3Secret(borrower)
	if err != nil {
		return r, err
	}
	now := time.Now
	if im.Now != nil {
		now = im.Now
	}
	if newest, err := im.DB.SeedImport(ctx, m.Borrower, m.Bucket, m.ExportedAt, now()); errors.Is(err, state.ErrOlderSeed) {
		return r, fmt.Errorf("this disk was exported at %s, before the export of the same bucket imported here (%s), "+
			"and would bring back what %s deleted since: %w; import the newer disk, or let %s's node send the bucket over the network",
			m.ExportedAt.UTC().Format(time.RFC3339), newest.UTC().Format(time.RFC3339), m.Borrower, err, m.Borrower)
	} else if err != nil {
		return r, err
	}
	order := make([]Object, 0, len(m.Objects))
	for _, meta := range []bool{false, true} {
		for _, o := range m.Objects {
			if o.meta() == meta {
				order = append(order, o)
			}
		}
	}
	c, err := replica.NewSeeder(borrower, secret, func(ctx context.Context) (net.Conn, error) {
		conn, err := im.Dial(ctx)
		if err != nil {
			return nil, err
		}
		return &answered{Conn: conn}, nil
	}, replica.Options{})
	if err != nil {
		return r, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return r, err
	}
	defer root.Close()
	dataRefused := false
	for i, o := range order {
		if o.meta() && dataRefused {
			r.Deferred++
			if im.Progress != nil {
				im.Progress(i+1, len(order), r.Bytes)
			}
			continue
		}
		reason, err := im.deleted(ctx, m, o)
		if err == nil && reason == "" {
			reason, err = im.put(ctx, c, root, m.Path(o.Key), m.Bucket, o)
		}
		if reason != "" && !o.meta() {
			dataRefused = true
		}
		switch {
		case err != nil:
			return r, err
		case reason != "":
			r.Refused = append(r.Refused, Refusal{Key: o.Key, Reason: reason})
		default:
			r.Objects++
			r.Bytes += o.Size
		}
		if im.Progress != nil {
			im.Progress(i+1, len(order), r.Bytes)
		}
	}
	return r, nil
}

// deleted is a reason when the borrower deleted o's key here (a
// tombstone, not a rewrite's replaced version): a put of the same hash
// would revive it (the gateway's tomb.go), and the disk may predate the
// delete (#95). If the borrower still holds the key, its node sends it.
func (im *Importer) deleted(ctx context.Context, m Manifest, o Object) (string, error) {
	ts, err := im.DB.TombstonesOf(ctx, m.Borrower, m.Bucket, o.Key)
	if err != nil {
		return "", err
	}
	for _, t := range ts {
		if !t.Rewrite {
			return fmt.Sprintf("the friend deleted it here at %s (kept as a tombstone), so the disk does not bring it back",
				t.DeletedAt.UTC().Format(time.RFC3339)), nil
		}
	}
	return "", nil
}

// put puts one object: a reason when the lender (or the disk) refuses
// that object alone, an error wrapping ErrStopped when the import
// cannot go on.
func (im *Importer) put(ctx context.Context, c *replica.Friend, root *os.Root, path, sealed string, o Object) (string, error) {
	f, err := openPlain(root, path)
	if errors.Is(err, fs.ErrNotExist) {
		return "missing from the disk", nil
	} else if errors.Is(err, errNotRegular) {
		return err.Error(), nil
	} else if err != nil {
		return "", fmt.Errorf("%w: reading %s: %w", ErrStopped, path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("%w: reading %s: %w", ErrStopped, path, err)
	}
	if fi.Size() != o.Size {
		return fmt.Sprintf("the disk holds %d bytes, the export wrote %d: damaged on the disk", fi.Size(), o.Size), nil
	}
	err = c.Put(ctx, replica.Object{Bucket: sealed, Key: o.Key, Size: o.Size, SHA256: o.SHA256, Body: f})
	switch {
	case err == nil:
		return "", nil
	case errors.Is(err, replica.ErrRejected):
		return "its bytes do not match the hash the export recorded: damaged on the disk", nil
	case errors.Is(err, replica.ErrImmutable):
		return "this node holds the key with other bytes, and keeps them (LEND-4)", nil
	case errors.Is(err, replica.ErrQuota):
		return "", fmt.Errorf("%w at %s: the friendship's quota is used up, or this node's disk is below its free-space floor (LEND-2): %w",
			ErrStopped, o.Key, err)
	case errors.Is(err, replica.ErrRemoved):
		return "", fmt.Errorf("%w: the exporting node is not a friend of this node: %w", ErrStopped, err)
	}
	return "", fmt.Errorf("%w at %s: %w", ErrStopped, o.Key, err)
}

// answered is a connection to the gateway on which a write after the
// gateway stopped reading succeeds, discarding the bytes, so the client
// goes on to read the answer the gateway gave before it closed: a
// refusal sent before the body was read (507 for quota) rather than a
// broken pipe. On the tunnel the bridge reads that answer for the
// borrower; here nothing stands between. If no answer came, the read
// fails and the put with it. A unix socket keeps what the gateway wrote
// readable after it closed.
type answered struct {
	net.Conn
	failed bool
}

func (a *answered) Write(p []byte) (int, error) {
	if a.failed {
		return len(p), nil
	}
	n, err := a.Conn.Write(p)
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		a.failed = true
		return len(p), nil
	}
	return n, err
}
