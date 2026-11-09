package seed

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/replica"
)

// Importer puts a seeding disk's objects into the borrower's bucket on
// the lender, through the lender's running gateway (Dial reaches its
// socket), so they pass the policy every borrower's put passes.
type Importer struct {
	Self *identity.Identity
	Dial func(ctx context.Context) (net.Conn, error)
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
}

// Refusal is an object the lender refused, and why.
type Refusal struct {
	Key, Reason string
}

// ErrStopped is in the chain of an import that stopped before its end:
// the friendship's quota or the floor, the borrower no longer a friend,
// or the gateway failing.
var ErrStopped = errors.New("import stopped")

// Import reads m's objects from dir and puts each, in manifest order. A
// refusal of one object (bytes that do not match the hash, a key held
// with other bytes, a file missing, of another size, or no regular
// file: a symbolic link or a device is never followed, openPlain) is
// recorded and the import goes on; a refusal no later object can pass
// stops it, with ErrStopped and the result so far.
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
	for i, o := range m.Objects {
		reason, err := im.put(ctx, c, root, m.Path(o.Key), m.Bucket, o)
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
			im.Progress(i+1, len(m.Objects), r.Bytes)
		}
	}
	return r, nil
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
