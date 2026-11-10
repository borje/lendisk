package seed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/borje/lendisk/internal/export"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Exporter writes a bucket's objects placed at a friend to a disk.
type Exporter struct {
	DB       *state.DB
	Self     *identity.Identity
	StoreDir string // the node's store: own buckets at <store>/<bucket>/<key>
	Now      func() time.Time
}

// ExportResult is what an export wrote.
type ExportResult struct {
	Objects int64
	Bytes   int64
	// Changed counts keys left out because the store's copy no longer
	// matches their captured hash (a write under way); the network sends
	// their new version.
	Changed int64
}

// ErrNothingPlaced is returned when the friend has nothing of the
// bucket placed there.
var ErrNothingPlaced = errors.New("nothing of the bucket is placed at that friend")

func (e *Exporter) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// Export writes bucket's objects placed at place to dst, a new or empty
// directory, in the layout kopia says, and starts the seed that holds
// place's queued rows of the bucket until its list confirms them. A
// failed export holds nothing; what it wrote is left for the caller to
// remove.
func (e *Exporter) Export(ctx context.Context, bucket string, place identity.NodeID, kopia bool, dst string) (ExportResult, error) {
	var r ExportResult
	objs, err := e.DB.SeedObjects(ctx, place.String(), bucket)
	if err != nil {
		return r, err
	}
	if len(objs) == 0 {
		return r, ErrNothingPlaced
	}
	layout := LayoutPlain
	if kopia {
		layout = LayoutKopia
		for _, o := range objs {
			if strings.Contains(o.Key, "/") {
				return r, fmt.Errorf("key %q is no Kopia blob ID, though bucket %s has the kopia profile", o.Key, bucket)
			}
		}
	}
	secret, err := e.Self.S3Secret(place)
	if err != nil {
		return r, err
	}
	if err := export.EmptyDir(dst); err != nil {
		return r, err
	}
	var through int64
	for _, o := range objs {
		through = max(through, o.Seq)
	}
	// Held from now: the worker sends nothing the disk will carry.
	if err := e.DB.StartSeed(ctx, place.String(), bucket, through, e.now()); err != nil {
		return r, err
	}
	ok := false
	defer func() {
		if !ok {
			e.DB.DropSeed(context.WithoutCancel(ctx), place.String(), bucket)
		}
	}()
	m := Manifest{Format: Format, Borrower: e.Self.NodeID().String(), Lender: place.String(),
		Bucket: e.Self.SealedName(bucket), Layout: layout}
	// The rows not written: the seed does not wait for them (#95).
	var skipped []int64
	for _, o := range objs {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if strings.HasSuffix(o.Key, "/") || !filepath.IsLocal(filepath.FromSlash(o.Key)) {
			skipped = append(skipped, o.Seq)
			continue // a directory marker: no lender stores one (SEC-5)
		}
		written, err := copyChecked(filepath.Join(e.StoreDir, bucket, filepath.FromSlash(o.Key)),
			filepath.Join(dst, m.Path(o.Key)), o)
		if err != nil {
			return r, fmt.Errorf("exporting %s: %w", o.Key, err)
		}
		if !written {
			r.Changed++
			skipped = append(skipped, o.Seq)
			continue
		}
		m.Objects = append(m.Objects, Object{Key: o.Key, Size: o.Size, SHA256: o.SHA256, Class: o.Class})
		r.Objects++
		r.Bytes += o.Size
	}
	if kopia {
		if err := export.WriteNew(filepath.Join(dst, ".shards"), strings.NewReader(export.KopiaShards)); err != nil {
			return r, err
		}
	}
	m.ExportedAt = e.now().UTC()
	if err := writeManifest(dst, m, secret); err != nil {
		return r, err
	}
	if err := e.DB.SeedWritten(ctx, place.String(), bucket, r.Objects, r.Bytes, skipped, e.now()); err != nil {
		return r, err
	}
	ok = true
	return r, nil
}

// copyChecked copies the store's object src to a new file dst, hashing
// it on the way; written is false, and nothing left at dst, when the
// store's copy is absent or no longer the captured one.
func copyChecked(src, dst string, o state.SeedObject) (written bool, err error) {
	f, err := os.Open(src)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer f.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return false, err
	}
	h := sha256.New()
	cr := &counted{r: io.TeeReader(f, h)}
	if err := export.WriteNew(dst, cr); err != nil {
		return false, err
	}
	if cr.n != o.Size || hex.EncodeToString(h.Sum(nil)) != o.SHA256 {
		return false, os.Remove(dst)
	}
	return true, nil
}

type counted struct {
	r io.Reader
	n int64
}

func (c *counted) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
