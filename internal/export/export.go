// Package export writes a borrower's replica, a lent bucket in this
// node's store, as plain files in the backup tool's own layout (LEND-6,
// COMPAT-4, P-5): one file per key at its key path, which is restic's
// repository layout, or Kopia's sharded filesystem layout. Exports from
// enough lenders copied into one directory are the repository.
//
// It reads the store directory directly, as the posix backend lays it
// out (DESIGN §3.10 COMPAT-4), so it needs no daemon: with lendisk
// stopped the store is quiet; with it running an object deleted while
// the export walks is left out, and one written after it passed is not
// in it.
//
// Rules a caller must keep:
//   - The destination is absent or an empty directory: an export never
//     merges into or overwrites anything.
//   - Only a lent bucket is exported, named as the store names it
//     (lent-<id16>-<sealed>, SEC-16); the lender does not know its real
//     name, nor its tool, which the caller says (kopia).
package export

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/borje/lendisk/internal/gateway"
)

// ErrNotLent is returned for a name that is not a lent bucket's.
var ErrNotLent = errors.New("not a lent bucket's stored name")

// Result is what an export wrote.
type Result struct {
	Objects int
	Bytes   int64
	// Gone counts objects that went from the store while the export
	// walked it (a running daemon tombed them).
	Gone int
	// Kopia is set when the bucket holds kopia.repository: it is a Kopia
	// repository, which only the Kopia layout makes a directory Kopia
	// opens.
	Kopia bool
}

// KopiaShards is the .shards file Kopia 0.23.1 writes in a new
// filesystem repository (repo/blob/sharded, sharded_parameters.go): blob
// IDs longer than 20 characters go into directories of their first 1,
// then the next 3, characters. An export writes it, so Kopia reads the
// layout from it rather than guessing.
const KopiaShards = `{"default":[1,3],"maxNonShardedLength":20}`

// kopiaBlobSuffix is what Kopia's sharded storage appends to a finished
// blob's file name (CompleteBlobSuffix); repoimport strips it.
const kopiaBlobSuffix = ".f"

// KopiaPath is where Kopia's filesystem storage, sharded as KopiaShards
// says, keeps blob id: the reverse of `bucket import`'s mapping (#9).
func KopiaPath(id string) string {
	if len(id) <= 20 {
		return id + kopiaBlobSuffix
	}
	return id[:1] + "/" + id[1:4] + "/" + id[4:] + kopiaBlobSuffix
}

// Lent exports the lent bucket name of the store at storeDir to dst, in
// Kopia's layout if kopia is set. A failure part way leaves what was
// written; the caller says to remove dst and run again.
func Lent(storeDir, name, dst string, kopia bool) (Result, error) {
	if !strings.HasPrefix(name, gateway.LentPrefix) || strings.ContainsAny(name, `/\`) || name == gateway.LentPrefix {
		return Result{}, fmt.Errorf("%q is %w: give the lent-<id16>-<sealed> name the borrower's recovery sheet gives", name, ErrNotLent)
	}
	src := filepath.Join(storeDir, name)
	if st, err := os.Stat(src); errors.Is(err, fs.ErrNotExist) || err == nil && !st.IsDir() {
		return Result{}, fmt.Errorf("the store %s holds no lent bucket %s; it holds: %s", storeDir, name, lentBuckets(storeDir))
	} else if err != nil {
		return Result{}, err
	}
	if err := EmptyDir(dst); err != nil {
		return Result{}, err
	}
	var r Result
	err := filepath.WalkDir(src, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p != src {
				return nil // a directory emptied and removed meanwhile
			}
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if de.IsDir() {
			if p != src && housekeeping(key) {
				return filepath.SkipDir
			}
			return nil
		}
		if !de.Type().IsRegular() {
			return nil
		}
		out := key
		if kopia {
			if strings.Contains(key, "/") {
				return fmt.Errorf("key %q is not a Kopia blob ID; this bucket is no Kopia repository: export it without --kopia-layout", key)
			}
			out = KopiaPath(key)
		}
		n, err := copyFile(p, filepath.Join(dst, filepath.FromSlash(out)))
		if errors.Is(err, fs.ErrNotExist) {
			r.Gone++
			return nil
		} else if err != nil {
			return err
		}
		r.Objects++
		r.Bytes += n
		r.Kopia = r.Kopia || key == "kopia.repository"
		return nil
	})
	if err != nil {
		return r, err
	}
	if kopia {
		if err := WriteNew(filepath.Join(dst, ".shards"), strings.NewReader(KopiaShards)); err != nil {
			return r, err
		}
	}
	return r, nil
}

// housekeeping reports whether a directory of a lent bucket is not part
// of the replica: lendisk's own (.lendisk/, the tombs, excluded at any
// depth as posix excludes it) or versitygw's at the bucket's top
// (gateway.ReservedDirs: .sgwtmp/ for temporary and multipart files).
func housekeeping(dir string) bool {
	if strings.EqualFold(filepath.Base(dir), gateway.HousekeepingDir) {
		return true
	}
	return !strings.Contains(dir, "/") && slices.ContainsFunc(gateway.ReservedDirs, func(d string) bool {
		return strings.EqualFold(dir, d)
	})
}

// lentBuckets lists the lent buckets in the store, for an error.
func lentBuckets(storeDir string) string {
	ents, err := os.ReadDir(storeDir)
	if err != nil {
		return err.Error()
	}
	var names []string
	for _, e := range ents {
		if e.IsDir() && strings.HasPrefix(e.Name(), gateway.LentPrefix) {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// EmptyDir makes dst an empty directory at 0700, refusing one that holds
// anything.
func EmptyDir(dst string) error {
	ents, err := os.ReadDir(dst)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(dst, 0o700); err != nil {
			return fmt.Errorf("export directory: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("export directory: %w", err)
	case len(ents) > 0:
		return fmt.Errorf("export directory %s is not empty: give a new or empty directory", dst)
	}
	return nil
}

// copyFile copies the object at src to a new file dst, making its
// directories; it returns the bytes copied.
func copyFile(src, dst string) (int64, error) {
	f, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return 0, err
	}
	cr := &countReader{r: f}
	err = WriteNew(dst, cr)
	return cr.n, err
}

// WriteNew writes r to a file that must not exist yet, at 0600, from
// the first byte.
func WriteNew(path string, r io.Reader) error {
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
