// Package seed moves a bucket's first copy to a friend on a removable
// disk instead of the network (REPL-8, #36), in three steps:
//
//   - Export, on the borrower: the keys of a bucket placed at the
//     friend, each its latest captured version, as plain files in the
//     backup tool's layout (export.KopiaPath for a Kopia bucket), plus
//     the seed manifest at ManifestPath. From then on the friend's
//     queued rows of the bucket are held from the network (state.Seed).
//   - Import, on the friend: each object of the manifest is put into the
//     borrower's bucket through the friend's own running gateway, signed
//     as the borrower, so the lender's quota, floor, hash and
//     existing-key checks (LEND-1, LEND-2, LEND-4) apply exactly as to a
//     put over the tunnel. Data goes before metadata, and metadata not
//     at all once data was refused; a disk older than one imported, and
//     a key the borrower deleted here, are refused (#95).
//   - Confirmer, in the borrower's daemon: lists the friend's copy
//     (NET-7 list) and acknowledges each held row the list shows with the
//     captured hash; the rest stay queued and go by the network once the
//     seed ends.
//
// Rules a caller must keep:
//   - The manifest is authenticated with the pair's derived secret
//     (NET-7a): only the borrower (or the lender itself) can write one the
//     lender accepts, and only the lender it names accepts it.
//   - Files and directories on the disk are created 0600 and 0700.
//   - Nothing on the disk is trusted for acknowledgement: only the
//     friend's own list, compared with this node's captured hash.
package seed

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/export"
	"github.com/borje/lendisk/internal/identity"
)

// ManifestPath is where an export keeps its manifest, below the
// exported files. .lendisk/ is reserved (no borrower key starts there,
// SEC-5), so it never collides with a key, and restic and Kopia ignore
// it.
const ManifestPath = ".lendisk/seed.json"

// Format is the manifest's format version.
const Format = 1

// Layouts of the exported files.
const (
	LayoutPlain = "plain" // one file per key at its key path (restic, generic)
	LayoutKopia = "kopia" // Kopia's sharded filesystem layout (export.KopiaPath)
)

// macLabel starts the manifest's MAC input, so the pair secret's MAC
// here never equals one it makes elsewhere.
const macLabel = "lendisk-seed-v1\n"

// Manifest says what a seeding disk holds and for whom.
type Manifest struct {
	Format     int       `json:"format"`
	Borrower   string    `json:"borrower"` // the node that exported, in display form
	Lender     string    `json:"lender"`   // the friend it is for
	Bucket     string    `json:"bucket"`   // the bucket's sealed name (SEC-16)
	Layout     string    `json:"layout"`
	ExportedAt time.Time `json:"exported_at"`
	Objects    []Object  `json:"objects"`
}

// Object is one exported object.
type Object struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // the capture's hash (REPL-10)
	// Class is the key's class at export (EP-7): data, metadata or
	// snapshot. Added in #95 within format 1: a manifest without it
	// counts every object as data.
	Class string `json:"class,omitempty"`
}

// meta reports whether o is metadata or a snapshot (REPL-12): put only
// after every data object (#95).
func (o Object) meta() bool { return o.Class != "" && o.Class != "data" }

// Path is where the object's file lies under the export directory.
func (m Manifest) Path(key string) string {
	if m.Layout == LayoutKopia {
		return filepath.FromSlash(export.KopiaPath(key))
	}
	return filepath.FromSlash(key)
}

type envelope struct {
	Seed json.RawMessage `json:"seed"`
	MAC  string          `json:"mac"` // HMAC-SHA256 of macLabel and seed, keyed by the pair secret
}

func mac(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(macLabel))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// writeManifest writes m under dir, authenticated with secret.
func writeManifest(dir string, m Manifest, secret string) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	b, err := json.Marshal(envelope{Seed: body, MAC: mac(secret, body)})
	if err != nil {
		return err
	}
	p := filepath.Join(dir, filepath.FromSlash(ManifestPath))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return export.WriteNew(p, strings.NewReader(string(b)+"\n"))
}

// MaxManifest bounds the seed manifest import reads: about 200 bytes an
// object, so some 2.5 million objects, a restic repository of 40 TB in
// its default 16 MiB packs.
const MaxManifest = 512 << 20

// errNotRegular is a path on the disk that is no regular file, or
// passes through a symbolic link.
var errNotRegular = errors.New("not a regular file on the disk (a symbolic link, directory or device); nothing is followed")

// openPlain opens the regular file rel under root for reading. It
// follows no symbolic link, at any component, and opens no FIFO or
// device: a disk is someone else's filesystem, and an import reads only
// the files the export wrote (#36 review).
func openPlain(root *os.Root, rel string) (*os.File, error) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	for i := range parts {
		fi, err := root.Lstat(filepath.Join(parts[:i+1]...))
		if err != nil {
			return nil, err
		}
		last := i == len(parts)-1
		if !last && !fi.IsDir() || last && !fi.Mode().IsRegular() {
			return nil, fmt.Errorf("%s: %w", rel, errNotRegular)
		}
	}
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		f.Close()
		return nil, err
	} else if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s: %w", rel, errNotRegular)
	}
	return f, nil
}

// ErrNoManifest is returned for a directory without a seed manifest.
var ErrNoManifest = errors.New("no seed manifest")

// ReadManifest reads the manifest under dir and checks it was exported
// for self by a borrower holding the pair's secret, and that every
// object names a file inside dir.
func ReadManifest(dir string, self *identity.Identity) (Manifest, error) {
	var m Manifest
	root, err := os.OpenRoot(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return m, fmt.Errorf("%s has %w (%s): give the directory `lendisk export <bucket> --for <friend>` wrote", dir, ErrNoManifest, ManifestPath)
	} else if err != nil {
		return m, err
	}
	defer root.Close()
	f, err := openPlain(root, filepath.FromSlash(ManifestPath))
	if errors.Is(err, fs.ErrNotExist) {
		return m, fmt.Errorf("%s has %w (%s): give the directory `lendisk export <bucket> --for <friend>` wrote", dir, ErrNoManifest, ManifestPath)
	} else if err != nil {
		return m, fmt.Errorf("the seed manifest %s: %w", ManifestPath, err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxManifest+1))
	if err != nil {
		return m, err
	}
	if len(b) > MaxManifest {
		return m, fmt.Errorf("the seed manifest %s is larger than %d bytes, the most an import reads", ManifestPath, MaxManifest)
	}
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return m, fmt.Errorf("the seed manifest %s is malformed: %w", ManifestPath, err)
	}
	if err := json.Unmarshal(env.Seed, &m); err != nil {
		return m, fmt.Errorf("the seed manifest %s is malformed: %w", ManifestPath, err)
	}
	if m.Format != Format {
		return m, fmt.Errorf("the seed manifest is in format %d; this lendisk reads format %d: upgrade lendisk", m.Format, Format)
	}
	if m.Lender != self.NodeID().String() {
		return m, fmt.Errorf("this disk was exported for node %s, not this node (%s): import it there", m.Lender, self.NodeID())
	}
	borrower, err := identity.ParseNodeID(m.Borrower)
	if err != nil {
		return m, fmt.Errorf("the seed manifest names no borrower: %w", err)
	}
	secret, err := self.S3Secret(borrower)
	if err != nil {
		return m, err
	}
	if !hmac.Equal([]byte(mac(secret, env.Seed)), []byte(env.MAC)) {
		return m, fmt.Errorf("the seed manifest does not authenticate as %s's for this node: it was changed, or not written by that node", m.Borrower)
	}
	if !identity.IsSealedName(m.Bucket) {
		return m, fmt.Errorf("the seed manifest's bucket %q is not a sealed name", m.Bucket)
	}
	if m.Layout != LayoutPlain && m.Layout != LayoutKopia {
		return m, fmt.Errorf("the seed manifest's layout %q is unknown", m.Layout)
	}
	for _, o := range m.Objects {
		if !filepath.IsLocal(filepath.FromSlash(o.Key)) || strings.HasSuffix(o.Key, "/") ||
			m.Layout == LayoutKopia && strings.Contains(o.Key, "/") {
			return m, fmt.Errorf("the seed manifest names key %q, which is no file of the export", o.Key)
		}
	}
	return m, nil
}
