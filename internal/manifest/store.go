package manifest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
)

// Dir is where a lender keeps its friends' manifests:
// <store>/.lendisk/manifests. It is in the store, so it survives the
// loss of lendisk.db and goes with a disk handed over (P-5), and its
// top directory's name begins with a dot, which no S3 bucket name may,
// so versitygw serves it to no account at all: no borrower, no bucket
// key, not even the gateway's root (PEER-8).
func Dir(storeDir string) string {
	return filepath.Join(storeDir, ".lendisk", "manifests")
}

// Store keeps one manifest per friend as the file <Dir>/<node ID>, 0600
// in 0700 directories. It implements control.ManifestStore.
type Store struct {
	Dir string
}

var _ control.ManifestStore = (*Store)(nil)

// Put replaces peer's manifest with blob: written to a temporary file,
// synced and renamed over the old one, so a crash leaves the old
// manifest or the new, never a mix. Any envelope of the right shape is
// kept, whatever its version (package comment).
func (s *Store) Put(peer identity.NodeID, blob []byte) error {
	if len(blob) < minEnvelope || len(blob) > control.MaxManifest || string(blob[:len(magicPrefix)]) != magicPrefix {
		return control.ErrBadManifest
	}
	if err := s.ensureDir(); err != nil {
		return err
	}
	f, err := os.CreateTemp(s.Dir, ".put-*") // 0600
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp) // after a successful rename, a no-op
	if _, err := f.Write(blob); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.Dir, peer.String())); err != nil {
		return err
	}
	d, err := os.Open(s.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Get returns peer's manifest, or control.ErrNoManifest.
func (s *Store) Get(peer identity.NodeID) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(s.Dir, peer.String()))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, control.ErrNoManifest
	}
	return b, err
}

// ensureDir creates Dir and its parent at 0700, and keeps them 0700.
func (s *Store) ensureDir() error {
	for _, d := range []string{filepath.Dir(s.Dir), s.Dir} {
		if err := os.Mkdir(d, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("manifest directory: %w", err)
		}
		if err := os.Chmod(d, 0o700); err != nil {
			return fmt.Errorf("manifest directory: %w", err)
		}
	}
	return nil
}
