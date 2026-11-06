// Package store owns the store directory: the posix tree holding the node's
// own buckets and what it lends, side by side (D15, EP-15). It checks that
// the filesystem can hold a store (EP-12) before anything is written there.
//
// The directory is versitygw's to write; nothing else in lendisk touches
// files under it except through the gateway's backend.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/versity/versitygw/backend/meta"
	"golang.org/x/sys/unix"

	"github.com/borje/lendisk/internal/config"
)

// Space is a filesystem's size and free space in bytes.
type Space struct {
	Total, Free uint64
	FSType      string
}

// Checker runs the EP-12 checks. Its fields are injectable for tests.
type Checker struct {
	// Xattr probes user extended attributes on dir.
	Xattr func(dir string) error
	// Statfs reports size, free space and filesystem type for dir.
	Statfs func(dir string) (Space, error)
}

// DefaultChecker uses the real filesystem.
var DefaultChecker = Checker{Xattr: probeXattr, Statfs: statfs}

// Prepare creates dir at mode 0700 if needed and checks it.
func Prepare(dir string, floor config.Floor) error {
	return DefaultChecker.Prepare(dir, floor)
}

// Prepare creates dir at mode 0700 if needed and checks it. Generic
// buckets hold plaintext, so an existing directory that group or others
// can reach is refused, unless it is empty, when it is set to 0700
// (#53).
func (c Checker) Prepare(dir string, floor config.Floor) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("store directory: %w", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("store directory: %w", err)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("store directory: %w", err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("store directory %s has mode %04o, so other users can read the buckets in it, "+
				"and generic buckets hold plaintext: run `chmod 700 %s`, or choose another store.path", dir, perm, dir)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("store directory %s has mode %04o and cannot be set to 0700: %w", dir, perm, err)
		}
	}
	return c.Check(dir, floor)
}

// Check refuses a store directory whose filesystem lacks user xattrs, which
// the posix backend keeps ETags and metadata in, or has less free space
// than the floor. The error names the filesystem and the missing property.
func (c Checker) Check(dir string, floor config.Floor) error {
	sp, err := c.Statfs(dir)
	if err != nil {
		return fmt.Errorf("store directory %s: %w", dir, err)
	}
	if err := c.Xattr(dir); err != nil {
		return fmt.Errorf("store directory %s is on %s, which does not support user extended attributes (xattrs); "+
			"the store needs them for object metadata. Use ext4, xfs, btrfs or zfs: %w", dir, sp.FSType, err)
	}
	if need := FloorBytes(floor, sp.Total); sp.Free < need {
		return fmt.Errorf("store directory %s is on %s with %s free, below the free-space floor of %s (store.free_space_floor)",
			dir, sp.FSType, human(sp.Free), human(need))
	}
	return nil
}

// FloorBytes converts a floor to bytes for a filesystem of the given size.
func FloorBytes(f config.Floor, total uint64) uint64 {
	if f.Bytes > 0 {
		return f.Bytes
	}
	return uint64(float64(total) * f.Percent / 100)
}

func probeXattr(dir string) error {
	return meta.XattrMeta{}.Test(dir)
}

func statfs(dir string) (Space, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return Space{}, err
	}
	bs := uint64(st.Bsize)
	return Space{Total: st.Blocks * bs, Free: st.Bavail * bs, FSType: fsName(int64(st.Type))}, nil
}

// fsName names the common filesystems by their statfs magic number.
func fsName(magic int64) string {
	names := map[int64]string{
		0xEF53: "ext4", 0x58465342: "xfs", 0x9123683E: "btrfs", 0x2FC12FC1: "zfs",
		0x01021994: "tmpfs", 0x794C7630: "overlayfs", 0x6969: "nfs", 0xFF534D42: "cifs",
		0xFE534D42: "smb2", 0x4D44: "vfat", 0x2011BAB0: "exfat", 0x5346544E: "ntfs",
		0x65735546: "fuse", 0x73717368: "squashfs", 0xF2F52010: "f2fs",
	}
	if n, ok := names[magic]; ok {
		return n
	}
	return fmt.Sprintf("a filesystem of type 0x%x", magic)
}

func human(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ErrBadBucketPath guards against a bucket name escaping the store.
var ErrBadBucketPath = errors.New("bucket name is not a single path element")

// BucketDir is the directory of a bucket in the store.
func BucketDir(storeDir, bucket string) (string, error) {
	if bucket == "" || bucket != filepath.Base(bucket) || bucket == "." || bucket == ".." {
		return "", ErrBadBucketPath
	}
	return filepath.Join(storeDir, bucket), nil
}
