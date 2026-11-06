package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// errLocked is returned by tryLock when another process holds the lock.
var errLocked = errors.New("locked")

// tryLock takes an exclusive advisory lock (flock) on dir/locks/name,
// creating the 0700 directory and the 0600 file as needed. It does not
// wait: a lock held by another process, or another open in this one,
// is errLocked. The kernel drops the lock when the process ends, even
// on a crash; release drops it sooner. The file is left in place, since
// removing a locked file lets a later run lock a new one while a
// waiting run still holds the old.
func tryLock(dir, name string) (release func(), err error) {
	locks := filepath.Join(dir, "locks")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(locks, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(locks, name), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errLocked
		}
		return nil, fmt.Errorf("locking %s: %w", f.Name(), err)
	}
	return func() { f.Close() }, nil
}
