//go:build !linux

package gateway

import (
	"errors"
	"os"
)

// openFiles cannot tell which files this process holds open where
// /proc/self/fd is not read (lendisk ships for Linux), so the reconcile
// leaves the store's temp files alone.
func openFiles() (func(os.FileInfo) bool, error) {
	return nil, errors.New("open files are not read on this platform")
}
