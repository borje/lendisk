//go:build !linux

package gateway

import (
	"fmt"
	"os"
)

// fileStamp is a file's size and modification time where the inode is
// not read (lendisk ships for Linux; this keeps other builds working).
func fileStamp(fi os.FileInfo) string {
	return fmt.Sprintf("0:%d:%d", fi.Size(), fi.ModTime().UnixNano())
}
