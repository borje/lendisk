//go:build !linux

package gateway

import (
	"fmt"
	"os"
	"time"
)

// fileStamp is a file's size and modification time where the inode is
// not read (lendisk ships for Linux; this keeps other builds working).
func fileStamp(fi os.FileInfo) string {
	return fmt.Sprintf("0:%d:%d", fi.Size(), fi.ModTime().UnixNano())
}

// changeTime is the modification time where the change time is not read.
func changeTime(fi os.FileInfo) time.Time { return fi.ModTime() }
