//go:build !linux

package gateway

import (
	"os"
	"time"
)

// changeTime is a file's modification time where the status change time
// is not read (lendisk ships for Linux; this keeps other builds working).
func changeTime(fi os.FileInfo) time.Time { return fi.ModTime() }
