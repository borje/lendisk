package gateway

import (
	"os"
	"syscall"
	"time"
)

// changeTime is a file's status change time (ctime): set by every write,
// rename, link and attribute change, and never by a user's touch, so a
// change in place since a time shows in it although the modification
// time was set back (rsync -t, cp -p).
func changeTime(fi os.FileInfo) time.Time {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return time.Unix(int64(st.Ctim.Sec), int64(st.Ctim.Nsec)) //nolint:unconvert // int32 on 32-bit ARM
	}
	return fi.ModTime()
}
