package gateway

import (
	"fmt"
	"os"
	"syscall"
)

// fileStamp is a file's identity as the bit-rot sampler and the store
// reconciler compare it (VER-5, #31): inode, size and modification time
// in nanoseconds. Writing a file changes its modification time; a
// replaced file has another inode. Rot changes none of them. Metadata
// (mode, owner, xattrs) is not part of it: a chmod is no edit.
func fileStamp(fi os.FileInfo) string {
	var ino uint64
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		ino = uint64(st.Ino) //nolint:unconvert // uint32 on some 32-bit platforms
	}
	return fmt.Sprintf("%d:%d:%d", ino, fi.Size(), fi.ModTime().UnixNano())
}
