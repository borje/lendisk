package gateway

import (
	"os"
	"path/filepath"
	"syscall"
)

type fileID struct{ dev, ino uint64 }

func idOf(fi os.FileInfo) (fileID, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fileID{}, false
	}
	return fileID{uint64(st.Dev), uint64(st.Ino)}, true //nolint:unconvert // narrower on some 32-bit platforms
}

// openFiles returns whether a file is held open by this process now, by
// its device and inode, read from /proc/self/fd (each entry stats as
// the file it is open on).
func openFiles() (func(os.FileInfo) bool, error) {
	const fds = "/proc/self/fd"
	es, err := os.ReadDir(fds)
	if err != nil {
		return nil, err
	}
	open := map[fileID]bool{}
	for _, e := range es {
		fi, err := os.Stat(filepath.Join(fds, e.Name()))
		if err != nil {
			continue // closed meanwhile, or the directory's own
		}
		if id, ok := idOf(fi); ok {
			open[id] = true
		}
	}
	return func(fi os.FileInfo) bool {
		id, ok := idOf(fi)
		return !ok || open[id] // unknown counts as open: left alone
	}, nil
}
