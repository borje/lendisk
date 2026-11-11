package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/versity/versitygw/backend/posix"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// This file is what the usage reconcile does with what a lender crash
// left under a lent bucket's .sgwtmp, which no S3 listing shows
// (LEND-1, LEND-4, #66, #112; DESIGN §3.5 LEND-1):
//
//   - an upload posix hid while completing it (<upload ID>.<claim
//     token>.inprogress in the key's directory);
//   - a named temp file of posix's fallback when O_TMPFILE is
//     unavailable (os.CreateTemp with the pattern "<sha256 hex>.", in
//     .sgwtmp for a put or a completion and in the key's directory for
//     a part).
//
// It never follows a symlink and touches nothing but those names, and
// only in .sgwtmp and .sgwtmp/multipart/<key's directory> of a lent
// bucket. The reconcile (midCompletion) holds the borrower's pub lock
// exclusively; a completion (leftBehind, verifyLanded) holds the key's
// lock and the pub lock shared, so no other completion of the key runs.

// inProgressSuffix ends the name posix gives an upload's directory
// while it completes it: <bucket>/.sgwtmp/multipart/<sha256 of the
// key>/<upload ID>.<claim token>.inprogress (versitygw's posix.go,
// inProgressSuffix).
const inProgressSuffix = ".inprogress"

// keyDirName is the name of posix's directory of key's uploads.
func keyDirName(key string) string {
	s := sha256.Sum256([]byte(key))
	return hex.EncodeToString(s[:])
}

// isTempName reports whether name is one posix's named temp file
// fallback gives: os.CreateTemp(dir, "<sha256 hex of the object>.")
// appends a decimal uint32 (versitygw's with_otmpfile.go, openMkTemp,
// and without_otmpfile.go).
func isTempName(name string) bool {
	h, n, ok := strings.Cut(name, ".")
	if !ok || len(h) != 64 || strings.ToLower(h) != h {
		return false
	}
	if _, err := hex.DecodeString(h); err != nil {
		return false
	}
	_, err := strconv.ParseUint(n, 10, 32)
	return err == nil
}

// realDir reports whether path is a directory itself, not a symlink to
// one; anything but its absence is logged.
func (l *Lending) realDir(path string) bool {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil || !fi.IsDir() {
		l.log().Warn("lending: skipping a store directory that cannot be read or is not a directory", "path", path,
			"mode", modeOf(fi), "err", err)
		return false
	}
	return true
}

func modeOf(fi os.FileInfo) string {
	if fi == nil {
		return ""
	}
	return fi.Mode().String()
}

// midCompletion sums what a crash left under bucket's .sgwtmp, and
// removes what is garbage (see the file comment). id is the borrower.
// An unreadable directory is skipped, so the rest is still counted.
func (l *Lending) midCompletion(ctx context.Context, peer string, id identity.NodeID, bucket string, cutoff time.Time, live map[string]bool) (int64, error) {
	if l.StoreDir == "" {
		return 0, nil
	}
	tmp := filepath.Join(l.StoreDir, bucket, posix.MetaTmpDir)
	if !l.realDir(filepath.Join(l.StoreDir, bucket)) || !l.realDir(tmp) {
		return 0, nil
	}
	es, err := os.ReadDir(tmp)
	if err != nil {
		l.log().Warn("lending: skipping a borrower's .sgwtmp that cannot be read", "peer", peer, "bucket", bucket, "err", err)
		return 0, nil
	}
	total := l.tempFiles(peer, bucket, tmp, es, cutoff)
	root := filepath.Join(l.StoreDir, bucket, filepath.FromSlash(posix.MetaTmpMultipartDir))
	if !l.realDir(root) {
		return total, nil
	}
	keys, err := os.ReadDir(root)
	if err != nil {
		l.log().Warn("lending: skipping a borrower's uploads directory that cannot be read", "peer", peer, "bucket", bucket, "err", err)
		return total, nil
	}
	verified := map[string]bool{} // uploads whose linked object hashed right
	for _, k := range keys {
		if !k.IsDir() { // a symlink is not
			continue
		}
		keyDir := filepath.Join(root, k.Name())
		es, err := os.ReadDir(keyDir)
		if err != nil {
			l.log().Warn("lending: skipping a key's uploads directory that cannot be read; its uploads are not counted",
				"peer", peer, "bucket", bucket, "dir", keyDir, "err", err)
			continue
		}
		total += l.tempFiles(peer, bucket, keyDir, es, cutoff)
		for _, e := range es {
			if !e.IsDir() || !strings.HasSuffix(e.Name(), inProgressSuffix) {
				continue
			}
			n, err := l.hiddenUpload(ctx, peer, id, bucket, keyDir, e.Name(), cutoff, live, verified)
			if err != nil {
				return 0, err
			}
			total += n
		}
	}
	return total, nil
}

// hiddenUpload handles one upload posix hid mid-completion, in keyDir
// under name, and returns what it counts. No completion runs now: a
// completion holds the pub lock shared until the store answers. So the
// directory is a crash's (or a failed rename back's).
//
//   - If the object at its key is linked under its upload ID (the crash
//     came after posix published it), the object is hashed against the
//     hash the upload declared. A match proves the directory a copy of
//     the object: it is removed at once and the row dropped. A mismatch
//     drops the object, as a completion would; the directory is kept and
//     counted, and the borrower sends it again: no request answers for
//     the object before the lender verified it (vouch, #113). An object
//     that cannot be read or removed, or whose upload ID cannot be read,
//     leaves everything as it is, past AbandonAfter too, until a
//     reconcile can tell.
//   - Otherwise, once the directory has been still for AbandonAfter
//     (its change time: the hiding rename sets it on Linux) it is
//     removed, as an abandoned upload is aborted.
//   - Kept, it counts as an open upload, and its row, if any, is kept
//     and set to its parts.
func (l *Lending) hiddenUpload(ctx context.Context, peer string, id identity.NodeID, bucket, keyDir, name string,
	cutoff time.Time, live, verified map[string]bool) (int64, error) {
	dir := filepath.Join(keyDir, name)
	uid, _, _ := strings.Cut(name, ".")
	fi, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil || !fi.IsDir() {
		l.log().Warn("lending: skipping a hidden upload that cannot be read", "peer", peer, "bucket", bucket, "dir", dir, "err", err)
		return 0, nil
	}
	row, err := l.DB.GetLentUpload(ctx, uid)
	hasRow := err == nil && row.Peer == peer
	if err != nil && !errors.Is(err, state.ErrNotFound) {
		return 0, err
	}
	var total int64
	bad, kept := false, false
	if !verified[uid] && hasRow && LentName(id, row.Sealed) == bucket && keyDirName(row.Key) == filepath.Base(keyDir) {
		linked, err := l.linkedBy(bucket, row.Key, uid)
		if err != nil {
			l.log().Warn("lending: the upload ID of an object a hidden upload may have assembled cannot be read; keeping both",
				"peer", peer, "bucket", bucket, "key", row.Key, "upload", uid, "err", err)
			kept = true
		} else if linked {
			delta, v := l.landed(ctx, peer, bucket, row)
			total += delta
			switch v {
			case landedGood:
				verified[uid] = true
			case landedBad:
				hasRow, bad = false, true // dropped; the directory is not removed on that evidence
			case landedKept:
				kept = true // never removed unverified (#113)
			}
		}
	}
	switch {
	case verified[uid]:
		if l.removeHidden(peer, bucket, uid, dir, "its object is linked and verified") {
			return total, nil
		}
	case !bad && !kept && changeTime(fi).Before(cutoff):
		if l.removeHidden(peer, bucket, uid, dir, "still for the abandon period") {
			if hasRow {
				l.dropUpload(uid)
			}
			return total, nil
		}
	}
	ps, err := partsOnDisk(dir)
	if err != nil {
		l.log().Warn("lending: skipping a hidden upload's parts that cannot be read", "peer", peer, "bucket", bucket, "dir", dir, "err", err)
		return total, nil
	}
	total += itemOverhead
	for _, n := range ps {
		total += n
	}
	if hasRow && !verified[uid] {
		live[uid] = true
		if err := l.DB.SetLentUploadParts(ctx, uid, ps); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// removeHidden removes an upload's hidden directory dir, and the key's
// directory if no other upload of the key is left in it. RemoveAll
// unlinks a symlink inside, never what it points to.
func (l *Lending) removeHidden(peer, bucket, uid, dir, why string) bool {
	l.log().Warn("lending: removing a borrower's upload a crash left mid-completion", "peer", peer, "bucket", bucket,
		"upload", uid, "why", why)
	if err := os.RemoveAll(dir); err != nil {
		l.log().Warn("lending: removing a hidden upload failed; it stays counted", "dir", dir, "err", err)
		return false
	}
	os.Remove(filepath.Dir(dir)) // fails while the key's directory holds anything
	return true
}

// linkedBy reports whether the object at key was assembled by upload
// uid; an error (errUploadUnreadable) means it cannot be told.
func (l *Lending) linkedBy(bucket, key, uid string) (bool, error) {
	if l.Uploads == nil {
		return false, nil
	}
	got, err := l.Uploads.UploadID(bucket, key)
	return err == nil && got == uid, err
}

type landing int

const (
	landedKept landing = iota // the object could not be read, or failed and could not be removed; nothing changed
	landedGood                // it hashed to the declared hash; the row is dropped
	landedBad                 // it did not; it is removed and the row dropped
)

// landed hashes the object that upload u assembled before a crash
// (linkedBy holds), for the reconcile. On a mismatch the object is
// removed, a rewrite's replaced version moved back (undoRewrite, as a
// completion does), and the returned delta takes the object out of the
// listing's sum; the moved-back version was counted as a tomb. The row
// is dropped only once the object is verified or gone (#113).
func (l *Lending) landed(ctx context.Context, peer, bucket string, u state.LentUpload) (int64, landing) {
	n, got, err := l.hashStored(ctx, bucket, u.Key)
	if err != nil {
		l.log().Warn("lending: reading an object a crash left unverified failed; trying again at the next reconcile",
			"peer", peer, "bucket", bucket, "key", u.Key, "err", err)
		return 0, landedKept
	}
	if got == u.SHA256 {
		l.dropUpload(u.UploadID)
		return 0, landedGood
	}
	l.log().Warn("lending: an object a crash left unverified does not match its declared hash; removed it so the borrower sends it again",
		"peer", peer, "bucket", bucket, "key", u.Key, "upload", u.UploadID)
	if !l.remove(bucket, u.Key) {
		l.log().Warn("lending: keeping the row of an object that failed its hash and could not be removed; trying again at the next reconcile",
			"peer", peer, "bucket", bucket, "key", u.Key, "upload", u.UploadID)
		return 0, landedKept
	}
	l.undoRewrite(ctx, bucket, u)
	l.dropUpload(u.UploadID)
	return -(n + itemOverhead), landedBad
}

// hiddenDirs lists the directories of upload uid of key that posix hid
// mid-completion and that are still on disk.
func (l *Lending) hiddenDirs(bucket, key, uid string) []string {
	if l.StoreDir == "" {
		return nil
	}
	keyDir := filepath.Join(l.StoreDir, bucket, filepath.FromSlash(posix.MetaTmpMultipartDir), keyDirName(key))
	es, err := os.ReadDir(keyDir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range es {
		if e.IsDir() && strings.HasPrefix(e.Name(), uid+".") && strings.HasSuffix(e.Name(), inProgressSuffix) {
			out = append(out, filepath.Join(keyDir, e.Name()))
		}
	}
	return out
}

// leftBehind is what a completion that failed left of upload uid on
// disk, counted as an open upload is: posix answers a completion
// retried over a directory a crash hid with a success and no object
// (#112), and the directory stays until the reconcile removes it.
func (l *Lending) leftBehind(bucket, key, uid string) int64 {
	var total int64
	for _, dir := range l.hiddenDirs(bucket, key, uid) {
		ps, err := partsOnDisk(dir)
		if err != nil {
			continue
		}
		total += itemOverhead
		for _, n := range ps {
			total += n
		}
	}
	return total
}

// partsOnDisk reads the parts in an upload's directory: posix names
// each by its number.
func partsOnDisk(dir string) (map[int32]int64, error) {
	es, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ps := map[int32]int64{}
	for _, e := range es {
		n, err := strconv.ParseInt(e.Name(), 10, 32)
		if err != nil || !e.Type().IsRegular() {
			continue
		}
		fi, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		ps[int32(n)] = fi.Size()
	}
	return ps, nil
}

// tempFiles sums the named temp files among es, the entries of dir,
// that a crash left, and removes those still for AbandonAfter. With the
// pub lock held exclusively, the only operations that can own one are
// writes whose body is still streaming, which hold it open in this
// process (posix keeps it open from its creation until it publishes or
// removes it; publishing begins only once the body has ended, under the
// pub lock shared). So a file this process holds open is skipped, and
// any other is a crash's or a failed write's leftover. A platform where
// the open files cannot be read leaves them all alone. A file is read
// as open after dir was listed, so one created later is not missed.
func (l *Lending) tempFiles(peer, bucket, dir string, es []fs.DirEntry, cutoff time.Time) int64 {
	var (
		total int64
		open  func(os.FileInfo) bool
	)
	for _, e := range es {
		if !e.Type().IsRegular() || !isTempName(e.Name()) {
			continue
		}
		if open == nil {
			var err error
			if open, err = openFiles(); err != nil {
				l.log().Debug("lending: leaving the store's temp files alone", "err", err)
				return 0
			}
		}
		path := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(path)
		if err != nil || !fi.Mode().IsRegular() || open(fi) {
			continue
		}
		if changeTime(fi).Before(cutoff) {
			l.log().Warn("lending: removing a temp file a crash left in a borrower's bucket", "peer", peer, "bucket", bucket,
				"file", path, "size", fi.Size(), "since", changeTime(fi))
			err := os.Remove(path)
			if err == nil || errors.Is(err, fs.ErrNotExist) {
				continue
			}
			l.log().Warn("lending: removing a temp file failed; it stays counted", "file", path, "err", err)
		}
		total += fi.Size()
	}
	return total
}
