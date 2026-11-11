package gateway

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/borje/lendisk/internal/state"
)

// This file judges a borrower's object whose upload-ID attribute
// (mp-metadata) cannot be read, for vouch and the usage reconcile
// (LEND-1, LEND-4, VER-5, #115; DESIGN §3.5 LEND-1). Without the upload
// ID the lender cannot tell which upload linked the object, and the
// hash in the object's own metadata (x-amz-meta-lendisk-sha256) is
// untrusted: it sits in the same damaged xattrs. Only a hash in
// lendisk.db is trusted: one a borrower declared for the key, kept in
// the row of an upload the store no longer has (a live upload has
// linked nothing). So:
//
//   - bytes that hash to such a row's hash are that upload's object,
//     verified (unreadGood);
//   - bytes that hash to the declared hash, a well-formed one, are an
//     object verified when it was stored (unreadSound); a row of
//     another hash did not link it, as an upload's object declares the
//     hash its create did;
//   - bytes that differ, read the same twice, from a declared hash that
//     a row also holds are that upload's object failing its hash
//     (unreadBad), and are removed so the borrower sends them again;
//   - anything else is rot without trusted proof (unreadRot): it is
//     never removed, but recorded and alerted as the bit-rot sampler
//     records a friend's rotten object (state.Rot), and not answered
//     for.
//
// Each object is hashed once while its file is unchanged (its inode,
// size and modification time), however many requests ask.

type unreadVerdict int

const (
	unreadGood  unreadVerdict = iota // the bytes match row's hash
	unreadSound                      // they match their declared hash, which no gone row holds
	unreadBad                        // they differ, twice, from the declared hash row holds
	unreadRot                        // no trusted hash vouches for them: recorded, kept
)

// unreadMemo remembers what an object's bytes hashed to, by its file's
// identity, so it is read once while unchanged.
type unreadMemo struct {
	mu   sync.Mutex
	sums map[string][2]string // bucket/key: stamp, sum
}

// fileStampOf is the identity of key's file in bucket, "" if unknown.
func (l *Lending) fileStampOf(bucket, key string) string {
	fp, ok := l.Uploads.(filePaths)
	if !ok {
		return ""
	}
	fi, err := os.Lstat(fp.Path(bucket, key))
	if err != nil || !fi.Mode().IsRegular() {
		return ""
	}
	return fileStamp(fi)
}

// sumOnce hashes key's object, or returns what it hashed to before if
// its file is unchanged (fresh false). A mismatch with want is read a second time,
// and must read the same, as the bit-rot sampler does.
func (l *Lending) sumOnce(ctx context.Context, bucket, key, want string) (n int64, sum string, fresh bool, err error) {
	id := bucket + "/" + key
	stamp := l.fileStampOf(bucket, key)
	l.unread.mu.Lock()
	m, ok := l.unread.sums[id]
	l.unread.mu.Unlock()
	if ok && stamp != "" && m[0] == stamp {
		return 0, m[1], false, nil
	}
	if l.afterVouchRead != nil {
		l.afterVouchRead()
	}
	n, sum, err = l.hashStored(ctx, bucket, key)
	if err != nil {
		return 0, "", false, err
	}
	if sum != want {
		n2, sum2, err := l.hashStored(ctx, bucket, key)
		if err != nil {
			return 0, "", false, err
		}
		if n2 != n || sum2 != sum {
			return 0, "", false, errChangedWhileRead
		}
	}
	if stamp != "" && l.fileStampOf(bucket, key) == stamp {
		l.unread.mu.Lock()
		if l.unread.sums == nil {
			l.unread.sums = map[string][2]string{}
		}
		l.unread.sums[id] = [2]string{stamp, sum}
		l.unread.mu.Unlock()
	}
	return n, sum, true, nil
}

type changedWhileRead struct{}

func (changedWhileRead) Error() string { return "the object changed while it was read" }

var errChangedWhileRead error = changedWhileRead{}

// judgeUnread judges old, the object at key whose upload ID cannot be
// read, against gone, the rows of peer's uploads of key the store no
// longer has (see the file comment). It returns the row its verdict
// names (unreadGood, unreadBad), and the object's size as read. An
// error means it could not be read: nothing is decided. The caller
// holds the key's lock, or the borrower's pub lock exclusively.
func (l *Lending) judgeUnread(ctx context.Context, peer, sealed, bucket, key string, old storedObject, gone []state.LentUpload) (unreadVerdict, state.LentUpload, error) {
	declared := strings.ToLower(old.sha256)
	var byDeclared *state.LentUpload
	for i := range gone {
		if gone[i].SHA256 == declared {
			byDeclared = &gone[i]
		}
	}
	_, sum, fresh, err := l.sumOnce(ctx, bucket, key, declared)
	if err != nil {
		return unreadRot, state.LentUpload{}, err
	}
	for _, u := range gone {
		if u.SHA256 == sum {
			return unreadGood, u, nil
		}
	}
	if isHash(declared) && sum == declared {
		return unreadSound, state.LentUpload{}, nil
	}
	if byDeclared != nil {
		return unreadBad, *byDeclared, nil
	}
	if !fresh {
		return unreadRot, state.LentUpload{}, nil // recorded when it was read
	}
	l.log().Error("lending: a friend's object here whose attributes are damaged does not match the hash it declares, and nothing this node keeps vouches for it; it is left as it is and not answered for. Check the disk",
		"alert", true, "friend", peer, "bucket", bucket, "key", key, "sha256", old.sha256, "found", sum, "bytes", old.size)
	if err := l.DB.RecordRot(ctx, state.Rot{Bucket: bucket, Key: key, Borrower: peer, Sealed: sealed,
		SHA256: declared, Found: sum, Size: old.size, FoundAt: l.now()}); err != nil {
		l.log().Warn("lending: recording a rotten object failed", "bucket", bucket, "key", key, "err", err)
	} else if l.alerted != nil {
		l.alerted()
	}
	return unreadRot, state.LentUpload{}, nil
}

// vouchUnread is vouchLocked for an object whose upload-ID attribute
// cannot be read (#115). With no row of peer's for the key, no
// unverified upload can have linked it, and it is answered for as
// before #115. Otherwise judgeUnread decides, against the rows whose
// upload is gone:
//
//   - unreadGood: the row is settled as the upload that linked the
//     object, and it is answered for. A row of the same hash that did
//     not link it (an upload of the same object again, aborted, its row
//     left by a crash) is then counted as the object too: an
//     overcount, never an under-, which the reconcile corrects;
//   - unreadSound: answered for; the rows are the reconcile's;
//   - unreadBad: removed, the row settled, errBadDigest. A rewrite is not
//     undone (undoRewrite): without the upload ID the replaced version's
//     tomb is not proven this upload's;
//   - unreadRot, or a read that fails: not answered for (errInternal).
//
// A live upload of the key is not held up by a sound object, and no
// object is removed but on a trusted hash's mismatch, read twice.
func (l *Lending) vouchUnread(ctx context.Context, peer, sealed, bucket, key string, unread error) (string, error) {
	us, err := l.rowsOfKey(ctx, peer, sealed, key)
	if err != nil {
		return "", errInternal
	}
	if len(us) == 0 {
		return "", nil
	}
	old, has, err := l.existing(ctx, bucket, key)
	if err != nil {
		return "", errInternal
	}
	if !has {
		return "", nil
	}
	var gone []state.LentUpload
	for _, u := range us {
		_, err := l.parts(ctx, bucket, key, u.UploadID)
		if err == nil {
			continue // live: its object is not linked yet
		}
		if !isCode(err, "NoSuchUpload") {
			return "", errInternal
		}
		gone = append(gone, u)
	}
	v, u, err := l.judgeUnread(ctx, peer, sealed, bucket, key, old, gone)
	if err != nil {
		l.log().Warn("lending: reading an object whose upload ID cannot be read failed; not answering for it",
			"peer", peer, "bucket", bucket, "key", key, "err", err, "upload_id_err", unread)
		return "", errInternal
	}
	switch v {
	case unreadGood:
		l.settle(peer, u.UploadID, old.size)
		return u.UploadID, nil
	case unreadSound:
		return "", nil
	case unreadBad:
		l.log().Warn("lending: an object whose upload ID cannot be read does not match the hash its upload declared; removed it so the borrower sends it again",
			"peer", peer, "bucket", bucket, "key", key, "upload", u.UploadID)
		if !l.remove(bucket, key) {
			return "", errInternal
		}
		l.settle(peer, u.UploadID, l.leftBehind(bucket, key, u.UploadID)-itemOverhead)
		return u.UploadID, errBadDigest
	}
	return "", errInternal
}

// unreadRow is the reconcile's judgement of row r, whose upload is gone
// and whose key's object has an upload ID that cannot be read: kept
// unless a trusted hash settles it (judgeUnread). It returns what the
// listing's sum loses and whether the row may be dropped.
func (l *Lending) unreadRow(ctx context.Context, peer, bucket string, r state.LentUpload) (int64, bool) {
	old, has, err := l.existing(ctx, bucket, r.Key)
	if err != nil {
		l.log().Warn("lending: an object a borrower's upload may have assembled cannot be read; keeping its row",
			"peer", peer, "bucket", bucket, "key", r.Key, "upload", r.UploadID, "err", err)
		return 0, false
	}
	if !has {
		return 0, true
	}
	v, _, err := l.judgeUnread(ctx, peer, r.Sealed, bucket, r.Key, old, []state.LentUpload{r})
	if err != nil {
		l.log().Warn("lending: an object a borrower's upload may have assembled cannot be read; keeping its row",
			"peer", peer, "bucket", bucket, "key", r.Key, "upload", r.UploadID, "err", err)
		return 0, false
	}
	switch v {
	case unreadGood, unreadSound:
		// Verified as this upload's, or proven another's.
		return 0, true
	case unreadBad:
		l.log().Warn("lending: an object a crash left unverified does not match its declared hash; removed it so the borrower sends it again",
			"peer", peer, "bucket", bucket, "key", r.Key, "upload", r.UploadID)
		if !l.remove(bucket, r.Key) {
			return 0, false
		}
		return -(old.size + itemOverhead), true
	}
	return 0, false // rot: recorded, kept
}
