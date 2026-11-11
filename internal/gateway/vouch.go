package gateway

import (
	"context"
	"errors"

	"github.com/borje/lendisk/internal/state"
)

// This file keeps a borrower from taking as stored an object the lender
// never verified (LEND-4, REPL-3, #113; DESIGN §3.5 LEND-1). A lender
// crash between posix linking a multipart object and completeLent
// hashing it leaves the object at its key, carrying its upload's ID,
// and the upload's lent_upload row. The borrower's retry then meets
// NoSuchUpload and asks for the key (#87), and HEAD answers the hash the
// upload declared, copied into the object's metadata, not one taken
// from its bytes. So before any request that can answer for such an
// object (HEAD of the key, ListParts answering NoSuchUpload, a
// completion, a put of the same hash, a delete that would tomb it, and
// so a revival from that tomb), vouch hashes it, holding the key's lock
// and the borrower's pub lock shared, as a completion would: verified,
// the row is dropped and the object counted; failed, the object is
// removed (a rewrite's replaced version moved back), so HEAD answers
// 404 and the borrower sends it again. The check is made once: the row
// goes with it.
//
// It costs a request one read of the object's upload-ID attribute and,
// for a multipart object, one lookup of its row by upload ID; only an
// object whose row is still there takes the locks and is read.

// vouch verifies the object at key if an upload of peer's linked it and
// was never verified (see the file comment). It returns the upload ID
// it checked, and errBadDigest when the object failed its hash and is
// gone; any other error means the object could not be checked, and the
// request must not answer for it.
func (l *Lending) vouch(ctx context.Context, peer, sealed, bucket, key string) (string, error) {
	if l.Uploads == nil {
		return "", nil
	}
	if uid, err := l.Uploads.UploadID(bucket, key); err == nil {
		if uid == "" {
			return "", nil // a put's object, or none
		}
		if _, ok, err := l.linkedRow(ctx, peer, sealed, key, uid); err == nil && !ok {
			return "", nil // verified already: its row went
		}
	}
	unlock, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return "", errInternal
	}
	defer unlock()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	return l.vouchLocked(ctx, peer, sealed, bucket, key)
}

// vouchLocked is vouch for a caller that holds the key's lock and the
// borrower's pub lock: the row and the object are read under them, so
// a reconcile cannot settle the upload between this read and its
// count (#113).
func (l *Lending) vouchLocked(ctx context.Context, peer, sealed, bucket, key string) (string, error) {
	if l.Uploads == nil {
		return "", nil
	}
	uid, err := l.Uploads.UploadID(bucket, key)
	if err != nil {
		return l.vouchUnread(ctx, peer, sealed, bucket, key, err)
	}
	if uid == "" {
		return "", nil
	}
	u, ok, err := l.linkedRow(ctx, peer, sealed, key, uid)
	if err != nil {
		return "", errInternal
	}
	if !ok {
		return "", nil
	}
	old, has, err := l.existing(ctx, bucket, key)
	if err != nil {
		return "", errInternal
	}
	if !has {
		return "", nil
	}
	if l.afterVouchRead != nil {
		l.afterVouchRead()
	}
	return uid, l.verifyLanded(ctx, peer, bucket, key, u, old)
}

// vouchUnread is vouchLocked for an object whose upload-ID attribute
// cannot be read (#115). With no row of peer's for the key, no
// unverified upload can have linked it, and it is answered for as
// before. Otherwise it is checked against the hash declared in its own
// metadata, which is what HEAD would answer:
//
//   - a row of the key declaring that hash, whose upload the store no
//     longer has, is the one that may have linked it (a live upload's
//     object is not yet linked; any upload's object carries the hash its
//     create declared), and the object is verified as that upload's
//     (verifyLanded: settled and counted, or removed and its row
//     dropped);
//   - otherwise it is an object verified when it was stored, and is
//     hashed again: a match answers for it, a mismatch removes it, so
//     the borrower sends it again (errBadDigest).
//
// So a live upload of the key, a rewrite in parts say, is never held up
// by the unreadable attribute, and no object is removed but on a full
// read that does not match. An object with no declared hash, or one
// that cannot be read or removed, is not answered for (errInternal).
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
	if old.sha256 == "" {
		l.log().Warn("lending: an object whose upload ID cannot be read declares no hash; not answering for it",
			"peer", peer, "bucket", bucket, "key", key, "err", unread)
		return "", errInternal
	}
	for _, u := range us {
		if u.SHA256 != old.sha256 {
			continue
		}
		_, err := l.parts(ctx, bucket, key, u.UploadID)
		if err == nil {
			continue // live: its object is not linked yet
		}
		if !isCode(err, "NoSuchUpload") {
			return "", errInternal
		}
		if l.afterVouchRead != nil {
			l.afterVouchRead()
		}
		return u.UploadID, l.verifyLanded(ctx, peer, bucket, key, u, old)
	}
	if l.afterVouchRead != nil {
		l.afterVouchRead()
	}
	_, got, err := l.hashStored(ctx, bucket, key)
	if err != nil {
		l.log().Warn("lending: reading an object whose upload ID cannot be read failed; not answering for it",
			"peer", peer, "bucket", bucket, "key", key, "err", err)
		return "", errInternal
	}
	if got == old.sha256 {
		return "", nil
	}
	l.log().Warn("lending: an object whose upload ID cannot be read does not match its declared hash; removed it so the borrower sends it again",
		"peer", peer, "bucket", bucket, "key", key)
	if !l.remove(bucket, key) {
		return "", errInternal
	}
	l.count(peer, -(old.size + itemOverhead))
	return "", errBadDigest
}

// rowsOfKey lists peer's recorded uploads of key in sealed.
func (l *Lending) rowsOfKey(ctx context.Context, peer, sealed, key string) ([]state.LentUpload, error) {
	us, err := l.DB.LentUploads(ctx, peer)
	if err != nil {
		return nil, err
	}
	var out []state.LentUpload
	for _, u := range us {
		if u.Sealed == sealed && u.Key == key {
			out = append(out, u)
		}
	}
	return out, nil
}

// linkedRow reads the row of upload uid if it is peer's upload of key
// in sealed.
func (l *Lending) linkedRow(ctx context.Context, peer, sealed, key, uid string) (state.LentUpload, bool, error) {
	u, err := l.DB.GetLentUpload(ctx, uid)
	if errors.Is(err, state.ErrNotFound) {
		return state.LentUpload{}, false, nil
	}
	if err != nil {
		return state.LentUpload{}, false, err
	}
	return u, u.Peer == peer && u.Sealed == sealed && u.Key == key, nil
}

// undoRewrite moves back the version a rewrite in parts replaced, as
// completeLent's undo does, when the object the rewrite published is
// removed for failing its hash after a crash: the key's newest tomb,
// if it is a rewrite's, made since upload u began, of the version u
// was marked as replacing. Its count moves from tomb to live unchanged.
func (l *Lending) undoRewrite(ctx context.Context, bucket string, u state.LentUpload) {
	if u.Replaces == "" {
		return
	}
	ts, err := l.DB.TombstonesOf(ctx, u.Peer, u.Sealed, u.Key)
	if err != nil {
		l.log().Error("lending: reading a rewrite's tomb to move it back failed; it stays in its tomb", "bucket", bucket, "key", u.Key, "err", err)
		return
	}
	if len(ts) == 0 {
		return
	}
	t := ts[len(ts)-1]
	if !t.Rewrite || t.SHA256 != u.Replaces || t.DeletedAt.Before(u.CreatedAt) {
		return
	}
	if _, err := l.revive(ctx, bucket, t); err != nil {
		l.log().Error("lending: moving a rewrite's replaced version back failed; it stays in its tomb", "bucket", bucket, "key", u.Key, "err", err)
		return
	}
	l.log().Warn("lending: moved a rewrite's replaced version back to its key", "bucket", bucket, "key", u.Key, "upload", u.UploadID)
}
