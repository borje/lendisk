package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

// This file is the lender's policy on a borrower's writes (LEND-1,
// LEND-2, LEND-4, SEC-5), in the Lending layer, above the capture
// decorator, so it never stands between the decorator and the store
// (#62). Every write of a borrower:
//
//   - names a key that checkKey accepts;
//   - declares the object's SHA-256 as x-amz-meta-lendisk-sha256 (a put
//     or multipart create), which the lender compares with the SHA-256
//     of the whole stream it stored, or for a multipart upload of the
//     whole assembled object, and removes and refuses on a mismatch;
//   - is refused before any byte is stored if the key holds an object
//     with another hash; one with the same hash is not stored again; a
//     put of an absent key is published only if it is still absent
//     (If-None-Match: *), since puts take no key lock;
//   - is admitted against the friendship's quota and the store's
//     free-space floor (reserve), and moves the borrower's byte counter
//     in lendisk.db once it completes.
//
// The counter covers a borrower's objects and the parts of its uploads
// in progress. reconcile sets it from a listing once a day; writes hold
// their borrower's share of a lock the reconcile takes exclusively, so a
// listing never races a write's count.

// metaSHA256 is the user metadata key carrying an object's SHA-256, in
// hex (REPL-10; replica.MetaSHA256 on the sending side).
const metaSHA256 = "lendisk-sha256"

// DefaultFloor is the free-space floor when none is configured (LEND-2,
// store.free_space_floor's default).
var DefaultFloor = config.Floor{Percent: 5}

// The refusals a borrower can tell apart (§3.4 Errors). All are returned
// unwrapped (Q-I.5).
var (
	// errQuotaExceeded is lendisk's own: versitygw's ErrQuotaExceeded is
	// a 403, which a borrower could not tell from other refusals.
	errQuotaExceeded = s3err.APIError{Code: "QuotaExceeded", HTTPStatusCode: 507,
		Description: "lendisk: this friendship's quota is used up, or the lender's disk is below its free-space floor; nothing was stored"}
	errNoHash = s3err.APIError{Code: "InvalidRequest", HTTPStatusCode: 400,
		Description: "lendisk: an object sent to a friend must carry its SHA-256, in hex, as x-amz-meta-lendisk-sha256"}
	errBadDigest = s3err.APIError{Code: "BadDigest", HTTPStatusCode: 400,
		Description: "lendisk: the bytes received do not match x-amz-meta-lendisk-sha256; nothing was stored"}
	errKeyChanged = s3err.APIError{Code: "PreconditionFailed", HTTPStatusCode: 412,
		Description: "lendisk: this key holds an object with another hash; a stored object is never replaced by other bytes"}
	errBadKey = s3err.APIError{Code: "InvalidArgument", HTTPStatusCode: 400,
		Description: "lendisk: an object key must be a relative path of non-empty segments, without . or .. segments, " +
			"control characters, or the reserved .lendisk, .sgwtmp and .vgwlocks directories"}
	errNoSuchUpload = s3err.GetAPIError(s3err.ErrNoSuchUpload)
	errNotFriend    = s3err.APIError{Code: "AccessDenied", HTTPStatusCode: 403, Description: "lendisk: " + state.NoRelationship}
)

// reservedDirs are the top-level directories of a bucket that belong to
// lendisk (tombstones, §3.6) or to versitygw's posix backend.
var reservedDirs = map[string]bool{".lendisk": true, ".sgwtmp": true, ".vgwlocks": true}

// checkKey accepts a borrower's object key (SEC-5): 1 to 1024 bytes of
// UTF-8 without control characters, segments separated by single
// slashes, none empty, "." or "..", and not under a reserved directory.
func checkKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) {
		return errBadKey
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return errBadKey
		}
	}
	segs := strings.Split(key, "/")
	if reservedDirs[segs[0]] {
		return errBadKey
	}
	for _, s := range segs {
		if s == "" || s == "." || s == ".." {
			return errBadKey
		}
	}
	return nil
}

// declared returns the SHA-256 a borrower declared, in lower-case hex.
func declared(meta map[string]string) (string, error) {
	v := strings.ToLower(meta[metaSHA256])
	if b, err := hex.DecodeString(v); err != nil || len(b) != sha256.Size {
		return "", errNoHash
	}
	return v, nil
}

// borrowerUse is one borrower's in-memory state: writes in flight hold
// rw shared, a reconcile holds it exclusively; reserved is what its
// admitted writes in flight may still add.
type borrowerUse struct {
	rw       sync.RWMutex
	reserved int64
}

func (l *Lending) use(peer string) *borrowerUse {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.peers == nil {
		l.peers = map[string]*borrowerUse{}
	}
	u := l.peers[peer]
	if u == nil {
		u = &borrowerUse{}
		l.peers[peer] = u
	}
	return u
}

// hold takes peer's share of the reconcile lock for a write that moves
// its counter; the returned function releases it.
func (l *Lending) hold(peer string) func() {
	u := l.use(peer)
	u.rw.RLock()
	return u.rw.RUnlock
}

func (l *Lending) log() *slog.Logger {
	if l.Log != nil {
		return l.Log
	}
	return slog.New(slog.DiscardHandler)
}

func (l *Lending) floor() config.Floor {
	if l.Floor == nil {
		return DefaultFloor
	}
	return *l.Floor
}

func (l *Lending) statfs() (store.Space, error) {
	if l.Statfs != nil {
		return l.Statfs(l.StoreDir)
	}
	return store.DefaultChecker.Statfs(l.StoreDir)
}

// reserve admits n more bytes from peer (LEND-1, LEND-2): refused with
// errQuotaExceeded when its counter, plus what its writes in flight
// may add, plus n (at least one byte) would exceed the friendship's
// quota, or when the store's free space less what every write in flight
// may add would fall below the floor. The returned function gives back
// what was reserved; the write records what it stored itself.
func (l *Lending) reserve(ctx context.Context, peer string, n int64) (release func(), err error) {
	f, err := l.DB.GetFriendship(ctx, peer)
	if errors.Is(err, state.ErrNotFound) {
		return nil, errNotFriend
	}
	if err != nil {
		l.log().Error("lending: reading the friendship failed; refusing the write", "peer", peer, "err", err)
		return nil, errInternal
	}
	used, _, err := l.DB.LentUsage(ctx, peer)
	if err != nil {
		l.log().Error("lending: reading the borrower's usage failed; refusing the write", "peer", peer, "err", err)
		return nil, errInternal
	}
	sp, err := l.statfs()
	if err != nil {
		l.log().Error("lending: reading the store's free space failed; refusing the write", "store", l.StoreDir, "err", err)
		return nil, errInternal
	}
	need := max(n, 1)
	floor := store.FloorBytes(l.floor(), sp.Total)
	u := l.use(peer)
	l.mu.Lock()
	defer l.mu.Unlock()
	if used+u.reserved+need > f.Quota() {
		l.log().Info("lending: refused a write over the friendship's quota", "peer", peer,
			"used", used, "in_flight", u.reserved, "size", n, "quota", f.Quota())
		return nil, errQuotaExceeded
	}
	if sp.Free < floor+uint64(l.reserved)+uint64(need) {
		l.log().Warn("lending: refused a write below the store's free-space floor", "peer", peer,
			"free", sp.Free, "in_flight", l.reserved, "size", n, "floor", floor)
		return nil, errQuotaExceeded
	}
	u.reserved += n
	l.reserved += n
	return func() {
		l.mu.Lock()
		u.reserved -= n
		l.reserved -= n
		l.mu.Unlock()
	}, nil
}

// count moves peer's counter by what a write stored or removed. A
// failure is logged; the daily reconcile sets the counter right.
func (l *Lending) count(peer string, delta int64) {
	if delta == 0 {
		return
	}
	if err := l.DB.AddLentUsage(context.Background(), peer, delta); err != nil {
		l.log().Error("lending: updating the borrower's usage failed; the daily reconcile corrects it", "peer", peer, "delta", delta, "err", err)
	}
}

// storedObject is what the store holds at a key.
type storedObject struct {
	size         int64
	sha256, etag string
}

// existing heads key in the store bucket: its size, its declared hash
// (every borrower's object carries it, COMPAT-4) and ETag.
func (l *Lending) existing(ctx context.Context, bucket, key string) (storedObject, bool, error) {
	out, err := l.Next.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if notFound(err) {
		return storedObject{}, false, nil
	}
	if err != nil {
		return storedObject{}, false, err
	}
	o := storedObject{sha256: out.Metadata[metaSHA256], etag: deref(out.ETag)}
	if out.ContentLength != nil {
		o.size = *out.ContentLength
	}
	return o, true, nil
}

// remove deletes an object that failed verification.
func (l *Lending) remove(bucket, key string) {
	if _, err := l.Next.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil && !notFound(err) {
		l.log().Error("lending: removing an object that failed verification failed", "bucket", bucket, "key", key, "err", err)
	}
}

// hashStored reads a stored object whole and returns its size and
// SHA-256.
func (l *Lending) hashStored(ctx context.Context, bucket, key string) (int64, string, error) {
	out, err := l.Next.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, out.Body})
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// putLent stores a borrower's object in its store bucket (in.Bucket is
// already mapped).
func (l *Lending) putLent(ctx context.Context, peer string, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	var none s3response.PutObjectOutput
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return none, err
	}
	want, err := declared(in.Metadata)
	if err != nil {
		return none, err
	}
	var size int64
	if in.ContentLength != nil && *in.ContentLength > 0 {
		size = *in.ContentLength
	}
	// No key lock: a put whose body stalls (its borrower was killed
	// mid-transfer) must not hold up the retry. The store publishes only
	// if the key is still absent (If-None-Match: *, checked by posix
	// under its publish lock), which closes the race with the check here.
	defer l.hold(peer)()
	old, has, err := l.existing(ctx, *in.Bucket, key)
	if err != nil {
		l.log().Error("lending: reading the stored object failed; refusing the put", "bucket", *in.Bucket, "key", key, "err", err)
		return none, errInternal
	}
	if has {
		return l.putAgain(*in.Bucket, key, in.Body, want, old)
	}
	release, err := l.reserve(ctx, peer, size)
	if err != nil {
		return none, err
	}
	defer release()
	var h *hashReader
	in.Body, h = hashing(in.Body)
	// A mismatch fails the read at the end of the body, so the store
	// never publishes it.
	h.atEOF = func(h *hashReader) error {
		if h.hex() != want {
			return errBadDigest
		}
		return nil
	}
	in.IfNoneMatch = &anyETag
	out, err := l.Next.PutObject(ctx, in)
	if h.n == size && h.hex() != want {
		if err == nil {
			l.remove(*in.Bucket, key) // the store published it anyway
		}
		return none, errBadDigest
	}
	if preconditionFailed(err) {
		// Another put of the key was published while this one streamed.
		old, has, herr := l.existing(ctx, *in.Bucket, key)
		if herr == nil && has && old.sha256 == want && h.n == old.size && h.hex() == want {
			return s3response.PutObjectOutput{ETag: old.etag}, nil
		}
		return none, errKeyChanged
	}
	if err != nil {
		return out, err
	}
	l.count(peer, h.n)
	return out, nil
}

var anyETag = "*"

// preconditionFailed reports whether the store refused a conditional
// write: posix answers with an s3err.PreconditionFailedError.
func preconditionFailed(err error) bool {
	var pf s3err.PreconditionFailedError
	return errors.As(err, &pf) || isCode(err, "PreconditionFailed")
}

// putAgain answers a put to a key already stored: refused if the hashes
// differ; with the same hash (an acknowledgement was lost) the body is
// hashed and discarded and the stored copy kept, so a body that only
// claims the hash is refused and changes nothing.
func (l *Lending) putAgain(bucket, key string, body io.Reader, want string, old storedObject) (s3response.PutObjectOutput, error) {
	if old.sha256 != want {
		return s3response.PutObjectOutput{}, errKeyChanged
	}
	if body == nil {
		body = eofReader{}
	}
	h := sha256.New()
	n, err := io.Copy(h, body)
	if err != nil {
		l.log().Warn("lending: reading a repeated put's body failed", "bucket", bucket, "key", key, "err", err)
		return s3response.PutObjectOutput{}, errInternal
	}
	if n != old.size || hex.EncodeToString(h.Sum(nil)) != want {
		return s3response.PutObjectOutput{}, errBadDigest
	}
	return s3response.PutObjectOutput{ETag: old.etag}, nil
}

// deleteLent deletes a borrower's object and frees its bytes.
func (l *Lending) deleteLent(ctx context.Context, peer string, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	key := deref(in.Key)
	defer l.hold(peer)()
	unlock, err := l.keys.lock(ctx, *in.Bucket, key)
	if err != nil {
		return nil, errInternal
	}
	defer unlock()
	old, has, err := l.existing(ctx, *in.Bucket, key)
	if err != nil {
		return nil, errInternal
	}
	out, err := l.Next.DeleteObject(ctx, in)
	if err == nil && has {
		l.count(peer, -old.size)
	}
	return out, err
}

// admitBucket refuses a borrower's new bucket when its friendship has no
// room or the store is below the floor (CreateBucket counts against
// quota, §3.7 NET-7). A bucket it already has is not refused.
func (l *Lending) admitBucket(ctx context.Context, peer, sealed string) error {
	has, err := l.DB.HasLentBucket(ctx, peer, sealed)
	if err != nil {
		return errInternal
	}
	if has {
		return nil
	}
	release, err := l.reserve(ctx, peer, 0)
	if err != nil {
		return err
	}
	release()
	return nil
}

// createUploadLent begins a borrower's multipart upload, recording the
// hash it declared.
func (l *Lending) createUploadLent(ctx context.Context, peer, sealed string, in s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	var none s3response.InitiateMultipartUploadResult
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return none, err
	}
	want, err := declared(in.Metadata)
	if err != nil {
		return none, err
	}
	unlock, err := l.keys.lock(ctx, *in.Bucket, key)
	if err != nil {
		return none, errInternal
	}
	defer unlock()
	old, has, err := l.existing(ctx, *in.Bucket, key)
	if err != nil {
		return none, errInternal
	}
	if has && old.sha256 != want {
		return none, errKeyChanged
	}
	release, err := l.reserve(ctx, peer, 0)
	if err != nil {
		return none, err
	}
	release()
	res, err := l.Next.CreateMultipartUpload(ctx, in)
	if err != nil {
		return res, err
	}
	if err := l.DB.AddLentUpload(context.Background(), state.LentUpload{UploadID: res.UploadId, Peer: peer, Sealed: sealed,
		Key: key, SHA256: want, CreatedAt: l.now()}); err != nil {
		l.log().Error("lending: recording a borrower's upload failed; refusing it", "bucket", *in.Bucket, "key", key, "err", err)
		id := res.UploadId
		l.Next.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{Bucket: in.Bucket, Key: in.Key, UploadId: &id})
		return none, errInternal
	}
	return res, nil
}

// upload returns the borrower's upload uploadID of key, or
// errNoSuchUpload if it is not one this borrower began here.
func (l *Lending) upload(ctx context.Context, peer, sealed, key string, uploadID *string) (state.LentUpload, error) {
	if uploadID == nil {
		return state.LentUpload{}, errNoSuchUpload
	}
	u, err := l.DB.GetLentUpload(ctx, *uploadID)
	if errors.Is(err, state.ErrNotFound) || err == nil && (u.Peer != peer || u.Sealed != sealed || u.Key != key) {
		return state.LentUpload{}, errNoSuchUpload
	}
	if err != nil {
		return state.LentUpload{}, errInternal
	}
	return u, nil
}

// partsSize sums the parts stored for an upload, or for one part number
// when part is above zero.
func (l *Lending) partsSize(ctx context.Context, bucket, key, uploadID string, part int32) (int64, error) {
	var total int64
	marker := ""
	max := int32(1000)
	if part > 0 {
		marker, max = strconv.Itoa(int(part-1)), 1
	}
	for {
		res, err := l.Next.ListParts(ctx, &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID,
			PartNumberMarker: &marker, MaxParts: &max})
		if err != nil {
			return 0, err
		}
		for _, p := range res.Parts {
			if part == 0 || p.PartNumber == int(part) {
				total += p.Size
			}
		}
		if part > 0 || !res.IsTruncated || res.NextPartNumberMarker == 0 {
			return total, nil
		}
		marker = strconv.Itoa(res.NextPartNumberMarker)
	}
}

// uploadPartLent stores a part of a borrower's upload, admitted for what
// it adds over a part of the same number already stored.
func (l *Lending) uploadPartLent(ctx context.Context, peer, sealed string, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if _, err := l.upload(ctx, peer, sealed, deref(in.Key), in.UploadId); err != nil {
		return nil, err
	}
	var size int64
	if in.ContentLength != nil && *in.ContentLength > 0 {
		size = *in.ContentLength
	}
	var number int32
	if in.PartNumber != nil {
		number = *in.PartNumber
	}
	defer l.hold(peer)()
	var old int64
	if number > 0 {
		var err error
		if old, err = l.partsSize(ctx, *in.Bucket, *in.Key, *in.UploadId, number); err != nil {
			return nil, err
		}
	}
	release, err := l.reserve(ctx, peer, max(size-old, 0))
	if err != nil {
		return nil, err
	}
	defer release()
	out, err := l.Next.UploadPart(ctx, in)
	if err != nil {
		return out, err
	}
	l.count(peer, size-old)
	return out, nil
}

// completeLent completes a borrower's upload and verifies the assembled
// object, whole, against the hash declared at create (LEND-4). A key
// that holds an object with another hash is refused before anything
// changes; with the same hash the upload is aborted and the stored
// object kept. A retried completion of an upload already verified is
// answered by the store.
func (l *Lending) completeLent(ctx context.Context, peer, sealed string, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	var none s3response.CompleteMultipartUploadResult
	bucket, key := *in.Bucket, deref(in.Key)
	if in.UploadId == nil {
		return none, "", errNoSuchUpload
	}
	uploadID := *in.UploadId
	defer l.hold(peer)()
	unlock, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return none, "", errInternal
	}
	defer unlock()
	u, uerr := l.upload(ctx, peer, sealed, key, in.UploadId)
	if uerr != nil && uerr != errNoSuchUpload {
		return none, "", uerr
	}
	old, has, err := l.existing(ctx, bucket, key)
	if err != nil {
		return none, "", errInternal
	}
	if has && l.Uploads != nil {
		if id, err := l.Uploads.UploadID(bucket, key); err == nil && id == uploadID {
			// This upload is the stored object: a retried completion.
			if uerr == nil {
				// Completed before a crash and not verified yet. Its
				// parts are gone; the reconcile settles the counter.
				if bad := l.verify(ctx, bucket, key, u); bad != nil {
					return none, "", bad
				}
			}
			return l.Next.CompleteMultipartUpload(ctx, in)
		}
	}
	if uerr != nil {
		return none, "", uerr
	}
	if has && old.sha256 != u.SHA256 {
		return none, "", errKeyChanged
	}
	parts, err := l.partsSize(ctx, bucket, key, uploadID, 0)
	if err != nil {
		return none, "", err
	}
	if has {
		// The same object again: keep the stored one.
		if err := l.Next.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: in.Bucket, Key: in.Key, UploadId: in.UploadId}); err != nil {
			return none, "", err
		}
		l.count(peer, -parts)
		l.dropUpload(uploadID)
		etag := old.etag
		return s3response.CompleteMultipartUploadResult{Bucket: in.Bucket, Key: in.Key, ETag: &etag}, "", nil
	}
	// A put of the key (which takes no key lock) may have been published
	// since the check above: then this one is refused.
	in.IfNoneMatch = &anyETag
	res, ver, err := l.Next.CompleteMultipartUpload(ctx, in)
	if preconditionFailed(err) {
		return res, ver, errKeyChanged
	}
	if err != nil {
		return res, ver, err
	}
	n, got, err := l.hashStored(ctx, bucket, key)
	if err != nil || got != u.SHA256 {
		l.remove(bucket, key)
		l.count(peer, -parts)
		l.dropUpload(uploadID)
		if err != nil {
			l.log().Error("lending: reading a completed upload to verify it failed; removed it", "bucket", bucket, "key", key, "err", err)
			return none, "", errInternal
		}
		return none, "", errBadDigest
	}
	l.count(peer, n-parts)
	l.dropUpload(uploadID)
	return res, ver, nil
}

// verify hashes a stored object that completed u; on a mismatch it is
// removed and errBadDigest returned. u is settled either way, except
// when the object cannot be read now.
func (l *Lending) verify(ctx context.Context, bucket, key string, u state.LentUpload) error {
	_, got, err := l.hashStored(ctx, bucket, key)
	if err != nil {
		return errInternal
	}
	l.dropUpload(u.UploadID)
	if got != u.SHA256 {
		l.remove(bucket, key)
		return errBadDigest
	}
	return nil
}

func (l *Lending) dropUpload(uploadID string) {
	if err := l.DB.DropLentUpload(context.Background(), uploadID); err != nil {
		l.log().Warn("lending: forgetting a finished upload failed", "upload", uploadID, "err", err)
	}
}

// abortLent aborts a borrower's upload and frees its parts.
func (l *Lending) abortLent(ctx context.Context, peer string, in *s3.AbortMultipartUploadInput) error {
	defer l.hold(peer)()
	var parts int64
	if in.UploadId != nil {
		parts, _ = l.partsSize(ctx, *in.Bucket, deref(in.Key), *in.UploadId, 0)
	}
	if err := l.Next.AbortMultipartUpload(ctx, in); err != nil {
		return err
	}
	l.count(peer, -parts)
	if in.UploadId != nil {
		l.dropUpload(*in.UploadId)
	}
	return nil
}

// reconcile sets each borrower's counter from a listing of its buckets
// (objects and the parts of uploads in progress) when its last
// reconcile is older than every (LEND-1). A borrower with writes in
// flight is left for the next call.
func (l *Lending) reconcile(ctx context.Context, every time.Duration) error {
	fs, err := l.DB.Friendships(ctx)
	if err != nil {
		return err
	}
	var first error
	for _, f := range fs {
		_, at, err := l.DB.LentUsage(ctx, f.Peer)
		if err != nil {
			return err
		}
		if !at.IsZero() && l.now().Sub(at) < every {
			continue
		}
		u := l.use(f.Peer)
		if !u.rw.TryLock() {
			continue
		}
		n, err := l.listed(ctx, f.Peer)
		if err == nil {
			err = l.DB.SetLentUsage(ctx, f.Peer, n, l.now())
		}
		u.rw.Unlock()
		if err != nil {
			l.log().Error("lending: reconciling a borrower's usage failed", "peer", f.Peer, "err", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

// listed sums what the store holds of peer's buckets.
func (l *Lending) listed(ctx context.Context, peer string) (int64, error) {
	id, err := identity.ParseNodeID(peer)
	if err != nil {
		return 0, err
	}
	bs, err := l.DB.LentBuckets(ctx, peer)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, b := range bs {
		bucket := LentName(id, b.Sealed)
		var token *string
		start, page := "", int32(1000) // posix needs both set
		for {
			res, err := l.Next.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, ContinuationToken: token,
				StartAfter: &start, MaxKeys: &page})
			if err != nil {
				return 0, err
			}
			for _, o := range res.Contents {
				if o.Size != nil {
					total += *o.Size
				}
			}
			if res.IsTruncated == nil || !*res.IsTruncated || res.NextContinuationToken == nil {
				break
			}
			token = res.NextContinuationToken
		}
		var keyMarker, idMarker string
		max := int32(1000)
		for {
			res, err := l.Next.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket,
				KeyMarker: &keyMarker, UploadIdMarker: &idMarker, MaxUploads: &max})
			if err != nil {
				return 0, err
			}
			for _, up := range res.Uploads {
				n, err := l.partsSize(ctx, bucket, up.Key, up.UploadID, 0)
				if err != nil && !isCode(err, "NoSuchUpload") {
					return 0, err
				}
				total += n
			}
			if !res.IsTruncated {
				break
			}
			keyMarker, idMarker = res.NextKeyMarker, res.NextUploadIDMarker
		}
	}
	return total, nil
}

// reconcileLoop reconciles at start and then every interval, each
// borrower once a day, until ctx ends.
func (l *Lending) reconcileLoop(ctx context.Context, interval time.Duration) {
	for {
		if err := l.reconcile(ctx, 24*time.Hour); err != nil && ctx.Err() == nil {
			l.log().Error("lending: reconciling borrowers' usage failed; trying again later", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
