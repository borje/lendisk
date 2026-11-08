package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

// This file is the lender's policy on a borrower's requests (LEND-1,
// LEND-2, LEND-4, SEC-5), in the Lending layer, above the capture
// decorator, so it never stands between the decorator and the store
// (#62).
//
//   - Every key a borrower names passes checkKey.
//   - A put or multipart create declares the object's SHA-256 as
//     x-amz-meta-lendisk-sha256. A put's body is hashed as it streams
//     and a mismatch fails its last read, so the store never publishes
//     it; a multipart object is read whole after completion and removed
//     on a mismatch.
//   - A write to a key holding an object with another hash is refused
//     before any byte is stored, unless it is a PutObject marked as a
//     rewrite of that very object (MetaReplaces, rewrite.go, #74), whose
//     replaced version becomes a tombstone; the same hash is accepted
//     unstored. A
//     put of an absent key, and a completion, is published only if the
//     key is still absent (If-None-Match: *, which posix checks under
//     its publish lock): puts take no key lock, so a stalled body never
//     holds up its retry.
//   - A borrower's body longer than its Content-Length ends the write.
//     A body that stalls or trickles is ended by the tunnel bridge's
//     read deadline (bridge.boundedBody), before it reaches the store.
//   - Writes are admitted against the friendship's quota and the
//     store's free-space floor (reserve), and move the borrower's
//     counter once published.
//
// The counter holds the bytes of a borrower's objects and of the parts
// of its open uploads, plus itemOverhead per object, bucket and open
// upload. It lives in memory under Lending.mu, loaded from lendisk.db
// once, and every change is written through. Each change is made while
// holding the borrower's pub lock shared, from the moment the store may
// publish (a body's end, or the store call) until the change is counted;
// reconcile holds it exclusively while it lists, so a listing sees
// every write either counted or not yet published. A body still
// streaming holds nothing, so reconcile waits only for publications.

// itemOverhead is what each object, bucket and open multipart upload
// costs against quota besides its bytes (LEND-1, SEC-5).
const itemOverhead = 4096

// DefaultAbandonAfter is how long a borrower's multipart upload may go
// without a part before the reconcile aborts it.
const DefaultAbandonAfter = 7 * 24 * time.Hour

// metaSHA256 is the user metadata key carrying an object's SHA-256, in
// hex (REPL-10; replica.MetaSHA256 on the sending side).
const metaSHA256 = "lendisk-sha256"

// MetaReplaces is the user metadata key that marks a borrower's put as a
// rewrite (LEND-4, EP-7, #74): the SHA-256, in hex, of the version it
// replaces, sent as x-amz-meta-lendisk-replaces (replica.MetaReplaces on
// the sending side). The put is stored only if the key holds that very
// version, which then becomes a tombstone (RET-1). The mark is never
// stored with the object.
const MetaReplaces = "lendisk-replaces"

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
		Description: "lendisk: this key holds an object with another hash; a stored object is replaced by other bytes " +
			"only by a rewrite marked with its hash (x-amz-meta-lendisk-replaces)"}
	errBadMark = s3err.APIError{Code: "InvalidRequest", HTTPStatusCode: 400,
		Description: "lendisk: x-amz-meta-lendisk-replaces must be the SHA-256, in hex, of the version a rewrite replaces"}
	errBadKey = s3err.APIError{Code: "InvalidArgument", HTTPStatusCode: 400,
		Description: "lendisk: an object key must be a relative path of non-empty segments, without . or .. segments, " +
			"control characters, or the reserved .lendisk, .sgwtmp and .vgwlocks directories"}
	errNoSuchUpload = s3err.GetAPIError(s3err.ErrNoSuchUpload)
	errTooLong      = s3err.APIError{Code: "InvalidRequest", HTTPStatusCode: 400,
		Description: "lendisk: the body is longer than its Content-Length"}
	errNotFriend = s3err.APIError{Code: "AccessDenied", HTTPStatusCode: 403, Description: "lendisk: " + state.NoRelationship}
)

// reservedDirs are the top-level directories of a bucket that belong to
// lendisk (tombstones, §3.6) or to versitygw's posix backend.
var reservedDirs = []string{".lendisk", ".sgwtmp", ".vgwlocks"}

// reserved reports whether a key's first segment names a reserved
// directory, in any case: a case-insensitive filesystem would put it
// there.
func reserved(seg string) bool {
	for _, d := range reservedDirs {
		if strings.EqualFold(seg, d) {
			return true
		}
	}
	return false
}

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
	if reserved(segs[0]) {
		return errBadKey
	}
	for _, s := range segs {
		if s == "" || s == "." || s == ".." {
			return errBadKey
		}
	}
	return nil
}

// borrowerKey checks the key a request names when a borrower made it.
func borrowerKey(ctx context.Context, key *string) error {
	if _, ok := borrower(ctx); !ok {
		return nil
	}
	return checkKey(deref(key))
}

// declared returns the SHA-256 a borrower declared, in lower-case hex,
// the rewrite mark if it sent one (MetaReplaces, also lower case), and
// the metadata with the hash so normalised and the mark taken out, which
// is what is stored.
func declared(meta map[string]string) (want, mark string, stored map[string]string, err error) {
	want = strings.ToLower(meta[metaSHA256])
	if !isHash(want) {
		return "", "", nil, errNoHash
	}
	stored = maps.Clone(meta)
	stored[metaSHA256] = want
	if m, ok := stored[MetaReplaces]; ok {
		mark = strings.ToLower(m)
		if !isHash(mark) {
			return "", "", nil, errBadMark
		}
		delete(stored, MetaReplaces)
	}
	return want, mark, stored, nil
}

// isHash reports whether s is a SHA-256 in hex.
func isHash(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size
}

// borrowerUse is one borrower's state. pub is held shared by a write
// from its publication until it is counted, and exclusively by a
// reconcile. used (once loaded) and reserved are under Lending.mu.
type borrowerUse struct {
	pub      sync.RWMutex
	loaded   bool
	used     int64 // what the borrower stores, as counted
	reserved int64 // what its admitted writes in flight may add
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

// loadLocked reads u's count from lendisk.db the first time; l.mu held.
func (l *Lending) loadLocked(peer string, u *borrowerUse) error {
	if u.loaded {
		return nil
	}
	n, _, err := l.DB.LentUsage(context.Background(), peer)
	if err != nil {
		return err
	}
	u.used, u.loaded = n, true
	return nil
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

func (l *Lending) abandonAfter() time.Duration {
	if l.AbandonAfter > 0 {
		return l.AbandonAfter
	}
	return DefaultAbandonAfter
}

// reserve admits n more bytes from peer (LEND-1, LEND-2), checked
// against its current count under Lending.mu: refused with
// errQuotaExceeded when the count, plus what its writes in flight may
// add, plus n would exceed the friendship's quota, or when the store's
// free space less what every write in flight may add, less n, would be
// below the floor. The returned function gives back the reservation;
// the write counts what it stored itself.
func (l *Lending) reserve(ctx context.Context, peer string, n int64) (release func(), err error) {
	f, err := l.DB.GetFriendship(ctx, peer)
	if errors.Is(err, state.ErrNotFound) {
		return nil, errNotFriend
	}
	if err != nil {
		l.log().Error("lending: reading the friendship failed; refusing the write", "peer", peer, "err", err)
		return nil, errInternal
	}
	sp, err := l.statfs()
	if err != nil {
		l.log().Error("lending: reading the store's free space failed; refusing the write", "store", l.StoreDir, "err", err)
		return nil, errInternal
	}
	floor := store.FloorBytes(l.floor(), sp.Total)
	u := l.use(peer)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadLocked(peer, u); err != nil {
		l.log().Error("lending: reading the borrower's usage failed; refusing the write", "peer", peer, "err", err)
		return nil, errInternal
	}
	if u.used+u.reserved+n > f.Quota() {
		l.log().Info("lending: refused a write over the friendship's quota", "peer", peer,
			"used", u.used, "in_flight", u.reserved, "size", n, "quota", f.Quota())
		return nil, errQuotaExceeded
	}
	if sp.Free < floor+uint64(l.reserved)+uint64(n) {
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

// reserveFloor admits n bytes against the floor alone: room the store
// needs briefly for bytes already counted (a completion's copy).
func (l *Lending) reserveFloor(n int64) (release func(), err error) {
	sp, err := l.statfs()
	if err != nil {
		l.log().Error("lending: reading the store's free space failed; refusing the write", "store", l.StoreDir, "err", err)
		return nil, errInternal
	}
	floor := store.FloorBytes(l.floor(), sp.Total)
	l.mu.Lock()
	defer l.mu.Unlock()
	if sp.Free < floor+uint64(l.reserved)+uint64(n) {
		l.log().Warn("lending: refused a completion below the store's free-space floor",
			"free", sp.Free, "in_flight", l.reserved, "size", n, "floor", floor)
		return nil, errQuotaExceeded
	}
	l.reserved += n
	return func() {
		l.mu.Lock()
		l.reserved -= n
		l.mu.Unlock()
	}, nil
}

// count moves peer's counter by what a write stored or removed; the
// caller holds the borrower's pub lock shared. lendisk.db is written
// first and memory follows only once it took the change; a failure is
// logged, and the reconcile corrects both.
func (l *Lending) count(peer string, delta int64) {
	if delta == 0 {
		return
	}
	u := l.use(peer)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadLocked(peer, u); err != nil {
		l.log().Error("lending: reading the borrower's usage failed; the reconcile corrects it", "peer", peer, "err", err)
		return
	}
	if err := l.DB.AddLentUsage(context.Background(), peer, delta); err != nil {
		l.log().Error("lending: updating the borrower's usage failed; the reconcile corrects it", "peer", peer, "delta", delta, "err", err)
		return
	}
	u.used = max(u.used+delta, 0)
}

// counted moves peer's counter in memory by a change lendisk.db already
// took (a settled part).
func (l *Lending) counted(peer string, delta int64) {
	u := l.use(peer)
	l.mu.Lock()
	defer l.mu.Unlock()
	if u.loaded {
		u.used = max(u.used+delta, 0)
	}
}

// body hashes a borrower's request body (LEND-4) and fails it with
// errTooLong past limit bytes (SEC-5), and with what onEOF returns at
// its end. Stalls are the bridge's to end (bridge.boundedBody).
type body struct {
	r        io.Reader
	limit, n int64
	h        hash.Hash
	failed   error
	ended    bool
	onEOF    func() error
	// onFull, if set, runs once when the body has read limit bytes,
	// before the reader that read them gets them: a rewrite moves the
	// replaced version then, whether or not the store reads on to EOF.
	onFull func() error
	full   bool
}

func (l *Lending) body(r io.Reader, limit int64) *body {
	if r == nil {
		r = eofReader{}
	}
	return &body{r: r, limit: limit, h: sha256.New()}
}

func (b *body) hex() string { return hex.EncodeToString(b.h.Sum(nil)) }

func (b *body) Read(p []byte) (int, error) {
	if b.failed != nil {
		return 0, b.failed
	}
	if b.ended {
		return 0, io.EOF
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	if b.n > b.limit {
		b.failed = errTooLong
		return 0, errTooLong
	}
	b.h.Write(p[:n])
	if b.onFull != nil && !b.full && b.n == b.limit {
		b.full = true
		if e := b.onFull(); e != nil {
			b.failed = e
			return 0, e
		}
	}
	if err == io.EOF {
		b.ended = true
		if b.onEOF != nil {
			if e := b.onEOF(); e != nil {
				b.failed = e
				return n, e
			}
		}
	}
	return n, err
}

// reader is b as the store reads it: versitygw's ChecksumReader on the
// original body stays visible (posix asks for it by type assertion).
func (b *body) reader(orig io.Reader) io.Reader {
	if cr, ok := orig.(middlewares.ChecksumReader); ok {
		return checksumBody{b, cr}
	}
	return b
}

type checksumBody struct {
	*body
	cr middlewares.ChecksumReader
}

func (c checksumBody) Algorithm() string { return c.cr.Algorithm() }
func (c checksumBody) Checksum() string  { return c.cr.Checksum() }

// published is a borrower's pub lock taken shared at most once, at the
// moment the store may publish, and released when the write is counted.
type published struct {
	u    *borrowerUse
	once sync.Once
	held bool
}

func (p *published) take() {
	p.once.Do(func() {
		p.u.pub.RLock()
		p.held = true
	})
}

func (p *published) release() {
	if p.held {
		p.held = false
		p.u.pub.RUnlock()
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
	o := storedObject{sha256: strings.ToLower(out.Metadata[metaSHA256]), etag: deref(out.ETag)}
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
func (l *Lending) putLent(ctx context.Context, peer, sealed string, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	var none s3response.PutObjectOutput
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return none, err
	}
	want, mark, meta, err := declared(in.Metadata)
	if err != nil {
		return none, err
	}
	in.Metadata = meta
	var size int64
	if in.ContentLength != nil && *in.ContentLength > 0 {
		size = *in.ContentLength
	}
	b := l.body(in.Body, size)
	old, has, err := l.existing(ctx, *in.Bucket, key)
	if err != nil {
		l.log().Error("lending: reading the stored object failed; refusing the put", "bucket", *in.Bucket, "key", key, "err", err)
		return none, errInternal
	}
	if has {
		if mark != "" && old.sha256 != want && old.sha256 == mark {
			return l.rewriteLent(ctx, peer, sealed, in, b, size, want, mark)
		}
		return l.putAgain(b, want, old)
	}
	ts, err := l.tombsOf(ctx, peer, sealed, key)
	if err != nil {
		return none, err
	}
	if len(ts) > 0 {
		return l.putOverTomb(ctx, peer, *in.Bucket, b, want, ts)
	}
	release, err := l.reserve(ctx, peer, size+itemOverhead)
	if err != nil {
		return none, err
	}
	defer release()
	pub := &published{u: l.use(peer)}
	defer pub.release()
	b.onEOF = func() error {
		if b.hex() != want {
			return errBadDigest
		}
		pub.take()
		return nil
	}
	if size == 0 {
		pub.take() // the store may publish without reading
	}
	orig := in.Body
	in.Body = b.reader(orig)
	in.IfNoneMatch = &anyETag
	out, err := l.Next.PutObject(ctx, in)
	if err == nil && (b.failed != nil || b.n != size || b.hex() != want) {
		// The store published a body this layer refused: remove it.
		l.remove(*in.Bucket, key)
		if b.failed != nil {
			return none, b.failed
		}
		return none, errBadDigest
	}
	if b.failed != nil {
		return none, b.failed
	}
	if preconditionFailed(err) {
		// Another put of the key was published while this one streamed.
		old, has, herr := l.existing(ctx, *in.Bucket, key)
		if herr == nil && has && old.sha256 == want && b.n == old.size && b.hex() == want {
			return s3response.PutObjectOutput{ETag: old.etag}, nil
		}
		return none, errKeyChanged
	}
	if err != nil {
		return out, err
	}
	l.count(peer, b.n+itemOverhead)
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
func (l *Lending) putAgain(b *body, want string, old storedObject) (s3response.PutObjectOutput, error) {
	if old.sha256 != want {
		return s3response.PutObjectOutput{}, errKeyChanged
	}
	if _, err := io.Copy(io.Discard, b); err != nil {
		if b.failed != nil {
			return s3response.PutObjectOutput{}, b.failed
		}
		return s3response.PutObjectOutput{}, errInternal
	}
	if b.n != old.size || b.hex() != want {
		return s3response.PutObjectOutput{}, errBadDigest
	}
	return s3response.PutObjectOutput{ETag: old.etag}, nil
}

// createBucketLent creates a borrower's bucket through create, which
// makes the bucket and its row. A new bucket is admitted like a write of
// itemOverhead bytes and counted (NET-7: CreateBucket counts against
// quota); one the borrower already has is not refused.
func (l *Lending) createBucketLent(ctx context.Context, peer, sealed string, create func() error) error {
	has, err := l.DB.HasLentBucket(ctx, peer, sealed)
	if err != nil {
		return errInternal
	}
	if has {
		return create()
	}
	release, err := l.reserve(ctx, peer, itemOverhead)
	if err != nil {
		return err
	}
	defer release()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	err = create()
	if err == nil || isCode(err, "BucketAlreadyOwnedByYou") {
		l.count(peer, itemOverhead)
	}
	return err
}

// createUploadLent begins a borrower's multipart upload, recording the
// hash it declared.
func (l *Lending) createUploadLent(ctx context.Context, peer, sealed string, in s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	var none s3response.InitiateMultipartUploadResult
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return none, err
	}
	// A rewrite is a single put (#74): a multipart create's mark is
	// dropped, and a key holding other bytes refuses it as ever.
	want, _, meta, err := declared(in.Metadata)
	if err != nil {
		return none, err
	}
	in.Metadata = meta
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
	if !has {
		ts, err := l.tombsOf(ctx, peer, sealed, key)
		if err != nil {
			return none, err
		}
		if _, same := sameHash(ts, want); len(ts) > 0 && !same {
			return none, errKeyChanged
		}
	}
	release, err := l.reserve(ctx, peer, itemOverhead)
	if err != nil {
		return none, err
	}
	defer release()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
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
	l.count(peer, itemOverhead)
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

// lockUpload serialises the accounting of one upload; the caller holds
// the borrower's pub lock shared first.
func (l *Lending) lockUpload(ctx context.Context, uploadID string) (func(), error) {
	return l.uploads.lock(ctx, "", uploadID)
}

// parts lists the parts stored for an upload, by number.
func (l *Lending) parts(ctx context.Context, bucket, key, uploadID string) (map[int32]int64, error) {
	out := map[int32]int64{}
	marker, page := "", int32(1000)
	for {
		res, err := l.Next.ListParts(ctx, &s3.ListPartsInput{Bucket: &bucket, Key: &key, UploadId: &uploadID,
			PartNumberMarker: &marker, MaxParts: &page})
		if err != nil {
			return nil, err
		}
		for _, p := range res.Parts {
			out[int32(p.PartNumber)] = p.Size
		}
		if !res.IsTruncated || res.NextPartNumberMarker == 0 {
			return out, nil
		}
		marker = strconv.Itoa(res.NextPartNumberMarker)
	}
}

// uploadPartLent stores a part of a borrower's upload, admitted for its
// whole size, and counts what it changes over the part of that number
// counted before (lent_part), in one transaction with the usage. From
// the body's end, before the store publishes the part, until it is
// counted, the request holds the borrower's pub lock and the upload's
// lock, so parts of one number are published and counted in the same
// order: re-sends, racing or not, count once and as the store holds
// them. A part that fails to store changes nothing.
func (l *Lending) uploadPartLent(ctx context.Context, peer, sealed string, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	key := deref(in.Key)
	u, err := l.upload(ctx, peer, sealed, key, in.UploadId)
	if err != nil {
		return nil, err
	}
	var size, number int64
	if in.ContentLength != nil && *in.ContentLength > 0 {
		size = *in.ContentLength
	}
	if in.PartNumber != nil {
		number = int64(*in.PartNumber)
	}
	release, err := l.reserve(ctx, peer, size)
	if err != nil {
		return nil, err
	}
	defer release()
	pub := &published{u: l.use(peer)}
	defer pub.release()
	var unlock func()
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	lockAll := func() error {
		pub.take()
		if unlock != nil {
			return nil
		}
		var err error
		unlock, err = l.lockUpload(ctx, u.UploadID)
		return err
	}
	b := l.body(in.Body, size)
	b.onEOF = lockAll
	if size == 0 {
		if err := lockAll(); err != nil {
			return nil, errInternal
		}
	}
	c := *in
	c.Body = b.reader(in.Body)
	out, err := l.Next.UploadPart(ctx, &c)
	if b.failed != nil {
		return nil, b.failed
	}
	if err != nil {
		return nil, err
	}
	if lerr := lockAll(); lerr != nil {
		return nil, errInternal
	}
	if l.afterPartStored != nil {
		l.afterPartStored(int32(number), b.n)
	}
	delta, err := l.DB.SettleLentPart(context.Background(), peer, u.UploadID, int32(number), b.n)
	if errors.Is(err, state.ErrNotFound) {
		return nil, errNoSuchUpload // finished meanwhile; its end counted it
	}
	if err != nil {
		l.log().Error("lending: counting a part failed; the reconcile corrects it", "upload", u.UploadID, "err", err)
		return out, nil
	}
	l.counted(peer, delta)
	return out, nil
}

// completeLent completes a borrower's upload and verifies the assembled
// object, whole, against the hash declared at create (LEND-4). A key
// that holds an object with another hash is refused before anything
// changes; with the same hash the upload is aborted and the stored
// object kept. The completion needs room for a copy of the parts until
// they are removed, which is reserved against the floor. A retried
// completion of an upload already in the store is verified if it was
// not, then answered by the store.
func (l *Lending) completeLent(ctx context.Context, peer, sealed string, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	var none s3response.CompleteMultipartUploadResult
	bucket, key := *in.Bucket, deref(in.Key)
	if err := checkKey(key); err != nil {
		return none, "", err
	}
	if in.UploadId == nil {
		return none, "", errNoSuchUpload
	}
	uploadID := *in.UploadId
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
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	if has && l.Uploads != nil {
		if id, err := l.Uploads.UploadID(bucket, key); err == nil && id == uploadID {
			// This upload is the stored object: a retried completion.
			if uerr == nil {
				// Completed before a crash and not verified yet.
				if bad := l.verifyLanded(ctx, peer, bucket, key, u, old); bad != nil {
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
	var tomb *state.Tombstone
	if !has {
		ts, err := l.tombsOf(ctx, peer, sealed, key)
		if err != nil {
			return none, "", err
		}
		t, same := sameHash(ts, u.SHA256)
		if len(ts) > 0 && !same {
			return none, "", errKeyChanged
		}
		if same {
			tomb = &t
		}
	}
	ulock, err := l.lockUpload(ctx, uploadID)
	if err != nil {
		return none, "", errInternal
	}
	defer ulock()
	cur, err := l.DB.GetLentUpload(ctx, uploadID)
	if err != nil {
		return none, "", errNoSuchUpload
	}
	parts := cur.Counted
	if tomb != nil {
		// The same object, tombstoned: revive it and drop the upload.
		if err := l.Next.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: in.Bucket, Key: in.Key, UploadId: in.UploadId}); err != nil {
			return none, "", err
		}
		l.count(peer, -(parts + itemOverhead))
		l.dropUpload(uploadID)
		etag, err := l.revive(ctx, bucket, *tomb)
		if err != nil {
			return none, "", err
		}
		return s3response.CompleteMultipartUploadResult{Bucket: in.Bucket, Key: in.Key, ETag: &etag}, "", nil
	}
	if has {
		// The same object again: keep the stored one.
		if err := l.Next.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: in.Bucket, Key: in.Key, UploadId: in.UploadId}); err != nil {
			return none, "", err
		}
		l.count(peer, -(parts + itemOverhead))
		l.dropUpload(uploadID)
		etag := old.etag
		return s3response.CompleteMultipartUploadResult{Bucket: in.Bucket, Key: in.Key, ETag: &etag}, "", nil
	}
	release, err := l.reserveFloor(parts)
	if err != nil {
		return none, "", err
	}
	defer release()
	if l.beforeComplete != nil {
		l.beforeComplete()
	}
	// A put of the key (which takes no key lock) may have been published
	// since the check above: then this one is refused.
	c := *in
	c.IfNoneMatch = &anyETag
	res, ver, err := l.Next.CompleteMultipartUpload(ctx, &c)
	if preconditionFailed(err) {
		return res, ver, errKeyChanged
	}
	if err != nil {
		return res, ver, err
	}
	n, got, err := l.hashStored(ctx, bucket, key)
	if err != nil || got != u.SHA256 {
		l.remove(bucket, key)
		l.count(peer, -(parts + itemOverhead))
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

// verifyLanded hashes an object that upload u completed before a crash;
// on a mismatch it is removed and errBadDigest returned. u is settled
// and counted either way, except when the object cannot be read now.
// The caller holds the borrower's pub lock.
func (l *Lending) verifyLanded(ctx context.Context, peer, bucket, key string, u state.LentUpload, old storedObject) error {
	_, got, err := l.hashStored(ctx, bucket, key)
	if err != nil {
		return errInternal
	}
	l.dropUpload(u.UploadID)
	if got != u.SHA256 {
		l.remove(bucket, key)
		l.count(peer, -(u.Counted + itemOverhead))
		return errBadDigest
	}
	l.count(peer, old.size-u.Counted)
	return nil
}

func (l *Lending) dropUpload(uploadID string) {
	if err := l.DB.DropLentUpload(context.Background(), uploadID); err != nil {
		l.log().Warn("lending: forgetting a finished upload failed", "upload", uploadID, "err", err)
	}
}

// abortLent aborts a borrower's upload and frees its parts.
func (l *Lending) abortLent(ctx context.Context, peer, sealed string, in *s3.AbortMultipartUploadInput) error {
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return err
	}
	u, err := l.upload(ctx, peer, sealed, key, in.UploadId)
	if err != nil {
		return err
	}
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	unlock, err := l.lockUpload(ctx, u.UploadID)
	if err != nil {
		return errInternal
	}
	defer unlock()
	cur, err := l.DB.GetLentUpload(ctx, u.UploadID)
	if err != nil {
		return errNoSuchUpload
	}
	if err := l.Next.AbortMultipartUpload(ctx, in); err != nil {
		return err
	}
	l.count(peer, -(cur.Counted + itemOverhead))
	l.dropUpload(u.UploadID)
	return nil
}

// reconcile sets each borrower's counter from a listing of what it
// holds here, when its last reconcile is older than every (LEND-1), and
// aborts its uploads open longer than AbandonAfter. It holds the
// borrower's pub lock while it lists, so it waits for publications in
// progress, never for bodies.
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
		if err := l.reconcileOne(ctx, f.Peer); err != nil {
			l.log().Error("lending: reconciling a borrower's usage failed", "peer", f.Peer, "err", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

func (l *Lending) reconcileOne(ctx context.Context, peer string) error {
	u := l.use(peer)
	u.pub.Lock()
	defer u.pub.Unlock()
	n, err := l.listed(ctx, peer)
	if err != nil {
		return err
	}
	if l.afterList != nil {
		l.afterList()
	}
	l.mu.Lock()
	err = l.DB.SetLentUsage(ctx, peer, n, l.now())
	if err == nil {
		u.used, u.loaded = n, true
	}
	l.mu.Unlock()
	return err
}

// listed sums what the store holds of peer's buckets: each bucket,
// object and open upload's overhead, the objects' bytes and the parts'.
// An upload with no part for AbandonAfter (posix's Initiated, which each
// part refreshes) is aborted, not counted, and its row dropped; each
// kept upload's row is set to its parts. A row whose upload the store
// no longer has is dropped; nothing else drops one.
func (l *Lending) listed(ctx context.Context, peer string) (int64, error) {
	id, err := identity.ParseNodeID(peer)
	if err != nil {
		return 0, err
	}
	bs, err := l.DB.LentBuckets(ctx, peer)
	if err != nil {
		return 0, err
	}
	cutoff := l.now().Add(-l.abandonAfter())
	live := map[string]bool{}
	var total int64
	for _, b := range bs {
		total += itemOverhead
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
				total += itemOverhead
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
		for {
			res, err := l.Next.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket,
				KeyMarker: &keyMarker, UploadIdMarker: &idMarker, MaxUploads: &page})
			if err != nil {
				return 0, err
			}
			for _, up := range res.Uploads {
				k, uid := up.Key, up.UploadID
				if up.Initiated.Before(cutoff) {
					l.log().Info("lending: aborting a borrower's abandoned upload", "peer", peer, "bucket", bucket,
						"key", k, "last_part", up.Initiated)
					if err := l.Next.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: &k,
						UploadId: &uid}); err != nil && !isCode(err, "NoSuchUpload") {
						return 0, err
					}
					l.dropUpload(uid)
					continue
				}
				ps, err := l.parts(ctx, bucket, k, uid)
				if err != nil && !isCode(err, "NoSuchUpload") {
					return 0, err
				}
				live[uid] = true
				total += itemOverhead
				for _, n := range ps {
					total += n
				}
				if err := l.DB.SetLentUploadParts(ctx, uid, ps); err != nil {
					return 0, err
				}
			}
			if !res.IsTruncated {
				break
			}
			keyMarker, idMarker = res.NextKeyMarker, res.NextUploadIDMarker
		}
	}
	// Tombstoned objects count as they did live (RET-1); posix lists
	// nothing under .lendisk.
	n, tb, err := l.DB.TombstoneUsage(ctx, peer)
	if err != nil {
		return 0, err
	}
	total += tb + n*itemOverhead
	rows, err := l.DB.LentUploads(ctx, peer)
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		if !live[r.UploadID] {
			l.dropUpload(r.UploadID)
		}
	}
	return total, nil
}

// reconcileLoop reconciles every borrower at start, whenever it was
// last reconciled (a run that did not finish leaves no trace to go by),
// and then every interval each borrower once a day, until ctx ends.
func (l *Lending) reconcileLoop(ctx context.Context, interval time.Duration) {
	for every := time.Duration(0); ; every = 24 * time.Hour {
		if err := l.reconcile(ctx, every); err != nil && ctx.Err() == nil {
			l.log().Error("lending: reconciling borrowers' usage failed; trying again later", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
