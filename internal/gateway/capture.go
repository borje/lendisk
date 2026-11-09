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
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// MetaCapture is the user metadata key every captured PutObject stores
// with the object: the token of its queue row, which tells startup whether
// the object on disk is the one a pending row was written for (EP-3).
const MetaCapture = "lendisk-capture"

// Decorator wraps the store's backend (D4). It captures every successful
// write and delete of an own bucket's key into the replication queue
// (EP-2, EP-3), classified by the bucket's tool profile (EP-7); a stored
// put of a key only Kopia format 1 writes marks the bucket, which is
// shown and alerted but never refused (COMPAT-1, noteFormat);
// tombstones are Lending's, above it (tomb.go, RET-1). A read of
// an own bucket's key the store lacks is served through the read-through
// (readthrough.go, EP-10, ID-6). Buckets that
// are not the node's own (lent-*) pass through: a borrower's writes are
// policed above, by Lending (quota.go).
//
// Capture is a pending row written before the backend call, marked done
// with the SHA-256 taken from the request stream as it passes (REPL-10),
// or removed when the call fails; ResolvePending settles rows a crash
// left, and ResolveStranded those a failed completion or removal left
// while running. Bodies are never buffered or read twice. A captured
// request holds its key's lock from before its row is written until the
// row is settled, so one key's rows reach the store in sequence order
// (#48); waiting for it ends with the request's context (#55).
//
// Overrides must return unwrapped s3err.APIError values for client-visible
// refusals (Q-I.5).
type Decorator struct {
	backend.Backend
	// Uploads reads which upload assembled a stored object: the store
	// the gateway opened, however Backend wraps it (#62).
	Uploads UploadIDs
	// DB holds the queue; nil passes everything through.
	DB       *state.DB
	Profiles profile.Set
	Log      *slog.Logger
	// Fetch opens objects at friends for the read-through (EP-10,
	// readthrough.go); nil fetches nothing.
	Fetch Fetcher
	// Now is the clock a bucket's format-1 mark is taken on (COMPAT-1);
	// nil means time.Now.
	Now func() time.Time
	// AfterStore, if set, is called after the store accepted a captured
	// request and before its row is completed: the crash window startup
	// settles. Tests only.
	AfterStore func()

	keys     keyLocks // one captured request per key at a time
	mu       sync.Mutex
	stranded map[int64]bool // rows to settle while running (strand)
	// unmatched holds the rows already reported as unmatchable to the
	// store (errUploadUnreadable), so each is warned of once per run.
	unmatched map[int64]bool

	// completing holds the claim on each upload being completed (under
	// mu), so a retry can wait for its outcome rather than for the key.
	completing map[string]*completion
	// reach is what the read-through saw of each place (under mu).
	reach map[string]reach

	// ctx bounds the background hashes of completed uploads (nil: never
	// cancelled); hashes counts them, so the gateway waits for them to
	// stop before it returns.
	ctx    context.Context
	hashes hashGroup
	// beforeHash, if set, is called before a completed upload is hashed;
	// seenUpload when a completion has looked up its upload's row, with
	// the row's state ("" for none). Tests only.
	beforeHash func()
	seenUpload func(state string)
}

var (
	errInternal       = s3err.GetAPIError(s3err.ErrInternalError)
	errCopyNotOffered = s3err.APIError{Code: "NotImplemented", HTTPStatusCode: 501,
		Description: "lendisk: server-side copy is not offered on a backed-up bucket; upload the object instead"}
)

// classify returns the class of an own bucket's key, or "" when the
// bucket is not the node's own or capture is off. An error means the
// state database could not be read; the request must fail rather than go
// uncaptured.
func (d *Decorator) classify(bucket, key *string) (profile.Class, error) {
	k, err := d.kind(bucket, key)
	return k.class, err
}

// keyKind is what a captured put records of its key: its class, whether
// the bucket's profile names it rewritable (EP-7, #74), and what it
// says of the repository's format (COMPAT-1, #79).
type keyKind struct {
	class      profile.Class
	rewritable bool
	format     profile.Format
}

// kind is classify, and the rest of keyKind.
func (d *Decorator) kind(bucket, key *string) (keyKind, error) {
	if d.DB == nil || bucket == nil || key == nil {
		return keyKind{}, nil
	}
	b, err := d.DB.GetBucket(context.Background(), *bucket)
	if errors.Is(err, state.ErrNotFound) {
		return keyKind{}, nil
	}
	if err != nil {
		d.Log.Error("capture: reading the bucket's profile failed; refusing the request", "bucket", *bucket, "err", err)
		return keyKind{}, errInternal
	}
	p := d.Profiles.Get(b.Profile)
	return keyKind{class: p.Classify(*key), rewritable: p.Rewritable(*key), format: p.Format(*key)}, nil
}

// noteFormat records, once the store has accepted a captured put, what
// its key says of the repository's format (COMPAT-1, #79): a key only
// Kopia format 1 writes marks the bucket, the upgrade's key ends the
// mark. The write stands whatever this finds; a failure is logged.
func (d *Decorator) noteFormat(bucket, key string, f profile.Format) {
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	switch f {
	case profile.Format1:
		marked, err := d.DB.MarkFormat1(context.Background(), bucket, key, now())
		if err != nil {
			d.Log.Error("capture: marking a bucket as holding a format-1 repository failed", "bucket", bucket, "key", key, "err", err)
		} else if marked {
			d.Log.Warn("bucket holds a Kopia repository in format version 1, which lendisk does not support; run `kopia repository set-parameters --upgrade`",
				"bucket", bucket, "key", key)
		}
	case profile.Upgraded:
		if err := d.DB.MarkUpgraded(context.Background(), bucket, now()); err != nil {
			d.Log.Error("capture: recording a repository's format upgrade failed", "bucket", bucket, "key", key, "err", err)
		}
	}
}

// captured reports whether a class is queued: an own bucket's key that
// is not local-only (EP-7).
func captured(c profile.Class) bool { return c != "" && c != profile.LocalOnly }

// PutObject captures a successful put (EP-2) with the hash of its stream.
func (d *Decorator) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	k, err := d.kind(in.Bucket, in.Key)
	if err != nil || !captured(k.class) {
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
		return d.Backend.PutObject(ctx, in)
	}
	unlock, err := d.keys.lock(ctx, *in.Bucket, *in.Key)
	if err != nil {
		return s3response.PutObjectOutput{}, errInternal
	}
	defer unlock()
	token := randomHex(8)
	in.Metadata = maps.Clone(in.Metadata)
	if in.Metadata == nil {
		in.Metadata = map[string]string{}
	}
	in.Metadata[MetaCapture] = token
	seq, err := d.begin(state.Capture{Bucket: *in.Bucket, Key: *in.Key, Op: state.OpPut, Class: string(k.class),
		Rewritable: k.rewritable, Token: token})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	var h *hashReader
	in.Body, h = hashing(in.Body)
	out, err := d.Backend.PutObject(ctx, in)
	if err != nil {
		d.drop(seq)
		return out, err
	}
	d.stored()
	d.noteFormat(*in.Bucket, *in.Key, k.format)
	if _, err := d.DB.FinishPut(context.Background(), seq, h.n, h.hex()); err != nil {
		// The object is stored and the row stays pending until it is
		// settled against the store. The client sees a retryable error.
		d.Log.Error("capture: completing a queue row failed", "bucket", *in.Bucket, "key", *in.Key, "seq", seq, "err", err)
		d.strand(seq)
		return s3response.PutObjectOutput{}, errInternal
	}
	// The bytes just hashed are this file's (VER-5): still holding the
	// key's lock, its identity is recorded with them.
	d.markHeld(context.Background(), *in.Bucket, *in.Key, h.hex(), d.stampOf(*in.Bucket, *in.Key))
	return out, nil
}

// CompleteMultipartUpload captures a completed upload (EP-2). Its parts
// arrive separately, possibly in parallel, so there is no single stream
// to hash on the way in; the hash is the SHA-256 of the whole object,
// read once from the store after it is assembled (§3.3, REPL-10). The
// client is answered as soon as the store has assembled the object; the
// hash runs after, in the background, still holding the key's lock, and
// a crash before it finishes leaves the row pending for startup (#49).
//
// One request per upload at a time claims it (a per-upload completion,
// registered before it waits for the key's lock); another completion of
// the same upload is a client's retry (#55, #59). A retry that finds a
// done row goes to the store uncaptured: posix answers it from the
// finished object without changing anything. A retry that finds a
// claim waits for the claimant's store call (not for the key's lock,
// which the claimant holds while it hashes): stored, it goes to the
// store as a done row's would; failed, it looks again. A retry that
// finds neither claims the upload itself and, under the key's lock,
// checks a pending row left stranded against the store: landed, it is
// answered from the store and the row is left for ResolveStranded to
// hash; not landed, the row is removed. Anything else is captured as a
// first completion. So no completion stores an object without a row,
// and no request hashes inline. ResolveStranded claims an upload too
// while it settles its row, and settles the claim as stored before it
// hashes, so a retry never waits for that hash either (#62).
func (d *Decorator) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	k, err := d.kind(in.Bucket, in.Key)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if !captured(k.class) || in.UploadId == nil || in.MultipartUpload == nil {
		return d.Backend.CompleteMultipartUpload(ctx, in)
	}
	bucket, key, uploadID := *in.Bucket, *in.Key, *in.UploadId
	id := uploadClaim(bucket, key, uploadID)
	for {
		_, st, found, err := d.DB.UploadEntry(context.Background(), bucket, key, uploadID)
		if err != nil {
			d.Log.Error("capture: reading the queue failed; refusing the request", "bucket", bucket, "key", key, "err", err)
			return s3response.CompleteMultipartUploadResult{}, "", errInternal
		}
		if d.seenUpload != nil {
			d.seenUpload(st)
		}
		if found && st != state.StatePending {
			return d.Backend.CompleteMultipartUpload(ctx, in)
		}
		f, mine := d.claim(id)
		if mine {
			return d.completeClaimed(ctx, in, k, id, f)
		}
		select {
		case <-f.done:
		case <-ctx.Done():
			return s3response.CompleteMultipartUploadResult{}, "", errInternal
		}
		if f.stored {
			return d.Backend.CompleteMultipartUpload(ctx, in)
		}
		// The claimant failed and removed its row, left it stranded, or
		// gave up before the store: look again.
	}
}

// completeClaimed completes an upload whose completion f this request
// has claimed. However it ends (a panic included), f is settled, the
// claim and the key's lock released, and a row it wrote and did not
// settle is stranded; on success the background hash does that.
func (d *Decorator) completeClaimed(ctx context.Context, in *s3.CompleteMultipartUploadInput, k keyKind, id string, f *completion) (res s3response.CompleteMultipartUploadResult, ver string, err error) {
	bucket, key, uploadID := *in.Bucket, *in.Key, *in.UploadId
	var (
		unlock     func()
		seq        int64
		open       bool // the row is written and not yet settled or handed on
		background bool
	)
	defer func() {
		if background {
			return
		}
		if open {
			d.strand(seq)
		}
		d.unclaim(id, f, unlock)
	}()
	unlock, err = d.keys.lock(ctx, bucket, key)
	if err != nil {
		return res, "", errInternal
	}
	// Under the key's lock no other completion of this upload is in the
	// store.
	landed, err := d.retryLanded(ctx, bucket, key, uploadID)
	if err != nil {
		d.Log.Error("capture: checking a retried completion against the store failed; refusing the request", "bucket", bucket, "key", key, "err", err)
		return res, "", errInternal
	}
	if landed {
		f.settle(true)
		return d.Backend.CompleteMultipartUpload(ctx, in)
	}
	etag, err := backend.ComputeMultipartETagFromPartETags(uploadID, in.MultipartUpload.Parts)
	if err != nil {
		return res, "", err
	}
	seq, err = d.begin(state.Capture{Bucket: bucket, Key: key, Op: state.OpPut, Class: string(k.class),
		Rewritable: k.rewritable, ETag: etag, UploadID: uploadID})
	if err != nil {
		return res, "", err
	}
	open = true
	res, ver, err = d.Backend.CompleteMultipartUpload(ctx, in)
	if err != nil {
		open = false
		d.drop(seq) // before the claim is released: a woken retry must find no row
		return res, ver, err
	}
	f.settle(true)
	d.stored()
	d.noteFormat(bucket, key, k.format)
	open = false
	if !d.hashes.start() {
		return res, ver, nil // stopping: the row stays pending for startup
	}
	background = true
	go func() {
		defer d.hashes.done()
		defer d.unclaim(id, f, unlock)
		ctx := d.hashCtx()
		if err := d.finishUpload(ctx, seq, bucket, key, deref(res.ETag)); err != nil {
			if ctx.Err() != nil {
				return // stopping: the row stays pending for startup
			}
			// The row stays pending until it is settled against the store.
			d.Log.Error("capture: hashing a completed upload failed", "bucket", bucket, "key", key, "seq", seq, "err", err)
			d.strand(seq)
		}
	}()
	return res, ver, nil
}

// completion is the claim on an upload of the one request completing it
// (or checking its stranded row): done is closed when that request's
// store call has returned, or it gave up before, and stored then tells
// whether the upload's object is in the store.
type completion struct {
	done   chan struct{}
	once   sync.Once
	stored bool
}

// settle records the outcome and wakes the waiting retries; only the
// first call counts.
func (f *completion) settle(stored bool) {
	f.once.Do(func() {
		f.stored = stored
		close(f.done)
	})
}

// uploadClaim names the claim on one upload of a key.
func uploadClaim(bucket, key, uploadID string) string {
	return lockName(bucket, key) + "\x00" + uploadID
}

// claim returns the claim on upload id and whether it is the caller's:
// a new one if nobody holds it, else the holder's.
func (d *Decorator) claim(id string) (f *completion, mine bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if f := d.completing[id]; f != nil {
		return f, false
	}
	if d.completing == nil {
		d.completing = map[string]*completion{}
	}
	f = &completion{done: make(chan struct{})}
	d.completing[id] = f
	return f, true
}

// unclaim releases a claim and the key's lock (if taken), then settles
// the claim as failed if it was not settled, so a woken retry finds
// neither.
func (d *Decorator) unclaim(id string, f *completion, unlock func()) {
	d.mu.Lock()
	if d.completing[id] == f {
		delete(d.completing, id)
	}
	d.mu.Unlock()
	if unlock != nil {
		unlock()
	}
	f.settle(false)
}

// retryLanded reports, under the key's lock, whether an upload's
// completion is already in the store: it has a done row, or a pending
// row (stranded: its request has ended) whose object landed, which is a
// HEAD and an attribute read, never a hash. A stranded row that landed
// stays pending for ResolveStranded to hash (#59); one that did not is
// removed.
func (d *Decorator) retryLanded(ctx context.Context, bucket, key, uploadID string) (bool, error) {
	seq, st, found, err := d.DB.UploadEntry(ctx, bucket, key, uploadID)
	if err != nil || !found || st != state.StatePending {
		return found, err
	}
	e, ok, err := d.DB.PendingCapture(ctx, seq)
	if err != nil {
		return false, err
	}
	if !ok {
		return d.retryLanded(ctx, bucket, key, uploadID)
	}
	landed, err := d.landed(ctx, e)
	if err != nil {
		return false, err
	}
	if landed {
		d.strand(seq)
		return true, nil
	}
	if err := d.DB.DropCaptures(context.Background(), seq); err != nil {
		d.strand(seq)
		return false, err
	}
	return false, nil
}

// hashGroup counts the background hashes of completed uploads, so the
// gateway waits for them before it returns. Once stopped it starts no
// more: a completion handler can outlive versitygw's shutdown (#55).
type hashGroup struct {
	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
}

// start counts a hash about to run; false means the group is stopped.
func (g *hashGroup) start() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.wg.Add(1)
	return true
}

func (g *hashGroup) done() { g.wg.Done() }

// stop starts no more hashes and waits for the running ones.
func (g *hashGroup) stop() {
	g.mu.Lock()
	g.stopped = true
	g.mu.Unlock()
	g.wg.Wait()
}

// hashCtx bounds the background hashes of completed uploads.
func (d *Decorator) hashCtx() context.Context {
	if d.ctx != nil {
		return d.ctx
	}
	return context.Background()
}

// finishUpload hashes a completed upload's object and marks its row done.
// If the key holds another object (it changed outside lendisk), the row
// is dropped. A row someone else settled first (FinishPut's ErrNotFound)
// is success.
func (d *Decorator) finishUpload(ctx context.Context, seq int64, bucket, key, etag string) error {
	if d.beforeHash != nil {
		d.beforeHash()
	}
	before := d.stampOf(bucket, key)
	out, err := d.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if notFound(err) {
		d.drop(seq)
		return nil
	}
	if err != nil {
		return err
	}
	defer out.Body.Close()
	if etag != "" && strings.Trim(deref(out.ETag), `"`) != strings.Trim(etag, `"`) {
		d.drop(seq)
		return nil
	}
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, out.Body})
	if err != nil {
		return err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	_, err = d.DB.FinishPut(ctx, seq, n, sum)
	if errors.Is(err, state.ErrNotFound) {
		d.Log.Debug("capture: a completed upload's row was settled elsewhere", "bucket", bucket, "key", key, "seq", seq)
		return nil
	}
	if err == nil && before != "" && d.stampOf(bucket, key) == before {
		d.markHeld(ctx, bucket, key, sum, before) // the file did not change while it was hashed
	}
	return err
}

// ctxReader stops a long read when ctx ends.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// DeleteObject captures a successful delete (EP-2).
func (d *Decorator) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return nil, err
	}
	if !captured(class) {
		return d.Backend.DeleteObject(ctx, in)
	}
	unlock, err := d.keys.lock(ctx, *in.Bucket, *in.Key)
	if err != nil {
		return nil, errInternal
	}
	defer unlock()
	seq, err := d.begin(state.Capture{Bucket: *in.Bucket, Key: *in.Key, Op: state.OpDelete, Class: string(class)})
	if err != nil {
		return nil, err
	}
	out, err := d.Backend.DeleteObject(ctx, in)
	if err != nil {
		d.drop(seq)
		return out, err
	}
	d.stored()
	if err := d.DB.FinishDelete(context.Background(), seq); err != nil {
		d.Log.Error("capture: completing a queue row failed", "bucket", *in.Bucket, "key", *in.Key, "seq", seq, "err", err)
		d.strand(seq)
		return nil, errInternal
	}
	return out, nil
}

// DeleteObjects captures each successfully deleted entry (EP-2).
func (d *Decorator) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if d.DB == nil || in.Delete == nil {
		return d.Backend.DeleteObjects(ctx, in)
	}
	var cs []state.Capture
	for _, o := range in.Delete.Objects {
		class, err := d.classify(in.Bucket, o.Key)
		if err != nil {
			return s3response.DeleteResult{}, err
		}
		if captured(class) {
			cs = append(cs, state.Capture{Bucket: *in.Bucket, Key: *o.Key, Op: state.OpDelete, Class: string(class)})
		}
	}
	if len(cs) == 0 {
		return d.Backend.DeleteObjects(ctx, in)
	}
	keys := make([]string, len(cs))
	for i, c := range cs {
		keys[i] = c.Key
	}
	unlock, err := d.keys.lock(ctx, *in.Bucket, keys...)
	if err != nil {
		return s3response.DeleteResult{}, errInternal
	}
	defer unlock()
	bg := context.Background()
	seqs, err := d.DB.BeginCaptures(bg, cs)
	if err != nil {
		d.Log.Error("capture: writing queue rows failed; refusing the request", "bucket", *in.Bucket, "err", err)
		return s3response.DeleteResult{}, errInternal
	}
	res, err := d.Backend.DeleteObjects(ctx, in)
	if err != nil {
		d.drop(seqs...)
		return res, err
	}
	deleted := map[string]bool{}
	for _, o := range res.Deleted {
		if o.Key != nil {
			deleted[*o.Key] = true
		}
	}
	var done, failed []int64
	for i, c := range cs {
		if deleted[c.Key] {
			done = append(done, seqs[i])
		} else {
			failed = append(failed, seqs[i])
		}
	}
	d.stored()
	d.drop(failed...)
	if err := d.DB.FinishDelete(bg, done...); err != nil {
		d.Log.Error("capture: completing queue rows failed", "bucket", *in.Bucket, "err", err)
		d.strand(done...)
		return s3response.DeleteResult{}, errInternal
	}
	return res, nil
}

// CopyObject is refused on own buckets: the copy's bytes never pass the
// decorator, so it could not be hashed from a stream (Q-H, §3.3). No
// backup tool lendisk serves needs it.
func (d *Decorator) CopyObject(ctx context.Context, in s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if class != "" {
		return s3response.CopyObjectOutput{}, errCopyNotOffered
	}
	return d.Backend.CopyObject(ctx, in)
}

// UploadPartCopy is refused on own buckets, as CopyObject is.
func (d *Decorator) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if class != "" {
		return s3response.CopyPartResult{}, errCopyNotOffered
	}
	return d.Backend.UploadPartCopy(ctx, in)
}

func (d *Decorator) stored() {
	if d.AfterStore != nil {
		d.AfterStore()
	}
}

func (d *Decorator) begin(c state.Capture) (int64, error) {
	seqs, err := d.DB.BeginCaptures(context.Background(), []state.Capture{c})
	if err != nil {
		d.Log.Error("capture: writing a queue row failed; refusing the request", "bucket", c.Bucket, "key", c.Key, "err", err)
		return 0, errInternal
	}
	return seqs[0], nil
}

func (d *Decorator) drop(seqs ...int64) {
	if err := d.DB.DropCaptures(context.Background(), seqs...); err != nil {
		// Left pending, the rows are settled against the store later.
		d.Log.Warn("capture: removing the rows of a failed request failed", "seqs", seqs, "err", err)
		d.strand(seqs...)
	}
}

// hashReader hashes what is read through it, so the hash is taken as the
// stream passes to the store.
type hashReader struct {
	r io.Reader
	h hash.Hash
	n int64
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.h.Write(p[:n])
		h.n += int64(n)
	}
	return n, err
}

func (h *hashReader) hex() string { return hex.EncodeToString(h.h.Sum(nil)) }

// checksumHashReader keeps versitygw's ChecksumReader visible through the
// wrapper: posix asks the body for a trailing checksum by type assertion.
type checksumHashReader struct {
	*hashReader
	cr middlewares.ChecksumReader
}

func (c checksumHashReader) Algorithm() string { return c.cr.Algorithm() }
func (c checksumHashReader) Checksum() string  { return c.cr.Checksum() }

// hashing wraps body; a nil body reads as empty.
func hashing(body io.Reader) (io.Reader, *hashReader) {
	if body == nil {
		body = eofReader{}
	}
	h := &hashReader{r: body, h: sha256.New()}
	if cr, ok := body.(middlewares.ChecksumReader); ok {
		return checksumHashReader{h, cr}, h
	}
	return h, h
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
