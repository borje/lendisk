package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

// ov is what each object, bucket and open upload costs besides its bytes.
const ov = int64(itemOverhead)

// hashMeta is the metadata a borrower sends with an object (REPL-10).
func hashMeta(body []byte) map[string]string { return map[string]string{"lendisk-sha256": sum(body)} }

// status is the HTTP status of a failed S3 call, 0 if none.
func status(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// lent is a borrower with a bucket at a served lender.
type lent struct {
	h      *harness
	id     *identity.Identity
	c      *s3.Client
	bucket *string
	dir    string // the bucket's directory in the lender's store
	store  string // the bucket's store name
}

// lendTo serves a lender to one borrower with the given quota, the
// borrower's bucket created (one overhead of the quota).
func lendTo(t *testing.T, quota int64, setup func(h *harness)) *lent {
	t.Helper()
	h := newHarness(t, nil)
	h.self = newIdentity(t)
	b := newIdentity(t)
	h.befriendWith(b, quota)
	if setup != nil {
		setup(h)
	}
	h.serve(false)
	// The gateway reconciles every borrower at start, in the background;
	// waiting for it keeps it from landing in the middle of a test (a
	// reconcile aborts uploads it judges abandoned).
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if _, at, err := h.db.LentUsage(context.Background(), b.NodeID().String()); err == nil && !at.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the gateway's start reconcile did not finish")
		}
	}
	l := &lent{h: h, id: b, c: h.pairClient(b), bucket: aws.String(b.SealedName("laptop"))}
	l.store = LentName(b.NodeID(), *l.bucket)
	l.dir = filepath.Join(h.store, l.store)
	if _, err := l.c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: l.bucket}); err != nil {
		t.Fatal(err)
	}
	return l
}

func (l *lent) peer() string      { return l.id.NodeID().String() }
func (l *lent) lending() *Lending { return l.h.gw.lending }

func (l *lent) put(key string, body []byte, meta map[string]string) error {
	_, err := l.c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: l.bucket, Key: aws.String(key),
		Body: bytes.NewReader(body), Metadata: meta})
	return err
}

// putDirect puts through Lending's borrower path without HTTP.
func (l *lent) putDirect(key string, body io.Reader, size int64, meta map[string]string) error {
	_, err := l.lending().putLent(context.Background(), l.peer(), *l.bucket, s3response.PutObjectInput{Bucket: &l.store,
		Key: aws.String(key), ContentLength: &size, Body: body, Metadata: meta})
	return err
}

func (l *lent) usage() int64 {
	l.h.t.Helper()
	n, _, err := l.h.db.LentUsage(context.Background(), l.peer())
	if err != nil {
		l.h.t.Fatal(err)
	}
	return n
}

func (l *lent) wantUsage(want int64) {
	l.h.t.Helper()
	if n := l.usage(); n != want {
		l.h.t.Fatalf("counter %d, want %d (%d overheads + %d)", n, want, want/ov, want%ov)
	}
}

func (l *lent) stored(key string) ([]byte, bool) {
	b, err := os.ReadFile(filepath.Join(l.dir, key))
	return b, err == nil
}

func (l *lent) create(key string, meta map[string]string) (*string, error) {
	up, err := l.c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: l.bucket,
		Key: aws.String(key), Metadata: meta})
	if err != nil {
		return nil, err
	}
	return up.UploadId, nil
}

func (l *lent) part(key string, id *string, n int32, body []byte) (types.CompletedPart, error) {
	out, err := l.c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String(key), UploadId: id,
		PartNumber: &n, Body: bytes.NewReader(body)})
	if err != nil {
		return types.CompletedPart{}, err
	}
	return types.CompletedPart{PartNumber: &n, ETag: out.ETag}, nil
}

func (l *lent) complete(key string, id *string, parts ...types.CompletedPart) error {
	_, err := l.c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: l.bucket,
		Key: aws.String(key), UploadId: id, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}})
	return err
}

// mpu uploads parts under key with the declared hash and returns the
// completion's error.
func (l *lent) mpu(key string, declared string, parts ...[]byte) error {
	id, err := l.create(key, map[string]string{"lendisk-sha256": declared})
	if err != nil {
		return err
	}
	var done []types.CompletedPart
	for i, p := range parts {
		cp, err := l.part(key, id, int32(i+1), p)
		if err != nil {
			return err
		}
		done = append(done, cp)
	}
	return l.complete(key, id, done...)
}

// LEND-1, LEND-2, Q-I.5: once a borrower's objects fill the friendship's
// quota, its next put, multipart create and new bucket are refused with
// lendisk's own 507 QuotaExceeded (unwrapped, so not a 500), nothing is
// stored, and the counter holds what was stored. A retried create of a
// bucket it has is not refused. A deleted object counts until purged.
func TestOverQuotaRefusedWith507(t *testing.T) {
	l := lendTo(t, 3*ov+100, nil)
	ctx := context.Background()
	first := bytes.Repeat([]byte("a"), 60)
	if err := l.put("data/one", first, hashMeta(first)); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 60)
	second := bytes.Repeat([]byte("b"), 60)
	err := l.put("data/two", second, hashMeta(second))
	if code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("put over quota: %v (status %d)", err, status(err))
	}
	if _, ok := l.stored("data/two"); ok {
		t.Fatal("the refused object is stored")
	}
	small := bytes.Repeat([]byte("c"), 40)
	if err := l.put("data/three", small, hashMeta(small)); err != nil {
		t.Fatalf("a put that fits: %v", err)
	}
	l.wantUsage(3*ov + 100)
	if _, err := l.create("data/four", hashMeta(small)); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("multipart create over quota: %v", err)
	}
	other := aws.String(l.id.SealedName("desktop"))
	if _, err := l.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: other}); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("bucket create over quota: %v", err)
	}
	if _, err := os.Stat(filepath.Join(l.h.store, LentName(l.id.NodeID(), *other))); !os.IsNotExist(err) {
		t.Fatal("the refused bucket was created")
	}
	if _, err := l.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: l.bucket}); err != nil && code(err) != "BucketAlreadyOwnedByYou" {
		t.Fatalf("retried create of the borrower's bucket: %v", err)
	}
	// A delete keeps the object as a tombstone, counted until it is
	// purged (RET-1); the purge makes room.
	if _, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: l.bucket, Key: aws.String("data/one")}); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(3*ov + 100)
	if err := l.put("data/two", second, hashMeta(second)); code(err) != "QuotaExceeded" {
		t.Fatalf("put while the deleted object is kept: %v", err)
	}
	l.lending().Now = func() time.Time { return time.Now().Add(DefaultRetention + time.Hour) }
	if err := l.lending().purge(ctx); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 40)
	if err := l.put("data/two", second, hashMeta(second)); err != nil {
		t.Fatalf("put after the purge: %v", err)
	}
}

// Q-I.5: the quota refusal is an unwrapped s3err.APIError with 507, the
// only form versitygw passes to the client intact.
func TestQuotaErrorIsLendisksOwn507(t *testing.T) {
	ae, ok := error(errQuotaExceeded).(s3err.APIError)
	if !ok || ae.HTTPStatusCode != 507 || ae.Code != "QuotaExceeded" {
		t.Fatalf("errQuotaExceeded = %#v", errQuotaExceeded)
	}
}

// LEND-1, SEC-5: every object, bucket and open upload costs a fixed
// overhead besides its bytes, so empty objects, buckets and uploads
// cannot be made without end; the reconcile counts them the same way.
func TestEveryItemCostsItsOverhead(t *testing.T) {
	l := lendTo(t, 4*ov-1, nil)
	ctx := context.Background()
	for _, k := range []string{"e1", "e2"} {
		if err := l.put(k, nil, hashMeta(nil)); err != nil {
			t.Fatalf("empty %s: %v", k, err)
		}
	}
	l.wantUsage(3 * ov)
	if err := l.put("e3", nil, hashMeta(nil)); code(err) != "QuotaExceeded" {
		t.Fatalf("a third empty object: %v", err)
	}
	if _, err := l.create("mp", hashMeta(nil)); code(err) != "QuotaExceeded" {
		t.Fatalf("an upload: %v", err)
	}
	if _, err := l.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(l.id.SealedName("other"))}); code(err) != "QuotaExceeded" {
		t.Fatalf("a bucket: %v", err)
	}
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(3 * ov)
}

// blockingStatfs reports space and, once armed, blocks the next caller
// until released.
type blockingStatfs struct {
	free    atomic.Uint64
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newBlockingStatfs(free uint64) *blockingStatfs {
	b := &blockingStatfs{entered: make(chan struct{}), release: make(chan struct{})}
	b.free.Store(free)
	return b
}

func (b *blockingStatfs) statfs(string) (store.Space, error) {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return store.Space{Total: 1 << 40, Free: b.free.Load()}, nil
}

// LEND-1, SEC-5: the quota is checked against the count as it is when
// the write is admitted, not as it was read before: of two 60-byte puts
// with room for one, a put admitted after the other completed is
// refused.
func TestQuotaCheckedAgainstTheCurrentCount(t *testing.T) {
	bs := newBlockingStatfs(1 << 40)
	l := lendTo(t, ov+(60+ov)+30, func(h *harness) { h.statfs = bs.statfs })
	a, b := bytes.Repeat([]byte("a"), 60), bytes.Repeat([]byte("b"), 60)
	bs.armed.Store(true)
	late := make(chan error, 1)
	go func() { late <- l.put("data/b", b, hashMeta(b)) }()
	within(t, "the late put admitting", bs.entered)
	if err := l.put("data/a", a, hashMeta(a)); err != nil {
		t.Fatal(err)
	}
	close(bs.release)
	if err := within(t, "the late put", late); code(err) != "QuotaExceeded" {
		t.Fatalf("the late put: %v", err)
	}
	l.wantUsage(2*ov + 60)
}

// LEND-2, SEC-5: below the free-space floor (default 5% of the
// filesystem) a put, a multipart create and a new bucket are refused
// with the quota error, whatever the quota; a put that would take free
// space below the floor is refused too.
func TestBelowTheFloorRefusedWithTheQuotaError(t *testing.T) {
	const total, floor = 1_000_000, 50_000 // the default floor is 5%
	var free atomic.Uint64
	free.Store(total)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: total, Free: free.Load()}, nil }
	})
	ctx := context.Background()
	body := []byte("pack")
	if err := l.put("data/fits", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	free.Store(uint64(floor + ov + 10))
	if err := l.put("data/small", body, hashMeta(body)); err != nil {
		t.Fatalf("a put that stays above the floor: %v", err)
	}
	big := bytes.Repeat([]byte("x"), 20)
	if err := l.put("data/big", big, hashMeta(big)); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("a put that would cross the floor: %v", err)
	}
	free.Store(40_000)
	if err := l.put("data/more", body, hashMeta(body)); code(err) != "QuotaExceeded" {
		t.Fatalf("put below the floor: %v", err)
	}
	if _, err := l.create("data/mp", hashMeta(body)); code(err) != "QuotaExceeded" {
		t.Fatalf("multipart create below the floor: %v", err)
	}
	if _, err := l.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(l.id.SealedName("desktop"))}); code(err) != "QuotaExceeded" {
		t.Fatalf("bucket create below the floor: %v", err)
	}
	for _, k := range []string{"data/big", "data/more"} {
		if _, ok := l.stored(k); ok {
			t.Errorf("%s stored below the floor", k)
		}
	}
	free.Store(total)
	if err := l.put("data/more", body, hashMeta(body)); err != nil {
		t.Fatalf("put once space is back: %v", err)
	}
}

// LEND-2: a configured floor in bytes replaces the default.
func TestConfiguredFloorApplies(t *testing.T) {
	var free atomic.Uint64
	free.Store(1_000_000)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: 1_000_000, Free: free.Load()}, nil }
		h.floor = &config.Floor{Bytes: 600_000}
	})
	free.Store(500_000)
	body := []byte("pack")
	if err := l.put("data/x", body, hashMeta(body)); code(err) != "QuotaExceeded" {
		t.Fatalf("put below a 600 kB floor: %v", err)
	}
}

// LEND-2: completing an upload needs room for the object again while
// its parts still exist (posix copies them into place), so it is
// refused with the quota error when that would cross the floor, and
// goes through once there is room.
func TestCompletionReservesItsCopyAgainstTheFloor(t *testing.T) {
	const total, floor = 1 << 30, 1 << 30 / 20
	var free atomic.Uint64
	free.Store(total)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: total, Free: free.Load()}, nil }
	})
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	whole := append(append([]byte{}, p1...), p2...)
	id, err := l.create("data/big", hashMeta(whole))
	if err != nil {
		t.Fatal(err)
	}
	c1, err := l.part("data/big", id, 1, p1)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := l.part("data/big", id, 2, p2)
	if err != nil {
		t.Fatal(err)
	}
	free.Store(floor + 1<<20) // room for small writes, not for a 5 MiB copy
	if err := l.complete("data/big", id, c1, c2); code(err) != "QuotaExceeded" {
		t.Fatalf("completion below the floor: %v", err)
	}
	if _, ok := l.stored("data/big"); ok {
		t.Fatal("completed below the floor")
	}
	free.Store(total)
	if err := l.complete("data/big", id, c1, c2); err != nil {
		t.Fatalf("completion with room: %v", err)
	}
	l.wantUsage(2*ov + int64(len(whole)))
}

// LEND-2 (#92): a completion whose copy fits above the floor alone, but
// not beside the writes in flight, is answered 503 SlowDown, which the
// borrower retries, as a put is (#89): a 507 would pause it for quota
// over a write in flight that may yet fail.
func TestACompletionThatFitsButForWritesInFlightIsRetried(t *testing.T) {
	const total, floor = 1 << 30, 1 << 30 / 20
	var free atomic.Uint64
	free.Store(total)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: total, Free: free.Load()}, nil }
	})
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	whole := append(append([]byte{}, p1...), p2...)
	id, err := l.create("data/big", hashMeta(whole))
	if err != nil {
		t.Fatal(err)
	}
	c1, err := l.part("data/big", id, 1, p1)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := l.part("data/big", id, 2, p2)
	if err != nil {
		t.Fatal(err)
	}
	// An abandoned send: admitted for 1 MiB, it ends short.
	s := newStalledReader([]byte("cut short"))
	done := make(chan error, 1)
	go func() { done <- l.putDirect("data/abandoned", s, 1<<20, hashMeta(bytes.Repeat([]byte("a"), 1<<20))) }()
	within(t, "the abandoned put reading", s.reading)

	free.Store(floor + uint64(len(whole)) + 1<<19) // room for the copy, not for the write in flight too
	if err := l.complete("data/big", id, c1, c2); code(err) != "SlowDown" || status(err) != 503 {
		t.Fatalf("a completion below the floor only beside a write in flight: %v (status %d)", err, status(err))
	}
	free.Store(floor + uint64(len(whole)) - 1) // no room for the copy alone
	if err := l.complete("data/big", id, c1, c2); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("a completion below the floor alone: %v (status %d)", err, status(err))
	}
	if _, ok := l.stored("data/big"); ok {
		t.Fatal("completed below the floor")
	}
	free.Store(floor + uint64(len(whole)) + 1<<19)
	close(s.release)
	if err := within(t, "the abandoned put", done); err == nil {
		t.Fatal("the short body was stored")
	}
	if err := l.complete("data/big", id, c1, c2); err != nil {
		t.Fatalf("the retry once the write in flight ended: %v", err)
	}
}

// LEND-4, REPL-10: a borrower's put without x-amz-meta-lendisk-sha256,
// or with something that is not a SHA-256, is refused and nothing is
// stored; so is a multipart create.
func TestPutWithoutTheHashRefused(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	body := []byte("pack")
	for name, meta := range map[string]map[string]string{"none": nil, "not hex": {"lendisk-sha256": "zz"}} {
		if err := l.put("data/"+name, body, meta); status(err) != 400 {
			t.Errorf("%s: %v", name, err)
		}
		if _, ok := l.stored("data/" + name); ok {
			t.Errorf("%s: stored", name)
		}
	}
	if _, err := l.create("data/mp", nil); status(err) != 400 {
		t.Fatalf("multipart create without the hash: %v", err)
	}
	l.wantUsage(ov)
}

// LEND-4: a hash sent in upper case is the same hash: the put is
// accepted, and so is a retry of it in either case.
func TestUpperCaseHashIsTheSameHash(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	body := []byte("pack")
	upper := map[string]string{"lendisk-sha256": strings.ToUpper(sum(body))}
	if err := l.put("data/x", body, upper); err != nil {
		t.Fatal(err)
	}
	if err := l.put("data/x", body, upper); err != nil {
		t.Fatalf("retried in upper case: %v", err)
	}
	if err := l.put("data/x", body, hashMeta(body)); err != nil {
		t.Fatalf("retried in lower case: %v", err)
	}
	if err := l.mpu("data/y", strings.ToUpper(sum(body)), body); err != nil {
		t.Fatalf("multipart with an upper-case hash: %v", err)
	}
	if err := l.put("data/y", body, hashMeta(body)); err != nil {
		t.Fatalf("a put over it in lower case: %v", err)
	}
}

// LEND-4: a body whose SHA-256 is not the one declared is refused with
// BadDigest and not kept; the counter does not move.
func TestMismatchedBodyRemovedAndRefused(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	body := []byte("the bytes sent")
	err := l.put("data/x", body, hashMeta([]byte("the bytes hashed")))
	if code(err) != "BadDigest" || status(err) != 400 {
		t.Fatalf("mismatched put: %v", err)
	}
	if _, ok := l.stored("data/x"); ok {
		t.Fatal("the mismatched object is kept")
	}
	l.wantUsage(ov)
}

// SEC-5: a body longer than its Content-Length is refused and nothing is
// stored, whatever its hash.
func TestBodyLongerThanItsLengthRefused(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	body := []byte("abcdef")
	if err := l.putDirect("data/x", bytes.NewReader(body), 3, hashMeta(body)); !isCode(err, "InvalidRequest") {
		t.Fatalf("a long body: %v", err)
	}
	if _, ok := l.stored("data/x"); ok {
		t.Fatal("stored")
	}
	l.wantUsage(ov)
}

// ignoringReads stores whatever a body gave, ignoring its read error: a
// backend that would publish an object lendisk's reader refused.
type ignoringReads struct{ *Store }

func (b ignoringReads) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	data, _ := io.ReadAll(in.Body)
	n := int64(len(data))
	in.Body, in.ContentLength = bytes.NewReader(data), &n
	return b.Store.PutObject(ctx, in)
}

// LEND-4: should the store publish a body lendisk refused at its end,
// the object is removed and the put refused.
func TestPublishedMismatchIsRemoved(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	if err := CreateBucket(ctx, be, "lent-x", "owner"); err != nil {
		t.Fatal(err)
	}
	if err := h.db.AddFriendship(ctx, state.Friendship{Peer: "peer", CreatedAt: time.Now(), OwnNumber: 1 << 30, PeerNumber: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	l := &Lending{Next: ignoringReads{be}, DB: h.db, Uploads: be, StoreDir: h.store, Log: quiet}
	body := []byte("bytes")
	n := int64(len(body))
	_, err = l.putLent(ctx, "peer", "sx", s3response.PutObjectInput{Bucket: aws.String("lent-x"), Key: aws.String("k"),
		ContentLength: &n, Body: bytes.NewReader(body), Metadata: hashMeta([]byte("other"))})
	if !isCode(err, "BadDigest") {
		t.Fatalf("put: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.store, "lent-x", "k")); !os.IsNotExist(err) {
		t.Fatal("the published mismatch is kept")
	}
}

// LEND-4, EP-11: a put to a key the borrower already stored with another
// hash is refused with 412 and the stored object is not touched; a put
// of the same bytes is accepted without rewriting it, and one that only
// claims the same hash is refused and leaves the stored copy.
func TestChangedKeyRefusedSameHashAccepted(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	orig := []byte("snapshot one")
	if err := l.put("snapshots/a", orig, hashMeta(orig)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(l.dir, "snapshots", "a")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := []byte("snapshot two")
	if err := l.put("snapshots/a", changed, hashMeta(changed)); code(err) != "PreconditionFailed" || status(err) != 412 {
		t.Fatalf("changed put: %v", err)
	}
	if err := l.put("snapshots/a", orig, hashMeta(orig)); err != nil {
		t.Fatalf("same bytes again: %v", err)
	}
	if err := l.put("snapshots/a", changed, hashMeta(orig)); code(err) != "BadDigest" {
		t.Fatalf("other bytes under the stored hash: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := l.stored("snapshots/a"); !bytes.Equal(data, orig) || !os.SameFile(before, after) ||
		before.Sys().(*syscall.Stat_t).Ino != after.Sys().(*syscall.Stat_t).Ino || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("the stored object changed: %q", data)
	}
	l.wantUsage(2*ov + int64(len(orig)))
	if err := l.mpu("snapshots/a", sum(changed), changed); code(err) != "PreconditionFailed" {
		t.Fatalf("multipart over a changed key: %v", err)
	}
}

// stalledReader signals when it is first read, then blocks until
// released, then gives its bytes.
type stalledReader struct {
	reading chan struct{}
	release chan struct{}
	once    sync.Once
	r       io.Reader
}

func newStalledReader(b []byte) *stalledReader {
	return &stalledReader{reading: make(chan struct{}), release: make(chan struct{}), r: bytes.NewReader(b)}
}

func (s *stalledReader) Read(p []byte) (int, error) {
	s.once.Do(func() { close(s.reading) })
	<-s.release
	return s.r.Read(p)
}

// LEND-4: a put whose body has stalled (a borrower killed mid-transfer)
// does not hold the key: the borrower's retry is stored, and the stalled
// put, once it ends, cannot replace it with other bytes.
func TestStalledPutDoesNotBlockTheKey(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	stale := []byte("stale")
	s := newStalledReader(stale)
	done := make(chan error, 1)
	go func() { done <- l.putDirect("data/x", s, int64(len(stale)), hashMeta(stale)) }()
	within(t, "the stalled put reading", s.reading)
	fresh := []byte("fresh")
	retried := make(chan error, 1)
	go func() { retried <- l.put("data/x", fresh, hashMeta(fresh)) }()
	if err := within(t, "the retry", retried); err != nil {
		t.Fatalf("retry: %v", err)
	}
	close(s.release)
	if err := within(t, "the stalled put", done); !preconditionFailed(err) {
		t.Fatalf("the stalled put ended with %v", err)
	}
	if data, _ := l.stored("data/x"); !bytes.Equal(data, fresh) {
		t.Fatalf("stored %q", data)
	}
	l.wantUsage(2*ov + int64(len(fresh)))
}

// LEND-1: a reconcile while a put's body is stalled does not wait for it
// and still corrects a counter that drifted.
func TestReconcileWhileAPutIsStalled(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	body := bytes.Repeat([]byte("z"), 300)
	if err := l.put("data/a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	s := newStalledReader([]byte("stalled"))
	done := make(chan error, 1)
	go func() { done <- l.putDirect("data/s", s, 7, hashMeta([]byte("stalled"))) }()
	t.Cleanup(func() { close(s.release); <-done })
	within(t, "the stalled put reading", s.reading)
	l.lending().count(l.peer(), 99_999) // drift
	reconciled := make(chan error, 1)
	go func() { reconciled <- l.lending().reconcile(ctx, 0) }()
	if err := within(t, "the reconcile", reconciled); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 300)
}

// SEC-5: a borrower's delete or multipart create naming a reserved or
// malformed key is refused, and what lendisk keeps under .lendisk/ is
// not touched.
func TestDeleteAndMultipartRefuseBadKeys(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	tomb := filepath.Join(l.dir, ".lendisk", "tomb", "x")
	if err := os.MkdirAll(filepath.Dir(tomb), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tomb, []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{".lendisk/tomb/x", "a/../b", ".lendisk"} {
		if _, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: l.bucket, Key: aws.String(k)}); status(err) != 400 {
			t.Errorf("delete %q: %v", k, err)
		}
	}
	if _, err := os.Stat(tomb); err != nil {
		t.Fatalf("the tomb copy: %v", err)
	}
	if _, err := l.create(".lendisk/tomb/y", hashMeta(nil)); status(err) != 400 {
		t.Fatalf("multipart create under .lendisk: %v", err)
	}
	if _, err := l.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: l.bucket, Key: aws.String(".lendisk/tomb/x")}); status(err) != 400 {
		t.Fatalf("head under .lendisk: %v", err)
	}
}

// LEND-4: a multipart object is verified by hashing the whole assembled
// object against the hash declared at create, whatever the part sizes;
// a mismatch is removed and refused. Parts count against quota while
// they are stored, and an aborted upload frees them.
func TestMultipartVerifiedAsAWhole(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	whole := append(append([]byte{}, p1...), p2...)
	if err := l.mpu("data/good", sum(whole), p1, p2); err != nil {
		t.Fatalf("multipart: %v", err)
	}
	if data, ok := l.stored("data/good"); !ok || !bytes.Equal(data, whole) {
		t.Fatal("the completed object is not stored")
	}
	l.wantUsage(2*ov + int64(len(whole)))
	if err := l.mpu("data/bad", sum([]byte("something else")), p1, p2); code(err) != "BadDigest" {
		t.Fatalf("mismatched multipart: %v", err)
	}
	if _, ok := l.stored("data/bad"); ok {
		t.Fatal("the mismatched multipart object is kept")
	}
	l.wantUsage(2*ov + int64(len(whole)))
	if err := l.mpu("data/good", sum(whole), p1, p2); err != nil {
		t.Fatalf("same multipart again: %v", err)
	}
	l.wantUsage(2*ov + int64(len(whole)))
	id, err := l.create("data/aborted", hashMeta(p2))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // a part sent again replaces the first; it is counted once
		if _, err := l.part("data/aborted", id, 1, p2); err != nil {
			t.Fatal(err)
		}
	}
	l.wantUsage(3*ov + int64(len(whole)+len(p2)))
	if _, err := l.c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/aborted"),
		UploadId: id}); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + int64(len(whole)))
}

// LEND-1, SEC-5: re-sends of one part, of different sizes and racing,
// are counted as the store holds them: the counter equals the bucket,
// the upload and the stored part, and the next put over quota is
// refused. The hook between A's part being published and its locks
// being taken offers B the interleaving the locks forbid (#66): B
// re-sends and publishes the part, B settles, then A settles, which
// would leave lent_part with A's size while the store holds B's. The
// locks taken at the body's end (onEOF) make B wait until A is counted.
func TestRacingPartResendsCountedAsStored(t *testing.T) {
	aStored, bDone := make(chan struct{}), make(chan error, 1)
	var once sync.Once
	l := lendTo(t, 3*ov+2500, func(h *harness) {
		h.lending = func(l *Lending) {
			l.afterPartStored = func(part int32, size int64) {
				if size != 1000 {
					return
				}
				once.Do(func() { close(aStored) })
				select { // B stores and settles here unless A holds the locks from its body's end
				case err := <-bDone:
					bDone <- err
				case <-time.After(500 * time.Millisecond):
				}
			}
		}
	})
	id, err := l.create("data/mp", hashMeta(nil))
	if err != nil {
		t.Fatal(err)
	}
	aDone := make(chan error, 1)
	go func() { _, err := l.part("data/mp", id, 1, bytes.Repeat([]byte("a"), 1000)); aDone <- err }()
	within(t, "A stored", aStored)
	go func() { _, err := l.part("data/mp", id, 1, bytes.Repeat([]byte("b"), 2000)); bDone <- err }()
	if err := within(t, "A", aDone); err != nil {
		t.Fatal(err)
	}
	if err := within(t, "B", bDone); err != nil {
		t.Fatal(err)
	}
	ps, err := l.lending().parts(context.Background(), l.store, "data/mp", *id)
	if err != nil || len(ps) != 1 {
		t.Fatalf("parts %v %v", ps, err)
	}
	l.wantUsage(2*ov + ps[1])
	if ps[1] != 2000 {
		t.Fatalf("the store holds a %d-byte part; B came last", ps[1])
	}
	over := bytes.Repeat([]byte("o"), 600)
	if err := l.put("data/over", over, hashMeta(over)); code(err) != "QuotaExceeded" {
		t.Fatalf("a put over quota after the re-sends: %v", err)
	}
}

// countingParts counts ListParts calls.
type countingParts struct {
	*Store
	n atomic.Int64
}

func (c *countingParts) ListParts(ctx context.Context, in *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	c.n.Add(1)
	return c.Store.ListParts(ctx, in)
}

// LEND-1: counting a part costs the same whatever the upload holds: no
// listing of its parts.
func TestCountingAPartListsNothing(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	if err := CreateBucket(ctx, be, "lent-x", "owner"); err != nil {
		t.Fatal(err)
	}
	if err := h.db.AddFriendship(ctx, state.Friendship{Peer: "peer", CreatedAt: time.Now(), OwnNumber: 1 << 30, PeerNumber: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	next := &countingParts{Store: be}
	l := &Lending{Next: next, DB: h.db, Uploads: be, StoreDir: h.store, Log: quiet}
	res, err := l.createUploadLent(ctx, "peer", "sx", s3response.CreateMultipartUploadInput{Bucket: aws.String("lent-x"),
		Key: aws.String("k"), Metadata: hashMeta(nil)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		n, body := int32(i+1), []byte("part")
		size := int64(len(body))
		if _, err := l.uploadPartLent(ctx, "peer", "sx", &s3.UploadPartInput{Bucket: aws.String("lent-x"), Key: aws.String("k"),
			UploadId: &res.UploadId, PartNumber: &n, ContentLength: &size, Body: bytes.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	if c := next.n.Load(); c != 0 {
		t.Fatalf("%d part listings for 5 parts", c)
	}
	if u, _ := h.db.GetLentUpload(ctx, res.UploadId); u.Counted != 20 {
		t.Fatalf("counted %d", u.Counted)
	}
}

// LEND-1: a reconcile's listing and the count it sets are one step for
// the borrower's writes: a put that would publish meanwhile waits, and
// is counted after, so the counter misses nothing.
func TestReconcileWaitsForPublications(t *testing.T) {
	var during atomic.Pointer[func()]
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) {
			l.afterList = func() {
				if f := during.Load(); f != nil {
					(*f)()
				}
			}
		}
	})
	body := bytes.Repeat([]byte("p"), 300)
	putDone := make(chan error, 1)
	f := func() {
		go func() { putDone <- l.put("data/p", body, hashMeta(body)) }()
		select { // the put publishes and counts here unless the reconcile holds it
		case err := <-putDone:
			putDone <- err
		case <-time.After(500 * time.Millisecond):
		}
	}
	during.Store(&f)
	if err := l.lending().reconcile(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	during.Store(nil)
	if err := within(t, "the put", putDone); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 300)
}

// LEND-1: an upload begun more than the abandon period ago that still
// gets parts keeps its row through the reconcile and goes on working:
// its parts are counted, and it completes.
func TestSlowUploadKeepsItsRow(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Add(-8 * 24 * time.Hour).Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	ctx := context.Background()
	p := []byte("slow part")
	id, err := l.create("data/slow", hashMeta(p))
	if err != nil {
		t.Fatal(err)
	}
	now.Store(time.Now().Unix()) // eight days on; the store saw a part just now
	cp, err := l.part("data/slow", id, 1, p)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.h.db.GetLentUpload(ctx, *id); err != nil {
		t.Fatalf("the slow upload's row: %v", err)
	}
	l.wantUsage(2*ov + int64(len(p)))
	if err := l.complete("data/slow", id, cp); err != nil {
		t.Fatalf("completing the slow upload: %v", err)
	}
	l.wantUsage(2*ov + int64(len(p)))
}

// LEND-1: the reconcile at start runs whenever the last one ran, so a
// count left wrong by a run that did not finish is set right.
func TestStartReconcilesWhateverTheLastRun(t *testing.T) {
	h := newHarness(t, nil)
	h.self = newIdentity(t)
	b := newIdentity(t)
	h.befriendWith(b, 1<<30)
	if err := h.db.SetLentUsage(context.Background(), b.NodeID().String(), 12345, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.serve(false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		n, _, _ := h.db.LentUsage(context.Background(), b.NodeID().String())
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("usage %d after start; want 0", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// LEND-1, SEC-5: a part that would take the borrower over quota is
// refused with the quota error.
func TestPartOverQuotaRefused(t *testing.T) {
	l := lendTo(t, 2*ov+100, nil)
	id, err := l.create("data/mp", hashMeta([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.part("data/mp", id, 1, bytes.Repeat([]byte("p"), 101)); code(err) != "QuotaExceeded" {
		t.Fatalf("part over quota: %v", err)
	}
}

// LEND-4: a completion is stored only if the key is still absent: an
// object published at the key while the upload is being checked wins,
// and the completion is refused as a changed key. A borrower's put now
// waits for the key's lock the completion holds (#74 review), so the
// object is published under the lock's back, straight into the store.
func TestCompletionRefusedWhenAPutLandsFirst(t *testing.T) {
	var putFirst func()
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.beforeComplete = func() { putFirst() } }
	})
	mp, other := []byte("from the upload"), []byte("from a put")
	putFirst = func() {
		be, err := OpenBackend(l.h.store)
		if err != nil {
			t.Error(err)
			return
		}
		n := int64(len(other))
		if _, err := be.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &l.store, Key: aws.String("data/k"),
			ContentLength: &n, Body: bytes.NewReader(other), Metadata: hashMeta(other)}); err != nil {
			t.Errorf("the put: %v", err)
		}
	}
	if err := l.mpu("data/k", sum(mp), mp); code(err) != "PreconditionFailed" {
		t.Fatalf("the completion: %v", err)
	}
	if data, _ := l.stored("data/k"); !bytes.Equal(data, other) {
		t.Fatalf("stored %q", data)
	}
}

// LEND-4: an upload completed in the store but never verified (a crash
// came between) is verified when its completion is retried: a mismatch
// is removed and refused.
func TestRetriedCompletionVerifiedAfterACrash(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	declared, sent := []byte("declared"), []byte("sent instead")
	id, err := l.create("data/k", hashMeta(declared))
	if err != nil {
		t.Fatal(err)
	}
	cp, err := l.part("data/k", id, 1, sent)
	if err != nil {
		t.Fatal(err)
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	in := &s3.CompleteMultipartUploadInput{Bucket: &l.store, Key: aws.String("data/k"), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{cp}}}
	if _, _, err := be.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := l.complete("data/k", id, cp); code(err) != "BadDigest" {
		t.Fatalf("the retried completion: %v", err)
	}
	if _, ok := l.stored("data/k"); ok {
		t.Fatal("the unverified object is kept")
	}
}

// SEC-5: the borrower's keys are plain relative paths.
func TestBorrowerKeysValidated(t *testing.T) {
	for _, k := range []string{"config", "data/ab/abcdef", "keys/1", "index/x.y", "p0123-abc_def", "kopia.repository",
		"xn0_1", ".hidden/x", "a/.b"} {
		if err := checkKey(k); err != nil {
			t.Errorf("%q refused: %v", k, err)
		}
	}
	for _, k := range []string{"", "/abs", "a/../b", "..", "../x", "a/./b", ".", "a//b", "a/", "a\x00b", "a\nb",
		".lendisk/tomb/x", ".lendisk", ".LENDISK/x", ".SgwTmp/x", ".sgwtmp/multipart/x", ".vgwlocks/x", string(bytes.Repeat([]byte("k"), 1025)), "\xff"} {
		if err := checkKey(k); err == nil {
			t.Errorf("%q accepted", k)
		}
	}
	l := lendTo(t, 1<<30, nil)
	body := []byte("forged tomb")
	if err := l.put(".lendisk/tomb/data/x/1", body, hashMeta(body)); status(err) != 400 {
		t.Fatalf("a put under .lendisk/: %v", err)
	}
	if _, err := os.Stat(filepath.Join(l.dir, ".lendisk")); !os.IsNotExist(err) {
		t.Fatal("the borrower wrote under .lendisk/")
	}
}

// LEND-1: the counter is reconciled against a listing of the borrower's
// buckets, objects and uploads in progress with their parts, and the
// reconcile time recorded; a counter that drifted is set right.
func TestCounterReconciledAgainstAListing(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	body := bytes.Repeat([]byte("z"), 300)
	if err := l.put("data/a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	id, err := l.create("data/mp", hashMeta(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.part("data/mp", id, 1, body[:50]); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 99_999, old); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	n, at, err := l.h.db.LentUsage(ctx, l.peer())
	if want := 3*ov + 350; err != nil || n != want || !at.After(old) {
		t.Fatalf("reconciled to %d at %v (%v); want %d now", n, at, err, want)
	}
}

// LEND-1: an upload left open longer than the abandon period is aborted
// by the reconcile, its row dropped and its bytes freed.
func TestAbandonedUploadsAborted(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	ctx := context.Background()
	id, err := l.create("data/mp", hashMeta(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.part("data/mp", id, 1, []byte("part")); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 4)
	now.Add(int64(8 * 24 * time.Hour / time.Second))
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(ov)
	if _, err := l.h.db.GetLentUpload(ctx, *id); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the upload's row: %v", err)
	}
	ups, err := l.lending().Next.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &l.store,
		KeyMarker: aws.String(""), UploadIdMarker: aws.String(""), MaxUploads: aws.Int32(100)})
	if err != nil || len(ups.Uploads) != 0 {
		t.Fatalf("uploads left: %+v %v", ups.Uploads, err)
	}
}

// LEND-1 (#66): an upload a crash left hidden mid-completion (posix's
// <upload ID>.<token>.inprogress directory, which no listing shows)
// still counts with its parts and keeps its row; once it has been still
// for the abandon period the reconcile removes it and frees its bytes.
func TestUploadLeftMidCompletionCountedThenRemoved(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	ctx := context.Background()
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	id, err := l.create("data/mp", hashMeta(append(append([]byte{}, p1...), p2...)))
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range [][]byte{p1, p2} {
		if _, err := l.part("data/mp", id, int32(i+1), p); err != nil {
			t.Fatal(err)
		}
	}
	dirs, err := filepath.Glob(filepath.Join(l.dir, ".sgwtmp", "multipart", "*", *id))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("the upload's directory: %v %v", dirs, err)
	}
	hidden := dirs[0] + ".0123456789abcdef0123456789abcdef-2" + inProgressSuffix
	if err := os.Rename(dirs[0], hidden); err != nil { // the crash, mid-completion
		t.Fatal(err)
	}
	want := 2*ov + int64(len(p1)+len(p2))
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 0, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(want)
	if u, err := l.h.db.GetLentUpload(ctx, *id); err != nil || u.Counted != int64(len(p1)+len(p2)) {
		t.Fatalf("the hidden upload's row: %+v %v", u, err)
	}
	now.Add(int64(8 * 24 * time.Hour / time.Second))
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(ov)
	if _, err := os.Stat(hidden); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the hidden upload's directory after the abandon period: %v", err)
	}
	if _, err := l.h.db.GetLentUpload(ctx, *id); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the hidden upload's row after the abandon period: %v", err)
	}
}

// LEND-1: the reconcile loop runs at start and then on schedule, each
// borrower once a day.
func TestReconcileLoopRunsAtStartAndThenDaily(t *testing.T) {
	var now atomic.Int64
	t0 := time.Now().Add(time.Hour).Truncate(time.Second)
	now.Store(t0.Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.reconcileEvery = 10 * time.Millisecond
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	reconciledAt := func(want time.Time) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, at, err := l.h.db.LentUsage(context.Background(), l.peer())
			if err == nil && at.Equal(want) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("reconciled at %v, want %v", at, want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	reconciledAt(t0)
	now.Store(t0.Add(time.Hour).Unix())
	time.Sleep(100 * time.Millisecond) // several runs: not due yet
	if _, at, _ := l.h.db.LentUsage(context.Background(), l.peer()); !at.Equal(t0) {
		t.Fatalf("reconciled again within the day, at %v", at)
	}
	now.Store(t0.Add(25 * time.Hour).Unix())
	reconciledAt(t0.Add(25 * time.Hour))
}

// EP-3 (#62): with the lending layer and its quota in place, the gateway
// still settles a completion a crash left pending at startup by the
// upload ID the store recorded.
func TestStartupSettlesAPendingCompletionWithQuotaInPlace(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	h.self = newIdentity(t)
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	_, in, body := twoPartUpload(t, be, "big")
	etag, err := computeETag(*in.UploadId, in.MultipartUpload.Parts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "big", Op: state.OpPut,
		Class: "data", ETag: etag, UploadID: *in.UploadId}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := be.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatal(err)
	}
	h.serve(false)
	h.settled()
	assertOneDoneRow(t, h, be, "big", body)
}

var _ backend.Backend = ignoringReads{}

// #89, LEND-2: a write that fits the quota and the floor, but not beside
// the borrower's writes still in flight, is answered 503 SlowDown, which
// the borrower retries, not the 507 that pauses it for quota: a send it
// abandoned may still hold its reservation for a moment. One that does
// not fit even alone is still 507, and the retry fits once the write in
// flight has ended.
func TestAWriteThatFitsButForWritesInFlightIsRetried(t *testing.T) {
	const total, floor = 1_000_000, 50_000 // the default floor is 5%
	var free atomic.Uint64
	free.Store(total)
	l := lendTo(t, 3*ov+100, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: total, Free: free.Load()}, nil }
	})
	// An abandoned send: admitted for 60 bytes, it ends short.
	s := newStalledReader([]byte("cut short"))
	done := make(chan error, 1)
	go func() { done <- l.putDirect("data/abandoned", s, 60, hashMeta(bytes.Repeat([]byte("a"), 60))) }()
	within(t, "the abandoned put reading", s.reading)

	second := bytes.Repeat([]byte("b"), 60)
	if err := l.put("data/two", second, hashMeta(second)); code(err) != "SlowDown" || status(err) != 503 {
		t.Fatalf("a put over the quota only beside a write in flight: %v (status %d)", err, status(err))
	}
	big := bytes.Repeat([]byte("c"), int(ov)+200)
	if err := l.put("data/big", big, hashMeta(big)); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("a put over the quota alone: %v (status %d)", err, status(err))
	}
	small := []byte("pack")
	free.Store(uint64(floor + ov + 4 + 30)) // room for it, not for the write in flight too
	if err := l.put("data/small", small, hashMeta(small)); code(err) != "SlowDown" || status(err) != 503 {
		t.Fatalf("a put below the floor only beside a write in flight: %v (status %d)", err, status(err))
	}
	free.Store(total)

	close(s.release)
	if err := within(t, "the abandoned put", done); err == nil {
		t.Fatal("the short body was stored")
	}
	if err := l.put("data/two", second, hashMeta(second)); err != nil {
		t.Fatalf("the retry once the write in flight ended: %v", err)
	}
	l.wantUsage(2*ov + 60)
}
