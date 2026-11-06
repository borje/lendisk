package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

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
}

// lendTo serves a lender to one borrower with the given quota, the
// borrower's bucket created.
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
	l := &lent{h: h, id: b, c: h.pairClient(b), bucket: aws.String(b.SealedName("laptop"))}
	l.dir = filepath.Join(h.store, LentName(b.NodeID(), *l.bucket))
	if _, err := l.c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: l.bucket}); err != nil {
		t.Fatal(err)
	}
	return l
}

func (l *lent) put(key string, body []byte, meta map[string]string) error {
	_, err := l.c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: l.bucket, Key: aws.String(key),
		Body: bytes.NewReader(body), Metadata: meta})
	return err
}

func (l *lent) usage() int64 {
	l.h.t.Helper()
	n, _, err := l.h.db.LentUsage(context.Background(), l.id.NodeID().String())
	if err != nil {
		l.h.t.Fatal(err)
	}
	return n
}

func (l *lent) stored(key string) ([]byte, bool) {
	b, err := os.ReadFile(filepath.Join(l.dir, key))
	return b, err == nil
}

// mpu uploads parts of body under key with the declared hash and
// returns the completion's error.
func (l *lent) mpu(key string, declared string, parts ...[]byte) error {
	ctx := context.Background()
	up, err := l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String(key),
		Metadata: map[string]string{"lendisk-sha256": declared}})
	if err != nil {
		return err
	}
	var done []types.CompletedPart
	for i, p := range parts {
		n := int32(i + 1)
		out, err := l.c.UploadPart(ctx, &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String(key), UploadId: up.UploadId,
			PartNumber: &n, Body: bytes.NewReader(p)})
		if err != nil {
			return err
		}
		done = append(done, types.CompletedPart{PartNumber: &n, ETag: out.ETag})
	}
	_, err = l.c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: l.bucket, Key: aws.String(key),
		UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: done}})
	return err
}

// LEND-1, LEND-2, Q-I.5: once a borrower's objects fill the friendship's
// quota, its next put, multipart create and new bucket are refused with
// lendisk's own 507 QuotaExceeded (unwrapped, so not a 500), nothing is
// stored, and the counter holds what was stored. A retried create of a
// bucket it has is not refused.
func TestOverQuotaRefusedWith507(t *testing.T) {
	l := lendTo(t, 100, nil)
	ctx := context.Background()
	first := bytes.Repeat([]byte("a"), 60)
	if err := l.put("data/one", first, hashMeta(first)); err != nil {
		t.Fatal(err)
	}
	if n := l.usage(); n != 60 {
		t.Fatalf("counter %d after a 60-byte put", n)
	}
	second := bytes.Repeat([]byte("b"), 60)
	err := l.put("data/two", second, hashMeta(second))
	if code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("put over quota: %v (status %d)", err, status(err))
	}
	if _, ok := l.stored("data/two"); ok {
		t.Fatal("the refused object is stored")
	}
	// What still fits is accepted.
	small := bytes.Repeat([]byte("c"), 40)
	if err := l.put("data/three", small, hashMeta(small)); err != nil {
		t.Fatalf("a put that fits: %v", err)
	}
	if n := l.usage(); n != 100 {
		t.Fatalf("counter %d, want 100", n)
	}
	_, err = l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/four"),
		Metadata: hashMeta(small)})
	if code(err) != "QuotaExceeded" || status(err) != 507 {
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
	// A delete frees its bytes.
	if _, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: l.bucket, Key: aws.String("data/one")}); err != nil {
		t.Fatal(err)
	}
	if n := l.usage(); n != 40 {
		t.Fatalf("counter %d after deleting 60 bytes, want 40", n)
	}
	if err := l.put("data/two", second, hashMeta(second)); err != nil {
		t.Fatalf("put after a delete: %v", err)
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

// LEND-2, SEC-5: below the free-space floor (default 5% of the
// filesystem) a put, a multipart create and a new bucket are refused
// with the quota error, whatever the quota; a put that would take free
// space below the floor is refused too.
func TestBelowTheFloorRefusedWithTheQuotaError(t *testing.T) {
	var free atomic.Uint64
	free.Store(1000)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: 1000, Free: free.Load()}, nil }
	})
	ctx := context.Background()
	body := []byte("pack")
	if err := l.put("data/fits", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	free.Store(60) // the default floor is 50 of 1000
	if err := l.put("data/small", body, hashMeta(body)); err != nil {
		t.Fatalf("a put that stays above the floor: %v", err)
	}
	big := bytes.Repeat([]byte("x"), 20)
	if err := l.put("data/big", big, hashMeta(big)); code(err) != "QuotaExceeded" || status(err) != 507 {
		t.Fatalf("a put that would cross the floor: %v", err)
	}
	free.Store(40)
	if err := l.put("data/more", body, hashMeta(body)); code(err) != "QuotaExceeded" {
		t.Fatalf("put below the floor: %v", err)
	}
	if _, err := l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/mp"),
		Metadata: hashMeta(body)}); code(err) != "QuotaExceeded" {
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
	free.Store(1000)
	if err := l.put("data/more", body, hashMeta(body)); err != nil {
		t.Fatalf("put once space is back: %v", err)
	}
}

// LEND-2: a configured floor in bytes replaces the default.
func TestConfiguredFloorApplies(t *testing.T) {
	var free atomic.Uint64
	free.Store(1000)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.statfs = func(string) (store.Space, error) { return store.Space{Total: 1000, Free: free.Load()}, nil }
		h.floor = &config.Floor{Bytes: 600}
	})
	free.Store(500)
	body := []byte("pack")
	if err := l.put("data/x", body, hashMeta(body)); code(err) != "QuotaExceeded" {
		t.Fatalf("put below a 600-byte floor: %v", err)
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
	if _, err := l.c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: l.bucket,
		Key: aws.String("data/mp")}); status(err) != 400 {
		t.Fatalf("multipart create without the hash: %v", err)
	}
	if n := l.usage(); n != 0 {
		t.Fatalf("counter %d", n)
	}
}

// LEND-4: a body whose SHA-256 is not the one declared is removed and
// refused with BadDigest; the counter does not move.
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
	if n := l.usage(); n != 0 {
		t.Fatalf("counter %d", n)
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
	if n := l.usage(); n != int64(len(orig)) {
		t.Fatalf("counter %d", n)
	}
	// A multipart upload is held to the same rules.
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
	peer := l.id.NodeID().String()
	bucket := LentName(l.id.NodeID(), *l.bucket)
	stale := []byte("stale")
	s := &stalledReader{reading: make(chan struct{}), release: make(chan struct{}), r: bytes.NewReader(stale)}
	n := int64(len(stale))
	done := make(chan error, 1)
	go func() {
		_, err := l.h.gw.lending.putLent(context.Background(), peer, s3response.PutObjectInput{Bucket: &bucket,
			Key: aws.String("data/x"), ContentLength: &n, Body: s, Metadata: hashMeta(stale)})
		done <- err
	}()
	within(t, "the stalled put reading", s.reading)
	fresh := []byte("fresh")
	retried := make(chan error, 1)
	go func() { retried <- l.put("data/x", fresh, hashMeta(fresh)) }()
	if err := within(t, "the retry", retried); err != nil {
		t.Fatalf("retry: %v", err)
	}
	close(s.release)
	if err := within(t, "the stalled put", done); !isCode(err, "PreconditionFailed") {
		t.Fatalf("the stalled put ended with %v", err)
	}
	if data, _ := l.stored("data/x"); !bytes.Equal(data, fresh) {
		t.Fatalf("stored %q", data)
	}
	if u := l.usage(); u != int64(len(fresh)) {
		t.Fatalf("counter %d", u)
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
	if n := l.usage(); n != int64(len(whole)) {
		t.Fatalf("counter %d, want %d", n, len(whole))
	}
	if err := l.mpu("data/bad", sum([]byte("something else")), p1, p2); code(err) != "BadDigest" {
		t.Fatalf("mismatched multipart: %v", err)
	}
	if _, ok := l.stored("data/bad"); ok {
		t.Fatal("the mismatched multipart object is kept")
	}
	if n := l.usage(); n != int64(len(whole)) {
		t.Fatalf("counter %d after a refused completion", n)
	}
	// The same object again is accepted and changes nothing.
	if err := l.mpu("data/good", sum(whole), p1, p2); err != nil {
		t.Fatalf("same multipart again: %v", err)
	}
	if n := l.usage(); n != int64(len(whole)) {
		t.Fatalf("counter %d after a repeated upload", n)
	}
	up, err := l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/aborted"),
		Metadata: hashMeta(p2)})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	if _, err := l.c.UploadPart(ctx, &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String("data/aborted"), UploadId: up.UploadId,
		PartNumber: &one, Body: bytes.NewReader(p2)}); err != nil {
		t.Fatal(err)
	}
	// A part sent again replaces the first; it is counted once.
	if _, err := l.c.UploadPart(ctx, &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String("data/aborted"), UploadId: up.UploadId,
		PartNumber: &one, Body: bytes.NewReader(p2)}); err != nil {
		t.Fatal(err)
	}
	if n := l.usage(); n != int64(len(whole)+len(p2)) {
		t.Fatalf("counter %d with a part stored", n)
	}
	if _, err := l.c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/aborted"),
		UploadId: up.UploadId}); err != nil {
		t.Fatal(err)
	}
	if n := l.usage(); n != int64(len(whole)) {
		t.Fatalf("counter %d after the abort", n)
	}
}

// LEND-1, SEC-5: a part that would take the borrower over quota is
// refused with the quota error.
func TestPartOverQuotaRefused(t *testing.T) {
	l := lendTo(t, 100, nil)
	ctx := context.Background()
	up, err := l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/mp"),
		Metadata: hashMeta([]byte("x"))})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	if _, err := l.c.UploadPart(ctx, &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String("data/mp"), UploadId: up.UploadId,
		PartNumber: &one, Body: bytes.NewReader(bytes.Repeat([]byte("p"), 101))}); code(err) != "QuotaExceeded" {
		t.Fatalf("part over quota: %v", err)
	}
}

// SEC-5: a borrower's key is a plain relative path: no dot segments, no
// empty segments, no leading or trailing slash, no control characters,
// and nothing under lendisk's or versitygw's own directories.
func TestBorrowerKeysValidated(t *testing.T) {
	for _, k := range []string{"config", "data/ab/abcdef", "keys/1", "index/x.y", "p0123-abc_def", "kopia.repository",
		"xn0_1", ".hidden/x", "a/.b"} {
		if err := checkKey(k); err != nil {
			t.Errorf("%q refused: %v", k, err)
		}
	}
	for _, k := range []string{"", "/abs", "a/../b", "..", "../x", "a/./b", ".", "a//b", "a/", "a\x00b", "a\nb",
		".lendisk/tomb/x", ".lendisk", ".sgwtmp/multipart/x", ".vgwlocks/x", string(bytes.Repeat([]byte("k"), 1025)), "\xff"} {
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
// buckets, objects and parts of uploads in progress, and the reconcile
// time recorded; a counter that drifted is set right.
func TestCounterReconciledAgainstAListing(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	body := bytes.Repeat([]byte("z"), 300)
	if err := l.put("data/a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	up, err := l.c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: l.bucket, Key: aws.String("data/mp"),
		Metadata: hashMeta(body)})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	if _, err := l.c.UploadPart(ctx, &s3.UploadPartInput{Bucket: l.bucket, Key: aws.String("data/mp"), UploadId: up.UploadId,
		PartNumber: &one, Body: bytes.NewReader(body[:50])}); err != nil {
		t.Fatal(err)
	}
	peer := l.id.NodeID().String()
	old := time.Now().Add(-48 * time.Hour)
	if err := l.h.db.SetLentUsage(ctx, peer, 99_999, old); err != nil {
		t.Fatal(err)
	}
	if err := l.h.gw.lending.reconcile(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	n, at, err := l.h.db.LentUsage(ctx, peer)
	if err != nil || n != 350 || !at.After(old) {
		t.Fatalf("reconciled to %d at %v (%v); want 350 now", n, at, err)
	}
	// Not due again for a day.
	if err := l.h.db.AddLentUsage(ctx, peer, 5); err != nil {
		t.Fatal(err)
	}
	if err := l.h.gw.lending.reconcile(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := l.h.db.LentUsage(ctx, peer); n != 355 {
		t.Fatalf("reconciled again within a day: %d", n)
	}
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
