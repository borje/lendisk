package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// stallingGets wraps the store below the decorator without an Unwrap
// method. GetObject fails while fail is set; while stall is set, the
// body's first Read closes reading and waits for release.
type stallingGets struct {
	backend.Backend
	fail, stall      atomic.Bool
	reading, release chan struct{}
	once             sync.Once
}

func newStallingGets(be backend.Backend) *stallingGets {
	return &stallingGets{Backend: be, reading: make(chan struct{}), release: make(chan struct{})}
}

func (s *stallingGets) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if s.fail.Load() {
		return nil, s3err.GetAPIError(s3err.ErrInternalError)
	}
	out, err := s.Backend.GetObject(ctx, in)
	if err == nil && s.stall.Load() {
		out.Body = stallingBody{out.Body, s}
	}
	return out, err
}

type stallingBody struct {
	io.ReadCloser
	s *stallingGets
}

func (b stallingBody) Read(p []byte) (int, error) {
	b.s.once.Do(func() { close(b.s.reading) })
	<-b.s.release
	return b.ReadCloser.Read(p)
}

// logBuffer collects a logger's output for the test to read.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// strandedUpload completes an upload of "big" in bucket files through a
// decorator over a non-unwrapping wrapper of the store, whose hash fails,
// so its row is left pending and stranded with the object landed.
func strandedUpload(t *testing.T) (h *harness, d *Decorator, be *stallingGets, store *Store,
	in *s3.CompleteMultipartUploadInput, body []byte, logs *logBuffer) {
	t.Helper()
	h = newHarness(t, map[string]string{"files": "generic"})
	store, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be = newStallingGets(store)
	be.fail.Store(true)
	set, _ := profile.Load("")
	logs = &logBuffer{}
	d = &Decorator{Backend: be, Uploads: store, DB: h.db, Profiles: set,
		Log: slog.New(slog.NewTextHandler(logs, nil))}
	_, in, body = twoPartUpload(t, store, "big")
	if _, _, err := d.CompleteMultipartUpload(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	finishes(t, "the failing hash", d.hashes.wg.Wait)
	if p, _ := h.db.PendingCaptures(context.Background()); len(p) != 1 || len(d.stranded) != 1 {
		t.Fatalf("pending %+v, stranded %v; want the upload's row", p, d.stranded)
	}
	be.fail.Store(false)
	return h, d, be, store, in, body, logs
}

// EP-3, REPL-10 (#62): while ResolveStranded hashes a stranded upload,
// a client's retry of its completion is answered from the store without
// waiting for the key's lock, which the resolver holds for the hash.
func TestRetryWhileTheResolverHashesAStrandedUploadDoesNotWait(t *testing.T) {
	h, d, be, store, in, body, _ := strandedUpload(t)
	ctx := context.Background()
	be.stall.Store(true)
	resolved := make(chan struct{})
	go func() { defer close(resolved); d.ResolveStranded(ctx) }()
	within(t, "the resolver hashing", be.reading)

	waited := make(chan string, 4)
	d.keys.waiting = func(name string) { waited <- name }
	retry := make(chan error, 1)
	go func() { _, _, err := d.CompleteMultipartUpload(ctx, in); retry <- err }()
	select {
	case err := <-retry:
		if err != nil {
			t.Fatalf("retried completion: %v", err)
		}
	case name := <-waited:
		t.Fatalf("the retry waited for the key's lock %q while the resolver hashed", name)
	case <-resolved:
		t.Fatal("the resolver finished before its hash was released")
	case <-time.After(10 * time.Second):
		t.Fatal("the retried completion did not answer")
	}
	close(be.release)
	within(t, "the resolver", resolved)
	if len(d.stranded) != 0 || len(d.completing) != 0 {
		t.Fatalf("stranded %v, claims %v left", d.stranded, d.completing)
	}
	assertOneDoneRow(t, h, store, "big", body)
}

// EP-3 (#62): a retry that claims a stranded upload first keeps the
// resolver off it; the row stays stranded for the next round.
func TestResolverLeavesAnUploadARetryHasClaimed(t *testing.T) {
	h, d, _, store, in, body, _ := strandedUpload(t)
	ctx := context.Background()
	f, mine := d.claim(uploadClaim("files", "big", *in.UploadId))
	if !mine {
		t.Fatal("upload already claimed")
	}
	d.ResolveStranded(ctx)
	if len(d.stranded) != 1 {
		t.Fatalf("stranded %v; the resolver settled a claimed upload", d.stranded)
	}
	d.unclaim(uploadClaim("files", "big", *in.UploadId), f, nil)
	d.ResolveStranded(ctx)
	assertOneDoneRow(t, h, store, "big", body)
}

// EP-3 (#62): the upload ID is read from the store the decorator is
// given, not found by unwrapping its backend: a wrapper below the
// decorator with no Unwrap method breaks neither startup nor a retry.
func TestUploadIDReadPastAWrapperThatDoesNotUnwrap(t *testing.T) {
	h, d, _, store, in, body, _ := strandedUpload(t)
	ctx := context.Background()
	if _, ok := d.Backend.(interface{ Unwrap() backend.Backend }); ok {
		t.Fatal("the wrapper unwraps")
	}
	if _, _, err := d.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatalf("retried completion: %v", err)
	}
	d.stranded = nil // as after a restart
	if done, dropped, err := d.ResolvePending(ctx); err != nil || done != 1 || dropped != 0 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want the row done", done, dropped, err)
	}
	assertOneDoneRow(t, h, store, "big", body)
}

func objectPath(h *harness, key string) string { return filepath.Join(h.store, "files", key) }

// EP-3 (#62): an upload-ID attribute that cannot be read does not stop
// the start or drop the row: the row is logged, stays pending and
// stranded, and is settled once the attribute reads again.
func TestUnreadableUploadIDLeavesTheRowPendingAndReported(t *testing.T) {
	h, d, _, store, _, body, logs := strandedUpload(t)
	ctx := context.Background()
	path, attr := objectPath(h, "big"), "user."+mpMetaAttr
	good := make([]byte, 64<<10)
	n, err := syscall.Getxattr(path, attr, good)
	if err != nil {
		t.Fatal(err)
	}
	good = good[:n]
	if err := syscall.Setxattr(path, attr, []byte("not upload metadata"), 0); err != nil {
		t.Fatal(err)
	}
	d.stranded = nil // as after a restart
	done, dropped, err := d.ResolvePending(ctx)
	if err != nil || done != 0 || dropped != 0 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want the row left", done, dropped, err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 || !d.stranded[p[0].Seq] {
		t.Fatalf("pending %+v, stranded %v; want the row pending and stranded", p, d.stranded)
	}
	if !strings.Contains(logs.String(), "cannot be matched") {
		t.Fatalf("not reported; log:\n%s", logs)
	}
	// #20 second review: the mark is in lendisk.db, for status and the
	// alert, also after a restart.
	if u, err := h.db.UnmatchableRows(ctx); err != nil || u["files"].Objects != 1 || u["files"].Key != "big" {
		t.Fatalf("unmatchable rows %+v %v", u, err)
	}
	// Retried every round, but reported once per row while the daemon runs.
	d.ResolveStranded(ctx)
	d.ResolveStranded(ctx)
	if len(d.stranded) != 1 || strings.Count(logs.String(), "level=WARN") != 1 {
		t.Fatalf("stranded %v; want one warning, log:\n%s", d.stranded, logs)
	}
	if err := syscall.Setxattr(path, attr, good, 0); err != nil {
		t.Fatal(err)
	}
	d.ResolveStranded(ctx)
	assertOneDoneRow(t, h, store, "big", body)
	if u, err := h.db.UnmatchableRows(ctx); err != nil || len(u) != 0 {
		t.Fatalf("unmatchable rows after settling %+v %v", u, err)
	}
}

// EP-3 (#62): a decorator given no store to read upload IDs from
// reports a completion's row as unreadable rather than dropping it.
func TestNoUploadIDSourceIsReportedNotADrop(t *testing.T) {
	h, d, _, _, _, _, logs := strandedUpload(t)
	ctx := context.Background()
	d.Uploads = nil
	d.stranded = nil
	if done, dropped, err := d.ResolvePending(ctx); err != nil || done+dropped != 0 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want the row left", done, dropped, err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 || !strings.Contains(logs.String(), "records no multipart upload IDs") {
		t.Fatalf("pending %+v; log:\n%s", p, logs)
	}
}

// stripXattrs removes every user.* attribute from path, as copying a
// store without extended attributes does.
func stripXattrs(t *testing.T, path string) {
	t.Helper()
	buf := make([]byte, 64<<10)
	n, err := syscall.Listxattr(path, buf)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range strings.Split(string(buf[:n]), "\x00") {
		if strings.HasPrefix(name, "user.") {
			if err := syscall.Removexattr(path, name); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// assertLeftAndReported runs startup and checks that it left the
// upload's row pending and stranded (neither done nor dropped) and
// logged that the object has no upload ID.
func assertLeftAndReported(t *testing.T, h *harness, d *Decorator, logs *logBuffer) {
	t.Helper()
	ctx := context.Background()
	d.stranded = nil // as after a restart
	if done, dropped, err := d.ResolvePending(ctx); err != nil || done != 0 || dropped != 0 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want the row left", done, dropped, err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 || !d.stranded[p[0].Seq] {
		t.Fatalf("pending %+v, stranded %v; want the row pending and stranded", p, d.stranded)
	}
	if !strings.Contains(logs.String(), "cannot be matched") || !strings.Contains(logs.String(), "no upload ID") {
		t.Fatalf("not reported; log:\n%s", logs)
	}
}

// EP-3 (#62): a landed upload's object whose attributes are all gone
// (a store copied without extended attributes: no upload ID, and posix
// reports no ETag) is reported and its row kept pending, never dropped
// silently.
func TestObjectWithNoAttributesKeepsTheUploadsRowAndReportsIt(t *testing.T) {
	h, d, _, _, _, _, logs := strandedUpload(t)
	stripXattrs(t, objectPath(h, "big"))
	assertLeftAndReported(t, h, d, logs)
}

// EP-3 (#62): a multipart object that lost only its upload ID is not
// taken as the row's upload by its ETag, which an upload of the same
// parts would also give: the row is reported and kept pending.
func TestMissingUploadIDIsReportedNotSettledByETag(t *testing.T) {
	h, d, _, store, _, _, logs := strandedUpload(t)
	if err := syscall.Removexattr(objectPath(h, "big"), "user."+mpMetaAttr); err != nil {
		t.Fatal(err)
	}
	if id, err := store.UploadID("files", "big"); err != nil || id != "" {
		t.Fatalf("upload ID %q (%v) after removing it", id, err)
	}
	assertLeftAndReported(t, h, d, logs)
}

// EP-3 (#62): a retried completion of an upload that never landed, with
// the same parts as the landed object that lost its upload ID, is not
// answered from the store by the matching ETag: posix would assemble it
// with no row describing it.
func TestRetryOfAnUnlandedUploadIsNotAnsweredByAMatchingETag(t *testing.T) {
	h, d, _, store, _, _, _ := strandedUpload(t)
	ctx := context.Background()
	d.ResolveStranded(ctx)                         // the landed upload's row is done
	_, second, _ := twoPartUpload(t, store, "big") // the same part as the landed upload
	etag, err := backend.ComputeMultipartETagFromPartETags(*second.UploadId, second.MultipartUpload.Parts)
	if err != nil {
		t.Fatal(err)
	}
	if es := h.entries("files"); len(es) != 1 || strings.Trim(es[0].ETag, `"`) != strings.Trim(etag, `"`) {
		t.Fatalf("queue %+v; want one row with the second upload's ETag %s", es, etag)
	}
	seqs, err := h.db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "big", Op: state.OpPut,
		Class: "data", ETag: etag, UploadID: *second.UploadId}})
	if err != nil {
		t.Fatal(err)
	}
	d.strand(seqs[0]) // its first completion never reached the store
	if err := syscall.Removexattr(objectPath(h, "big"), "user."+mpMetaAttr); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.CompleteMultipartUpload(ctx, second); err == nil {
		t.Error("the retry was answered")
	}
	if id, _ := store.UploadID("files", "big"); id == *second.UploadId {
		t.Fatal("the store assembled the retried upload without a row")
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("pending %+v; want the retried upload's row kept", p)
	}
}

// EP-3 (#62): a stranded upload's key overwritten since by a PutObject
// (no upload ID, a plain ETag) did not take the upload: the row is
// removed.
func TestStrandedUploadOverwrittenByAPutIsDropped(t *testing.T) {
	h, d, _, store, _, _, _ := strandedUpload(t)
	ctx := context.Background()
	data := []byte("replaced")
	n := int64(len(data))
	if _, err := store.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String("files"),
		Key: aws.String("big"), ContentLength: &n, Body: bytes.NewReader(data)}); err != nil {
		t.Fatal(err)
	}
	d.stranded = nil
	if done, dropped, err := d.ResolvePending(ctx); err != nil || done != 0 || dropped != 1 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want the row dropped", done, dropped, err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 0 {
		t.Fatalf("pending %+v", p)
	}
}

// EP-3 (#62): hashing a stored object stops when its context ends,
// mid-read, and a stranded row whose hash was cancelled stays stranded.
func TestHashingAStoredObjectStopsWhenItsContextEnds(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	store, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := newStallingGets(store)
	be.stall.Store(true)
	d := &Decorator{Backend: be, Uploads: store, DB: h.db, Log: quiet}
	data := bytes.Repeat([]byte("x"), 1<<20) // more than one read
	n := int64(len(data))
	if _, err := store.PutObject(context.Background(), s3response.PutObjectInput{Bucket: aws.String("files"),
		Key: aws.String("k"), ContentLength: &n, Body: bytes.NewReader(data)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	hashed := make(chan error, 1)
	go func() { _, _, err := d.hashStored(ctx, "files", "k"); hashed <- err }()
	within(t, "the hash reading", be.reading)
	cancel()
	close(be.release)
	if err := within(t, "the hash", hashed); !errors.Is(err, context.Canceled) {
		t.Fatalf("hash ended with %v, want context.Canceled", err)
	}

	// The same, for a stranded row: it stays pending and stranded, and
	// a stop is not reported as a failure.
	logs := &logBuffer{}
	d.Log = slog.New(slog.NewTextHandler(logs, nil))
	be = newStallingGets(store)
	be.stall.Store(true)
	d.Backend = be
	seqs, err := h.db.BeginCaptures(context.Background(), []state.Capture{{Bucket: "files", Key: "k",
		Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	d.strand(seqs[0])
	ctx, cancel = context.WithCancel(context.Background())
	resolved := make(chan struct{})
	go func() { defer close(resolved); d.ResolveStranded(ctx) }()
	within(t, "the resolver reading", be.reading)
	cancel()
	close(be.release)
	within(t, "the resolver", resolved)
	if p, _ := h.db.PendingCaptures(context.Background()); len(p) != 1 || !d.stranded[seqs[0]] {
		t.Fatalf("pending %+v, stranded %v; want the row left", p, d.stranded)
	}
	if logs.String() != "" {
		t.Fatalf("a stop was reported:\n%s", logs)
	}
}
