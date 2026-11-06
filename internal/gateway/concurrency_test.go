package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// EP-3, REPL-3 (#47): parallel uploads and deletes finishing while the
// replication worker acknowledges placements never fail a request that
// the store accepted, and leave no row pending.
func TestConcurrentCapturesAndAcksSeeNoErrors(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	ctx := context.Background()
	if err := h.db.AddFriendship(ctx, state.Friendship{Peer: "friend", Address: "tcfriend", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	h.serve(false)
	c := h.client("laptop")
	b := aws.String("laptop")

	stop := make(chan struct{})
	acked := make(chan int)
	go func() { // the worker's side: acknowledge each queued row
		n := 0
		defer func() { acked <- n }()
		for {
			select {
			case <-stop:
				return
			default:
			}
			p, ok, err := h.db.NextPlacement(ctx, "friend")
			if err != nil {
				t.Errorf("NextPlacement: %v", err)
				return
			}
			if !ok {
				time.Sleep(time.Millisecond)
				continue
			}
			if err := h.db.AckPlacement(ctx, "friend", p.Seq, p.SHA256, time.Now()); err != nil {
				t.Errorf("AckPlacement: %v", err)
				return
			}
			n++
		}
	}()

	const workers, ops = 8, 12
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ops {
				key := aws.String(fmt.Sprintf("data/%d/%d", w, i))
				if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: key,
					Body: bytes.NewReader([]byte(*key))}); err != nil {
					t.Errorf("put %s: %v", *key, err)
					continue
				}
				if i%3 == 0 {
					if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: key}); err != nil {
						t.Errorf("delete %s: %v", *key, err)
					}
				}
				if i%4 == 0 {
					if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{
						Objects: []types.ObjectIdentifier{{Key: key}}}}); err != nil {
						t.Errorf("multi-delete %s: %v", *key, err)
					}
				}
			}
		}()
	}
	finishes(t, "the requests", wg.Wait)
	close(stop)
	if n := within(t, "the acknowledging worker", acked); n == 0 {
		t.Error("no placement was acknowledged while the requests ran")
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 0 {
		t.Fatalf("%d rows left pending", len(p))
	}
}

// EP-3, REPL-1 (#48): overlapping writes to one key are applied in the
// order of their queue rows, so the key's last done row always describes
// the object on disk; multi-deletes naming the same keys in opposite
// orders do not deadlock with them.
func TestOverlappingWritesToOneKeyKeepTheQueueInStep(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	h.serve(false)
	c := h.client("files")
	ctx := context.Background()
	b := aws.String("files")
	keys := []string{"a", "b"}
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 8 {
				key := keys[(w+i)%2]
				// Different lengths too, so a mismatch shows in the size.
				body := bytes.Repeat([]byte{byte('a' + w)}, 1+w*7+i)
				if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: &key, Body: bytes.NewReader(body)}); err != nil {
					t.Errorf("put %s: %v", key, err)
				}
				if i%3 == 2 {
					ids := []types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("b")}}
					if w%2 == 1 {
						ids[0], ids[1] = ids[1], ids[0]
					}
					if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b,
						Delete: &types.Delete{Objects: ids}}); err != nil {
						t.Errorf("multi-delete: %v", err)
					}
				}
			}
		}()
	}
	finishes(t, "the requests", wg.Wait)
	last := map[string]state.Entry{}
	for _, e := range h.entries("files") {
		if e.State != state.StateDone {
			t.Errorf("row %d left %s", e.Seq, e.State)
		}
		last[e.Key] = e
	}
	for _, key := range keys {
		e := last[key]
		got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String(key)})
		if code(err) == "NoSuchKey" {
			if e.Op != state.OpDelete {
				t.Errorf("%s: gone from the store, last row %+v", key, e)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(got.Body)
		got.Body.Close()
		if e.Op != state.OpPut || e.SHA256 != sum(body) || e.Size != int64(len(body)) {
			t.Errorf("%s: store holds %d bytes %s, last row %+v", key, len(body), sum(body), e)
		}
	}
}

// EP-3, REPL-1 (#48): the issue's interleaving, forced. X is stored as
// row 1. A put of Y is stored and stalls before its row is completed; a
// put of X arriving meanwhile must not be stored, queued or dropped as a
// duplicate of row 1 until Y's row is done, so the key's last done row
// is X, as on disk.
func TestSameKeyWriteWaitsForTheOneInFlight(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	stall, stalled := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	put := func(body string) error {
		n := int64(len(body))
		_, err := d.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String("files"), Key: aws.String("k"),
			ContentLength: &n, Body: bytes.NewReader([]byte(body))})
		return err
	}
	if err := put("X"); err != nil {
		t.Fatal(err)
	}
	d.AfterStore = func() {
		if !first.Swap(true) {
			close(stalled)
			<-stall
		}
	}
	errY := make(chan error, 1)
	go func() { errY <- put("YY") }()
	within(t, "the first put reaching the store", stalled)
	errX := make(chan error, 1)
	go func() { errX <- put("X") }()
	select {
	case err := <-errX:
		t.Errorf("the second put finished while the first was in flight (err %v)", err)
		errX <- err
	case <-time.After(300 * time.Millisecond):
	}
	close(stall)
	if err := within(t, "the first put", errY); err != nil {
		t.Fatal(err)
	}
	if err := within(t, "the second put", errX); err != nil {
		t.Fatal(err)
	}
	es := h.entries("files")
	last := es[len(es)-1]
	got, err := be.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("files"), Key: aws.String("k")})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if last.SHA256 != sum(body) {
		t.Fatalf("store holds %q; last row %+v of %d", body, last, len(es))
	}
	if len(es) != 3 {
		t.Errorf("%d rows, want X, Y, X", len(es))
	}
}

// EP-3, REPL-10 (#49): a completion answers once the store has assembled
// the object, and the whole-object hash follows in the background. A
// second completion of the same upload, sent while the first is in the
// store and again while its hash runs, succeeds without hashing again,
// and the upload ends as exactly one done row.
func TestRetriedCompletionWhileTheFirstIsInFlight(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	b, key := aws.String("files"), aws.String("big")
	up, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: b, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	body := "the whole object"
	n := int64(len(body))
	part, err := be.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: key, UploadId: &up.UploadId,
		PartNumber: &one, ContentLength: &n, Body: bytes.NewReader([]byte(body))})
	if err != nil {
		t.Fatal(err)
	}
	in := &s3.CompleteMultipartUploadInput{Bucket: b, Key: key, UploadId: &up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: &one, ETag: part.ETag}}}}
	complete := func() chan error {
		c := make(chan error, 1)
		go func() { _, _, err := d.CompleteMultipartUpload(ctx, in); c <- err }()
		return c
	}
	answered := func(what string, c chan error) {
		t.Helper()
		select {
		case err := <-c:
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: no answer", what)
		}
	}

	stored, release := make(chan struct{}), make(chan struct{})
	d.AfterStore = func() { close(stored); <-release }
	var hashes atomic.Int32
	hashing, finish := make(chan struct{}), make(chan struct{})
	d.beforeHash = func() {
		if hashes.Add(1) == 1 {
			close(hashing)
			<-finish
		}
	}
	first := complete()
	within(t, "the first completion reaching the store", stored)
	answered("completion sent while the first is in the store", complete())
	close(release)
	answered("first completion, before its hash", first)
	within(t, "the first completion's hash", hashing)
	answered("completion sent while the first's hash runs", complete())
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("%d rows pending while hashing, want 1", len(p))
	}
	close(finish)
	finishes(t, "the background hashes", d.hashes.stop)
	es := h.entries("files")
	if len(es) != 1 || es[0].State != state.StateDone || es[0].SHA256 != sum([]byte(body)) || es[0].Size != n {
		t.Fatalf("queue: %+v", es)
	}
	if hashes.Load() != 1 {
		t.Errorf("hashed %d times", hashes.Load())
	}
}

// EP-3 (#49): a completion whose row another settled first (FinishPut
// answers ErrNotFound) has succeeded; it is not an error.
func TestFinishingAnUploadSettledElsewhereIsSuccess(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Log: quiet}
	n := int64(1)
	if _, err := be.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String("files"), Key: aws.String("k"),
		ContentLength: &n, Body: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal(err)
	}
	seqs, err := h.db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "k", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.FinishPut(ctx, seqs[0], 1, sum([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := d.finishUpload(ctx, seqs[0], "files", "k", ""); err != nil {
		t.Fatalf("finishing a row already done: %v", err)
	}
}

// EP-3 (#47): a row whose request ended without completing it (the state
// database refused the write) is settled against the store while the
// gateway runs, not left pending until the next start.
func TestStrandedRowsSettledWhileRunning(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	c := h.client("laptop")
	ctx := context.Background()
	b := aws.String("laptop")
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("data/gone"),
		Body: bytes.NewReader([]byte("g"))}); err != nil {
		t.Fatal(err)
	}
	// Completing a row fails while the trigger stands.
	if _, err := h.db.ExecContext(ctx, `CREATE TRIGGER refuse BEFORE UPDATE ON queue
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("data/kept"),
		Body: bytes.NewReader([]byte("kept"))}); code(err) != "InternalError" {
		t.Fatalf("put with completion refused: %v", err)
	}
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: aws.String("data/gone")}); code(err) != "InternalError" {
		t.Fatalf("delete with completion refused: %v", err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 2 {
		t.Fatalf("%d rows pending, want the two stranded", len(p))
	}
	if _, err := h.db.ExecContext(ctx, `DROP TRIGGER refuse`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for p, _ := h.db.PendingCaptures(ctx); len(p) != 0; p, _ = h.db.PendingCaptures(ctx) {
		if time.Now().After(deadline) {
			t.Fatalf("still pending: %+v", p)
		}
		time.Sleep(10 * time.Millisecond)
	}
	es := h.entries("laptop")
	if len(es) != 3 || es[1].Key != "data/kept" || es[1].SHA256 != sum([]byte("kept")) ||
		es[2].Key != "data/gone" || es[2].Op != state.OpDelete || es[2].State != state.StateDone {
		t.Fatalf("queue: %+v", es)
	}
}

// failingFirstCompletion fails the first CompleteMultipartUpload after
// release is closed, without touching the store, as posix answers after
// rolling the upload back; later ones reach the store.
type failingFirstCompletion struct {
	backend.Backend
	calls            atomic.Int32
	entered, release chan struct{}
}

func (f *failingFirstCompletion) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if f.calls.Add(1) == 1 {
		close(f.entered)
		<-f.release
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrInternalError)
	}
	return f.Backend.CompleteMultipartUpload(ctx, in)
}

// twoPartUpload starts an upload of key in bucket files with two small
// parts and returns its completion inputs: the first lists both parts,
// the retry only part 2, so the two give different ETags. The object the
// retry assembles holds part 2's bytes.
func twoPartUpload(t *testing.T, be backend.Backend, key string) (first, retry *s3.CompleteMultipartUploadInput, body []byte) {
	t.Helper()
	ctx := context.Background()
	b := aws.String("files")
	up, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: b, Key: &key})
	if err != nil {
		t.Fatal(err)
	}
	var parts []types.CompletedPart
	for i, data := range []string{"part one", "the second part"} {
		pn, n := int32(i+1), int64(len(data))
		p, err := be.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: &key, UploadId: &up.UploadId,
			PartNumber: &pn, ContentLength: &n, Body: bytes.NewReader([]byte(data))})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: &pn, ETag: p.ETag})
	}
	in := func(ps ...types.CompletedPart) *s3.CompleteMultipartUploadInput {
		return &s3.CompleteMultipartUploadInput{Bucket: b, Key: &key, UploadId: &up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: ps}}
	}
	return in(parts...), in(parts[1]), []byte("the second part")
}

// within waits for c or fails the test.
func within[T any](t *testing.T, what string, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out", what)
		panic("unreachable")
	}
}

// finishes runs f, which waits for something, and fails the test if it
// has not returned within a minute.
func finishes(t *testing.T, what string, f func()) {
	t.Helper()
	c := make(chan struct{})
	go func() { defer close(c); f() }()
	select {
	case <-c:
	case <-time.After(time.Minute):
		t.Fatalf("%s: timed out", what)
	}
}

// assertOneDoneRow checks that key's object in the store is described by
// exactly one done row with its hash, size and ETag.
func assertOneDoneRow(t *testing.T, h *harness, be backend.Backend, key string, body []byte) {
	t.Helper()
	head, err := be.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("files"), Key: &key})
	if err != nil {
		t.Fatalf("the object is not in the store: %v", err)
	}
	es := h.entries("files")
	if len(es) != 1 || es[0].State != state.StateDone || es[0].SHA256 != sum(body) || es[0].Size != int64(len(body)) ||
		strings.Trim(es[0].ETag, `"`) != strings.Trim(deref(head.ETag), `"`) {
		t.Fatalf("store holds %q (ETag %s); queue: %+v", body, deref(head.ETag), es)
	}
	if p, _ := h.db.PendingCaptures(context.Background()); len(p) != 0 {
		t.Fatalf("rows left pending: %+v", p)
	}
}

// EP-3, REPL-1 (#55): a retried completion that saw the first's pending
// row, whose first then fails (posix rolls the upload back and the row
// is dropped), assembles the object with its own row: the store never
// holds an object the queue does not describe. The retry lists other
// parts, so its ETag differs from the first's.
func TestRetryAfterTheFirstCompletionFailsIsCaptured(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &failingFirstCompletion{Backend: posix, entered: make(chan struct{}), release: make(chan struct{})}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	inFirst, inRetry, body := twoPartUpload(t, posix, "big")
	seen := make(chan struct{})
	var once sync.Once
	d.seenUpload = func(st string) {
		if st == state.StatePending {
			once.Do(func() { close(seen) })
		}
	}
	complete := func(in *s3.CompleteMultipartUploadInput) chan error {
		c := make(chan error, 1)
		go func() { _, _, err := d.CompleteMultipartUpload(ctx, in); c <- err }()
		return c
	}
	first := complete(inFirst)
	within(t, "first completion reaching the store", be.entered)
	retry := complete(inRetry)
	within(t, "retry seeing the pending row", seen)
	close(be.release)
	if err := within(t, "first completion", first); err == nil {
		t.Fatal("the first completion succeeded")
	}
	if err := within(t, "retried completion", retry); err != nil {
		t.Fatalf("retried completion: %v", err)
	}
	finishes(t, "the background hashes", d.hashes.stop)
	assertOneDoneRow(t, h, posix, "big", body)
}

// EP-3, REPL-1 (#55): a first completion that failed and could not
// remove its row leaves it stranded. A retry settles it before deciding,
// whether ResolveStranded drops it after the retry has seen it or the
// retry settles it itself, and then stores the object with a row.
func TestRetryOfAStrandedCompletionIsCaptured(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resolver bool
	}{{"settled by ResolveStranded", true}, {"settled by the retry", false}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, map[string]string{"files": "generic"})
			ctx := context.Background()
			posix, err := OpenBackend(h.store)
			if err != nil {
				t.Fatal(err)
			}
			be := &failingFirstCompletion{Backend: posix, entered: make(chan struct{}), release: make(chan struct{})}
			close(be.release)
			set, _ := profile.Load("")
			d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
			inFirst, inRetry, body := twoPartUpload(t, posix, "big")
			// Removing the failed completion's row fails while the trigger stands.
			if _, err := h.db.ExecContext(ctx, `CREATE TRIGGER refuse BEFORE DELETE ON queue
				BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
				t.Fatal(err)
			}
			if _, _, err := d.CompleteMultipartUpload(ctx, inFirst); err == nil {
				t.Fatal("the first completion succeeded")
			}
			if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
				t.Fatalf("%d rows pending, want the stranded one", len(p))
			}
			if _, err := h.db.ExecContext(ctx, `DROP TRIGGER refuse`); err != nil {
				t.Fatal(err)
			}
			if tc.resolver {
				var once sync.Once
				d.seenUpload = func(st string) {
					if st == state.StatePending {
						once.Do(func() { d.ResolveStranded(ctx) })
					}
				}
			}
			if _, _, err := d.CompleteMultipartUpload(ctx, inRetry); err != nil {
				t.Fatalf("retried completion: %v", err)
			}
			finishes(t, "the background hashes", d.hashes.stop)
			assertOneDoneRow(t, h, posix, "big", body)
		})
	}
}

// EP-3 (#55): a request waiting for its key's lock gives up when its
// context ends, writing nothing.
func TestWaitingForAKeyRespectsTheRequestContext(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	waiting := make(chan struct{})
	d.keys.waiting = func(string) { close(waiting) }
	unlock, err := d.keys.lock(context.Background(), "files", "k")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		n := int64(1)
		_, err := d.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String("files"), Key: aws.String("k"),
			ContentLength: &n, Body: bytes.NewReader([]byte("x"))})
		done <- err
	}()
	within(t, "put waiting for the key", waiting)
	cancel()
	if err := within(t, "put after its client gave up", done); err == nil {
		t.Fatal("the put succeeded")
	}
	if es := h.entries("files"); len(es) != 0 {
		t.Fatalf("queue: %+v", es)
	}
	if _, err := be.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("files"), Key: aws.String("k")}); !notFound(err) {
		t.Fatalf("the object was stored: %v", err)
	}
}

// EP-3 (#55): a DeleteObjects batch waiting for one busy key holds none
// of its other keys meanwhile.
func TestDeleteObjectsWaitsHoldingNoOtherKey(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	waiting := make(chan struct{})
	var once sync.Once
	d.keys.waiting = func(name string) {
		if name == lockName("files", "b") {
			once.Do(func() { close(waiting) })
		}
	}
	unlockB, err := d.keys.lock(context.Background(), "files", "b")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := d.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("files"),
			Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("b")}}}})
		done <- err
	}()
	within(t, "batch waiting for b", waiting)
	unlockA, free := d.keys.tryLock("files", "a")
	if !free {
		t.Fatal("the batch holds a while it waits for b")
	}
	unlockA()
	unlockB()
	if err := within(t, "batch", done); err != nil {
		t.Fatal(err)
	}
}

// EP-3 (#55): a completion answered after the gateway has begun waiting
// for its hashes starts none: its row stays pending for startup, and the
// key's lock is released.
func TestCompletionAfterStopLeavesItsRowPending(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	d.beforeHash = func() { t.Error("a hash started after stop") }
	_, in, body := twoPartUpload(t, be, "big")
	finishes(t, "the background hashes", d.hashes.stop)
	if _, _, err := d.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatal(err)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("%d rows pending, want 1", len(p))
	}
	unlock, free := d.keys.tryLock("files", "big")
	if !free {
		t.Fatal("the key's lock is still held")
	}
	unlock()
	d.beforeHash = nil
	if done, _, err := d.ResolvePending(ctx); err != nil || done != 1 {
		t.Fatalf("startup settled %d: %v", done, err)
	}
	assertOneDoneRow(t, h, be, "big", body)
}

// failingGets fails GetObject while fail is set and counts the calls.
type failingGets struct {
	backend.Backend
	fail atomic.Bool
	gets atomic.Int32
}

func (f *failingGets) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	f.gets.Add(1)
	if f.fail.Load() {
		return nil, s3err.GetAPIError(s3err.ErrInternalError)
	}
	return f.Backend.GetObject(ctx, in)
}

// EP-3, REPL-10 (#59): a completion whose background hash failed leaves
// its row stranded. A retry checks only that the object landed, answers
// from the store without reading the object, and leaves the row pending
// for ResolveStranded, which hashes it.
func TestRetryOfAStrandedButLandedUploadDoesNotHash(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &failingGets{Backend: posix}
	be.fail.Store(true)
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	_, in, body := twoPartUpload(t, posix, "big")
	if _, _, err := d.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatal(err)
	}
	finishes(t, "the background hash", d.hashes.wg.Wait)
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("%d rows pending after the failed hash, want 1", len(p))
	}
	gets := be.gets.Load()
	if _, _, err := d.CompleteMultipartUpload(ctx, in); err != nil {
		t.Fatalf("retried completion: %v", err)
	}
	if n := be.gets.Load() - gets; n != 0 {
		t.Fatalf("the retry read the object %d times", n)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("%d rows pending after the retry, want the stranded one", len(p))
	}
	if len(d.completing) != 0 {
		t.Fatalf("completions left registered: %v", d.completing)
	}
	be.fail.Store(false)
	d.ResolveStranded(ctx)
	assertOneDoneRow(t, h, posix, "big", body)
}

// EP-3 (#59): two retries that both found no row (the first completion
// failed) do not both wait for the key's lock: one captures, and the
// other waits for its store call, not for its hash.
func TestConcurrentRetriesAfterAFailedCompletionDoNotWaitForTheHash(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &failingFirstCompletion{Backend: posix, entered: make(chan struct{}), release: make(chan struct{})}
	close(be.release)
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	inFirst, inRetry, body := twoPartUpload(t, posix, "big")
	if _, _, err := d.CompleteMultipartUpload(ctx, inFirst); err == nil {
		t.Fatal("the first completion succeeded")
	}
	// The second retry reads "no row", then stalls until the first retry
	// is hashing.
	var noRow atomic.Int32
	stalled, gate := make(chan struct{}), make(chan struct{})
	d.seenUpload = func(st string) {
		if st == "" && noRow.Add(1) == 1 {
			close(stalled)
			<-gate
		}
	}
	hashing, finish := make(chan struct{}), make(chan struct{})
	d.beforeHash = func() { close(hashing); <-finish }
	waited := make(chan string, 4)
	d.keys.waiting = func(name string) { waited <- name }
	complete := func() chan error {
		c := make(chan error, 1)
		go func() { _, _, err := d.CompleteMultipartUpload(ctx, inRetry); c <- err }()
		return c
	}
	second := complete()
	within(t, "second retry reading no row", stalled)
	if err := within(t, "first retry", complete()); err != nil {
		t.Fatalf("first retry: %v", err)
	}
	within(t, "first retry's hash", hashing)
	close(gate)
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second retry: %v", err)
		}
	case name := <-waited:
		t.Fatalf("the second retry waited for the key's lock %q", name)
	case <-time.After(10 * time.Second):
		t.Fatal("second retry: timed out")
	}
	close(finish)
	finishes(t, "the background hashes", d.hashes.stop)
	assertOneDoneRow(t, h, posix, "big", body)
}

// EP-3 (#55, #59): a retry waiting for the first completion's store call
// gives up when its context ends, without the key's lock.
func TestWaitingForARunningCompletionRespectsTheRequestContext(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &failingFirstCompletion{Backend: posix, entered: make(chan struct{}), release: make(chan struct{})}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	in, _, _ := twoPartUpload(t, posix, "big")
	first := make(chan error, 1)
	go func() { _, _, err := d.CompleteMultipartUpload(context.Background(), in); first <- err }()
	within(t, "first completion reaching the store", be.entered)
	d.keys.waiting = func(name string) { t.Errorf("a retry waited for the key's lock %q", name) }
	ctx, cancel := context.WithCancel(context.Background())
	d.seenUpload = func(string) { cancel() }
	if _, _, err := d.CompleteMultipartUpload(ctx, in); err == nil {
		t.Fatal("the retry succeeded after its client gave up")
	}
	close(be.release)
	within(t, "first completion", first)
}

// panickingCompletion panics in CompleteMultipartUpload.
type panickingCompletion struct{ backend.Backend }

func (panickingCompletion) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	panic("injected")
}

// EP-3 (#59): a completion that panics in the store releases its
// per-upload signal and the key's lock, and strands its row.
func TestPanickingCompletionReleasesTheKey(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: panickingCompletion{posix}, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	in, _, _ := twoPartUpload(t, posix, "big")
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		d.CompleteMultipartUpload(context.Background(), in)
	}()
	if len(d.completing) != 0 {
		t.Fatalf("completion left registered: %v", d.completing)
	}
	unlock, free := d.keys.tryLock("files", "big")
	if !free {
		t.Fatal("the key's lock is still held")
	}
	unlock()
	if len(d.stranded) != 1 {
		t.Fatalf("stranded: %v, want the row", d.stranded)
	}
}

// EP-3 (#59): a pending completion's row is matched to its own upload,
// not to its ETag: another upload with the same parts, already on disk,
// does not make it look landed.
func TestPendingCompletionMatchedByUploadNotETag(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	_, a, body := twoPartUpload(t, be, "big")
	_, b, _ := twoPartUpload(t, be, "big")
	if _, _, err := d.CompleteMultipartUpload(ctx, a); err != nil {
		t.Fatal(err)
	}
	finishes(t, "the background hashes", d.hashes.stop)
	if id, err := be.UploadID("files", "big"); err != nil || id != *a.UploadId {
		t.Fatalf("store records upload %q (%v), want %q", id, err, *a.UploadId)
	}
	etag, err := backend.ComputeMultipartETagFromPartETags(*b.UploadId, b.MultipartUpload.Parts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "big", Op: state.OpPut,
		Class: "data", ETag: etag, UploadID: *b.UploadId}}); err != nil {
		t.Fatal(err)
	}
	if done, dropped, err := d.ResolvePending(ctx); err != nil || done != 0 || dropped != 1 {
		t.Fatalf("startup settled %d done, %d dropped (%v); want B's row dropped", done, dropped, err)
	}
	assertOneDoneRow(t, h, be, "big", body)
}
