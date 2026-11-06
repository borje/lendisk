package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	wg.Wait()
	close(stop)
	if n := <-acked; n == 0 {
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
	wg.Wait()
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
	d := &Decorator{Backend: be, DB: h.db, Profiles: set, Log: quiet}
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
	<-stalled
	errX := make(chan error, 1)
	go func() { errX <- put("X") }()
	select {
	case err := <-errX:
		t.Errorf("the second put finished while the first was in flight (err %v)", err)
		errX <- err
	case <-time.After(300 * time.Millisecond):
	}
	close(stall)
	if err := <-errY; err != nil {
		t.Fatal(err)
	}
	if err := <-errX; err != nil {
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
	d := &Decorator{Backend: be, DB: h.db, Profiles: set, Log: quiet}
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
	<-stored
	answered("completion sent while the first is in the store", complete())
	close(release)
	answered("first completion, before its hash", first)
	<-hashing
	answered("completion sent while the first's hash runs", complete())
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 1 {
		t.Fatalf("%d rows pending while hashing, want 1", len(p))
	}
	close(finish)
	d.hashes.Wait()
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
	d := &Decorator{Backend: be, DB: h.db, Log: quiet}
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
