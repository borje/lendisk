package gateway

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

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
