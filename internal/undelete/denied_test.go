package undelete

import (
	"context"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// RET-3, S3-5 (#98): at a versioned target whose credential may not
// delete, a user delete was refused, so the target holds no marker and
// the key is still live there while the index holds it deleted.
// Undelete takes the refused delete as a held one: the object is
// already there, so it is recorded with no copy made, the refused
// delete is no longer retried, and that holds days later too, before
// or after a retry was due.
func TestUndeleteTakesAKeyWhoseDeleteTheTargetRefusedAsPresent(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningEnabled)
	ctx := context.Background()
	place := "overflow:g"
	body := bodyOf(packA)
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packA, Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), hash(body)); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], hash(body), *now); err != nil {
		t.Fatal(err)
	}
	ft.bodies[packA], ft.live[packA] = body, body
	*now = now.Add(time.Minute)
	seqs, err = db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packA, Op: state.OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if held, err := db.HoldOverflowDelete(ctx, place, seqs[0], *now); err != nil || held {
		t.Fatalf("a versioned target's delete held: %v, %v", held, err)
	}
	// The worker sends it, the provider answers 403 AccessDenied.
	if err := db.DenyOverflowDelete(ctx, place, seqs[0], *now); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], "", *now); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(3 * 24 * time.Hour) // past the window and the retry's due time
	p, res := runUndelete(t, db, r, time.Time{})
	if p.Keys != 1 || p.Friends[0].Keys != 1 {
		t.Fatalf("preview %+v", p)
	}
	if fr := res.Friends[0]; fr.Restored != 1 || fr.Recorded != 1 || fr.Error != "" || len(ft.restored) != 0 {
		t.Fatalf("result %+v, copied at the target %v", fr, ft.restored)
	}
	if live, _ := db.KeyLive(ctx, "laptop", packA); !live {
		t.Fatal("not live in the index")
	}
	if holds, err := db.OverflowTombHolds(ctx); err != nil || holds["g"].Denied != 0 {
		t.Fatalf("the refused delete is still retried: %+v, %v", holds, err)
	}
}
