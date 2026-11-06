package state

import (
	"context"
	"testing"
	"time"
)

func capture(t *testing.T, db *DB, bucket, key, op string) int64 {
	t.Helper()
	ctx := context.Background()
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: bucket, Key: key, Op: op, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if op == OpPut {
		if _, err := db.FinishPut(ctx, seqs[0], int64(len(key)), "h-"+key); err != nil {
			t.Fatal(err)
		}
	} else if err := db.FinishDelete(ctx, seqs[0]); err != nil {
		t.Fatal(err)
	}
	return seqs[0]
}

func friend(t *testing.T, db *DB, peer string) {
	t.Helper()
	if err := db.AddFriendship(context.Background(), Friendship{Peer: peer, Address: "tc" + peer, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

// REPL-1, REPL-3 (#18): a completed capture is placed, in the same
// transaction, at every friend; each place's FIFO is the write
// sequence; a pending or dropped capture is placed nowhere.
func TestDoneCapturesArePlacedAtEveryFriendInWriteOrder(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	friend(t, db, "C")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	pending, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/p", Op: OpPut, Class: "data"}})
	s2 := capture(t, db, "laptop", "index/1", OpPut)
	s3 := capture(t, db, "laptop", "data/1", OpDelete)
	for _, place := range []string{"B", "C"} {
		var got []int64
		for {
			p, ok, err := db.NextPlacement(ctx, place)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			got = append(got, p.Seq)
			if err := db.AckPlacement(ctx, place, p.Seq, p.SHA256, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		if len(got) != 3 || got[0] != s1 || got[1] != s2 || got[2] != s3 {
			t.Fatalf("%s FIFO %v, want %d %d %d (pending %v placed nowhere)", place, got, s1, s2, s3, pending)
		}
	}
	// The index of acknowledged hashes (REPL-10).
	h, ok, err := db.AckedHash(ctx, "B", "laptop", "index/1")
	if err != nil || !ok || h != "h-index/1" {
		t.Fatalf("acked hash %q %v %v", h, ok, err)
	}
	// Acknowledging twice changes nothing and does not fail (a retry
	// after a crash between the send and the record).
	if err := db.AckPlacement(ctx, "B", s1, "h-data/1", time.Now()); err != nil {
		t.Fatal(err)
	}
}

// The FIFO head carries what the worker sends: bucket, key, op, size
// and the capture's hash.
func TestNextPlacementCarriesTheQueueRow(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s := capture(t, db, "laptop", "data/ab", OpPut)
	p, ok, err := db.NextPlacement(ctx, "B")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want := Placement{Seq: s, Place: "B", Bucket: "laptop", Key: "data/ab", Op: OpPut, Size: 7, SHA256: "h-data/ab", State: PlacementQueued}
	if p.Seq != want.Seq || p.Place != want.Place || p.Bucket != want.Bucket || p.Key != want.Key || p.Op != want.Op ||
		p.Size != want.Size || p.SHA256 != want.SHA256 || p.State != want.State || p.CreatedAt.IsZero() {
		t.Fatalf("head %+v", p)
	}
}

// A put that a later write or delete of the same key replaced is
// superseded: the store no longer holds its bytes, so it is cancelled
// rather than sent.
func TestSupersededPutIsCancelled(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	if sup, err := db.Superseded(ctx, s1); err != nil || sup {
		t.Fatal(sup, err)
	}
	// A later write still in flight does not supersede, but is a later
	// capture.
	pending, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpPut, Class: "data"}})
	if sup, _ := db.Superseded(ctx, s1); sup {
		t.Fatal("superseded by a pending write")
	}
	if later, err := db.LaterCapture(ctx, s1); err != nil || !later {
		t.Fatal(later, err)
	}
	db.DropCaptures(ctx, pending...)
	s2 := capture(t, db, "laptop", "data/1", OpDelete)
	if sup, err := db.Superseded(ctx, s1); err != nil || !sup {
		t.Fatal(sup, err)
	}
	if err := db.CancelPlacement(ctx, "B", s1); err != nil {
		t.Fatal(err)
	}
	p, ok, _ := db.NextPlacement(ctx, "B")
	if !ok || p.Seq != s2 {
		t.Fatalf("head after cancel %+v", p)
	}
}

// EP-11, REPL-10: a refusal the place will repeat (immutability, a
// rejected hash) leaves the FIFO and stays visible.
func TestRefusedPlacementLeavesTheFIFO(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	s2 := capture(t, db, "laptop", "data/2", OpPut)
	if err := db.RefusePlacement(ctx, "B", s1, "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if p, _, _ := db.NextPlacement(ctx, "B"); p.Seq != s2 {
		t.Fatalf("head %+v", p)
	}
	st, err := db.PlaceStats(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st["B"].Refused != 1 || st["B"].Queued != 1 {
		t.Fatalf("stats %+v", st["B"])
	}
}

// `status` (#18): per place, the queued objects and bytes and the lag,
// the age of the oldest queued row; and the place's condition.
func TestPlaceStatsAndCondition(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	capture(t, db, "laptop", "data/1", OpPut)
	capture(t, db, "laptop", "data/22", OpPut)
	capture(t, db, "laptop", "data/1", OpDelete)
	later := time.Now().Add(90 * time.Second)
	st, err := db.PlaceStats(ctx, later)
	if err != nil {
		t.Fatal(err)
	}
	b := st["B"]
	if b.Queued != 3 || b.QueuedBytes != int64(len("data/1")+len("data/22")) || b.Lag < 89*time.Second {
		t.Fatalf("stats %+v", b)
	}
	now := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(ctx, "B", ConditionQuota, "QuotaExceeded", now); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "B", ConditionQuota, "QuotaExceeded again", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	cs, err := db.PlaceConditions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := cs["B"]
	if c.Condition != ConditionQuota || c.Detail != "QuotaExceeded again" || !c.Since.Equal(now) {
		t.Fatalf("condition %+v", c)
	}
	if err := db.SetPlaceCondition(ctx, "B", ConditionOK, "", now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	cs, _ = db.PlaceConditions(ctx)
	if c := cs["B"]; c.Condition != ConditionOK || !c.LastOK.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("condition %+v", c)
	}
}
