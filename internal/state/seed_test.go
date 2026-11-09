package state

import (
	"context"
	"testing"
	"time"
)

// fifo drains place's FIFO as the worker would, acknowledging each row.
func fifo(t *testing.T, db *DB, place string) []string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	for {
		p, ok, err := db.NextPlacement(ctx, place)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return keys
		}
		keys = append(keys, p.Bucket+"/"+p.Key)
		if err := db.AckPlacement(ctx, place, p.Seq, p.SHA256, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

// REPL-8 (#36): an export lists exactly the keys placed at the friend;
// the seed holds the friend's rows of that bucket from the network; only
// rows the list shows with the captured hash are acknowledged, and once
// the seed ends the rest, and what was written since, are sent in order.
func TestSeedHoldsRowsUntilTheFriendsListConfirmsThem(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	friend(t, db, "C")
	capture(t, db, "laptop", "data/1", OpPut)
	capture(t, db, "laptop", "data/2", OpPut)
	capture(t, db, "laptop", "gone", OpPut)
	capture(t, db, "laptop", "gone", OpDelete)
	capture(t, db, "other", "x", OpPut)

	objs, err := db.SeedObjects(ctx, "B", "laptop")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var through int64
	for _, o := range objs {
		keys = append(keys, o.Key)
		through = max(through, o.Seq)
		if o.SHA256 != "h-"+o.Key || o.Size != int64(len(o.Key)) {
			t.Errorf("%s: %+v", o.Key, o)
		}
	}
	if len(keys) != 2 || keys[0] != "data/1" || keys[1] != "data/2" {
		t.Fatalf("seed objects %v; want data/1 and data/2 (gone is deleted)", keys)
	}
	if err := db.StartSeed(ctx, "B", "laptop", through, time.Now()); err != nil {
		t.Fatal(err)
	}
	capture(t, db, "laptop", "data/3", OpPut) // a backup after the export

	// Held at B only; C and B's other bucket go on.
	if got := fifo(t, db, "B"); len(got) != 1 || got[0] != "other/x" {
		t.Fatalf("B's FIFO while seeding: %v; want other/x only", got)
	}
	if got := fifo(t, db, "C"); len(got) != 4 {
		t.Fatalf("C's FIFO: %v", got)
	}

	// The list shows data/1 as captured, data/2 with other bytes, and a
	// key never exported.
	n, err := db.ConfirmSeeded(ctx, "B", "laptop", through, []SeedListed{
		{Key: "data/1", SHA256: "h-data/1"}, {Key: "data/2", SHA256: "h-other"}, {Key: "data/3", SHA256: "h-data/3"},
	}, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("confirmed %d, %v; want 1", n, err)
	}
	if h, ok, _ := db.AckedHash(ctx, "B", "laptop", "data/1"); !ok || h != "h-data/1" {
		t.Fatalf("data/1 acknowledged as %q %v", h, ok)
	}
	if left, err := db.SeedLeft(ctx, "B", "laptop", through); err != nil || left != 1 {
		t.Fatalf("left %d %v; want 1 (data/2)", left, err)
	}
	if got := fifo(t, db, "B"); len(got) != 0 {
		t.Fatalf("B's FIFO before the seed ends: %v", got)
	}
	if err := db.EndSeed(ctx, "B", "laptop", SeedQuiet, time.Now()); err != nil {
		t.Fatal(err)
	}
	got := fifo(t, db, "B")
	if len(got) != 2 || got[0] != "laptop/data/2" || got[1] != "laptop/data/3" {
		t.Fatalf("B's FIFO after the seed: %v; want data/2, then data/3 (data/1 never again)", got)
	}
	s, err := db.GetSeed(ctx, "B", "laptop")
	if err != nil || s.Outcome != SeedQuiet || s.DoneAt.IsZero() {
		t.Fatalf("seed %+v %v", s, err)
	}
}
