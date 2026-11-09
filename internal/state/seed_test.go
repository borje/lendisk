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

// REPL-8, REPL-1, REPL-11 (#36 review): a pending seed at B holds only
// B. Its held data does not hold the metadata barrier, nor its held
// metadata a data delete, anywhere else: a metadata put after a data put
// reaches C, as for a friend unreachable past reroute_after (#21), and
// the watermark advances as C acknowledges and, at B, as the seed
// confirms.
func TestASeedHoldsOnlyTheSeededFriend(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.bucket("laptop", 1)
	p.friend("B", 100)
	p.friend("C", 100)
	d0 := p.put("laptop", "data/0", "data", 1)
	p.wantPlaces("data/0", d0, "B")
	if err := p.db.StartSeed(ctx, "B", "laptop", d0, p.now); err != nil {
		t.Fatal(err)
	}
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "C")
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	p.wantPlaces("index/1", idx, "B", "C")
	held := func(seq int64) bool {
		t.Helper()
		h, err := p.db.Held(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	if !held(idx) {
		t.Fatal("index/1 not held while data/1 is unsent at C")
	}
	p.ackRow("C", d1)
	if held(idx) {
		t.Fatal("index/1 held by data/0, which only the seed at B holds")
	}
	if n, ok, _ := p.db.NextPlacement(ctx, "C"); !ok || n.Seq != idx {
		t.Fatalf("C's next row: %+v %v; want index/1", n, ok)
	}
	p.ackRow("C", idx)
	// The mirror: index/1 is queued only at B, held by the seed.
	gone := p.del("laptop", "data/1", "data")
	if held(gone) {
		t.Fatal("the delete of data/1 held by index/1, which only the seed at B holds")
	}
	if w := p.watermark("laptop"); w != d0-1 {
		t.Fatalf("watermark %d while data/0 waits for B; want %d", w, d0-1)
	}
	// B's list confirms data/0: the watermark passes everything.
	if n, err := p.db.ConfirmSeeded(ctx, "B", "laptop", d0, []SeedListed{{Key: "data/0", SHA256: "h-data/0"}}, p.now); err != nil || n != 1 {
		t.Fatalf("confirmed %d %v", n, err)
	}
	if w := p.watermark("laptop"); w != gone {
		t.Fatalf("watermark %d after the seed confirmed data/0 and C acknowledged; want %d", w, gone)
	}
}
