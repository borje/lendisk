package state

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"
)

func (p *pool) removeOverflow(name string, keepData bool) {
	p.t.Helper()
	if err := p.db.RemoveOverflow(context.Background(), name, keepData, p.now); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pool) finishRemovals() []string {
	p.t.Helper()
	names, err := p.db.FinishOverflowRemovals(context.Background())
	if err != nil {
		p.t.Fatal(err)
	}
	return names
}

// holdDrops holds every dropping or queued delete row at target as the
// worker's send does (HoldOverflowDelete), and acknowledges each held.
func (p *pool) holdDrops(target string) {
	p.t.Helper()
	ctx := context.Background()
	seqs, err := column(ctx, p.db, `SELECT seq FROM placement WHERE place = ?
		AND (state = 'dropping' OR state = 'queued' AND op = 'delete') ORDER BY seq`, target)
	if err != nil {
		p.t.Fatal(err)
	}
	for _, s := range seqs {
		seq, _ := strconv.ParseInt(s, 10, 64)
		held, err := p.db.HoldOverflowDelete(ctx, target, seq, p.now)
		if err != nil || !held {
			p.t.Fatalf("row %d at %s held %v, %v", seq, target, held, err)
		}
		if err := p.db.AckPlacement(ctx, target, seq, "", p.now); err != nil {
			p.t.Fatal(err)
		}
	}
}

func (p *pool) tombsAt(place string) int {
	p.t.Helper()
	var n int
	p.db.QueryRow(`SELECT count(*) FROM overflow_tomb WHERE place = ?`, place).Scan(&n)
	return n
}

// #96: removing an overflow target first stops placing there, then
// places each of its copies at a friend, keeping `copies` acknowledged
// copies throughout, then drops its copies there, held as client-side
// tombs at a target without versioning, and drops the target only once
// those deletes are sent.
func TestRemovingATargetPlacesItsCopiesElsewhereBeforeDeletingThem(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	d1 := p.put("r", "data/1", "data", 1)
	d2 := p.put("r", "data/2", "data", 1)
	p.ackQueued()
	p.wantPlaces("data/1 with one friend", d1, "A", o)

	p.removeOverflow("b2", false)
	// Step 1: no new copy goes there, and it counts in the pool no more.
	d3 := p.put("r", "data/3", "data", 1)
	p.wantPlaces("data captured while the target is removed", d3, "A")
	if n, _ := p.db.PoolSize(ctx); n != 1 {
		t.Fatalf("pool size %d with the target removing, want 1", n)
	}
	// With no other room, its copies stay: the objects keep two.
	p.settled()
	p.wantPlaces("data/1 with no room elsewhere", d1, "A", o)
	if got := p.finishRemovals(); len(got) != 0 {
		t.Fatalf("removed %v while it holds the only second copies", got)
	}

	// Step 2: a friend with room. The new copies are placed while the
	// target's stay acknowledged.
	p.friend("B", 10)
	p.replace()
	p.wantState(o, d1, PlacementAcked)
	p.wantState("B", d1, PlacementQueued)
	p.ackQueued() // B takes them; the target's become dropping
	p.wantState(o, d1, PlacementDropping)
	for _, d := range []int64{d1, d2, d3} {
		if got := p.places(d); !slices.Equal(got, []string{"A", "B"}) {
			t.Fatalf("row %d at %v, want A and B", d, got)
		}
	}
	// Step 3: the target's deletes are held for its window (no
	// versioning), metadata too once it holds no data.
	p.holdDrops(o)
	p.replace()
	p.holdDrops(o)
	if n := p.tombsAt(o); n != 3 {
		t.Fatalf("%d deletes held at the target, want its 2 data copies and its metadata", n)
	}
	if got := p.finishRemovals(); len(got) != 0 {
		t.Fatalf("removed %v while deletes are held there", got)
	}
	if _, err := p.db.GetOverflow(ctx, "b2"); err != nil {
		t.Fatal(err)
	}
	// Step 4: once the held deletes are sent, nothing depends on it.
	for _, tb := range p.due(o) {
		t.Fatalf("a delete due before the window: %+v", tb)
	}
	p.now = p.now.Add(31 * 24 * time.Hour)
	for _, tb := range p.due(o) {
		if err := p.db.DropOverflowTomb(ctx, o, tb.Seq); err != nil {
			t.Fatal(err)
		}
	}
	if got := p.finishRemovals(); !slices.Equal(got, []string{"b2"}) {
		t.Fatalf("removed %v, want b2", got)
	}
	if _, err := p.db.GetOverflow(ctx, "b2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the target is still there: %v", err)
	}
	var rows int
	p.db.QueryRow(`SELECT (SELECT count(*) FROM placement WHERE place = ?1) + (SELECT count(*) FROM place_use WHERE place = ?1)`,
		o).Scan(&rows)
	if rows != 0 {
		t.Fatalf("%d rows of the target as a place left", rows)
	}
}

// #96: a target removed with keep_data is drained the same way, but no
// delete is sent there: its held deletes are forgotten, every drop is
// acknowledged unsent, and the row goes once its data is held
// elsewhere, its metadata left there.
func TestRemovingATargetKeepingItsDataSendsNoDelete(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	d1 := p.put("r", "data/1", "data", 1)
	p.put("r", "data/2", "data", 1)
	p.ackQueued()
	p.del("r", "data/2", "data")
	p.holdDrops(o)
	if p.tombsAt(o) != 1 {
		t.Fatal("the prune's delete is not held")
	}
	p.removeOverflow("b2", true)
	if p.tombsAt(o) != 0 {
		t.Fatal("a held delete stays to be sent")
	}
	p.friend("B", 10)
	p.settled()
	p.ackQueued()
	p.wantPlaces("data/1", d1, "A", "B")
	p.holdDrops(o)
	if p.tombsAt(o) != 0 {
		t.Fatal("a drop was held as a delete to send")
	}
	if got := p.finishRemovals(); !slices.Equal(got, []string{"b2"}) {
		t.Fatalf("removed %v, want b2", got)
	}
	if _, err := p.db.GetOverflow(ctx, "b2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the target is still there: %v", err)
	}
}

// #96: removing a target cancels its unsent copies no sender took, and
// a split no longer counts it.
func TestRemovingATargetCancelsItsUnsentCopies(t *testing.T) {
	p := newPool(t)
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data", d, "A", o)
	p.removeOverflow("b2", false)
	p.wantPlaces("data once its target copy is cancelled", d, "A")
	if got := p.finishRemovals(); !slices.Equal(got, []string{"b2"}) {
		t.Fatalf("removed %v, want b2: it never held anything", got)
	}
}

func TestRemovingAnUnknownTargetIsNotFound(t *testing.T) {
	p := newPool(t)
	if err := p.db.RemoveOverflow(context.Background(), "nope", false, p.now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

// #96 (drop order, #38 review): with more copies than `copies`, the
// copies at overflow targets go first. While placement is split, an
// object keeps one copy at a target, so a second target's copy goes
// before any friend's, and the last target's after them.
func TestSurplusCopiesGoFromOverflowTargetsFirst(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	p.friend("B", 2) // nearly full: by fraction it would go first
	o := p.overflow("b2", 100)
	p.bucket("r", 3)
	d := p.put("r", "data/1", "data", 1)
	p.ackQueued()
	p.wantPlaces("three copies", d, "A", "B", o)
	if err := p.db.SetCopies(context.Background(), "r", 2); err != nil {
		t.Fatal(err)
	}
	p.replace()
	p.wantState(o, d, PlacementDropping)
	p.wantState("B", d, PlacementAcked)
}

func TestWhileSplitASecondTargetsCopyGoesBeforeAFriends(t *testing.T) {
	p := newPool(t)
	p.friend("A", 1.5) // the friends cannot hold two copies: split
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	o1 := p.overflow("b2", 100)
	o2 := p.overflow("wasabi", 100)
	ctx := context.Background()
	// The object at the friend and both targets (as a pool that changed
	// under it may leave it).
	for _, pl := range []string{o1, o2} {
		if _, err := p.db.Exec(`INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			VALUES (?, ?, 'r', 'data/1', 'put', 'acked', 'h-data/1', ?, ?)`, d, pl, p.now.Unix(), unit); err != nil {
			t.Fatal(err)
		}
	}
	p.ackQueued()
	p.wantPlaces("three copies", d, "A", o1, o2)
	if split, err := p.db.overflowSplit(ctx, p.db); err != nil || !split {
		t.Fatalf("split %v, %v", split, err)
	}
	p.replace()
	p.wantState("A", d, PlacementAcked)
	st1, _ := p.rowOf(o1, d)
	st2, _ := p.rowOf(o2, d)
	if (st1 == PlacementDropping) == (st2 == PlacementDropping) {
		t.Fatalf("targets' rows %s and %s: want exactly one dropping", st1, st2)
	}
}
