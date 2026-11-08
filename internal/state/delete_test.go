package state

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// start marks place's row seq as taken by its sender, as the worker does
// before it sends; ok is false when the row is no longer queued.
func (p *pool) start(place string, seq int64) bool {
	p.t.Helper()
	ok, err := p.db.StartPlacement(context.Background(), place, seq, p.now)
	if err != nil {
		p.t.Fatal(err)
	}
	return ok
}

// REPL-2 (#21): a delete is queued at every place whose placement for
// the key is acknowledged or queued and started; a queued put no sender
// has started is cancelled there instead, and no delete is sent for it.
func TestDeleteCancelsAnUnstartedPutInsteadOfFollowingIt(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d, "B", "C")
	if !p.start("B", d) { // in flight at B; still unsent at C
		t.Fatal("start of a queued row refused")
	}
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("put of data/1 after its delete", d, "B")
	p.wantPlaces("delete of data/1", del, "B")

	// A key every place acknowledged, then written again (a rewritable
	// key's new version, unsent anywhere), then deleted: the unsent
	// version is cancelled, and the delete goes everywhere for the
	// version they hold.
	m1 := p.put("laptop", "index/1", "metadata", 0.5)
	for _, f := range []string{"B", "C", "D"} {
		p.ack(f, m1)
	}
	seqs, err := p.db.BeginCaptures(context.Background(), []Capture{{Bucket: "laptop", Key: "index/1", Op: OpPut, Class: "metadata"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.FinishPut(context.Background(), seqs[0], 100, "h-other"); err != nil {
		t.Fatal(err)
	}
	m2 := seqs[0]
	p.wantPlaces("index/1's second version", m2, "B", "C", "D")
	mdel := p.del("laptop", "index/1", "metadata")
	p.wantPlaces("index/1's unsent second version", m2)
	p.wantPlaces("delete of index/1", mdel, "B", "C", "D")

	// Metadata written between the put and its delete does not keep the
	// put: the store no longer holds its bytes, so it could only be
	// cancelled when its turn came (#21 review). It is cancelled now,
	// and the index no longer waits for it.
	w := p.put("laptop", "data/3", "data", 1)
	idx := p.put("laptop", "index/2", "metadata", 0.1)
	wdel := p.del("laptop", "data/3", "data")
	p.wantPlaces("data/3 after an index and its delete", w)
	p.wantPlaces("delete of data/3", wdel)
	if p.held(idx) {
		t.Fatal("the index waits for a put its delete cancelled")
	}

	// A put cancelled by a delete is no longer the sender's to send.
	x := p.put("laptop", "data/2", "data", 1)
	p.del("laptop", "data/2", "data")
	if p.start("B", x) || p.start("C", x) {
		t.Fatal("a sender started a put its delete had cancelled")
	}
}

// REPL-2, REPL-12, REPL-4 (#21): once a deleted object's delete is
// acknowledged, the place's count no longer holds it, the object no
// longer counts below copies, and the place's FIFO is empty. A delete
// that cancelled an unsent put frees its bytes at once.
func TestAcknowledgedDeleteFreesThePlace(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.bucket("laptop", 2) // one friend: every object is below copies
	use := func() int64 {
		t.Helper()
		u, err := p.db.PlaceUse(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return u["B"]
	}
	below := func() BelowStat {
		t.Helper()
		b, err := p.db.BelowCopies(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return b["laptop"]
	}
	d1 := p.put("laptop", "data/1", "data", 2)
	p.ack("B", d1)
	d2 := p.put("laptop", "data/2", "data", 3)
	if use() != 5*unit || below().Objects != 2 {
		t.Fatalf("use %d, below %+v", use(), below())
	}
	p.del("laptop", "data/2", "data") // unsent: cancelled, nothing to send
	p.wantPlaces("data/2", d2)
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("delete of data/1", del, "B")
	p.ack("B", del)
	if use() != 0 || below().Objects != 0 {
		t.Fatalf("after the deletes: use %d, below %+v", use(), below())
	}
	stats, err := p.db.PlaceStats(ctx, p.now)
	if err != nil {
		t.Fatal(err)
	}
	if s := stats["B"]; s.Queued != 0 || s.Refused != 0 {
		t.Fatalf("B's FIFO %+v", s)
	}
}

// REPL-2, REPL-1 (#21): a delete is behind every earlier write of the
// key at the place, and a later write of the key is behind the delete.
func TestDeleteKeepsItsPlaceInEachFIFO(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.bucket("laptop", 1)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.start("B", d1)
	del := p.del("laptop", "data/1", "data")
	d2 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1 written again", d2, "B")
	var got []int64
	for {
		nx, ok, err := p.db.NextPlacement(ctx, "B")
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, nx.Seq)
		p.ack("B", nx.Seq)
	}
	if len(got) != 3 || got[0] != d1 || got[1] != del || got[2] != d2 {
		t.Fatalf("B's FIFO %v, want %d %d %d", got, d1, del, d2)
	}
}

// rowState is place's row seq's state, or "" if it has none.
func (p *pool) rowState(place string, seq int64) string {
	p.t.Helper()
	var st string
	p.db.QueryRow(`SELECT state FROM placement WHERE place = ? AND seq = ?`, place, seq).Scan(&st)
	return st
}

// REPL-2 (#21 review): a put taken at a place may have landed there
// even when its row is later cancelled as superseded (its answer lost,
// then a new write of the key captured): a later delete still reaches
// that place, whether the new write went elsewhere or waits, untaken,
// at the same place.
func TestDeleteReachesAPlaceWhoseTakenPutWasSuperseded(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 1)
	p1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", p1, "B")
	p.start("B", p1) // sent; the answer was lost
	p2 := p.rewrite("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1 written again", p2, "C")
	if err := p.db.CancelPlacement(ctx, "B", p1); err != nil { // its sender finds it superseded
		t.Fatal(err)
	}
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("delete of data/1", del, "B")

	use, err := p.db.PlaceUse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if use["B"] != 0 || use["C"] != 0 {
		t.Fatalf("use %v after every write was deleted", use)
	}

	// The new write waits at the same place, not taken.
	p = newPool(t)
	p.friend("B", 10)
	p.bucket("laptop", 1)
	q1 := p.put("laptop", "data/2", "data", 1)
	p.start("B", q1)
	q2 := p.rewrite("laptop", "data/2", "data", 1)
	p.wantPlaces("data/2 written again", q2, "B")
	if err := p.db.CancelPlacement(ctx, "B", q1); err != nil {
		t.Fatal(err)
	}
	del = p.del("laptop", "data/2", "data")
	p.wantPlaces("delete of data/2", del, "B")
	if st := p.rowState("B", q2); st != "" {
		t.Fatalf("the untaken new write is %q, want cancelled", st)
	}
	if st := p.rowState("B", q1); st != PlacementAbandoned {
		t.Fatalf("the taken superseded write is %q", st)
	}
	if use, _ := p.db.PlaceUse(ctx); use["B"] != 0 {
		t.Fatalf("use %v after every write was deleted", use)
	}
}

// REPL-2, §3.4 (#21 review): re-routing a taken row away from a friend
// that stopped answering leaves that friend a row a later delete still
// follows: the send may have landed there.
func TestRerouteKeepsATakenRowsPlaceForItsDelete(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 1)
	d := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d, "B")
	p.start("B", d)
	p.condition("B", ConditionRetrying, p.now.Add(-2*time.Hour), p.now.Add(-3*time.Hour))
	moved, _, err := p.db.Reroute(ctx, nil, nil)
	if err != nil || moved != 1 {
		t.Fatalf("moved %d %v", moved, err)
	}
	p.wantPlaces("data/1 re-routed", d, "C")
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("delete of data/1", del, "B")
}

// #21 review: rows queued when migration 21 runs may have been sent by
// an older lendisk, so they count as taken: a delete follows them.
func TestRowsQueuedBeforeTheUpgradeCountAsTaken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	quiet := slog.New(slog.DiscardHandler)
	old, err := open(ctx, dir, migrations[:20], quiet)
	if err != nil {
		t.Fatal(err)
	}
	friend(t, old, "B")
	seqs, err := old.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.FinishPut(ctx, seqs[0], 5, "h"); err != nil {
		t.Fatal(err)
	}
	old.Close()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	del := capture(t, db, "laptop", "data/1", OpDelete)
	var n int
	db.QueryRow(`SELECT count(*) FROM placement WHERE place = 'B' AND seq IN (?, ?) AND state = 'queued'`, seqs[0], del).Scan(&n)
	if n != 2 {
		t.Fatalf("%d of the put and its delete queued at B, want both", n)
	}
}
