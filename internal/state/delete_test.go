package state

import (
	"context"
	"testing"
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

	// A put that metadata written after it waits for (REPL-1) is not
	// cancelled: the metadata was written while the put was the key's
	// bytes, so they go first and the delete follows.
	w := p.put("laptop", "data/3", "data", 1)
	at := p.places(w)
	idx := p.put("laptop", "index/2", "metadata", 0.1)
	wdel := p.del("laptop", "data/3", "data")
	p.wantPlaces("data/3 after an index and its delete", w, at...)
	p.wantPlaces("delete of data/3", wdel, at...)
	if !p.held(idx) {
		t.Fatal("the index written before the delete no longer waits for data/3")
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
