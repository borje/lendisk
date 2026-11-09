package state

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// purge answers place's quota call as a lender does after purging its
// tombs (RET-1): it counts only the copies it holds, so the room the
// drained ones took is free again.
func (p *pool) purge(place string, quota float64) {
	p.t.Helper()
	var lent int64
	if err := p.db.QueryRow(`SELECT coalesce(sum(p.size), 0) FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.state = 'acked' AND p.op = 'put' AND `+liveQ, place).Scan(&lent); err != nil {
		p.t.Fatal(err)
	}
	n := int64(quota * float64(unit))
	if _, err := p.db.QuotaAnswered(context.Background(), place, n, p.now, QuotaAnswer{Number: n, Lent: lent}, p.now); err != nil {
		p.t.Fatal(err)
	}
}

// settled runs passes and acknowledges what they queue until nothing is
// left to send.
func (p *pool) settled() {
	p.t.Helper()
	for range 6 {
		p.replace()
		p.ackQueued()
	}
}

// rowsIn counts place's put rows in state.
func (p *pool) rowsIn(place, state string) int {
	p.t.Helper()
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM placement WHERE place = ? AND state = ? AND op = 'put'`, place, state).Scan(&n); err != nil {
		p.t.Fatal(err)
	}
	return n
}

// drainedBack counts the copies drain-back placed: rows at friends that
// replace a copy at an overflow target.
func (p *pool) drainedBack() int {
	p.t.Helper()
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM placement WHERE replaces GLOB 'overflow:*'`).Scan(&n); err != nil {
		p.t.Fatal(err)
	}
	return n
}

// scenario12 is scenario 12 scaled down: two friends at 10 units that
// cannot hold two copies of 15 units of data, so with a target placement
// is split, and each object is at one friend and the target.
func scenario12(t *testing.T) (*pool, string, []int64) {
	p := newPool(t)
	p.friend("A", 10)
	p.friend("B", 10)
	o := p.overflow("b2", 40)
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	var data []int64
	for i := range 15 {
		data = append(data, p.put("r", fmt.Sprintf("data/%02d", i), "data", 1))
	}
	p.settled()
	for _, d := range data {
		if at := p.places(d); len(at) != 2 || !slices.Contains(at, o) {
			t.Fatalf("row %d at %v, want one friend and the target", d, at)
		}
	}
	return p, o, data
}

// REPL-13, scenario 12's second half (#40): friend F joins with room for
// what the target holds. Every copy at the target moves to a friend,
// each deleted at the target only once its replacement at a friend is
// acknowledged; then the target's copies of the metadata go too, and
// the target holds nothing.
func TestDrainBackEmptiesTheTargetOnceAFriendJoins(t *testing.T) {
	p, o, data := scenario12(t)
	p.friend("F", 40)
	p.replace()
	if n := p.rowsIn("F", PlacementQueued); n == 0 {
		t.Fatal("nothing queued at F")
	}
	// Nothing at the target goes before its replacement is acknowledged.
	if n := p.rowsIn(o, PlacementAcked); n != len(data)+1 {
		t.Fatalf("%d copies acknowledged at the target before F acknowledged anything, want %d", n, len(data)+1)
	}
	if n := p.rowsIn(o, PlacementDropping); n != 0 {
		t.Fatalf("%d copies at the target being deleted before their replacements landed", n)
	}
	// One replacement acknowledged: its object's copy at the target goes,
	// and only that one.
	var first int64
	if err := p.db.QueryRow(`SELECT seq FROM placement WHERE place = 'F' AND replaces = ? ORDER BY seq LIMIT 1`, o).Scan(&first); err != nil {
		t.Fatal(err)
	}
	p.ack("F", first)
	p.wantState(o, first, PlacementDropping)
	if n := p.rowsIn(o, PlacementDropping); n != 1 {
		t.Fatalf("%d copies at the target being deleted after one replacement landed, want 1", n)
	}
	p.settled()
	for _, d := range data {
		at := p.places(d)
		if len(at) != 2 || slices.Contains(at, o) {
			t.Errorf("row %d at %v, want two friends", d, at)
		}
	}
	holds, err := p.db.OverflowHolds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h := holds["b2"]; h.DataObjects != 0 || h.Use != 0 {
		t.Fatalf("the target holds %+v after drain-back, want nothing", h)
	}
	if b, _ := p.db.BelowCopies(context.Background()); b["r"].Objects != 0 {
		t.Fatalf("below copies %+v", b)
	}
}

// REPL-13, #38 review's guard (keepReplaced): a drain-back copy
// acknowledged at a friend does not delete the copy at the target while
// the object would be left with fewer than copies acknowledged copies at
// intact places: here the other friend's replica failed a challenge
// meanwhile.
func TestADrainedTargetCopyStaysWhileTheObjectNeedsIt(t *testing.T) {
	p, o, data := scenario12(t)
	p.friend("F", 40)
	p.replace()
	d := data[0]
	var friend string
	for _, pl := range p.places(d) {
		if pl != o && pl != "F" {
			friend = pl
		}
	}
	if st, rep := p.rowOf("F", d); st != PlacementQueued || rep != o {
		t.Fatalf("F's row of %d: %s replacing %q", d, st, rep)
	}
	if err := p.db.RecordUnverified(context.Background(), friend, "r", "missing", p.now); err != nil {
		t.Fatal(err)
	}
	p.ack("F", d)
	p.wantState(o, d, PlacementAcked)
}

// REPL-13 (#40): drain-back has the lowest priority. In the friend's
// FIFO a copy moving back from a target comes after every other row,
// even newer ones, and holds back no metadata.
func TestDrainBackComesLastInTheFriendsFIFOAndHoldsNoBarrier(t *testing.T) {
	p, _, data := scenario12(t)
	p.friend("F", 40)
	p.replace()
	// F's copy of the bucket's metadata goes first.
	meta := p.rowsAt("F")[0]
	next, ok, err := p.db.NextPlacement(context.Background(), "F")
	if err != nil || !ok || next.Seq != meta || next.DrainBack {
		t.Fatalf("F's head %+v %t %v, want its metadata %d", next, ok, err, meta)
	}
	p.ack("F", meta)
	// A new snapshot: its index is not held back by the drain-back
	// copies written before it, and it and its pack go to F before them.
	fresh := p.put("r", "data/new", "data", 0.5)
	idx := p.put("r", "index/2", "metadata", 0.1)
	if !slices.Contains(p.rowsAt("F"), fresh) {
		t.Fatalf("data/new not placed at F: %v", p.places(fresh))
	}
	for _, want := range []int64{fresh, idx} {
		next, ok, err := p.db.NextPlacement(context.Background(), "F")
		if !ok || err != nil || next.Seq != want || next.DrainBack {
			t.Fatalf("F's head is %+v (%t %v), want %d before the drain-back copies", next, ok, err, want)
		}
		if want == fresh {
			p.ackAll(fresh) // wherever it is placed
			continue
		}
		if p.held(idx) {
			t.Fatal("drain-back copies queued before index/2 hold it back")
		}
		p.ack("F", want)
	}
	next, ok, err = p.db.NextPlacement(context.Background(), "F")
	if err != nil || !ok || !next.DrainBack || !slices.Contains(data, next.Seq) {
		t.Fatalf("F's head %+v %t %v, want a drain-back copy", next, ok, err)
	}
}

// The hysteresis between the split and drain-back (#40): the repository
// grows past what the friends hold at two copies (the split moves one
// friend copy of each object to the target), shrinks back below it, and
// grows again, many times within the window. No copy ever moves back
// from the target; only once the repository has stayed small for the
// whole window (DrainWindow) do the copies move back, and they then
// stay.
func TestTheSplitAndDrainBackNeverMoveACopyBackAndForth(t *testing.T) {
	p := newPool(t)
	p.friend("A", 10)
	p.friend("B", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	for i := range 9 {
		p.put("r", fmt.Sprintf("data/%02d", i), "data", 1)
	}
	p.settled()
	if n := p.rowsIn(o, PlacementAcked); n != 0 {
		t.Fatalf("%d copies at the target while the friends hold everything", n)
	}
	moved := func() map[int64]int { // each object's copies dropped at friends
		out := map[int64]int{}
		rows, err := p.db.Query(`SELECT seq, count(*) FROM placement WHERE place != ? AND state IN ('dropping', 'drained')
			AND op = 'put' GROUP BY seq`, o)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var s int64
			var n int
			rows.Scan(&s, &n)
			out[s] = n
		}
		return out
	}
	extra := 0
	for round := range 4 {
		// Grow past the threshold (9.9 units of data at two friends of
		// 10): split, and the friends' second copies move to the target.
		var grown []string
		for i := range 2 {
			k := fmt.Sprintf("data/x%d-%d", round, i)
			p.put("r", k, "data", 1)
			grown = append(grown, k)
			extra++
		}
		p.settled()
		for _, f := range []string{"A", "B"} {
			p.purge(f, 10)
		}
		p.settled()
		if len(moved()) == 0 {
			t.Fatalf("round %d: the split moved no friend copy to the target", round)
		}
		// Shrink below it again (a prune): the split turns off, and
		// within the window nothing moves back.
		for _, k := range grown {
			p.del("r", k, "data")
		}
		p.settled()
		for _, f := range []string{"A", "B"} {
			p.purge(f, 10)
		}
		p.settled()
		if n := p.drainedBack(); n != 0 {
			t.Fatalf("round %d: %d copies moved back from the target within the window", round, n)
		}
		if n := p.rowsIn(o, PlacementDropping) + p.rowsIn(o, PlacementDrained); n != 0 {
			t.Fatalf("round %d: %d copies left the target", round, n)
		}
		for s, n := range moved() {
			if n > 1 {
				t.Fatalf("round %d: row %d left friends %d times", round, s, n)
			}
		}
		p.now = p.now.Add(24 * time.Hour)
	}
	// The repository stays small for the whole window: the copies move
	// back to the friends, and then stay there.
	p.now = p.now.Add(DrainWindow)
	p.settled()
	for _, f := range []string{"A", "B"} {
		p.purge(f, 10)
	}
	p.settled()
	if n := p.drainedBack(); n == 0 {
		t.Fatal("nothing moved back once the window had passed")
	}
	holds, err := p.db.OverflowHolds(context.Background())
	if err != nil || holds["b2"].DataObjects != 0 {
		t.Fatalf("the target holds %+v (%v) once the window has passed", holds["b2"], err)
	}
	before := p.drainedBack()
	p.settled()
	holds, err = p.db.OverflowHolds(context.Background())
	if err != nil || holds["b2"].DataObjects != 0 || holds["b2"].Use != 0 || p.drainedBack() != before {
		t.Fatalf("copies moved again after drain-back: the target holds %+v (%v)", holds["b2"], err)
	}
}

// REPL-13 (#40): drain-back turning off (the repository grew past what
// the friends hold, so placement is split again) cancels its unsent
// copies, as the split's unsent moves are cancelled when it turns off;
// the copies at the target stay.
func TestAnUnsentDrainBackCopyIsCancelledWhenDrainBackTurnsOff(t *testing.T) {
	p, o, data := scenario12(t)
	p.friend("F", 40)
	p.replace()
	if p.drainedBack() == 0 {
		t.Fatal("no drain-back copy queued")
	}
	// 19.8 units of data is all A, B and F hold at two copies.
	for i := range 6 {
		p.put("r", fmt.Sprintf("data/more%d", i), "data", 1)
	}
	p.replace()
	for _, d := range data {
		if st, _ := p.rowOf("F", d); st == PlacementQueued {
			t.Fatalf("F's drain-back copy of %d still queued", d)
		}
		p.wantState(o, d, PlacementAcked)
	}
}

// S3-5 (#40): a held delete the target refuses for want of permission
// stays a tomb, counted in the target's use and as denied rather than
// held for the window, and is due again only a day later.
func TestARefusedHeldDeleteIsKeptCountedAndTriedAgainDaily(t *testing.T) {
	ctx := context.Background()
	p, target, _ := tombPool(t, t.TempDir())
	before := p.use(target)
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("held %v, %v", held, err)
	}
	p.ack(target, del)
	p.now = p.now.Add(24 * time.Hour)
	if ts := p.due(target); len(ts) != 1 {
		t.Fatalf("due %+v", ts)
	}
	if err := p.db.DenyOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	holds, err := p.db.OverflowTombHolds(ctx)
	if h := holds["garage"]; err != nil || h.Tombs != 0 || h.Denied != 1 || h.DeniedBytes != 2*unit || !h.NextDue.IsZero() {
		t.Fatalf("tomb holds %+v, %v", h, err)
	}
	if got := p.use(target); got != before {
		t.Fatalf("use %d, want %d: the object is still at the target", got, before)
	}
	p.now = p.now.Add(DeniedRetry - time.Second)
	if ts := p.due(target); len(ts) != 0 {
		t.Fatalf("tried again within the day: %+v", ts)
	}
	p.now = p.now.Add(time.Second)
	if ts := p.due(target); len(ts) != 1 || ts[0].Seq != del {
		t.Fatalf("not tried again after a day: %+v", ts)
	}
}
