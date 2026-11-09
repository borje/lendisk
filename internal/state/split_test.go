package state

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// ackQueued acknowledges every queued or dropping row, as the places would.
func (p *pool) ackQueued() {
	p.t.Helper()
	rows, err := p.db.Query(`SELECT p.place, p.seq, coalesce(q.sha256, '') FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.state IN ('queued', 'dropping') ORDER BY p.seq`)
	if err != nil {
		p.t.Fatal(err)
	}
	type row struct {
		place string
		seq   int64
		sha   string
	}
	var rs []row
	for rows.Next() {
		var r row
		rows.Scan(&r.place, &r.seq, &r.sha)
		rs = append(rs, r)
	}
	rows.Close()
	for _, r := range rs {
		if err := p.db.AckPlacement(context.Background(), r.place, r.seq, r.sha, p.now); err != nil {
			p.t.Fatal(err)
		}
	}
}

// REPL-12 as of v0.11.5, scenario 12 (#38, Q-AD): four friends of equal
// quotas that cannot hold two copies of the repository. Data captured
// before the target was added sits at two friends until they are full,
// and then nowhere. Once a target is added, placement is split: each
// object at one friend and the target. Re-placement moves one friend's
// copy of each doubly held object to the target, which frees friend room
// for the objects that had none, and every object ends at one friend and
// the target; new captures go there too.
func TestSplitPlacementGivesEveryObjectOneFriendAndTheTarget(t *testing.T) {
	p := newPool(t)
	for _, f := range []string{"A", "B", "C", "D"} {
		p.friend(f, 10)
	}
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	var data []int64
	for i := range 30 {
		data = append(data, p.put("r", fmt.Sprintf("data/%02d", i), "data", 1))
	}
	p.ackQueued()
	if b, _ := p.db.BelowCopies(context.Background()); b["r"].Objects == 0 {
		t.Fatal("the friends held two copies of everything without a target")
	}
	o := p.overflow("b2", 40)
	for range 4 { // move, acknowledge, place, acknowledge
		if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
			t.Fatal(err)
		}
		p.ackQueued()
	}
	data = append(data, p.put("r", "data/new", "data", 1))
	p.ackQueued()
	for _, d := range data {
		at := p.places(d)
		if len(at) != 2 || !slices.Contains(at, o) {
			t.Errorf("row %d at %v, want one friend and the target", d, at)
		}
	}
	if b, _ := p.db.BelowCopies(context.Background()); b["r"].Objects != 0 {
		t.Fatalf("below copies %+v", b)
	}
}

// While friends hold copies of everything, placement is not split.
func TestNoSplitWhileFriendsHoldEverything(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	p.friend("B", 100)
	p.overflow("b2", 100)
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data with room at two friends", d, "A", "B")
}

// #38 review: a friend lacking some of the bucket's metadata is room
// all the same: its metadata is queued there first, and the copy does
// not fall through to an overflow target.
func TestAFriendLackingMetadataIsRoomNotOverflow(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	p.bucket("r", 2)
	m := p.put("r", "index/1", "metadata", 0.1)
	p.overflow("b2", 100)
	p.friend("B", 100) // added later: lacks index/1
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data", d, "A", "B")
	p.wantPlaces("metadata, queued at B with it", m, "A", "B")
	if got := p.rowsAt("B"); !slices.Equal(got, []int64{m, d}) {
		t.Fatalf("B's FIFO %v", got)
	}
}

// #38 review: a failing overflow target is no candidate, at capture or
// in re-placement, so its queued copies cannot hold the barrier.
func TestAFailingOverflowTargetIsNotChosen(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	o := p.overflow("b2", 100)
	p.condition(o, ConditionRetrying, p.now.Add(-2*time.Hour), time.Time{})
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data", d, "A")
	if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	p.wantPlaces("data after re-placement", d, "A")
}

// #38 review: re-placement re-reads a target's use in its transaction,
// so a capture between its scan and its batch cannot take it past
// max_bytes.
func TestReplacementAndCaptureNeverPassMaxBytes(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	p.bucket("r", 2)
	p.bucket("s", 2)
	d := p.put("r", "data/1", "data", 2)
	p.overflow("b2", 3)
	raced := false
	p.db.beforeApply = func(bucket string, meta bool) {
		if bucket == "r" && !meta && !raced {
			raced = true
			p.put("s", "data/x", "data", 2) // takes the target's room
		}
	}
	if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !raced {
		t.Fatal("no batch ran")
	}
	p.wantPlaces("data the target no longer has room for", d, "A")
	holds, err := p.db.OverflowHolds(context.Background())
	if err != nil || holds["b2"].Use > 3*unit {
		t.Fatalf("target use %d of %d (%v)", holds["b2"].Use, 3*unit, err)
	}
}
