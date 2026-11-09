package state

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// moveThenBreak runs the #38 review's probe: three friends at 2 units,
// objects at two of them until they are full, then a target is added
// and re-placement moves one friend copy of data/1 to it. Before the
// target acknowledges, broken makes the other friend copy not count.
// The friend copy being moved must then stay: dropping it would leave
// fewer than copies.
func moveThenBreak(t *testing.T, broken func(p *pool, other string)) {
	p := newPool(t)
	ctx := context.Background()
	for _, f := range []string{"A", "B", "C"} {
		p.friend(f, 2)
	}
	p.bucket("r", 2)
	d1 := p.put("r", "data/1", "data", 1)
	p.ackQueued()
	for i := 2; i <= 4; i++ {
		p.put("r", fmt.Sprintf("data/%d", i), "data", 1)
	}
	p.ackQueued()
	o := p.overflow("b2", 100)
	if _, err := p.db.Replace(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	var moved string
	if err := p.db.QueryRow(`SELECT coalesce(replaces, '') FROM placement WHERE seq = ? AND place = ? AND state = 'queued'`,
		d1, o).Scan(&moved); err != nil || moved == "" {
		t.Fatalf("no move of data/1 to the target: %q %v", moved, err)
	}
	var other string
	for _, pl := range p.places(d1) {
		if pl != o && pl != moved {
			other = pl
		}
	}
	broken(p, other)
	if _, err := p.db.Replace(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	p.ack(o, d1)
	var st string
	if err := p.db.QueryRow(`SELECT state FROM placement WHERE seq = ? AND place = ?`, d1, moved).Scan(&st); err != nil || st != PlacementAcked {
		t.Fatalf("the moved friend copy at %s is %q (%v), want acked: the target alone would hold data/1", moved, st, err)
	}
}

// #38 review: a move's friend copy is dropped only while copies are left.
func TestAMoveKeepsTheFriendCopyWhenTheOtherFriendIsRemoved(t *testing.T) {
	moveThenBreak(t, func(p *pool, other string) {
		p.condition(other, ConditionRemoved, p.now.Add(-time.Minute), p.now.Add(-time.Hour))
	})
}

func TestAMoveKeepsTheFriendCopyWhenTheOtherFriendFailsVerification(t *testing.T) {
	moveThenBreak(t, func(p *pool, other string) {
		if err := p.db.RecordUnverified(context.Background(), other, "r", "wrong answer", p.now); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAMoveKeepsTheFriendCopyWhenTheOtherFriendIsGone(t *testing.T) {
	moveThenBreak(t, func(p *pool, other string) {
		p.condition(other, ConditionRetrying, p.now.Add(-8*24*time.Hour), p.now.Add(-8*24*time.Hour))
	})
}

// #38 review: moves never take a target's room from objects with no
// copy: a split bucket's pass places missing copies first and moves
// with what room is left.
func TestMovesNeverStarveObjectsWithNoCopy(t *testing.T) {
	p := newPool(t)
	for _, f := range []string{"A", "B", "C", "D"} {
		p.friend(f, 10)
	}
	p.bucket("r", 2)
	var data []int64
	for i := range 30 {
		data = append(data, p.put("r", fmt.Sprintf("data/%02d", i), "data", 1))
	}
	p.ackQueued()
	zero := 0
	for _, d := range data {
		if len(p.places(d)) == 0 {
			zero++
		}
	}
	if zero == 0 {
		t.Fatal("no object without a copy to start with")
	}
	p.overflow("b2", 12)
	if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, d := range data {
		if len(p.places(d)) == 0 {
			t.Fatalf("row %d still has no copy, with %d objects that had none and a target of 12 units", d, zero)
		}
	}
}

// #38 review: a copies-1 bucket is not split: its copy goes to a friend
// with room, and to a target only when none has.
func TestACopiesOneBucketUsesFriendsFirst(t *testing.T) {
	p := newPool(t)
	p.friend("A", 2)
	p.friend("B", 2)
	p.bucket("big", 2)
	p.bucket("one", 1)
	for i := range 4 {
		p.put("big", fmt.Sprintf("data/%d", i), "data", 1)
	}
	p.overflow("b2", 100)
	p.friend("C", 50)
	d := p.put("one", "data/x", "data", 0.1)
	p.wantPlaces("a copies-1 object", d, "C")
	big := p.put("big", "data/y", "data", 0.1)
	if at := p.places(big); len(at) != 2 || !slices.Contains(at, "overflow:b2") {
		t.Fatalf("a copies-2 object at %v, want a friend and the target", at)
	}
}

// #38 review: placeTx returns what fails in choosing places; it was
// lost to a shadowed err.
func TestAFailureChoosingPlacesFailsTheCapture(t *testing.T) {
	p := newPool(t)
	p.friend("A", 100)
	p.bucket("r", 2)
	if _, err := p.db.Exec(`DROP TABLE replica_check`); err != nil { // choose reads it
		t.Fatal(err)
	}
	ctx := context.Background()
	seqs, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: "r", Key: "data/1", Op: OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.FinishPut(ctx, seqs[0], 100, "h"); err == nil {
		t.Fatal("the capture succeeded with placement failing")
	}
}

// #38 review: the split decision follows the pool at once, whichever
// process changed it: friends with room join, and new data is no longer
// split, however long the answer is cached.
func TestTheSplitFollowsThePoolAtOnce(t *testing.T) {
	p := newPool(t)
	p.db.splitTTL = time.Hour // the daemon's cache, kept long
	p.friend("A", 3)
	p.bucket("r", 2)
	p.overflow("b2", 100)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("split", d, "A", "overflow:b2")
	p.friend("B", 100)
	p.friend("C", 100)
	d2 := p.put("r", "data/2", "data", 1)
	if at := p.places(d2); slices.Contains(at, "overflow:b2") {
		t.Fatalf("data after friends with room joined at %v", at)
	}
}
