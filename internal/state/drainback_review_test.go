package state

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// #40 review: once the target holds no live data of a bucket, its
// metadata of the bucket is dropped once, and later passes do not place
// it there again: an old put row of a key deleted since does not count
// as data the target holds, for placing the metadata or for dropping it.
func TestAnIdleTargetsMetadataIsNotPlacedAndDroppedOnEveryPass(t *testing.T) {
	p, o, _ := scenario12(t)
	p.del("r", "data/00", "data") // its put row at the target stays, acknowledged
	p.settled()
	p.friend("F", 40)
	p.settled()
	for i := range 6 {
		s := p.replace()
		p.ackQueued()
		var n int
		if err := p.db.QueryRow(`SELECT count(*) FROM placement p JOIN queue q ON q.seq = p.seq WHERE p.place = ?
			AND q.class IN ('metadata', 'snapshot') AND p.state IN ('queued', 'acked', 'dropping')`, o).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 || s.Placed != 0 || s.Dropping != 0 || s.Cancelled != 0 {
			t.Fatalf("pass %d after drain-back: %+v, %d metadata rows at the idle target", i, s, n)
		}
	}
	holds, err := p.db.OverflowHolds(context.Background())
	if err != nil || holds["b2"].Use != 0 {
		t.Fatalf("the target holds %+v (%v)", holds["b2"], err)
	}
}

// #40 review: a friend copy that once moved an object's copy back from
// the target keeps no claim on the target's copy placed there again
// later (by the split): when drain-back runs again, that copy counts as
// one of the object's copies, the object is not below copies, and what
// drain-back queues is a drain-back copy.
func TestAStaleDrainBackPointerDoesNotCountATargetCopyOut(t *testing.T) {
	p, o, data := scenario12(t)
	quotas := map[string]float64{"A": 10, "B": 10, "F": 40}
	purgeAll := func() {
		for f, q := range quotas {
			p.purge(f, q)
		}
	}
	p.friend("F", 40)
	p.settled()
	purgeAll()
	p.settled()
	// The repository grows past what A, B and F hold: the split moves
	// copies to the target again.
	var more []string
	for i := range 8 {
		k := fmt.Sprintf("data/grow%d", i)
		p.put("r", k, "data", 1)
		more = append(more, k)
	}
	for range 3 {
		p.settled()
		purgeAll()
	}
	back := 0
	for _, d := range data {
		st, _ := p.rowOf(o, d)
		if st != PlacementAcked {
			continue
		}
		back++
		for _, f := range []string{"A", "B", "F"} {
			if _, rep := p.rowOf(f, d); rep == o {
				t.Fatalf("row %d: %s's copy still names the target's copy, placed there again, as the one it replaces", d, f)
			}
		}
	}
	if back == 0 {
		t.Fatal("the split moved no copy back to the target")
	}
	// It shrinks, and the window passes: drain-back runs again.
	for _, k := range more {
		p.del("r", k, "data")
	}
	p.settled()
	p.now = p.now.Add(DrainWindow + 48*time.Hour)
	purgeAll()
	s := p.replace()
	if s.Below != 0 {
		t.Fatalf("objects at copies counted below copies: %+v", s)
	}
	for _, d := range data {
		var n int
		if err := p.db.QueryRow(`SELECT count(*) FROM placement WHERE seq = ? AND state = 'queued'
			AND coalesce(replaces, '') NOT GLOB 'overflow:*'`, d).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("row %d: a plain copy queued, at %v; want only drain-back copies", d, p.places(d))
		}
	}
}

// #40 review: repository_peak under a clock that jumps. A jump ahead
// deletes none of the history, and the sample taken while the clock ran
// ahead counts from today once it is back, so it ages out with the
// window instead of holding drain-back back for years.
func TestTheRepositoryPeakSurvivesAClockThatJumps(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("A", 100)
	p.overflow("b2", 100)
	p.bucket("r", 1)
	start := p.now
	drain := func() bool {
		t.Helper()
		ok, err := p.db.overflowDrain(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	p.put("r", "data/big", "data", 150) // more than A holds
	if drain() {
		t.Fatal("drain-back while A cannot hold the repository")
	}
	p.del("r", "data/big", "data")
	// The clock jumps two years ahead, and back.
	p.now = start.Add(2 * 365 * 24 * time.Hour)
	drain()
	p.now = start
	if drain() {
		t.Fatal("the jump ahead deleted the day the repository was largest")
	}
	var future int
	if err := p.db.QueryRow(`SELECT count(*) FROM repository_peak WHERE day > ?`, peakDay(p.now)).Scan(&future); err != nil || future != 0 {
		t.Fatalf("%d days after today kept (%v)", future, err)
	}
	// Past the window, nothing from before or from ahead holds it back.
	p.now = start.Add(DrainWindow + 2*24*time.Hour)
	if !drain() {
		t.Fatal("drain-back held back past the window")
	}
}
