package state

import (
	"context"
	"testing"
	"time"
)

// VER-1a (#105): a friend's replica is due a slice of its rolling full
// check at once, then a day after the last, under the daily challenge's
// rules: not while its FIFO is busy until more than a day overdue, not
// within the hour of an error, not while unverified. The pass's cursor
// and counts are kept in lendisk.db, so a restart goes on from them; a
// completed pass is the replica's AllCheckedAt and the next starts
// over; a failure ends the pass unfinished, and a passing full check
// counts as a completed one.
func TestFullCyclesAreDueDailyAndKeepTheirCursor(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 1)
	day := 24 * time.Hour
	due := func() []FullCycle {
		t.Helper()
		cs, err := p.db.DueCycles(ctx, p.now, day, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return cs
	}
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due with nothing acked: %+v", cs)
	}
	a := p.put("laptop", "data/a", "data", 0.1)
	p.ackAll(a)
	b := p.put("laptop", "data/b", "data", 0.1)
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due while replicating: %+v", cs)
	}
	p.ackAll(b)
	cs := due()
	if len(cs) != 1 || cs[0].Place != "B" || cs[0].Bucket != "laptop" || cs[0].Cycle != 0 || cs[0].After != 0 ||
		!cs[0].StartedAt.IsZero() {
		t.Fatalf("due %+v", cs)
	}
	if n, err := p.db.ChallengeableAfter(ctx, "B", "laptop", 0); err != nil || n != 2 {
		t.Fatalf("challengeable after 0: %d %v", n, err)
	}
	if n, err := p.db.ChallengeableAfter(ctx, "B", "laptop", a); err != nil || n != 1 {
		t.Fatalf("challengeable after the first: %d %v", n, err)
	}

	// A slice that got no answer keeps its progress and is not due
	// within the hour.
	c := cs[0]
	c.StartedAt, c.After, c.Checked, c.Skipped = p.now, a, 1, 0
	if err := p.db.SaveCycle(ctx, c, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := p.db.RecordCheckError(ctx, "B", "laptop", "network down", p.now); err != nil {
		t.Fatal(err)
	}
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due just after an error: %+v", cs)
	}
	p.now = p.now.Add(time.Hour)
	cs = due()
	if len(cs) != 1 || cs[0].After != a || cs[0].Checked != 1 || !cs[0].StartedAt.Equal(p.now.Add(-time.Hour)) {
		t.Fatalf("after the retry: %+v", cs)
	}

	// A slice that ended is due a day later.
	c = cs[0]
	if err := p.db.SaveCycle(ctx, c, p.now); err != nil {
		t.Fatal(err)
	}
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due just after a slice: %+v", cs)
	}
	p.now = p.now.Add(day)
	p.put("laptop", "data/c", "data", 0.1) // stays queued
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due a day later while replicating: %+v", cs)
	}
	p.now = p.now.Add(day + time.Minute)
	if cs := due(); len(cs) != 1 || cs[0].After != a {
		t.Fatalf("not due more than a day overdue: %+v", cs)
	}

	// Completed: AllCheckedAt, and the next pass starts over.
	if err := p.db.CompleteCycle(ctx, "B", "laptop", p.now); err != nil {
		t.Fatal(err)
	}
	rc, err := p.db.ReplicaChecks(ctx)
	if err != nil || len(rc) != 1 || !rc[0].AllCheckedAt.Equal(p.now) || !rc[0].FullAt.IsZero() {
		t.Fatalf("checks %+v %v", rc, err)
	}
	p.now = p.now.Add(2*day + time.Minute)
	if cs := due(); len(cs) != 1 || cs[0].After != 0 || !cs[0].StartedAt.IsZero() || cs[0].Checked != 0 {
		t.Fatalf("after a completed pass: %+v", cs)
	}

	// A failure ends the pass unfinished; the replica is not due while
	// unverified; a passing full check counts as a completed pass.
	c = cs[0]
	c.StartedAt, c.After, c.Checked = p.now, a, 1
	if err := p.db.SaveCycle(ctx, c, p.now); err != nil {
		t.Fatal(err)
	}
	if err := p.db.RecordUnverified(ctx, "B", "laptop", "lost data/b", p.now); err != nil {
		t.Fatal(err)
	}
	p.now = p.now.Add(3 * day)
	if cs := due(); len(cs) != 0 {
		t.Fatalf("an unverified replica is due: %+v", cs)
	}
	if err := p.db.RecordVerified(ctx, "B", "laptop", true, p.now); err != nil {
		t.Fatal(err)
	}
	rc, _ = p.db.ReplicaChecks(ctx)
	if !rc[0].AllCheckedAt.Equal(p.now) || !rc[0].FullAt.Equal(p.now) {
		t.Fatalf("after a full check: %+v", rc[0])
	}
	if cs := due(); len(cs) != 1 || cs[0].After != 0 || !cs[0].StartedAt.IsZero() {
		t.Fatalf("after the full check: %+v", cs)
	}
}

// VER-1a, VER-2 (#105): an overflow target has a rolling full check
// only once given one with `overflow set --full-cycle`, as each object
// it checks is downloaded and costs egress; its cycle is its own.
func TestATargetsFullCycleIsOptIn(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 1)
	place := p.overflow("g", 10)
	s := p.put("laptop", "data/a", "data", 0.1)
	if err := p.db.AckPlacement(ctx, place, s, "h-data/a", p.now); err != nil {
		t.Fatal(err)
	}
	due := func() []FullCycle {
		t.Helper()
		cs, err := p.db.DueCycles(ctx, p.now, 24*time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return cs
	}
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due by default: %+v", cs)
	}
	o, err := p.db.GetOverflow(ctx, "g")
	if err != nil || o.FullCycle != 0 {
		t.Fatalf("target %+v %v", o, err)
	}
	o.FullCycle = 90 * 24 * time.Hour
	if _, err := p.db.UpdateOverflow(ctx, o, "", p.now); err != nil {
		t.Fatal(err)
	}
	if o, _ := p.db.GetOverflow(ctx, "g"); o.FullCycle != 90*24*time.Hour {
		t.Fatalf("full cycle %v", o.FullCycle)
	}
	if cs := due(); len(cs) != 1 || cs[0].Place != place || cs[0].Cycle != 90*24*time.Hour {
		t.Fatalf("due once opted in: %+v", cs)
	}
	o.FullCycle = 0
	if _, err := p.db.UpdateOverflow(ctx, o, "", p.now); err != nil {
		t.Fatal(err)
	}
	if cs := due(); len(cs) != 0 {
		t.Fatalf("due once off again: %+v", cs)
	}
}
