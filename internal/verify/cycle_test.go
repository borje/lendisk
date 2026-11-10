package verify

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
)

const day = 24 * time.Hour

// cycleHours runs the rolling full check every hour of the injected
// clock for d, stopping early when stop says so; it returns the time
// stop first held (zero if never).
func (n *node) cycleHours(d time.Duration, stop func() bool) time.Time {
	n.t.Helper()
	for end := n.clock().Add(d); !n.clock().After(end); n.advance(time.Hour) {
		n.v.cycles(n.t.Context())
		if stop != nil && stop() {
			return n.clock()
		}
	}
	return time.Time{}
}

// wholeAsked counts the whole-object challenges B was sent, by key.
func (n *node) wholeAsked() map[string]int {
	out := map[string]int{}
	for _, r := range n.b.requests() {
		if !r.Whole {
			n.t.Fatalf("a ranged challenge from the full check: %+v", r)
		}
		out[r.Key]++
	}
	return out
}

// VER-1a (#105): a friend that silently loses one object of many, just
// after the rolling full check passed it, is found within one cycle on
// the injected clock: the replica is marked unverified, naming the
// object, and the alert service is told, as for the daily challenge.
func TestASmallLossIsFoundWithinOneCycle(t *testing.T) {
	n := newNode(t, 100)
	n.v.FullCycle = 10 * day
	n.v.cycles(t.Context())
	first := n.wholeAsked()
	if len(first) != 10 {
		t.Fatalf("the first slice challenged %d objects, want 10 of 100 over 10 days", len(first))
	}
	var lost string
	for k := range first {
		lost = k
		break
	}
	delete(n.b.held, lost) // a bad sector, behind the cursor
	at := n.clock()
	found := n.cycleHours(11*day, func() bool { return n.record().Unverified() })
	if found.IsZero() || found.Sub(at) > n.v.FullCycle {
		t.Fatalf("lost at %v, found at %v: not within the cycle of %v", at, found, n.v.FullCycle)
	}
	if c := n.record(); !strings.Contains(c.Detail, lost) || n.alerts.Load() != 1 || !c.AllCheckedAt.Equal(at.Add(9*day)) {
		t.Fatalf("record %+v, %d alerts", c, n.alerts.Load())
	}
}

// VER-1a (#105): the rolling full check challenges every object at the
// friend whole exactly once a cycle, a slice a day of what the pass has
// left over the days left, so an object written mid-pass joins it. Its
// cursor is in lendisk.db: a new verifier (a restart) goes on where the
// last stopped. A completed pass is recorded, and the next starts over.
func TestTheFullCycleCoversEveryObjectOnceACycle(t *testing.T) {
	n := newNode(t, 40)
	n.v.FullCycle = 20 * day
	start := n.clock()
	n.cycleHours(5*day-time.Hour, nil) // 5 slices of 2
	if got := len(n.wholeAsked()); got != 10 {
		t.Fatalf("%d objects after 5 days, want 10", got)
	}
	n.v = &Verifier{DB: n.db, StoreDir: n.store, Self: n.self, Call: n.b.call, Now: n.clock, Log: n.v.Log,
		FullCycle: 20 * day}
	body := make([]byte, 500)
	rand.Read(body)
	n.put("data/written-mid-pass", body)
	n.cycleHours(15*day-time.Hour, nil)
	asked := n.wholeAsked()
	if len(asked) != 41 {
		t.Fatalf("%d of 41 objects challenged in the cycle", len(asked))
	}
	for k, c := range asked {
		if c != 1 {
			t.Fatalf("%s challenged %d times in one cycle", k, c)
		}
	}
	c := n.record()
	if c.AllCheckedAt.IsZero() || c.AllCheckedAt.After(start.Add(20*day)) || c.Unverified() || n.alerts.Load() != 0 {
		t.Fatalf("record %+v", c)
	}
	n.cycleHours(20*day-time.Hour, nil)
	for k, c := range n.wholeAsked() {
		if c != 2 {
			t.Fatalf("%s challenged %d times in two cycles", k, c)
		}
	}
}

// VER-1a (#105): a slice's verdicts are the daily challenge's. No answer
// decides nothing: the error is recorded, the cursor kept, and the
// slice goes on an hour later. An object whose local copy is gone is
// skipped and counted, and the pass still completes. A failed proof
// marks the replica unverified and alerts, and an unverified replica
// gets no slices.
func TestAFullCycleSliceFollowsTheChallengeVerdicts(t *testing.T) {
	ctx := t.Context()
	n := newNode(t, 6)
	n.v.FullCycle = 3 * day
	n.v.cycles(ctx)
	if got := len(n.wholeAsked()); got != 2 {
		t.Fatalf("first slice: %d", got)
	}

	n.advance(day)
	n.b.fail = errors.New("connection reset by peer")
	n.v.cycles(ctx)
	if c := n.record(); c.Unverified() || c.Error == "" || n.alerts.Load() != 0 {
		t.Fatalf("after no answer: %+v", c)
	}
	n.b.fail = nil
	n.advance(30 * time.Minute)
	before := len(n.b.requests())
	n.v.cycles(ctx)
	if len(n.b.requests()) != before {
		t.Fatal("retried within the hour")
	}
	n.advance(30 * time.Minute)
	os.Remove(filepath.Join(n.store, "laptop", "data/c")) // the third object: this slice's
	n.v.cycles(ctx)
	asked := n.wholeAsked()
	// data/c was asked once, in the slice that got no answer; skipped
	// since, it was not asked again.
	if asked["data/a"] != 1 || asked["data/b"] != 1 || asked["data/c"] != 1 || asked["data/d"] != 1 || asked["data/e"] != 0 {
		t.Fatalf("after the retry: %v", asked)
	}
	n.advance(day)
	n.v.cycles(ctx)
	c := n.record()
	if c.AllCheckedAt != n.clock() || c.Unverified() {
		t.Fatalf("a pass that skipped one object: %+v", c)
	}

	n.advance(day)
	n.b.fail = &control.CallError{Status: 404, Code: control.CodeNoSuchKey}
	n.v.cycles(ctx)
	if c := n.record(); !c.Unverified() || !strings.Contains(c.Detail, "data/a") || n.alerts.Load() != 1 {
		t.Fatalf("after a failed proof: %+v", c)
	}
	n.b.fail = nil
	before = len(n.b.requests())
	n.advance(2 * day)
	n.v.cycles(ctx)
	if len(n.b.requests()) != before {
		t.Fatal("an unverified replica got a slice")
	}
}

// VER-1a, VER-2 (#105): an overflow target gets no rolling full check
// unless given a cycle of its own (overflow set --full-cycle): each of
// its objects would be downloaded. Given one, every object is
// downloaded once that cycle.
func TestATargetsFullCycleIsOptInAndDownloads(t *testing.T) {
	ctx := t.Context()
	n, pr := targetNode(t, 6, 1000)
	n.cycleHours(4*day, nil)
	if pr.served() != 0 {
		t.Fatalf("downloaded %d bytes with no cycle set", pr.served())
	}
	o, err := n.db.GetOverflow(ctx, "g")
	if err != nil {
		t.Fatal(err)
	}
	o.FullCycle = 3 * day
	if _, err := n.db.UpdateOverflow(ctx, o, "", n.clock()); err != nil {
		t.Fatal(err)
	}
	n.cycleHours(3*day-time.Hour, nil)
	if pr.served() != 6000 {
		t.Fatalf("downloaded %d bytes in the cycle, want every object once (6000)", pr.served())
	}
	if c := n.targetRecord(); c.AllCheckedAt.IsZero() || c.Unverified() {
		t.Fatalf("record %+v", c)
	}
}
