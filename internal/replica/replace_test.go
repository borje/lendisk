package replica

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

func (fx *fixture) rowState(place string, seq int64) string {
	fx.t.Helper()
	var s string
	fx.db.QueryRow(`SELECT state FROM placement WHERE place = ? AND seq = ?`, place, seq).Scan(&s)
	return s
}

// EP-6, REPL-13 (#26): the worker re-places as soon as the pool changes.
// Raising a bucket's copies sends a second copy, read from the store;
// lowering it again deletes the surplus copy at the friend, which the
// friend then no longer holds, and the row is drained.
func TestTheWorkerAddsAndRemovesCopiesWhenCopiesChange(t *testing.T) {
	fx := newFixture(t, "B", "C", "D")
	fx.bucket("laptop", 1)
	seq := fx.put("laptop", "data/1", "data", "pack one")
	fx.run()
	waitFor(t, "data/1 at B", func() bool { return fx.holds("B", "laptop", "data/1") })
	if err := fx.db.SetCopies(t.Context(), "laptop", 2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a second copy at C", func() bool { return fx.holds("C", "laptop", "data/1") })
	if err := fx.db.SetCopies(t.Context(), "laptop", 1); err != nil {
		t.Fatal(err)
	}
	// B and C are as full as each other: the later node ID goes.
	waitFor(t, "the surplus copy deleted at C", func() bool {
		return !fx.holds("C", "laptop", "data/1") && fx.rowState("C", seq) == state.PlacementDrained
	})
	if !fx.holds("B", "laptop", "data/1") || fx.holds("D", "laptop", "data/1") {
		t.Fatal("data/1 should stay at B only")
	}
}

// REPL-13, PEER-5 (#26, from the #25 review): removing a friend
// triggers re-placement of its copies at once; no lender forwards them
// (PEER-11): the new copy is read from this node's store.
func TestRemovingAFriendReplacesItsCopies(t *testing.T) {
	fx := newFixture(t, "B", "C", "D")
	fx.bucket("laptop", 2)
	fx.put("laptop", "data/1", "data", "pack one")
	fx.run()
	waitFor(t, "data/1 at B and C", func() bool {
		return fx.holds("B", "laptop", "data/1") && fx.holds("C", "laptop", "data/1")
	})
	if _, err := fx.db.RemoveFriendship(t.Context(), "C", false, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data/1 re-placed at D", func() bool { return fx.holds("D", "laptop", "data/1") })
	o, err := fx.db.Offsite(t.Context(), "laptop", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "offsite again", func() bool {
		o, err = fx.db.Offsite(t.Context(), "laptop", 0)
		return err == nil && o.Complete()
	})
}

// REPL-13 (#26): the pass also runs hourly, for what no pool change
// announces: here a friend refuses a copy for good, and an hour later
// the object is placed at another friend.
func TestReplacementRunsHourly(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("laptop", 1)
	var mu sync.Mutex
	now := time.Now()
	fx.w.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	fx.places["B"].fail = func(op, key string) error { return ErrRejected }
	seq := fx.put("laptop", "data/1", "data", "pack one")
	fx.run()
	waitFor(t, "B refuses data/1", func() bool { return fx.rowState("B", seq) == state.PlacementRefused })
	time.Sleep(100 * time.Millisecond) // many worker loops: no pool change, no pass
	if fx.rowState("C", seq) != "" {
		t.Fatal("re-placed before the hour")
	}
	mu.Lock()
	now = now.Add(61 * time.Minute)
	mu.Unlock()
	waitFor(t, "data/1 at C", func() bool { return fx.holds("C", "laptop", "data/1") })
}

// REPL-13 (#26 review): a pass that failed is run again at the next
// re-routing tick, not an hour later.
func TestAFailedPassIsRetriedAtTheNextTick(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("laptop", 1)
	fx.w.RerouteEvery = 20 * time.Millisecond
	fx.put("laptop", "data/1", "data", "pack one")
	if _, err := fx.db.Exec(`CREATE TRIGGER fail BEFORE INSERT ON placement WHEN NEW.place = 'C'
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	fx.run()
	waitFor(t, "data/1 at B", func() bool { return fx.holds("B", "laptop", "data/1") })
	if err := fx.db.SetCopies(t.Context(), "laptop", 2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // the pass for the change fails
	if _, err := fx.db.Exec(`DROP TRIGGER fail`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data/1 at C", func() bool { return fx.holds("C", "laptop", "data/1") })
}

// REPL-13 (#26): a pass and the worker's sending set share the worker's
// lock, so a row being sent is never cancelled as surplus.
func TestASurplusCopyBeingSentIsNotCancelled(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("laptop", 2)
	fx.places["C"].hold = make(chan struct{})
	fx.w.RerouteEvery = 20 * time.Millisecond // when a pass skipped a row, the next tick looks again
	seq := fx.put("laptop", "data/1", "data", "pack one")
	fx.run()
	waitFor(t, "C sending data/1", func() bool {
		fx.w.flight.Lock()
		defer fx.w.flight.Unlock()
		return fx.w.sending["C"] == seq
	})
	waitFor(t, "B holds data/1", func() bool { return fx.holds("B", "laptop", "data/1") })
	if err := fx.db.SetCopies(context.Background(), "laptop", 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if s := fx.rowState("C", seq); s != state.PlacementQueued {
		t.Fatalf("C's row in flight is %s", s)
	}
	close(fx.places["C"].hold)
	// Acknowledged at C, it is now a surplus acknowledged copy: one of
	// the two is deleted.
	waitFor(t, "one copy left", func() bool {
		return fx.holds("B", "laptop", "data/1") != fx.holds("C", "laptop", "data/1")
	})
}
