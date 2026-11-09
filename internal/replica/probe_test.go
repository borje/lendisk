package replica

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// clock is a test clock that moves only when told.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_800_000_000, 0)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// prober is an in-memory place that answers the reachability probe
// (#65) with the error set by answer, counting the probes.
type prober struct {
	*place
	probes atomic.Int32
	emu    sync.Mutex
	err    error
}

func (p *prober) Probe(ctx context.Context) error {
	p.probes.Add(1)
	p.emu.Lock()
	defer p.emu.Unlock()
	return p.err
}

// answer sets what the probes answer from now on.
func (p *prober) answer(err error) {
	p.emu.Lock()
	p.err = err
	p.emu.Unlock()
}

// probed gives every friend of fx a prober and the worker clk.
func (fx *fixture) probed(clk *clock) map[string]*prober {
	ps := map[string]*prober{}
	for name, pl := range fx.places {
		ps[name] = &prober{place: pl}
	}
	fx.w.Now = clk.now
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return ps[f.Peer], nil }
	return ps
}

// still waits polls of the sender and checks the count has not moved.
func (fx *fixture) still(what string, n int32, got func() int32) {
	fx.t.Helper()
	time.Sleep(20 * fx.w.Poll)
	if g := got(); g != n {
		fx.t.Fatalf("%s: %d probes, want %d", what, g, n)
	}
}

// OPS-2, D6 (#65): a friend with nothing to send is probed at once and
// then at most every ProbeEvery while nothing else reached it. A probe
// answered advances last_ok and makes the place ok; one failing with a
// network error makes it retrying, and one refused as no friend removed,
// though nothing is queued.
func TestAnIdleFriendIsProbedAtMostEveryInterval(t *testing.T) {
	fx := newFixture(t, "B")
	clk := newClock()
	ps := fx.probed(clk)
	b := ps["B"]
	fx.run()
	waitFor(t, "the first probe", func() bool { return fx.condition("B").Condition == state.ConditionOK })
	if c := fx.condition("B"); !c.LastOK.Equal(clk.now()) || b.probes.Load() != 1 {
		t.Fatalf("after the first probe: %+v, %d probes", c, b.probes.Load())
	}
	fx.still("within the interval", 1, b.probes.Load)
	clk.advance(14 * time.Minute)
	fx.still("after 14 minutes", 1, b.probes.Load)
	clk.advance(time.Minute)
	waitFor(t, "the second probe", func() bool { return fx.condition("B").LastOK.Equal(clk.now()) })
	answered := clk.now()

	b.answer(errors.New("dial tcB: i/o timeout"))
	clk.advance(15 * time.Minute)
	waitFor(t, "retrying", func() bool { return fx.condition("B").Condition == state.ConditionRetrying })
	if c := fx.condition("B"); !c.LastOK.Equal(answered) || !c.Since.Equal(clk.now()) || c.Detail == "" {
		t.Fatalf("after a failed probe: %+v", c)
	}
	n := b.probes.Load()
	fx.still("after a failed probe", n, b.probes.Load)

	b.answer(ErrRemoved)
	clk.advance(15 * time.Minute)
	waitFor(t, "removed", func() bool { return fx.condition("B").Condition == state.ConditionRemoved })
	if c := fx.condition("B"); !c.RefusedAt.Equal(clk.now()) {
		t.Fatalf("after a refused probe: %+v", c)
	}

	b.answer(nil)
	clk.advance(15 * time.Minute)
	waitFor(t, "ok again", func() bool { return fx.condition("B").Condition == state.ConditionOK })
	if c := fx.condition("B"); !c.LastOK.Equal(clk.now()) || !c.FailingSince.IsZero() {
		t.Fatalf("after it answered again: %+v", c)
	}
}

// #65: the probe never competes with replication: none while a send is
// in flight, however long it takes, and a send that reached the friend
// counts as contact, so the next probe is an interval after it.
func TestNoProbeWhileASendIsInFlight(t *testing.T) {
	fx := newFixture(t, "B")
	clk := newClock()
	b := fx.probed(clk)["B"]
	b.hold = make(chan struct{})
	fx.write("laptop", "data/1", "one") // before the sender starts: it is never idle
	fx.run()
	waitFor(t, "the send in flight", func() bool {
		b.place.mu.Lock()
		defer b.place.mu.Unlock()
		return b.inFlight == 1
	})
	for i := 0; i < 4; i++ {
		clk.advance(time.Hour)
		fx.still("while the send is in flight", 0, b.probes.Load)
	}
	close(b.hold)
	fx.drained("B")
	waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
	fx.still("right after the send", 0, b.probes.Load)
	clk.advance(15 * time.Minute)
	waitFor(t, "a probe an interval after the send", func() bool { return b.probes.Load() == 1 })
}

// #65: a friend whose FIFO waits at the metadata barrier sends nothing,
// and is probed like an idle one, so it is still judged.
func TestAFriendWaitingAtTheBarrierIsProbed(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("repo", 1)
	clk := newClock()
	ps := fx.probed(clk)
	d := fx.put("repo", "data/1", "data", "pack") // at B: ties go by node ID
	fx.put("repo", "index/1", "metadata", "index")
	if p, ok, err := fx.db.NextPlacement(t.Context(), "B"); err != nil || !ok || p.Seq != d {
		t.Fatalf("B's head %+v %t %v, want data/1", p, ok, err)
	}
	ps["B"].hold = make(chan struct{})
	defer close(ps["B"].hold)
	ps["C"].answer(errors.New("dial tcC: no route to host"))
	fx.run()
	waitFor(t, "C probed while its index waits for B", func() bool {
		return fx.condition("C").Condition == state.ConditionRetrying
	})
	if ps["B"].probes.Load() != 0 {
		t.Fatalf("B probed while sending")
	}
}

// #65, #57: a failed probe does not hide a wait on this node's store
// (the condition store, and its alert), left by an earlier run while
// the friend's FIFO is held.
func TestAFailedProbeLeavesAWaitOnTheStore(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("repo", 1)
	ps := fx.probed(newClock())
	fx.put("repo", "data/1", "data", "pack") // at B
	fx.put("repo", "index/1", "metadata", "index")
	ps["B"].hold = make(chan struct{})
	defer close(ps["B"].hold)
	ps["C"].answer(errors.New("dial tcC: no route to host"))
	if err := fx.db.SetPlaceCondition(t.Context(), "C", state.ConditionStore, "EIO", time.Now()); err != nil {
		t.Fatal(err)
	}
	fx.run()
	waitFor(t, "C probed", func() bool { return ps["C"].probes.Load() == 1 })
	time.Sleep(10 * fx.w.Poll)
	if c := fx.condition("C"); c.Condition != state.ConditionStore {
		t.Fatalf("C shows %+v", c)
	}
}

// #65 (from the #21 review): a friend whose client cannot be built
// gets a condition, retrying with the reason, so it is judged
// unreachable and alerted like one that does not answer, instead of
// counting as answering for ever.
func TestAFriendWhoseClientCannotBeBuiltIsRetrying(t *testing.T) {
	fx := newFixture(t, "B")
	clk := newClock()
	fx.w.Now = clk.now
	fx.w.Connect = func(f state.Friendship) (Replica, error) {
		return nil, errors.New("the friend's node ID is malformed")
	}
	start := clk.now()
	fx.run()
	waitFor(t, "retrying", func() bool { return fx.condition("B").Condition == state.ConditionRetrying })
	clk.advance(time.Hour)
	time.Sleep(20 * fx.w.Poll) // tried again at every pass
	c := fx.condition("B")
	if !c.Since.Equal(start) || c.Detail == "" || !c.LastOK.IsZero() {
		t.Fatalf("condition %+v", c)
	}
}
