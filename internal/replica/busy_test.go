package replica

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/metrics"
	"github.com/borje/lendisk/internal/state"
)

// #92, LEND-2: a friend that answers 503 SlowDown (busy: the write fits,
// but not beside its writes in flight) answered. The worker shows it as
// busy, not retrying: its last answer moves last_ok on and starts no run
// of failing answers, so it is never rerouted or alerted as unreachable
// for it. The send is retried with backoff, not paused for quota, and
// counted as a busy retry; no alert is raised. Once the friend accepts,
// it is ok.
func TestABusyFriendIsRetriedWithoutFailing(t *testing.T) {
	fx := newFixture(t, "B")
	fx.w.PauseRetry = time.Hour // a pause for quota would stall the test
	c := &metrics.Counters{}
	fx.w.Metrics = c
	var mu sync.Mutex
	busy := true
	var tries atomic.Int32
	fx.places["B"].fail = func(op, key string) error {
		mu.Lock()
		defer mu.Unlock()
		if op == "put" && busy {
			tries.Add(1)
			return fmt.Errorf("sending %s: %w: SlowDown", key, ErrBusy)
		}
		return nil
	}
	start := time.Now()
	fx.write("laptop", "data/1", "x")
	fx.run()
	waitFor(t, "busy", func() bool { return fx.condition("B").Condition == state.ConditionBusy })
	waitFor(t, "retries", func() bool { return tries.Load() >= 3 })
	cond := fx.condition("B")
	if cond.LastOK.Before(start.Truncate(time.Second)) || !cond.FailingSince.IsZero() || cond.Detail == "" {
		t.Fatalf("busy: %+v", cond)
	}
	if n := fx.alerted.Load(); n != 0 {
		t.Fatalf("%d alerts for a busy friend", n)
	}
	v := metrics.Values(c.Families())
	if v[`lendisk_replication_retries_total{place="B",reason="busy"}`] < 3 ||
		v[`lendisk_replication_retries_total{place="B",reason="network"}`] != 0 {
		t.Fatalf("retries: %v", v)
	}
	mu.Lock()
	busy = false
	mu.Unlock()
	fx.drained("B")
	waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
}

// #92: a probe answered busy (the friend's cap of requests in flight
// from this node) is an answer: last_ok moves on and a network failure
// ends, as at any other answer.
func TestABusyProbeAnswerIsASignOfLife(t *testing.T) {
	fx := newFixture(t, "B")
	clk := newClock()
	b := fx.probed(clk)["B"]
	b.answer(fmt.Errorf("probing the friend: %w", ErrBusy))
	if err := fx.db.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial tcB: i/o timeout",
		clk.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	fx.run()
	waitFor(t, "the first probe", func() bool { return fx.condition("B").Condition == state.ConditionOK })
	if c := fx.condition("B"); !c.LastOK.Equal(clk.now()) || !c.FailingSince.IsZero() {
		t.Fatalf("after a busy probe: %+v", c)
	}
}
