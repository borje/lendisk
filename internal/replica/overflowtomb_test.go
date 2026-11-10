package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// fixtureClock is a settable clock for the worker.
type fixtureClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixtureClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fixtureClock) add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// withTarget adds overflow target name with versioning as given and a
// retention window of an hour, served by an in-memory place.
func (fx *fixture) withTarget(name, versioning string) *place {
	fx.t.Helper()
	o := state.Overflow{Name: name, Endpoint: "http://provider", Bucket: "offsite", Prefix: "lendisk", MaxBytes: 1 << 40,
		EnvPrefix: "LENDISK_OVERFLOW_X_", Retention: time.Hour, Versioning: versioning, AddedAt: time.Now()}
	if err := fx.db.AddOverflow(fx.t.Context(), o); err != nil {
		fx.t.Fatal(err)
	}
	p := newPlace(o.Place())
	fx.places[o.Place()] = p
	fx.w.ConnectOverflow = func(o state.Overflow) (Replica, error) { return fx.places[o.Place()], nil }
	return p
}

func (p *place) holds(bucket, key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.objs[bucket+"/"+key]
	return ok
}

// S3-4 (#39): a target without versioning is never sent a delete at
// once: the worker holds it as a client-side tombstone and sends it only
// once the target's retention window has passed, so the object is there
// to undelete until then. A friend gets its delete at once.
func TestAnUnversionedTargetGetsItsDeleteOnlyAfterTheWindow(t *testing.T) {
	fx := newFixture(t, "B")
	target := fx.withTarget("garage", state.VersioningOff)
	clock := &fixtureClock{now: time.Now()}
	fx.w.Now = clock.Now
	fx.write("laptop", "data/1", "pack one")
	fx.run()
	waitFor(t, "data/1 at B and the target", func() bool {
		return fx.places["B"].holds("laptop", "data/1") && target.holds("laptop", "data/1")
	})
	fx.remove("laptop", "data/1")
	fx.drained("overflow:garage")
	fx.drained("B")
	if fx.places["B"].holds("laptop", "data/1") {
		t.Fatal("the friend kept data/1")
	}
	time.Sleep(100 * time.Millisecond) // a few polls of the sender
	if !target.holds("laptop", "data/1") {
		t.Fatal("the unversioned target was sent the delete before the window ended")
	}
	clock.add(time.Hour - time.Second)
	time.Sleep(100 * time.Millisecond)
	if !target.holds("laptop", "data/1") {
		t.Fatal("deleted a second before the window ended")
	}
	clock.add(time.Second)
	waitFor(t, "the held delete sent once the window ended", func() bool { return !target.holds("laptop", "data/1") })
	waitFor(t, "the tomb dropped", func() bool {
		h, err := fx.db.OverflowTombHolds(context.Background())
		return err == nil && h["garage"].Tombs == 0
	})
}

// S3-4: a target with versioning is sent the delete at once (a delete
// marker there, which the lifecycle rule expires).
func TestAVersionedTargetGetsItsDeleteAtOnce(t *testing.T) {
	fx := newFixture(t, "B")
	target := fx.withTarget("b2", state.VersioningEnabled)
	fx.write("laptop", "data/1", "pack one")
	fx.run()
	waitFor(t, "data/1 at the target", func() bool { return target.holds("laptop", "data/1") })
	fx.remove("laptop", "data/1")
	waitFor(t, "the delete at the target", func() bool { return !target.holds("laptop", "data/1") })
}

// records returns how many log records at level have a message
// containing msg.
func (l *logBuffer) records(level, msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range strings.Split(l.b.String(), "\n") {
		var r map[string]any
		if json.Unmarshal([]byte(line), &r) == nil && r["level"] == level && strings.Contains(fmt.Sprint(r["msg"]), msg) {
			n++
		}
	}
	return n
}

// S3-4 (#97): while an overflow target is unreachable and deletes are
// due there, the sender tries them with backoff, not every poll (each
// attempt holds the target's FIFO for one timeout), and warns once, not
// at every try; once the target answers it says so and sends them.
func TestHeldDeletesAtAnUnreachableTargetBackOffAndWarnOnce(t *testing.T) {
	fx := newFixture(t, "B")
	target := fx.withTarget("garage", state.VersioningOff)
	clock := &fixtureClock{now: time.Now()}
	fx.w.Now = clock.Now
	fx.w.MinBackoff, fx.w.MaxBackoff = 100*time.Millisecond, 400*time.Millisecond
	logs := fx.logTo()
	fx.write("laptop", "data/1", "pack one")
	fx.run()
	waitFor(t, "data/1 at the target", func() bool { return target.holds("laptop", "data/1") })
	fx.remove("laptop", "data/1")
	fx.drained("overflow:garage")
	var tries atomic.Int64
	var down atomic.Bool
	down.Store(true)
	target.mu.Lock()
	target.fail = func(op, _ string) error {
		if op == "delete" && down.Load() {
			tries.Add(1)
			return errors.New("dial tcp: connection refused")
		}
		return nil
	}
	target.mu.Unlock()
	clock.add(time.Hour)
	waitFor(t, "a first try of the held delete", func() bool { return tries.Load() > 0 })
	time.Sleep(time.Second) // 100 polls of the sender
	if n := tries.Load(); n > 8 {
		t.Fatalf("the held delete was tried %d times in a second: no backoff", n)
	}
	if n := logs.records("WARN", "sending a held delete"); n != 1 {
		t.Fatalf("warned %d times while the target stayed unreachable, want once", n)
	}
	down.Store(false)
	waitFor(t, "the held delete sent once the target answers", func() bool { return !target.holds("laptop", "data/1") })
	waitFor(t, "the recovery logged", func() bool { return logs.records("INFO", "held deletes go to the overflow target again") == 1 })
}

// #102: a target removed with keep_data while the worker sends a batch
// of held deletes there gets no more of them: the worker asks before
// each (state.TombToSend), so only the delete on its way when the user
// ran `overflow remove --keep-data` goes, and the rest stay there.
func TestKeepDataStopsTheHeldDeletesTheWorkerIsSending(t *testing.T) {
	fx := newFixture(t, "B")
	target := fx.withTarget("garage", state.VersioningOff)
	clock := &fixtureClock{now: time.Now()}
	fx.w.Now = clock.Now
	keys := []string{"data/1", "data/2", "data/3"}
	for _, k := range keys {
		fx.write("laptop", k, "pack "+k)
	}
	fx.run()
	waitFor(t, "every pack at the target", func() bool {
		for _, k := range keys {
			if !target.holds("laptop", k) {
				return false
			}
		}
		return true
	})
	for _, k := range keys {
		fx.remove("laptop", k)
	}
	fx.drained("overflow:garage")
	var deletes atomic.Int64
	target.mu.Lock()
	target.fail = func(op, _ string) error {
		if op == "delete" && deletes.Add(1) == 1 {
			// The user removes the target keeping its data while this
			// first delete is on its way.
			if err := fx.db.RemoveOverflow(context.Background(), "garage", true, clock.Now()); err != nil {
				t.Error(err)
			}
		}
		return nil
	}
	target.mu.Unlock()
	clock.add(time.Hour)
	waitFor(t, "the held deletes settled", func() bool {
		h, err := fx.db.OverflowTombHolds(context.Background())
		return deletes.Load() > 0 && err == nil && h["garage"].Tombs == 0
	})
	time.Sleep(100 * time.Millisecond) // a few polls of the sender
	if n := deletes.Load(); n != 1 {
		t.Fatalf("%d deletes sent, want only the one on its way", n)
	}
	held := 0
	for _, k := range keys {
		if target.holds("laptop", k) {
			held++
		}
	}
	if held != 2 {
		t.Fatalf("the target holds %d of the packs, want the two not yet deleted", held)
	}
}
