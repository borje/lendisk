package replica

import (
	"context"
	"sync"
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
