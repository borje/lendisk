package state

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// overflow adds an overflow target with maxUnits of room (S3-1).
func (p *pool) overflow(name string, maxUnits float64) string {
	p.t.Helper()
	o := Overflow{Name: name, Endpoint: "https://s3.example", Bucket: "b", Prefix: "lendisk",
		MaxBytes: int64(maxUnits * float64(unit)), EnvPrefix: "LENDISK_OVERFLOW_X_", Retention: 30 * 24 * time.Hour,
		AddedAt: p.now}
	if err := p.db.AddOverflow(context.Background(), o); err != nil {
		p.t.Fatal(err)
	}
	return o.Place()
}

// rowsAt lists place's queued or acked rows in FIFO order.
func (p *pool) rowsAt(place string) []int64 {
	p.t.Helper()
	rows, err := p.db.Query(`SELECT seq FROM placement WHERE place = ? AND state IN ('queued', 'acked') ORDER BY seq`, place)
	if err != nil {
		p.t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var s int64
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}

func TestOverflowRowRoundTripsAndNamesAreUnique(t *testing.T) {
	p := newPool(t)
	place := p.overflow("b2", 10)
	if place != "overflow:b2" {
		t.Fatalf("place %q", place)
	}
	if name, ok := OverflowName(place); !ok || name != "b2" {
		t.Fatalf("OverflowName(%q) = %q, %v", place, name, ok)
	}
	if _, ok := OverflowName("nodeid"); ok {
		t.Fatal("a friend's node ID taken for an overflow target")
	}
	err := p.db.AddOverflow(context.Background(), Overflow{Name: "b2", Endpoint: "https://x", Bucket: "c", AddedAt: p.now})
	if !errors.Is(err, ErrExists) {
		t.Fatalf("second b2: %v", err)
	}
	o, err := p.db.GetOverflow(context.Background(), "b2")
	if err != nil || o.MaxBytes != 10*unit || o.Prefix != "lendisk" || o.Retention != 30*24*time.Hour || o.EnvPrefix != "LENDISK_OVERFLOW_X_" {
		t.Fatalf("GetOverflow = %+v, %v", o, err)
	}
	if _, err := p.db.GetOverflow(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing target: %v", err)
	}
}

// REPL-12, D6 step 3: a data copy goes to an overflow target only when
// fewer than `copies` friends have room for it, and a target receiving
// its first data copy of a bucket gets every live metadata object of the
// bucket first, then every later metadata put and the deletes of what it
// holds.
func TestOverflowTakesOnlyCopiesNoFriendHasRoom(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	p.friend("C", 3)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	m1 := p.put("r", "index/1", "metadata", 0.1)
	d1 := p.put("r", "data/1", "data", 1)
	p.wantPlaces("metadata before any data at the target", m1, "B", "C")
	p.wantPlaces("data with room at two friends", d1, "B", "C")

	// C has no room for 2 more units: B and the overflow target.
	d2 := p.put("r", "data/2", "data", 2)
	p.wantPlaces("data C has no room for", d2, "B", o)
	if got := p.rowsAt(o); !slices.Equal(got, []int64{m1, d2}) {
		t.Fatalf("overflow FIFO %v, want the bucket's metadata %d ahead of its data %d", got, m1, d2)
	}
	// Metadata now goes there too, and a delete of what it holds.
	m2 := p.put("r", "index/2", "metadata", 0.1)
	p.wantPlaces("later metadata", m2, "B", "C", o)
	p.ack("B", d2)
	p.ack(o, d2)
	del := p.del("r", "data/2", "data")
	p.wantPlaces("delete of an object the target holds", del, "B", o)

	// Another bucket's metadata is not sent to a target holding none of
	// its data.
	p.bucket("s", 2)
	ms := p.put("s", "index/1", "metadata", 0.1)
	p.wantPlaces("metadata of a bucket the target holds no data of", ms, "B", "C")
}

// S3-7: placement stops at a target's max_bytes; what does not fit stays
// below copies.
func TestOverflowStopsAtMaxBytes(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	o := p.overflow("b2", 3)
	p.bucket("r", 2)
	m := p.put("r", "index/1", "metadata", 0.5)
	d1 := p.put("r", "data/1", "data", 2)
	p.wantPlaces("first copy beyond the one friend", d1, "B", o)
	// 0.5 + 2 used of 3: one more unit does not fit.
	d2 := p.put("r", "data/2", "data", 1)
	p.wantPlaces("copy past max_bytes", d2, "B")
	p.wantPlaces("metadata", m, "B", o)
	below, err := p.db.BelowCopies(context.Background())
	if err != nil || below["r"].Objects != 1 {
		t.Fatalf("below copies %+v, %v", below, err)
	}
	holds, err := p.db.OverflowHolds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h := holds["b2"]; h.DataObjects != 1 || h.Use != int64(2.5*float64(unit)) {
		t.Fatalf("hold %+v", h)
	}
}

// #20 review, box 8: a copy at an overflow target counts towards the
// offsite watermark, so the snapshot is offsite-complete with every data
// object at two places, one of them a friend (scenario 12).
func TestOverflowCountsAsHolderForOffsite(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	m := p.put("r", "index/1", "metadata", 0.1)
	d := p.put("r", "data/1", "data", 1)
	s := p.put("r", "snapshots/1", "snapshot", 0.01)
	for _, place := range []string{"B", o} {
		for _, seq := range []int64{m, d, s} {
			if slices.Contains(p.places(seq), place) {
				p.ack(place, seq)
			}
		}
	}
	p.wantPlaces("snapshot", s, "B", o)
	if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	off, err := p.db.Offsite(context.Background(), "r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !off.Complete() || off.Stuck.Objects != 0 {
		t.Fatalf("offsite %+v", off)
	}
}

// Scenario 12: objects below copies because friends lack room are placed
// at an overflow target added later by re-placement, with the bucket's
// metadata first; friends with room are still preferred.
func TestReplacePlacesBelowCopiesAtANewOverflowTarget(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	p.bucket("r", 2)
	m := p.put("r", "index/1", "metadata", 0.1)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data with one friend", d, "B")
	o := p.overflow("b2", 100)
	st, err := p.db.Replace(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Below != 0 {
		t.Fatalf("still below copies: %+v", st)
	}
	p.wantPlaces("data after the target was added", d, "B", o)
	p.wantPlaces("metadata with it", m, "B", o)
	if got := p.rowsAt(o); !slices.Equal(got, []int64{m, d}) {
		t.Fatalf("overflow FIFO %v", got)
	}
	// A second pass changes nothing.
	if st, err := p.db.Replace(context.Background(), nil, nil); err != nil || st.Placed != 0 || st.Dropping != 0 {
		t.Fatalf("second pass %+v, %v", st, err)
	}

	// A friend with room added later is preferred for new data.
	p.friend("C", 100)
	if _, err := p.db.Replace(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	d2 := p.put("r", "data/2", "data", 1)
	p.wantPlaces("new data with two friends", d2, "B", "C")
}

// Re-routing never moves a copy to an overflow target, whose copies
// come only from D6's step 3; unsent data at a target failing for long
// waits while no friend has room, and is dropped once the target is
// gone past replace_after, so it holds no metadata barrier for good.
func TestRerouteNeverMovesToAnOverflowTarget(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	p.friend("C", 1.5)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	p.wantPlaces("data", d, "B", "C")
	// C fails: its copy waits, never going to the overflow target.
	p.condition("C", ConditionRetrying, p.now.Add(-48*time.Hour), time.Time{})
	if moved, dropped, err := p.db.Reroute(context.Background(), nil, nil); err != nil || moved != 0 || dropped != 0 {
		t.Fatalf("reroute moved %d dropped %d: %v", moved, dropped, err)
	}
	p.wantPlaces("data after C failed", d, "B", "C")

	d2 := p.put("r", "data/2", "data", 2)
	p.wantPlaces("data C has no room for", d2, "B", o)
	p.condition(o, ConditionRetrying, p.now.Add(-48*time.Hour), time.Time{})
	if moved, dropped, err := p.db.Reroute(context.Background(), nil, nil); err != nil || moved != 0 || dropped != 0 {
		t.Fatalf("reroute with the target failing: moved %d dropped %d: %v", moved, dropped, err)
	}
	p.condition(o, ConditionRetrying, p.now.Add(-8*24*time.Hour), time.Time{})
	if _, dropped, err := p.db.Reroute(context.Background(), nil, nil); err != nil || dropped != 1 {
		t.Fatalf("reroute with the target gone dropped %d, want its data copy: %v", dropped, err)
	}
	p.wantPlaces("data after the target was gone", d2, "B")
}

// OPS-2, EP-6: an overflow target is a place of the pool.
func TestOverflowCountsInThePoolSize(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	p.overflow("b2", 100)
	if n, err := p.db.PoolSize(context.Background()); err != nil || n != 2 {
		t.Fatalf("pool size %d, %v", n, err)
	}
	before, err := p.db.PoolFingerprint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.overflow("wasabi", 10)
	after, err := p.db.PoolFingerprint(context.Background())
	if err != nil || after == before {
		t.Fatalf("adding a target left the pool fingerprint as it was (%v)", err)
	}
}
