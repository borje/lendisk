package state

import (
	"context"
	"testing"
	"time"
)

// S3-4 (#97): a read of a target's versioning that finds another than
// the row's is recorded as a change, and the target's retention follows
// it from then on: a delete after a suspension is held as a client-side
// tomb. A read that finds the same only records when it was read.
func TestAVersioningChangeSwitchesTheTargetToClientSideTombs(t *testing.T) {
	ctx := context.Background()
	p, target, _ := targetPool(t, t.TempDir(), VersioningEnabled)
	added := p.now
	p.now = p.now.Add(24 * time.Hour)
	ch, err := p.db.RecordOverflowVersioning(ctx, "garage", VersioningEnabled, p.now)
	if err != nil || ch != nil {
		t.Fatalf("an unchanged read recorded a change: %+v, %v", ch, err)
	}
	o, err := p.db.GetOverflow(ctx, "garage")
	if err != nil || !o.VersioningCheckedAt.Equal(p.now) || o.ClientSide() {
		t.Fatalf("after an unchanged read: %+v, %v", o, err)
	}
	p.now = p.now.Add(24 * time.Hour)
	ch, err = p.db.RecordOverflowVersioning(ctx, "garage", VersioningSuspended, p.now)
	if err != nil || ch == nil || ch.Previous != VersioningEnabled || ch.Versioning != VersioningSuspended || !ch.At.Equal(p.now) {
		t.Fatalf("the suspension recorded as %+v, %v", ch, err)
	}
	o, _ = p.db.GetOverflow(ctx, "garage")
	if !o.ClientSide() || o.Versioning != VersioningSuspended || !o.WasVersioned || !o.AddedAt.Equal(added) {
		t.Fatalf("after the suspension: %+v", o)
	}
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("a delete after the suspension was not held: %v, %v", held, err)
	}
	last, err := p.db.LastVersioningChanges(ctx)
	if err != nil || last["garage"] != *ch {
		t.Fatalf("last changes %+v, %v", last, err)
	}
}

// S3-4 (#97): a target whose versioning is enabled after it was added
// gets the deletes made from then on as delete markers; the deletes it
// held before stay held, due when their window ends, and undeletable
// meanwhile.
func TestVersioningEnabledLaterKeepsTheTombsAlreadyHeld(t *testing.T) {
	ctx := context.Background()
	p, target, _ := targetPool(t, t.TempDir(), VersioningOff)
	d2 := p.put("r", "data/2", "data", 2)
	p.ack("B", d2)
	p.ack(target, d2)
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("held %v, %v", held, err)
	}
	p.ack(target, del)
	p.now = p.now.Add(time.Hour)
	if ch, err := p.db.RecordOverflowVersioning(ctx, "garage", VersioningEnabled, p.now); err != nil || ch == nil {
		t.Fatalf("enabling recorded as %+v, %v", ch, err)
	}
	del2 := p.del("r", "data/2", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del2, p.now); err != nil || held {
		t.Fatalf("a delete after versioning was enabled was held: %v, %v", held, err)
	}
	p.ack(target, del2)
	if ts, err := p.db.UndeletableTombs(ctx, target, "r", time.Time{}, p.now); err != nil || len(ts) != 1 || ts[0].Seq != del {
		t.Fatalf("the tomb held before the change is not undeletable: %+v, %v", ts, err)
	}
	if ok, err := p.db.MarkerDeleted(ctx, target, "r", "data/1"); err != nil || ok {
		t.Fatalf("a delete held before the change counted as a marker: %v, %v", ok, err)
	}
	if ok, err := p.db.MarkerDeleted(ctx, target, "r", "data/2"); err != nil || !ok {
		t.Fatalf("a delete sent after the change did not count as a marker: %v, %v", ok, err)
	}
	p.now = p.now.Add(23 * time.Hour)
	if ts := p.due(target); len(ts) != 1 || ts[0].Seq != del {
		t.Fatalf("due after the window %+v", ts)
	}
}

// S3-4 (#97): the deletes a target was sent as delete markers while it
// had versioning stay markers after a suspension: undelete may bring
// them back from the provider, and they keep the bucket's metadata there
// for their window.
func TestMarkersSentBeforeASuspensionStillCount(t *testing.T) {
	ctx := context.Background()
	p, target, _ := targetPool(t, t.TempDir(), VersioningEnabled)
	meta, _ := p.metaAt(target)
	p.ack(target, meta)
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || held {
		t.Fatalf("held %v, %v", held, err)
	}
	p.ack(target, del)
	p.now = p.now.Add(time.Hour)
	if _, err := p.db.RecordOverflowVersioning(ctx, "garage", VersioningSuspended, p.now); err != nil {
		t.Fatal(err)
	}
	if ok, err := p.db.MarkerDeleted(ctx, target, "r", "data/1"); err != nil || !ok {
		t.Fatalf("a marker sent before the suspension no longer counts: %v, %v", ok, err)
	}
	p.now = p.now.Add(22 * time.Hour)
	if s := p.replace(); s.Dropping != 0 {
		t.Fatalf("the metadata went inside the marker's window after a suspension: %+v", s)
	}
	p.wantState(target, meta, PlacementAcked)
	p.now = p.now.Add(time.Hour)
	if s := p.replace(); s.Dropping != 1 {
		t.Fatalf("the metadata stayed past the marker's window: %+v", s)
	}
}

// S3-4 (#97): for a target whose versioning never changed, a sent
// delete is a marker exactly when its versioning is enabled.
func TestMarkerDeletedFollowsTheTargetsVersioning(t *testing.T) {
	ctx := context.Background()
	for _, v := range []string{VersioningEnabled, VersioningOff} {
		p, target, _ := targetPool(t, t.TempDir(), v)
		del := p.del("r", "data/1", "data")
		if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
			t.Fatal(err)
		}
		p.ack(target, del)
		if ok, err := p.db.MarkerDeleted(ctx, target, "r", "data/1"); err != nil || ok != (v == VersioningEnabled) {
			t.Fatalf("versioning %s: marker %v, %v", v, ok, err)
		}
	}
}
