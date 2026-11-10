package undelete

import (
	"context"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// putAndDelete captures key written with body, acknowledged at overflow
// target g, then deleted at now: held there as a client-side tomb if
// the target's versioning (as last read) says so, else sent as a delete
// marker. It reports whether the delete was held.
func putAndDelete(t *testing.T, db *state.DB, ft *fakeTarget, key, body string, now time.Time) bool {
	t.Helper()
	ctx := context.Background()
	place := "overflow:g"
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), hash(body)); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], hash(body), now); err != nil {
		t.Fatal(err)
	}
	ft.bodies[key] = body
	seqs, err = db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	held, err := db.HoldOverflowDelete(ctx, place, seqs[0], now)
	if err != nil {
		t.Fatal(err)
	}
	if held {
		ft.live[key] = body
	} else {
		delete(ft.live, key)
		ft.deleted = append(ft.deleted, replica.Deleted{Key: key, DeletedAt: now, VersionID: "v1",
			Size: int64(len(body)), SHA256: hash(body)})
	}
	if err := db.AckPlacement(ctx, place, seqs[0], "", now); err != nil {
		t.Fatal(err)
	}
	return held
}

// RET-3, S3-4 (#97): a target whose versioning was suspended after it
// was sent a delete as a marker holds both kinds: undelete copies the
// version before the marker back and cancels the delete held since the
// suspension.
func TestUndeleteAfterASuspensionBringsBackMarkersAndHeldDeletes(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningEnabled, packA)
	ctx := context.Background()
	*now = now.Add(time.Hour)
	if _, err := db.RecordOverflowVersioning(ctx, "g", state.VersioningSuspended, *now); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Minute)
	if !putAndDelete(t, db, ft, packB, bodyOf(packB), *now) {
		t.Fatal("a delete after the suspension was not held")
	}
	*now = now.Add(time.Minute)
	p, res := runUndelete(t, db, r, now.Add(-10*time.Hour))
	if p.Keys != 2 || p.Friends[0].Keys != 2 {
		t.Fatalf("preview %+v", p)
	}
	if len(ft.restored) != 1 || ft.restored[0] != packA {
		t.Fatalf("copied back at the target: %v", ft.restored)
	}
	if fr := res.Friends[0]; fr.Restored != 2 || fr.Recorded != 2 || fr.Error != "" || fr.Adopted != 0 {
		t.Fatalf("result %+v", fr)
	}
	for _, k := range []string{packA, packB} {
		if live, _ := db.KeyLive(ctx, "laptop", k); !live {
			t.Fatalf("%s not live in the index", k)
		}
	}
	if holds, _ := db.OverflowTombHolds(ctx); holds["g"].Tombs != 0 {
		t.Fatalf("held deletes left: %+v", holds)
	}
}

// RET-3, S3-4 (#97): after a suspension, a key the target still holds
// under a delete held before since is left as it is: neither restored
// nor adopted as a copy whose record was lost, as it would be at a
// versioned target.
func TestAHeldDeleteBeforeSinceIsNotAdoptedAfterASuspension(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningEnabled)
	ctx := context.Background()
	*now = now.Add(time.Hour)
	if _, err := db.RecordOverflowVersioning(ctx, "g", state.VersioningSuspended, *now); err != nil {
		t.Fatal(err)
	}
	putAndDelete(t, db, ft, packB, bodyOf(packB), *now)
	*now = now.Add(time.Hour)
	_, res := runUndelete(t, db, r, now.Add(-time.Minute))
	if fr := res.Friends[0]; fr.Restored != 0 || fr.Adopted != 0 {
		t.Fatalf("result %+v", fr)
	}
	if live, _ := db.KeyLive(ctx, "laptop", packB); live {
		t.Fatal("a delete before since came back")
	}
	if holds, _ := db.OverflowTombHolds(ctx); holds["g"].Tombs != 1 {
		t.Fatalf("the held delete was dropped: %+v", holds)
	}
}

// RET-3, S3-4 (#97): a target whose versioning was enabled after it held
// deletes keeps them undeletable, and brings back a delete made after
// the change from its marker.
func TestUndeleteAfterVersioningIsEnabledBringsBackBoth(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningOff, packA)
	ctx := context.Background()
	*now = now.Add(time.Hour)
	if _, err := db.RecordOverflowVersioning(ctx, "g", state.VersioningEnabled, *now); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Minute)
	if putAndDelete(t, db, ft, packB, bodyOf(packB), *now) {
		t.Fatal("a delete after versioning was enabled was held")
	}
	_, res := runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; fr.Restored != 2 || fr.Recorded != 2 || len(ft.restored) != 1 || ft.restored[0] != packB {
		t.Fatalf("result %+v, copied back %v", fr, ft.restored)
	}
}
