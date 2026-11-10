package state

import (
	"context"
	"testing"
	"time"
)

// tombPool is a pool over a database in dir, with friend B and an
// overflow target without versioning holding a copy of data/1, its
// window a day.
func tombPool(t *testing.T, dir string) (*pool, string, int64) {
	t.Helper()
	return targetPool(t, dir, VersioningOff)
}

// targetPool is tombPool with the target's versioning as given.
func targetPool(t *testing.T, dir, versioning string) (*pool, string, int64) {
	t.Helper()
	db, err := Open(context.Background(), dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p := &pool{t: t, db: db, now: time.Unix(1_800_000_000, 0)}
	p.db.Clock = func() time.Time { return p.now }
	p.db.splitTTL = -1
	p.friend("B", 10)
	o := Overflow{Name: "garage", Endpoint: "http://g", Bucket: "b", Prefix: "lendisk", MaxBytes: 100 * unit,
		EnvPrefix: "LENDISK_OVERFLOW_GARAGE_", Retention: 24 * time.Hour, Versioning: versioning, AddedAt: p.now}
	if err := p.db.AddOverflow(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	p.bucket("r", 2)
	p.put("r", "index/1", "metadata", 0.1)
	d := p.put("r", "data/1", "data", 2) // one friend for two copies: B and the target
	p.wantPlaces("data/1", d, "B", o.Place())
	p.ack("B", d)
	p.ack(o.Place(), d)
	return p, o.Place(), d
}

func (p *pool) use(place string) int64 {
	p.t.Helper()
	var n int64
	p.db.QueryRow(`SELECT coalesce((SELECT bytes FROM place_use WHERE place = ?), 0)`, place).Scan(&n)
	return n
}

func (p *pool) due(place string) []OverflowTomb {
	p.t.Helper()
	ts, err := p.db.DueOverflowTombs(context.Background(), place, p.now, 100)
	if err != nil {
		p.t.Fatal(err)
	}
	return ts
}

// S3-4 (#39): a delete for a target without versioning is held as a
// client-side tombstone in lendisk.db, survives a restart, is due only
// once the target's retention window has passed, and keeps the object
// counted at the target until the delete is issued.
func TestAClientSideTombstoneIsDueOnlyAfterTheWindowAndSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p, target, d := tombPool(t, dir)
	before := p.use(target)
	del := p.del("r", "data/1", "data")
	p.wantPlaces("delete of data/1", del, "B", target)
	held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now)
	if err != nil || !held {
		t.Fatalf("held %v, %v", held, err)
	}
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("holding again: %v, %v", held, err)
	}
	p.ack(target, del)
	if got := p.use(target); got != before {
		t.Fatalf("use at the target %d after the held delete, want %d: the object is still there", got, before)
	}
	// A friend's delete is never held.
	if held, err := p.db.HoldOverflowDelete(ctx, "B", del, p.now); err != nil || held {
		t.Fatalf("held at a friend: %v, %v", held, err)
	}

	// The daemon restarts: the tomb is still there, and not due early.
	p.db.Close()
	p, _, _ = reopenPool(t, dir, p.now)
	p.now = p.now.Add(24*time.Hour - time.Second)
	if ts := p.due(target); len(ts) != 0 {
		t.Fatalf("due a second before the window ends: %+v", ts)
	}
	holds, err := p.db.OverflowTombHolds(ctx)
	if err != nil || holds["garage"].Tombs != 1 || holds["garage"].Bytes != 2*unit {
		t.Fatalf("tomb holds %+v, %v", holds, err)
	}
	p.now = p.now.Add(time.Second)
	ts := p.due(target)
	if len(ts) != 1 || ts[0].Key != "data/1" || ts[0].Seq != del || ts[0].SHA256 != "h" || ts[0].Size != 2*unit || ts[0].Drained {
		t.Fatalf("due %+v", ts)
	}
	// Claimed: not handed out twice while it is being issued.
	if again := p.due(target); len(again) != 0 {
		t.Fatalf("handed out twice: %+v", again)
	}
	if err := p.db.DropOverflowTomb(ctx, target, del); err != nil {
		t.Fatal(err)
	}
	if got := p.use(target); got != before-2*unit {
		t.Fatalf("use after the delete was issued %d, want %d", got, before-2*unit)
	}
	_ = d
}

func reopenPool(t *testing.T, dir string, now time.Time) (*pool, string, int64) {
	t.Helper()
	db, err := Open(context.Background(), dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p := &pool{t: t, db: db, now: now}
	p.db.Clock = func() time.Time { return p.now }
	p.db.splitTTL = -1
	return p, "", 0
}

// S3-4: a target with versioning is sent the delete (a delete marker,
// left to the lifecycle rule); nothing is held.
func TestAVersionedTargetHoldsNoDelete(t *testing.T) {
	p, target, _ := tombPool(t, t.TempDir())
	if _, err := p.db.Exec(`UPDATE overflow SET versioning = 'enabled'`); err != nil {
		t.Fatal(err)
	}
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(context.Background(), target, del, p.now); err != nil || held {
		t.Fatalf("held at a versioned target: %v, %v", held, err)
	}
}

// S3-4: a key the target holds again before its tomb is due (written
// again and acknowledged there) keeps it: the tomb is dropped, never
// issued.
func TestATombIsNeverIssuedOverTheKeyWrittenAgain(t *testing.T) {
	ctx := context.Background()
	p, target, _ := tombPool(t, t.TempDir())
	del := p.del("r", "data/1", "data")
	if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	again := p.put("r", "data/1", "data", 2)
	p.ack(target, again)
	p.now = p.now.Add(48 * time.Hour)
	if ts := p.due(target); len(ts) != 0 {
		t.Fatalf("a tomb over a key written again is due: %+v", ts)
	}
	holds, _ := p.db.OverflowTombHolds(ctx)
	if holds["garage"].Tombs != 0 {
		t.Fatalf("superseded tomb kept: %+v", holds)
	}
}

// RET-3 (#39): an undelete claims only tombs inside the window, of
// deletes (not drained copies), deleted since its time; a claimed tomb
// is not issued, and one given back is.
func TestAnUndeleteClaimsTombsInsideTheWindowOnly(t *testing.T) {
	ctx := context.Background()
	p, target, _ := tombPool(t, t.TempDir())
	t0 := p.now
	del := p.del("r", "data/1", "data")
	if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	p.now = p.now.Add(time.Hour)
	if ts, err := p.db.UndeletableTombs(ctx, target, "r", t0.Add(time.Minute), p.now); err != nil || len(ts) != 0 {
		t.Fatalf("since after the delete: %+v, %v", ts, err)
	}
	ts, err := p.db.ClaimOverflowTombs(ctx, target, "r", t0, p.now)
	if err != nil || len(ts) != 1 || ts[0].Seq != del {
		t.Fatalf("claimed %+v, %v", ts, err)
	}
	if again, _ := p.db.ClaimOverflowTombs(ctx, target, "r", t0, p.now); len(again) != 0 {
		t.Fatalf("claimed twice: %+v", again)
	}
	// Given back, it is due at the end of its window as before.
	if err := p.db.UnclaimOverflowTombs(ctx, target, []int64{del}); err != nil {
		t.Fatal(err)
	}
	p.now = t0.Add(24 * time.Hour)
	if ts, _ := p.db.ClaimOverflowTombs(ctx, target, "r", time.Time{}, p.now); len(ts) != 0 {
		t.Fatalf("an undelete claimed a tomb past its window: %+v", ts)
	}
	if ts := p.due(target); len(ts) != 1 {
		t.Fatalf("due %+v", ts)
	}
}

// #39 review: a claim holds the key. A key with a drained copy's tomb,
// due, and a later delete's, inside the window: an undelete claims both,
// and the worker sends neither while the claim lives; given back, the
// due one is sent. A key whose newest tomb is stale (superseded) is
// judged by the newest live one.
func TestAnUndeleteClaimsEveryTombOfTheKey(t *testing.T) {
	ctx := context.Background()
	p, target, d := tombPool(t, t.TempDir())
	t0 := p.now
	if _, err := p.db.Exec(`UPDATE placement SET state = 'dropping' WHERE place = ? AND seq = ?`, target, d); err != nil {
		t.Fatal(err)
	}
	if held, err := p.db.HoldOverflowDelete(ctx, target, d, p.now); err != nil || !held {
		t.Fatalf("drain held %v, %v", held, err)
	}
	p.ack(target, d)
	p.now = t0.Add(23 * time.Hour)
	del := p.del("r", "data/1", "data")
	if p.rowState(target, del) == "" {
		if _, err := p.db.Exec(`INSERT INTO placement (seq, place, bucket, key, op, state, size) VALUES (?, ?, 'r', 'data/1', 'delete', 'queued', 0)`,
			del, target); err != nil {
			t.Fatal(err)
		}
	}
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("delete held %v, %v", held, err)
	}
	p.now = t0.Add(24 * time.Hour)
	ts, err := p.db.ClaimOverflowTombs(ctx, target, "r", time.Time{}, p.now)
	if err != nil || len(ts) != 2 || ts[0].Seq != d || !ts[0].Drained || ts[1].Seq != del {
		t.Fatalf("claimed %+v, %v", ts, err)
	}
	if due := p.due(target); len(due) != 0 {
		t.Fatalf("the worker took a tomb of a claimed key: %+v", due)
	}
	// Renewed, the claims outlive the hour.
	p.now = p.now.Add(50 * time.Minute)
	if err := p.db.RenewOverflowClaims(ctx, target, []int64{d, del}, p.now); err != nil {
		t.Fatal(err)
	}
	p.now = p.now.Add(50 * time.Minute)
	if due := p.due(target); len(due) != 0 {
		t.Fatalf("a renewed claim lapsed: %+v", due)
	}
	if err := p.db.UnclaimOverflowTombs(ctx, target, []int64{d, del}); err != nil {
		t.Fatal(err)
	}
	if due := p.due(target); len(due) != 1 || due[0].Seq != d {
		t.Fatalf("due after the claim was given back: %+v", due)
	}
}

// metaAt is target's row of tombPool's metadata object index/1: its
// sequence and state.
func (p *pool) metaAt(target string) (int64, string) {
	p.t.Helper()
	var seq int64
	if err := p.db.QueryRow(`SELECT seq FROM queue WHERE bucket = 'r' AND key = 'index/1'`).Scan(&seq); err != nil {
		p.t.Fatal(err)
	}
	st, _ := p.rowOf(target, seq)
	return seq, st
}

// REPL-12, S3-4, RET-3 (#40 regression): a pruned object's copy stays at
// a target without versioning while its delete is held, and an undelete
// brings it back there, so the bucket's metadata stays too: re-placement
// does not delete it until the held delete is sent.
func TestAPrunedObjectsMetadataStaysWhileItsDeleteIsHeld(t *testing.T) {
	ctx := context.Background()
	p, target, _ := tombPool(t, t.TempDir())
	meta, st := p.metaAt(target)
	if st != PlacementQueued {
		t.Fatalf("the target's metadata row is %q, want queued with its first data", st)
	}
	p.ack(target, meta)
	del := p.del("r", "data/1", "data")
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || !held {
		t.Fatalf("held %v, %v", held, err)
	}
	p.ack(target, del)
	if s := p.replace(); s.Dropping != 0 || s.Cancelled != 0 {
		t.Fatalf("re-placement deleted the metadata at the target while a pruned object's delete is held: %+v", s)
	}
	p.wantState(target, meta, PlacementAcked)
	// The window ends and the delete is sent: the target holds no data
	// of the bucket, and its metadata goes, that delete held in turn.
	p.now = p.now.Add(24 * time.Hour)
	if ts := p.due(target); len(ts) != 1 || ts[0].Seq != del {
		t.Fatalf("due %+v", ts)
	}
	if err := p.db.DropOverflowTomb(ctx, target, del); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Dropping != 1 {
		t.Fatalf("the metadata of an emptied target not dropped: %+v", s)
	}
	p.wantState(target, meta, PlacementDropping)
}

// REPL-12, I2 (#103, found by TestRemovalModel): a pruned object's
// delete queued at the target and not sent yet still leaves the copy
// there, which its tomb or marker then keeps: a pass in between does
// not drop the bucket's metadata at the target.
func TestAPrunedObjectsMetadataStaysWhileItsDeleteWaitsToBeSent(t *testing.T) {
	for _, v := range []string{VersioningOff, VersioningEnabled} {
		t.Run(v, func(t *testing.T) {
			p, target, _ := targetPool(t, t.TempDir(), v)
			meta, _ := p.metaAt(target)
			p.ack(target, meta)
			del := p.del("r", "data/1", "data")
			p.wantPlaces("delete of data/1", del, "B", target)
			if s := p.replace(); s.Dropping != 0 || s.Cancelled != 0 {
				t.Fatalf("re-placement deleted the metadata at the target before a pruned object's delete was sent: %+v", s)
			}
			p.wantState(target, meta, PlacementAcked)
		})
	}
}

// S3-5 (#40): a copy the target refused to delete stays there, so the
// bucket's metadata stays too.
func TestADeniedDeleteKeepsTheMetadataAtTheTarget(t *testing.T) {
	ctx := context.Background()
	p, target, _ := tombPool(t, t.TempDir())
	meta, _ := p.metaAt(target)
	p.ack(target, meta)
	del := p.del("r", "data/1", "data")
	if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	p.now = p.now.Add(24 * time.Hour)
	if ts := p.due(target); len(ts) != 1 {
		t.Fatalf("due %+v", ts)
	}
	if err := p.db.DenyOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Dropping != 0 {
		t.Fatalf("the metadata dropped beside a copy the target refused to delete: %+v", s)
	}
	p.wantState(target, meta, PlacementAcked)
}

// #40: a delete of a key whose copy drain-back already took off the
// target holds nothing there: the target's metadata of the bucket still
// goes.
func TestADeleteAfterDrainBackKeepsNoMetadataAtTheTarget(t *testing.T) {
	ctx := context.Background()
	p, target, d := tombPool(t, t.TempDir())
	meta, _ := p.metaAt(target)
	p.ack(target, meta)
	if _, err := p.db.Exec(`UPDATE placement SET state = 'dropping' WHERE place = ? AND seq = ?`, target, d); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.HoldOverflowDelete(ctx, target, d, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, d)
	del := p.del("r", "data/1", "data")
	if p.rowState(target, del) != "" {
		if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
			t.Fatal(err)
		}
		p.ack(target, del)
	}
	p.replace()
	p.wantState(target, meta, PlacementDropping)
}

// S3-4: a delete at a target without versioning whose placement row is
// gone by the time its sender holds it (the place removed meanwhile) is
// not sent there at once: every delete at such a target goes through a
// held tomb or nowhere.
func TestAVanishedRowsDeleteIsNotSentAtOnce(t *testing.T) {
	p, target, _ := tombPool(t, t.TempDir())
	del := p.del("r", "data/1", "data")
	if _, err := p.db.Exec(`DELETE FROM placement WHERE place = ? AND seq = ?`, target, del); err != nil {
		t.Fatal(err)
	}
	if held, err := p.db.HoldOverflowDelete(context.Background(), target, del, p.now); err != nil || !held {
		t.Fatalf("held %v, %v: the worker would send the delete at once", held, err)
	}
}

// REPL-12, S3-4, RET-3 (#40 regression, the versioned twin of
// TestAPrunedObjectsMetadataStaysWhileItsDeleteIsHeld): a versioned
// target is sent a pruned object's delete at once, as a delete marker,
// and keeps the version for its window, which an undelete brings back.
// So the bucket's metadata stays there for that window: re-placement
// does not delete it, and a metadata put meanwhile goes there too.
func TestAPrunedObjectsMetadataStaysAtAVersionedTargetForItsWindow(t *testing.T) {
	ctx := context.Background()
	p, target, _ := targetPool(t, t.TempDir(), VersioningEnabled)
	meta, _ := p.metaAt(target)
	p.ack(target, meta)
	del := p.del("r", "data/1", "data")
	p.wantPlaces("delete of data/1", del, "B", target)
	if held, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil || held {
		t.Fatalf("held %v, %v: a versioned target is sent the delete", held, err)
	}
	p.ack(target, del)
	p.now = p.now.Add(24*time.Hour - time.Second)
	if s := p.replace(); s.Dropping != 0 || s.Cancelled != 0 {
		t.Fatalf("re-placement deleted the metadata at a versioned target inside the window of a pruned object's delete: %+v", s)
	}
	p.wantState(target, meta, PlacementAcked)
	// A metadata put inside the window goes there too, and stays.
	idx := p.put("r", "index/2", "metadata", 0.1)
	p.wantPlaces("index/2", idx, "B", target)
	p.ack(target, idx)
	if s := p.replace(); s.Dropping != 0 || s.Cancelled != 0 {
		t.Fatalf("re-placement deleted the metadata it was just given: %+v", s)
	}
}

// #40: once a versioned target's window after a pruned object's delete
// has passed, it holds no data of the bucket: its metadata goes, once,
// and a metadata put after that is not sent there.
func TestAVersionedTargetsMetadataGoesAfterTheWindow(t *testing.T) {
	ctx := context.Background()
	p, target, _ := targetPool(t, t.TempDir(), VersioningEnabled)
	meta, _ := p.metaAt(target)
	p.ack(target, meta)
	del := p.del("r", "data/1", "data")
	if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	p.now = p.now.Add(24 * time.Hour)
	if s := p.replace(); s.Dropping != 1 {
		t.Fatalf("the metadata of a target past its window not dropped: %+v", s)
	}
	p.wantState(target, meta, PlacementDropping)
	if s := p.replace(); s.Dropping != 0 || s.Placed != 0 {
		t.Fatalf("a second pass moved the metadata again: %+v", s)
	}
	idx := p.put("r", "index/2", "metadata", 0.1)
	p.wantPlaces("index/2", idx, "B")
}

// S3-5, RET-3 (#98): a user delete the target refused for want of
// permission keeps the object there after its retention window, the
// daily retry notwithstanding; status counts those apart. At a
// client-side target the delete is refused at the window's end, so it
// counts at once; at a versioned one, refused at once, it counts once
// the window has passed since the delete, retries or not.
func TestADeniedUserDeleteHeldPastItsWindowIsCounted(t *testing.T) {
	ctx := context.Background()
	holds := func(p *pool) TombHold {
		t.Helper()
		h, err := p.db.OverflowTombHolds(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return h["garage"]
	}
	p, target, _ := tombPool(t, t.TempDir())
	del := p.del("r", "data/1", "data")
	if _, err := p.db.HoldOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	if h := holds(p); h.DeniedPastWindow != 0 {
		t.Fatalf("a held delete inside its window: %+v", h)
	}
	p.now = p.now.Add(24 * time.Hour)
	if err := p.db.DenyOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	if h := holds(p); h.Denied != 1 || h.DeniedPastWindow != 1 || h.DeniedPastWindowBytes != h.DeniedBytes || h.DeniedBytes <= 0 {
		t.Fatalf("client-side, refused at the window's end: %+v", h)
	}

	p, target, _ = targetPool(t, t.TempDir(), VersioningEnabled)
	del = p.del("r", "data/1", "data")
	if err := p.db.DenyOverflowDelete(ctx, target, del, p.now); err != nil {
		t.Fatal(err)
	}
	p.ack(target, del)
	if h := holds(p); h.Denied != 1 || h.DeniedPastWindow != 0 {
		t.Fatalf("versioned, refused at once: %+v", h)
	}
	p.now = p.now.Add(24*time.Hour - time.Second)
	if err := p.db.DenyOverflowDelete(ctx, target, del, p.now); err != nil { // the daily retry
		t.Fatal(err)
	}
	if h := holds(p); h.DeniedPastWindow != 0 {
		t.Fatalf("a second before the window ends: %+v", h)
	}
	p.now = p.now.Add(time.Second)
	if h := holds(p); h.Denied != 1 || h.DeniedPastWindow != 1 || h.DeniedPastWindowBytes != h.DeniedBytes {
		t.Fatalf("versioned, the window passed: %+v", h)
	}
}
