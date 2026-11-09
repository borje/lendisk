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
		EnvPrefix: "LENDISK_OVERFLOW_GARAGE_", Retention: 24 * time.Hour, Versioning: VersioningOff, AddedAt: p.now}
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
