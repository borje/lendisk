package state

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #118: a queue_tally that drifted from the rows (changed by hand, a
// bucket's row lost, a row for a bucket with none) is found and
// corrected by RepairQueueTally, which reports each bucket it corrected
// with the stored and computed tallies. Writes that commit between its
// read and its correction are kept: it adds the difference it read,
// never the stale value. A tally that matches is left alone.
func TestADriftedQueueTallyIsRepairedKeepingTheWritesMeanwhile(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.bucket("photos", 2)
	p.friend("B", 100)
	p.put("laptop", "data/1", "data", 1)
	p.put("laptop", "data/2", "data", 1)
	p.put("laptop", "index/1", "metadata", 0.5)
	p.del("laptop", "data/2", "data")
	p.put("photos", "data/1", "data", 1)
	same := func(when string) {
		t.Helper()
		if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" {
			t.Fatalf("%s: %s %v", when, diff, err)
		}
	}
	same("before any drift")
	if drift, err := p.db.RepairQueueTally(ctx); err != nil || len(drift) != 0 {
		t.Fatalf("a tally that matches the rows: drift %+v (%v)", drift, err)
	}
	var stored Tally
	if err := p.db.QueryRow(`SELECT done, live_data_bytes FROM queue_tally WHERE bucket = 'laptop'`).
		Scan(&stored.Done, &stored.LiveDataBytes); err != nil {
		t.Fatal(err)
	}
	mustExec(t, p.db, `UPDATE queue_tally SET done = done - 1, live_data_bytes = live_data_bytes + 123 WHERE bucket = 'laptop'`)
	mustExec(t, p.db, `DELETE FROM queue_tally WHERE bucket = 'photos'`)
	mustExec(t, p.db, `INSERT INTO queue_tally (bucket, done, live_meta) VALUES ('ghost', 3, 1)`)
	if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff == "" {
		t.Fatalf("the drift is not seen by status's reads: %q %v", diff, err)
	}
	// Writes the read does not see, to buckets drifted and not.
	p.db.beforeTallyRepair = func() {
		p.put("laptop", "data/3", "data", 2)
		p.del("photos", "data/1", "data")
		p.put("laptop", "index/2", "snapshot", 0.25)
	}
	drift, err := p.db.RepairQueueTally(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.db.beforeTallyRepair = nil
	got := map[string]TallyDrift{}
	for _, d := range drift {
		got[d.Bucket] = d
	}
	if len(drift) != 3 || got["laptop"].Stored.Done != stored.Done-1 || got["laptop"].Computed.Done != stored.Done ||
		got["laptop"].Stored.LiveDataBytes != stored.LiveDataBytes+123 ||
		got["photos"].Stored != (Tally{}) || got["photos"].Computed.LiveData != 1 ||
		got["ghost"].Stored.Done != 3 || got["ghost"].Computed != (Tally{}) {
		t.Fatalf("drift reported: %+v", drift)
	}
	same("after the repair")
	if drift, err := p.db.RepairQueueTally(ctx); err != nil || len(drift) != 0 {
		t.Fatalf("a repaired tally: drift %+v (%v)", drift, err)
	}
	p.now = p.now.Add(time.Minute)
	p.put("photos", "data/2", "data", 1)
	same("after a write once repaired")
}

// #119: an overflow target's stored hold that drifted from its copies
// (its total changed by hand, a live copy's row lost, a row for a copy
// that is not live, a target's total with no copies) is found and
// corrected by RepairQueueTally beside the queue tallies, reported per
// target with the stored and computed holds. Writes that commit between
// its read and its correction are kept, and a hold that matches is
// left alone.
func TestADriftedOverflowHoldIsRepairedKeepingTheWritesMeanwhile(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	s3 := p.overflow("s3", 100)
	d1 := p.put("laptop", "data/1", "data", 1)
	d2 := p.put("laptop", "data/2", "data", 1)
	p.put("laptop", "index/1", "metadata", 0.5)
	if !slices.Contains(p.places(d1), s3) || !slices.Contains(p.places(d2), s3) {
		t.Fatalf("data at %v and %v, want a copy at %s each", p.places(d1), p.places(d2), s3)
	}
	b2 := p.overflow("b2", 100)
	same := func(when string) {
		t.Helper()
		if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" {
			t.Fatalf("%s: %s %v", when, diff, err)
		}
	}
	same("before any drift")
	if drift, err := p.db.RepairQueueTally(ctx); err != nil || len(drift) != 0 {
		t.Fatalf("holds that match the copies: drift %+v (%v)", drift, err)
	}
	before, err := p.db.OverflowHolds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, p.db, `UPDATE overflow_hold SET objects = objects + 7 WHERE place = ?`, s3)
	mustExec(t, p.db, `DELETE FROM overflow_live WHERE place = ? AND seq = ?`, s3, d2)
	mustExec(t, p.db, `INSERT INTO overflow_live (place, seq, bucket, key, size) VALUES (?, ?, 'laptop', 'data/1', 5)`, b2, d1)
	mustExec(t, p.db, `INSERT INTO overflow_hold (place, objects, bytes) VALUES ('overflow:ghost', 2, 9)`)
	if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff == "" {
		t.Fatalf("the drift is not seen by status's reads: %q %v", diff, err)
	}
	// Writes the read does not see: a copy at a target more, and d1's
	// key deleted, so its copy at s3 is no longer live.
	p.db.beforeTallyRepair = func() {
		p.put("laptop", "data/3", "data", 2)
		p.del("laptop", "data/1", "data")
	}
	drift, err := p.db.RepairQueueTally(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.db.beforeTallyRepair = nil
	got := map[string]TallyDrift{}
	for _, d := range drift {
		got[d.Place] = d
	}
	w := before["s3"]
	if len(drift) != 3 || got[s3].StoredHeld.Objects != w.DataObjects+7 || got[s3].ComputedHeld.Objects != w.DataObjects ||
		got[s3].ComputedHeld.Bytes != w.DataBytes || got[s3].Copies != 1 ||
		got[b2].ComputedHeld != (TargetHeld{}) || got[b2].Copies != 1 ||
		got["overflow:ghost"].StoredHeld != (TargetHeld{2, 9}) || got["overflow:ghost"].ComputedHeld != (TargetHeld{}) {
		t.Fatalf("drift reported: %+v (holds before %+v)", drift, before)
	}
	same("after the repair")
	if h, err := p.db.OverflowHolds(ctx); err != nil || h["s3"].DataObjects+h["b2"].DataObjects != w.DataObjects {
		t.Fatalf("holds %+v (%v): want data/2 and data/3, data/1 deleted", h, err)
	}
	if drift, err := p.db.RepairQueueTally(ctx); err != nil || len(drift) != 0 {
		t.Fatalf("a repaired hold: drift %+v (%v)", drift, err)
	}
}

// #118, #119: what the upgrade to schema 54 (queue_tally's fill), 55,
// 56 and 57 (the overflow holds' fill) costs a million-object bucket,
// once, inside each migration's transaction; then what the daemon's
// hourly tally check costs, and DroppedBelowCopies, DueReplicas and
// OverflowHolds against the reads they replaced, and a capture. A
// hundredth of the data has a copy dropped, and D's copies (two thirds
// of the data) are at an overflow target. Opt-in, as
// TestStoredOffsiteOfAMillionObjectBucketIsFast:
//
//	LENDISK_BIG_UPGRADE=1000000 go test -run TestTheUpgradeAndTallyCheckOfAMillionObjectBucket -v ./internal/state
func TestTheUpgradeAndTallyCheckOfAMillionObjectBucket(t *testing.T) {
	objects, _ := strconv.Atoi(os.Getenv("LENDISK_BIG_UPGRADE"))
	if objects <= 0 {
		t.Skip("LENDISK_BIG_UPGRADE is not set")
	}
	ctx := context.Background()
	dir := t.TempDir()
	old, err := open(ctx, dir, migrations[:53], quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	p := &pool{t: t, db: old, now: time.Unix(1_800_000_000, 0)}
	start := time.Now()
	p.largeRows(objects)
	mustExec(t, old, `UPDATE placement SET state = 'dropped' WHERE place = 'B' AND op = 'put' AND seq % 100 = 1
		AND seq IN (SELECT seq FROM queue WHERE class = 'data')`)
	// D's copies are an overflow target's (#119); no fold has run, so
	// the marks the move makes need not stay.
	mustExec(t, old, `INSERT INTO overflow (name, endpoint, bucket, max_bytes, env_prefix, retention, added_at)
		VALUES ('s3', 'https://x', 'b', 1125899906842624, 'X', 0, 0)`)
	mustExec(t, old, `UPDATE placement SET place = 'overflow:s3' WHERE place = 'D'`)
	mustExec(t, old, `DELETE FROM offsite_dirty`)
	t.Logf("seeded %d objects at schema 53 in %v", objects, time.Since(start))
	for _, m := range migrations[53:] {
		start := time.Now()
		if err := old.apply(ctx, m); err != nil {
			t.Fatal(err)
		}
		t.Logf("migration %d took %v", m.Version, time.Since(start))
	}
	old.Close()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.ReplaceAfter = 7 * 24 * time.Hour
	timed := func(what string, f func() error) {
		t.Helper()
		start := time.Now()
		if err := f(); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s took %v", what, time.Since(start))
	}
	timed("the tally check (no drift)", func() error {
		d, err := db.RepairQueueTally(ctx)
		if err == nil && len(d) != 0 {
			t.Errorf("drift after the upgrade: %+v", d)
		}
		return err
	})
	var got, want map[string]BelowStat
	timed("DroppedBelowCopies", func() (err error) { got, err = db.DroppedBelowCopies(ctx); return err })
	timed("DroppedBelowCopies as before #118", func() (err error) { want, err = droppedBelowFull(ctx, db); return err })
	if !reflect.DeepEqual(got, want) || got["laptop"].Objects == 0 {
		t.Errorf("DroppedBelowCopies %+v, before %+v", got, want)
	}
	var due, dueWas []ReplicaKey
	timed("DueReplicas", func() (err error) { due, err = db.DueReplicas(ctx, p.now, 24*time.Hour, time.Hour); return err })
	timed("DueReplicas as before #118", func() (err error) { dueWas, err = dueReplicasFull(ctx, db, p.now); return err })
	if !reflect.DeepEqual(due, dueWas) {
		t.Errorf("DueReplicas %+v, before %+v", due, dueWas)
	}
	// #119: the target's hold, from the stored totals.
	var holds, holdsWas map[string]OverflowHold
	timed("OverflowHolds", func() (err error) { holds, err = db.OverflowHolds(ctx); return err })
	timed("OverflowHolds as before #119", func() (err error) { holdsWas, err = overflowHoldsFull(ctx, db); return err })
	if !reflect.DeepEqual(holds, holdsWas) || holds["s3"].DataObjects == 0 {
		t.Errorf("OverflowHolds %+v, before %+v", holds, holdsWas)
	}
	// #119: a replica never answered whose FIFO is busy (a copy queued
	// at B), a day after its first acknowledgement, once a fold has
	// stored it.
	mustExec(t, db, `UPDATE placement SET state = 'queued', acked_at = NULL WHERE place = 'B' AND seq =
		(SELECT max(seq) FROM placement WHERE place = 'B' AND state = 'acked')`)
	timed("the first fold", func() error { return db.FoldOffsite(ctx) })
	later := p.now.Add(25 * time.Hour)
	timed("DueReplicas, B busy and never answered", func() (err error) {
		due, err = db.DueReplicas(ctx, later, 24*time.Hour, time.Hour)
		return err
	})
	timed("DueReplicas, B busy, as before #119", func() (err error) { dueWas, err = dueReplicasFull(ctx, db, later); return err })
	if !reflect.DeepEqual(due, dueWas) || !slices.Contains(due, ReplicaKey{Place: "B", Bucket: "laptop"}) {
		t.Errorf("DueReplicas %+v, before %+v", due, dueWas)
	}
	// #119: a capture's metadata reads: one put, finished and placed.
	before := strings.Replace(unsentMetadataSQL, " INDEXED BY queue_metadata", "", 1)
	for what, q := range map[string]string{"unsentMetadata": unsentMetadataSQL, "unsentMetadata as before #119": before} {
		timed(what, func() error {
			var n int64
			return db.QueryRowContext(ctx, q, "laptop", "B", ObjectOverhead).Scan(&n)
		})
	}
	for i := range 3 {
		timed(fmt.Sprintf("capture %d (begin, finish and place)", i), func() error {
			seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: fmt.Sprintf("new/%d", i), Op: OpPut, Class: "data"}})
			if err != nil {
				return err
			}
			_, err = db.FinishPut(ctx, seqs[0], 1000, fmt.Sprintf("n%d", i))
			return err
		})
	}
	timed("the tally check after", func() error {
		d, err := db.RepairQueueTally(ctx)
		if err == nil && len(d) != 0 {
			t.Errorf("drift: %+v", d)
		}
		return err
	})
}
