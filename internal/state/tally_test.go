package state

import (
	"context"
	"os"
	"reflect"
	"strconv"
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

// #118: what the upgrade to schema 54 (queue_tally's fill), 55 and 56
// costs a million-object bucket, once, inside each migration's
// transaction; then what the daemon's hourly tally check costs, and
// DroppedBelowCopies and DueReplicas against the reads they replaced.
// A hundredth of the data has a copy dropped. Opt-in, as
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
}
