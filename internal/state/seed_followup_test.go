package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// REPL-8 (#95): rows of the bucket the export did not write (a
// directory marker, a key whose copy changed while it ran) do not keep
// the seed from ending confirmed. They stay held while it is pending,
// then go by the network in order. A new export forgets the old skips.
func TestASeedEndsConfirmedWithoutTheRowsItsExportSkipped(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	capture(t, db, "laptop", "data/1", OpPut)
	marker := capture(t, db, "laptop", "dir/", OpPut)
	through := capture(t, db, "laptop", "data/2", OpPut)
	if err := db.StartSeed(ctx, "B", "laptop", through, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.SeedWritten(ctx, "B", "laptop", 2, 12, []int64{marker}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmSeeded(ctx, "B", "laptop", through, []SeedListed{
		{Key: "data/1", SHA256: "h-data/1"}, {Key: "data/2", SHA256: "h-data/2"},
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if left, err := db.SeedLeft(ctx, "B", "laptop", through); err != nil || left != 0 {
		t.Fatalf("left %d %v; want 0: the skipped marker is not waited for", left, err)
	}
	if got := fifo(t, db, "B"); len(got) != 0 {
		t.Fatalf("B's FIFO while the seed is pending: %v; want nothing", got)
	}
	if err := db.EndSeed(ctx, "B", "laptop", SeedConfirmed, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := fifo(t, db, "B"); len(got) != 1 || got[0] != "laptop/dir/" {
		t.Fatalf("B's FIFO after the seed: %v; want the skipped marker", got)
	}

	// A new export of the bucket replaces the seed and its skips.
	capture(t, db, "laptop", "data/3", OpPut)
	skipped := capture(t, db, "laptop", "data/4", OpPut)
	if err := db.StartSeed(ctx, "B", "laptop", skipped, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.SeedWritten(ctx, "B", "laptop", 1, 6, []int64{skipped}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if left, err := db.SeedLeft(ctx, "B", "laptop", skipped); err != nil || left != 1 {
		t.Fatalf("left %d %v; want 1 (data/3)", left, err)
	}
	if err := db.StartSeed(ctx, "B", "laptop", skipped, time.Now()); err != nil {
		t.Fatal(err)
	}
	if left, err := db.SeedLeft(ctx, "B", "laptop", skipped); err != nil || left != 2 {
		t.Fatalf("left after a new export %d %v; want 2 (data/3 and data/4)", left, err)
	}
}

// REPL-8 (#95): ConfirmSeeded checks that a row is still queued and
// acknowledges it in one transaction: a row abandoned between the two
// (re-placement moving it) stays abandoned and is not counted.
func TestConfirmSeededAcknowledgesOnlyRowsStillQueued(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	seq := capture(t, db, "laptop", "data/1", OpPut)
	if err := db.StartSeed(ctx, "B", "laptop", seq, time.Now()); err != nil {
		t.Fatal(err)
	}
	db.beforeSeedAck = func(tx *sql.Tx) {
		if _, err := tx.ExecContext(ctx, `UPDATE placement SET state = 'abandoned' WHERE place = 'B' AND seq = ?`, seq); err != nil {
			t.Error(err)
		}
	}
	n, err := db.ConfirmSeeded(ctx, "B", "laptop", seq, []SeedListed{{Key: "data/1", SHA256: "h-data/1"}}, time.Now())
	if err != nil || n != 0 {
		t.Fatalf("confirmed %d %v; want 0", n, err)
	}
	var st string
	if err := db.QueryRowContext(ctx, `SELECT state FROM placement WHERE place = 'B' AND seq = ?`, seq).Scan(&st); err != nil || st != "abandoned" {
		t.Fatalf("row %q %v; want abandoned", st, err)
	}
}

// REPL-8 (#95): a pending seed can be cancelled; its rows then go by the
// network. Only a pending seed is cancelled.
func TestCancelledSeedReleasesItsRows(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	seq := capture(t, db, "laptop", "data/1", OpPut)
	if err := db.CancelSeed(ctx, "B", "laptop", time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel with no seed: %v", err)
	}
	if err := db.StartSeed(ctx, "B", "laptop", seq, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := fifo(t, db, "B"); len(got) != 0 {
		t.Fatalf("held: %v", got)
	}
	if err := db.CancelSeed(ctx, "B", "laptop", time.Now()); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetSeed(ctx, "B", "laptop"); s.Outcome != SeedCancelled || s.DoneAt.IsZero() {
		t.Fatalf("seed %+v", s)
	}
	if got := fifo(t, db, "B"); len(got) != 1 {
		t.Fatalf("after the cancel: %v; want data/1", got)
	}
	if err := db.CancelSeed(ctx, "B", "laptop", time.Now()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel of an ended seed: %v", err)
	}
}

// REPL-8 (#95): the lender keeps the newest export of each borrower's
// bucket it imported and refuses an older one; the same export again
// (an import run again) and a newer one pass. Other buckets and
// borrowers are separate.
func TestAnOlderSeedingDiskIsRefused(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	t0 := time.Unix(1_800_000_000, 5)
	pass := func(borrower, sealed string, at time.Time) {
		t.Helper()
		if _, err := db.SeedImport(ctx, borrower, sealed, at, time.Now()); err != nil {
			t.Fatalf("%s %s at %v: %v", borrower, sealed, at, err)
		}
	}
	pass("A", "s1", t0)
	pass("A", "s1", t0)
	pass("A", "s1", t0.Add(time.Hour))
	newest, err := db.SeedImport(ctx, "A", "s1", t0.Add(time.Hour-time.Nanosecond), time.Now())
	if !errors.Is(err, ErrOlderSeed) || !newest.Equal(t0.Add(time.Hour)) {
		t.Fatalf("older disk: %v %v", newest, err)
	}
	pass("A", "s2", t0)
	pass("C", "s1", t0)
}

// REPL-8 (#95): SeedObjects says each key's class, so an export can put
// data before metadata in its manifest.
func TestSeedObjectsCarryTheirClass(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	capture(t, db, "laptop", "data/1", OpPut)
	objs, err := db.SeedObjects(ctx, "B", "laptop")
	if err != nil || len(objs) != 1 || objs[0].Class != "data" {
		t.Fatalf("%+v %v", objs, err)
	}
}
