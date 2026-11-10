package state

import (
	"context"
	"testing"
	"time"
)

// OPS-1 (#110): a snapshot's reads all see the database as of its first
// read; a writer (the daemon, another connection) commits meanwhile
// without waiting for it, and the snapshot refuses to write.
func TestSnapshotReadsOneMomentWithoutBlockingWriters(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	daemon, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()

	snap, done, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if got, err := snap.Overflows(ctx); err != nil || len(got) != 0 {
		t.Fatalf("before the write: %v, %v", got, err)
	}
	// Far below busy_timeout: a writer blocked by the snapshot fails.
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := daemon.AddOverflow(wctx, Overflow{Name: "b2", Endpoint: "https://s3.example", Bucket: "offsite",
		MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, AddedAt: time.Now()}); err != nil {
		t.Fatalf("a write while a snapshot is open: %v", err)
	}
	if got, err := snap.Overflows(ctx); err != nil || len(got) != 0 {
		t.Fatalf("the snapshot saw a later write: %v, %v", got, err)
	}
	if hs, err := snap.OverflowHolds(ctx); err != nil || len(hs) != 0 {
		t.Fatalf("the snapshot's other reads saw a later write: %v, %v", hs, err)
	}
	if _, _, err := snap.OverflowDrainStatus(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Overflows(ctx); err != nil || len(got) != 1 {
		t.Fatalf("the database itself: %v, %v", got, err)
	}
	if err := snap.SetNode(ctx, "k", "v"); err == nil {
		t.Fatal("a snapshot wrote")
	}
	if _, err := snap.BeginTx(ctx, nil); err == nil {
		t.Fatal("a snapshot began a transaction")
	}
	if err := done(); err != nil {
		t.Fatal(err)
	}
	if err := db.SetNode(ctx, "k", "v"); err != nil {
		t.Fatalf("the database after the snapshot: %v", err)
	}
}
