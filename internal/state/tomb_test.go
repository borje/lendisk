package state

import (
	"context"
	"testing"
	"time"
)

// RET-1: a tombstone row records a borrower's deleted object (hash, size,
// where its bytes are kept, when it may be purged); it is pending until
// its move is done, purged rows free their bytes in the same transaction,
// and the list reads the newest tombstone of each key in key order.
func TestTombstoneRows(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	add := func(peer, key, hash string, at time.Time, purge time.Duration) int64 {
		t.Helper()
		id, err := db.AddTombstone(ctx, Tombstone{Peer: peer, Sealed: "sb", Key: key, Tomb: ".lendisk/tomb/" + key + "/1",
			SHA256: hash, ETag: "e-" + hash, Size: 10, DeletedAt: at, PurgeAfter: at.Add(purge)})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a := add("bob", "data/a", "h1", now, time.Hour)
	b := add("bob", "data/b", "h2", now, 2*time.Hour)
	b2 := add("bob", "data/b", "h3", now.Add(time.Minute), 2*time.Hour)
	add("carol", "data/a", "h4", now, time.Hour)

	if p, err := db.PendingTombstones(ctx); err != nil || len(p) != 4 || !p[0].Pending {
		t.Fatalf("pending %+v %v", p, err)
	}
	for _, id := range []int64{a, b, b2} {
		if err := db.KeepTombstone(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if p, _ := db.PendingTombstones(ctx); len(p) != 1 || p[0].Peer != "carol" {
		t.Fatalf("pending after keep %+v", p)
	}
	ts, err := db.TombstonesOf(ctx, "bob", "sb", "data/b")
	if err != nil || len(ts) != 2 || ts[0].SHA256 != "h2" || ts[1].ETag != "e-h3" || ts[0].Pending {
		t.Fatalf("data/b's tombstones %+v %v", ts, err)
	}
	if !ts[0].DeletedAt.Equal(now) || !ts[0].PurgeAfter.Equal(now.Add(2*time.Hour)) || ts[0].Tomb != ".lendisk/tomb/data/b/1" {
		t.Fatalf("times or tomb %+v", ts[0])
	}

	page, err := db.TombstonedKeys(ctx, "bob", "sb", "", 10)
	if err != nil || len(page) != 2 || page[0].Key != "data/a" || page[1].Key != "data/b" || page[1].SHA256 != "h3" {
		t.Fatalf("listed %+v %v", page, err)
	}
	if page, _ := db.TombstonedKeys(ctx, "bob", "sb", "data/a", 10); len(page) != 1 || page[0].Key != "data/b" {
		t.Fatalf("listed after data/a %+v", page)
	}

	if n, bytes, err := db.TombstoneUsage(ctx, "bob"); err != nil || n != 3 || bytes != 30 {
		t.Fatalf("bob's tombstones %d, %d bytes, %v", n, bytes, err)
	}

	// Due: kept rows whose purge time has come; the pending one is not.
	due, err := db.DueTombstones(ctx, now.Add(time.Hour), 10)
	if err != nil || len(due) != 1 || due[0].ID != a {
		t.Fatalf("due %+v %v", due, err)
	}
	if err := db.AddLentUsage(ctx, "bob", 100); err != nil {
		t.Fatal(err)
	}
	if err := db.PurgeTombstone(ctx, a, "bob", 10+4096); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := db.LentUsage(ctx, "bob"); n != 0 {
		t.Fatalf("usage after purge %d", n)
	}
	if ts, _ := db.TombstonesOf(ctx, "bob", "sb", "data/a"); len(ts) != 0 {
		t.Fatalf("purged row kept %+v", ts)
	}
	if err := db.PendTombstone(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := db.DropTombstone(ctx, b2); err != nil {
		t.Fatal(err)
	}
	if ts, _ := db.TombstonesOf(ctx, "bob", "sb", "data/b"); len(ts) != 1 || !ts[0].Pending {
		t.Fatalf("after pend and drop %+v", ts)
	}
}
