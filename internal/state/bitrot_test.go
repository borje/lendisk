package state

import (
	"context"
	"testing"
	"time"
)

// VER-5 (#31): the sampler reads what the index holds of a key: the
// version, whether it is held locally and since when, whether a write
// is in flight and whether EP-11 held it back.
func TestOwnVersionForTheSampler(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	at := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return at }
	seq := putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data"}, 3, "h-a")
	v, err := db.OwnVersion(ctx, "laptop", "data/a")
	if err != nil {
		t.Fatal(err)
	}
	if !v.Held() || v.Seq != seq || v.SHA256 != "h-a" || v.Size != 3 || !v.Local.Equal(at) || v.Pending || v.Changed ||
		v.Stamp != "" {
		t.Fatalf("version %+v; want held, unverified", v)
	}
	// The file's identity is recorded with the hash it was verified at;
	// another hash records nothing.
	if err := db.MarkLocal(ctx, "laptop", "data/a", "h-other", "1:3:5"); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.OwnVersion(ctx, "laptop", "data/a"); v.Stamp != "" {
		t.Fatalf("stamped for another hash: %+v", v)
	}
	if err := db.MarkLocal(ctx, "laptop", "data/a", "h-a", "1:3:5"); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.OwnVersion(ctx, "laptop", "data/a"); v.Stamp != "1:3:5" {
		t.Fatalf("not stamped: %+v", v)
	}
	// An identical re-upload is another file: unverified until stamped.
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data"}, 3, "h-a")
	if v, _ := db.OwnVersion(ctx, "laptop", "data/a"); v.Stamp != "" || v.Seq != seq {
		t.Fatalf("after a re-upload: %+v", v)
	}
	if v, _ := db.OwnVersion(ctx, "laptop", "data/none"); v.Seq != 0 || v.Held() {
		t.Fatalf("unknown key: %+v", v)
	}

	// A write in flight.
	if _, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/a", Op: OpPut, Class: "data"}}); err != nil {
		t.Fatal(err)
	}
	if v, _ := db.OwnVersion(ctx, "laptop", "data/a"); !v.Pending {
		t.Fatalf("pending not seen: %+v", v)
	}

	// EP-11: B acknowledged data/b; another version is held back.
	if _, err := db.ExecContext(ctx, `INSERT INTO friendship (peer_key, created_at) VALUES ('B', 0)`); err != nil {
		t.Fatal(err)
	}
	s1 := putDone(t, db, Capture{Bucket: "laptop", Key: "data/b", Class: "data"}, 1, "h-b1")
	if _, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO placement (seq, place, bucket, key, op, state, sha256, acked_at)
		VALUES (?, 'B', 'laptop', 'data/b', 'put', 'acked', 'h-b1', 0)`, s1); err != nil {
		t.Fatal(err)
	}
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/b", Class: "data"}, 1, "h-b2")
	if v, _ := db.OwnVersion(ctx, "laptop", "data/b"); !v.Changed || v.SHA256 != "h-b2" {
		t.Fatalf("held back not seen: %+v", v)
	}
}

// VER-5 (#31): the sweep's place and the rotten objects survive a
// restart; a row is replaced when its key rots again and dropped on
// request.
func TestSweepAndRotRecords(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, ok, err := db.Sweep(ctx); err != nil || ok {
		t.Fatalf("a new node has a sweep: %v %v", ok, err)
	}
	sw := Sweep{Bucket: "laptop", Key: "data/b", CycleAt: time.Unix(100, 0), Cycles: 2, Objects: 5, Bytes: 500,
		CheckedAt: time.Unix(200, 0), SteppedAt: time.Unix(210, 0), Owed: -42.5}
	if err := db.SaveSweep(ctx, sw); err != nil {
		t.Fatal(err)
	}
	sw.Key = "data/c"
	if err := db.SaveSweep(ctx, sw); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := db.Sweep(ctx); err != nil || !ok || got != sw {
		t.Fatalf("sweep %+v %v %v; want %+v", got, ok, err, sw)
	}

	own := Rot{Bucket: "laptop", Key: "data/a", Seq: 7, Stamp: "1:3:5", SHA256: "h-a", Found: "h-x", Size: 3, FoundAt: time.Unix(300, 0)}
	lent := Rot{Bucket: "lent-0123456789abcdef-s1", Key: "data/z", Borrower: "A", Sealed: "s1", SHA256: "h-z", Found: "h-y",
		Size: 9, FoundAt: time.Unix(310, 0)}
	for _, r := range []Rot{own, lent} {
		if err := db.RecordRot(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	own.Refetch, own.RepairedAt, own.Detail = true, time.Unix(302, 0), "re-fetched from B"
	if err := db.RecordRot(ctx, own); err != nil {
		t.Fatal(err)
	}
	rs, err := db.Rots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 || rs[0] != own || rs[1] != lent {
		t.Fatalf("rots %+v", rs)
	}
	if err := db.DropRot(ctx, lent.Bucket, lent.Key); err != nil {
		t.Fatal(err)
	}
	if rs, _ := db.Rots(ctx); len(rs) != 1 || rs[0] != own {
		t.Fatalf("after drop: %+v", rs)
	}
}

// VER-5 (#31): the sample's size is a share of what the store holds:
// the own objects held locally and what friends store here.
func TestStoredBytesCountsHeldAndLent(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data"}, 100, "h-a")
	s := putDone(t, db, Capture{Bucket: "laptop", Key: "data/b", Class: "data"}, 50, "h-b")
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/c", Class: "data"}, 1, "h-c")
	seqs, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/c", Op: OpDelete, Class: "data"}})
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE queue SET local = NULL WHERE seq = ?`, s); err != nil {
		t.Fatal(err)
	}
	if err := db.AddLentUsage(ctx, "A", 1000); err != nil {
		t.Fatal(err)
	}
	if n, err := db.StoredBytes(ctx); err != nil || n != 1100 {
		t.Fatalf("stored %d %v; want 1100", n, err)
	}
	if err := db.AddLentBucket(ctx, "A", "s2", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := db.AddLentBucket(ctx, "B", "s1", time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	bs, err := db.AllLentBuckets(ctx)
	if err != nil || len(bs) != 2 || bs[0].Peer != "A" || bs[1].Peer != "B" {
		t.Fatalf("lent buckets %+v %v", bs, err)
	}
}
