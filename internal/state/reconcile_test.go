package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// indexed returns bucket's indexed keys as key → local, all pages.
func indexed(t *testing.T, db *DB, bucket string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	after := ""
	for {
		ks, err := db.IndexedKeys(context.Background(), bucket, after, "", 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(ks) == 0 {
			return out
		}
		for _, k := range ks {
			out[k.Key] = k.Local
		}
		after = ks[len(ks)-1].Key
	}
}

func putDone(t *testing.T, db *DB, c Capture, size int64, sha string) int64 {
	t.Helper()
	c.Op = OpPut
	seqs, err := db.BeginCaptures(context.Background(), []Capture{c})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(context.Background(), seqs[0], size, sha); err != nil {
		t.Fatal(err)
	}
	return seqs[0]
}

// EP-14 (#8): a key is held locally once this node knows its object was
// in its own store: a capture stored it, the read-through fetched it, or
// a reconcile pass saw it. A recovered key the node never read is
// indexed but not held, so its absence from the store is no delete. A
// captured delete ends the key's index entry.
func TestHeldLocallyOnlyWhenTheStoreHadIt(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data"}, 1, "h-a")
	// An identical re-upload is dropped and the key stays held.
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data"}, 1, "h-a")

	// Recovery records data/b and data/c as listed at B.
	if err := db.AddRelists(ctx, []string{"B"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordListed(ctx, "B", "laptop", []Listed{
		{Key: "data/b", Size: 1, SHA256: "h-b", Class: "data"},
		{Key: "data/c", Size: 1, SHA256: "h-c", Class: "data"},
	}, 2, "", true); err != nil {
		t.Fatal(err)
	}
	if err := db.SettleFinished(ctx); err != nil {
		t.Fatal(err)
	}
	if got := indexed(t, db, "laptop"); len(got) != 3 || !got["data/a"] || got["data/b"] || got["data/c"] {
		t.Fatalf("indexed %v; want data/a held, the recovered keys not", got)
	}

	// The read-through fetched data/b; a stale hash marks nothing.
	if err := db.MarkLocal(ctx, "laptop", "data/c", "h-other", ""); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkLocal(ctx, "laptop", "data/b", "h-b", ""); err != nil {
		t.Fatal(err)
	}
	if got := indexed(t, db, "laptop"); !got["data/b"] || got["data/c"] {
		t.Fatalf("indexed %v after the read-through fetched data/b", got)
	}
	// A pass saw data/c in the store.
	k, err := db.KeyNow(ctx, "laptop", "data/c")
	if err != nil || !k.Indexed() || k.Local || k.Pending {
		t.Fatalf("data/c %+v %v", k, err)
	}
	if err := db.MarkLocalSeqs(ctx, k.Seq); err != nil {
		t.Fatal(err)
	}
	if k, _ := db.KeyNow(ctx, "laptop", "data/c"); !k.Local {
		t.Fatalf("data/c not held after the pass saw it: %+v", k)
	}

	// A captured delete: no longer indexed; a pending write shows.
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/a", Op: OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if k, _ := db.KeyNow(ctx, "laptop", "data/a"); !k.Pending || !k.Indexed() {
		t.Fatalf("data/a with a pending delete: %+v", k)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if got := indexed(t, db, "laptop"); len(got) != 2 {
		t.Fatalf("indexed %v after data/a's delete", got)
	}
	if k, _ := db.KeyNow(ctx, "laptop", "data/a"); k.Indexed() || k.Pending {
		t.Fatalf("data/a after its delete: %+v", k)
	}

	// Ranges: after and upto bound the page.
	ks, err := db.IndexedKeys(ctx, "laptop", "data/b", "data/c", 10)
	if err != nil || len(ks) != 1 || ks[0].Key != "data/c" {
		t.Fatalf("range (data/b, data/c]: %+v %v", ks, err)
	}
}

// EP-14 (#8): reconciled rows are counted per bucket; deletes a pass
// held back stay held until the user allows at least as many, which the
// next pass consumes.
func TestReconcileCountsAndHeldDeletes(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	now := time.Unix(1_800_000_000, 0)
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/a", Class: "data", Reconciled: true}, 1, "h-a")
	putDone(t, db, Capture{Bucket: "laptop", Key: "data/b", Class: "data"}, 1, "h-b")
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/b", Op: OpDelete, Class: "data", Reconciled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	// A pending reconciled row is not counted.
	if _, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/c", Op: OpPut, Class: "data", Reconciled: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AllowDeletes(ctx, "laptop", 5); !errors.Is(err, ErrNothingHeld) {
		t.Fatalf("allow with nothing held: %v", err)
	}
	if err := db.FinishReconcile(ctx, "laptop", now, 40, 50, 0); err != nil {
		t.Fatal(err)
	}
	st, err := db.ReconcileStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := st["laptop"]
	if r.Writes != 1 || r.Deletes != 1 || r.Held != 40 || r.HeldOf != 50 || !r.HeldAt.Equal(now) || !r.PassedAt.Equal(now) || r.Allowed != 0 {
		t.Fatalf("state %+v", r)
	}
	if held, err := db.AllowDeletes(ctx, "laptop", 39); !errors.Is(err, ErrAllowTooFew) || held != 40 {
		t.Fatalf("allow fewer than held: %d %v", held, err)
	}
	if _, err := db.AllowDeletes(ctx, "laptop", 40); err != nil {
		t.Fatal(err)
	}
	if bs, err := db.AllowedBuckets(ctx); err != nil || len(bs) != 1 || bs[0] != "laptop" {
		t.Fatalf("allowed buckets %v %v", bs, err)
	}
	// The next pass, held again (still held since the first), consumes
	// the allowance.
	later := now.Add(time.Hour)
	if err := db.FinishReconcile(ctx, "laptop", later, 0, 0, 40); err != nil {
		t.Fatal(err)
	}
	r, err = db.ReconcileState(ctx, "laptop")
	if err != nil || r.Held != 0 || r.Allowed != 0 || !r.HeldAt.IsZero() || !r.PassedAt.Equal(later) {
		t.Fatalf("after the allowed pass %+v %v", r, err)
	}
	if bs, _ := db.AllowedBuckets(ctx); len(bs) != 0 {
		t.Fatalf("allowance not consumed: %v", bs)
	}
	// A hold keeps the time it began.
	db.FinishReconcile(ctx, "laptop", later, 3, 3, 0)
	db.FinishReconcile(ctx, "laptop", later.Add(time.Hour), 4, 4, 0)
	if r, _ := db.ReconcileState(ctx, "laptop"); !r.HeldAt.Equal(later) || r.Held != 4 {
		t.Fatalf("hold %+v", r)
	}
	// An allowance set while a pass ran is kept by that pass.
	db.AllowDeletes(ctx, "laptop", 9)
	db.FinishReconcile(ctx, "laptop", later, 4, 4, 5)
	if r, _ := db.ReconcileState(ctx, "laptop"); r.Allowed != 9 {
		t.Fatalf("allowance %+v", r)
	}
	if err := db.DeleteBucket(ctx, "laptop"); err != nil {
		t.Fatal(err)
	}
	if r, _ := db.ReconcileState(ctx, "laptop"); r.Held != 0 || r.Allowed != 0 {
		t.Fatalf("after the bucket's delete %+v", r)
	}
}
