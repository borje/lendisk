package state

import (
	"context"
	"testing"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// EP-3: a capture is a pending row before the backend call, then done
// with its hash, or gone; REPL-1: rows keep the write sequence.
func TestCaptureLifecycle(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	seqs, err := db.BeginCaptures(ctx, []Capture{
		{Bucket: "b", Key: "data/1", Op: OpPut, Class: "data", Token: "t1"},
		{Bucket: "b", Key: "index/1", Op: OpPut, Class: "metadata", Token: "t2"},
		{Bucket: "c", Key: "x", Op: OpDelete, Class: "data"},
	})
	if err != nil || len(seqs) != 3 || !(seqs[0] < seqs[1] && seqs[1] < seqs[2]) {
		t.Fatalf("seqs %v %v", seqs, err)
	}
	p, err := db.PendingCaptures(ctx)
	if err != nil || len(p) != 3 || p[0].Token != "t1" {
		t.Fatalf("pending %+v %v", p, err)
	}
	if dup, err := db.FinishPut(ctx, seqs[0], 5, "aa"); err != nil || dup {
		t.Fatal(dup, err)
	}
	if err := db.DropCaptures(ctx, seqs[1]); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs[2]); err != nil {
		t.Fatal(err)
	}
	if p, _ := db.PendingCaptures(ctx); len(p) != 0 {
		t.Fatalf("still pending: %+v", p)
	}
	es, _ := db.QueueEntries(ctx, "b")
	if len(es) != 1 || es[0].Key != "data/1" || es[0].SHA256 != "aa" || es[0].Size != 5 || es[0].State != StateDone {
		t.Fatalf("entries %+v", es)
	}
	st, err := db.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st["b"] != (QueueStat{Entries: 1, Bytes: 5}) || st["c"] != (QueueStat{Entries: 1}) {
		t.Fatalf("stats %+v", st)
	}
}

// EP-11, EP-3: an identical re-upload of a key whose latest entry is a
// put of the same hash is not queued again; after a delete it is.
func TestIdenticalReuploadNotQueuedTwice(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	put := func(hash string) bool {
		t.Helper()
		s, err := db.BeginCaptures(ctx, []Capture{{Bucket: "b", Key: "k", Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		dup, err := db.FinishPut(ctx, s[0], 1, hash)
		if err != nil {
			t.Fatal(err)
		}
		return dup
	}
	if put("h1") || !put("h1") || put("h2") || put("h1") {
		t.Fatal("dedupe wrong")
	}
	s, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "b", Key: "k", Op: OpDelete, Class: "data"}})
	db.FinishDelete(ctx, s[0])
	if put("h1") {
		t.Fatal("put after delete deduped")
	}
	if es, _ := db.QueueEntries(ctx, "b"); len(es) != 5 {
		t.Fatalf("%d entries", len(es))
	}
}

// EP-3, multipart: a completed upload's row is found by its upload ID,
// pending or done, so a retried completion is not queued twice.
func TestUploadEntryFindsCompletion(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if _, _, ok, err := db.UploadEntry(ctx, "b", "k", "up1"); ok || err != nil {
		t.Fatalf("found before capture: %v", err)
	}
	s, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "b", Key: "k", Op: OpPut, Class: "data", UploadID: "up1", ETag: `"e-2"`}})
	seq, st, ok, err := db.UploadEntry(ctx, "b", "k", "up1")
	if !ok || err != nil || seq != s[0] || st != StatePending {
		t.Fatalf("pending: %d %s %v %v", seq, st, ok, err)
	}
	if _, err := db.FinishPut(ctx, s[0], 14, "whole"); err != nil {
		t.Fatal(err)
	}
	if _, st, ok, _ := db.UploadEntry(ctx, "b", "k", "up1"); !ok || st != StateDone {
		t.Fatalf("done: %s %v", st, ok)
	}
	if _, _, ok, _ := db.UploadEntry(ctx, "b", "k", "up2"); ok {
		t.Fatal("another upload matched")
	}
}
