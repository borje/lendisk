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

// EP-3, multipart: part hashes are kept per upload as parts succeed and
// move to the entry when the upload completes, in one transaction.
func TestUploadPartsMoveToEntry(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	db.SaveUploadPart(ctx, "up1", Part{Number: 1, Size: 10, SHA256: "p1"})
	db.SaveUploadPart(ctx, "up1", Part{Number: 2, Size: 3, SHA256: "old"})
	db.SaveUploadPart(ctx, "up1", Part{Number: 2, Size: 4, SHA256: "p2"}) // re-uploaded part
	db.SaveUploadPart(ctx, "up2", Part{Number: 1, Size: 1, SHA256: "q"})
	parts, err := db.UploadParts(ctx, "up1")
	if err != nil || len(parts) != 2 || parts[2].SHA256 != "p2" || parts[2].Size != 4 {
		t.Fatalf("parts %+v %v", parts, err)
	}
	if done, _ := db.UploadCaptured(ctx, "b", "k", "up1"); done {
		t.Fatal("captured before completion")
	}
	s, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "b", Key: "k", Op: OpPut, Class: "data", UploadID: "up1", ETag: `"e-2"`,
		Parts: []Part{parts[1], parts[2]}}})
	if got, _ := db.EntryParts(ctx, s[0]); len(got) != 2 {
		t.Fatal("pending entry has no parts")
	}
	if _, err := db.FinishPut(ctx, s[0], 14, "c-2"); err != nil {
		t.Fatal(err)
	}
	if p, _ := db.UploadParts(ctx, "up1"); len(p) != 0 {
		t.Fatal("upload parts left after completion")
	}
	if p, _ := db.UploadParts(ctx, "up2"); len(p) != 1 {
		t.Fatal("another upload's parts removed")
	}
	if done, _ := db.UploadCaptured(ctx, "b", "k", "up1"); !done {
		t.Fatal("completed upload not found")
	}
	got, err := db.EntryParts(ctx, s[0])
	if err != nil || len(got) != 2 || got[0].SHA256 != "p1" || got[1].Number != 2 {
		t.Fatalf("entry parts %+v %v", got, err)
	}
	// A dropped capture takes its parts with it.
	s2, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "b", Key: "k2", Op: OpPut, Class: "data", UploadID: "up2",
		Parts: []Part{{Number: 1, Size: 1, SHA256: "q"}}}})
	db.DropCaptures(ctx, s2[0])
	if got, _ := db.EntryParts(ctx, s2[0]); len(got) != 0 {
		t.Fatal("dropped entry's parts kept")
	}
	db.DropUpload(ctx, "up2")
	if p, _ := db.UploadParts(ctx, "up2"); len(p) != 0 {
		t.Fatal("aborted upload's parts kept")
	}
}
