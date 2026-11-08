package state

import (
	"context"
	"log/slog"
	"testing"
)

// REPL-6 (#23): an open multipart upload of a placement row and its
// completed parts are kept in lendisk.db, so a restart continues at the
// next part; dropping the transfer forgets them.
func TestATransferAndItsPartsSurviveAReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	friend(t, db, "B")
	seq := capture(t, db, "laptop", "data/big", OpPut)
	if _, ok, err := db.Transfer(ctx, "B", seq); err != nil || ok {
		t.Fatalf("a transfer before any: %v %v", ok, err)
	}
	if err := db.BeginTransfer(ctx, Transfer{Place: "B", Seq: seq, UploadID: "u1", PartSize: 16 << 20, Replaces: "r"}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []TransferPart{{Number: 2, ETag: "e2", SHA256: "s2", Size: 7}, {Number: 1, ETag: "e1", SHA256: "s1", Size: 16 << 20}} {
		if err := db.AddTransferPart(ctx, "B", seq, p); err != nil {
			t.Fatal(err)
		}
	}
	// A part sent again replaces its record.
	if err := db.AddTransferPart(ctx, "B", seq, TransferPart{Number: 2, ETag: "e2b", SHA256: "s2", Size: 7}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr, ok, err := db.Transfer(ctx, "B", seq)
	if err != nil || !ok || tr.UploadID != "u1" || tr.PartSize != 16<<20 || tr.Replaces != "r" || len(tr.Parts) != 2 ||
		tr.Parts[0].Number != 1 || tr.Parts[1].ETag != "e2b" || tr.Parts[1].Size != 7 {
		t.Fatalf("%+v %v %v", tr, ok, err)
	}
	// Beginning again (the upload was gone at the place) starts over.
	if err := db.BeginTransfer(ctx, Transfer{Place: "B", Seq: seq, UploadID: "u2", PartSize: 8 << 20}); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := db.Transfer(ctx, "B", seq); tr.UploadID != "u2" || len(tr.Parts) != 0 || tr.Replaces != "" {
		t.Fatalf("begun again: %+v", tr)
	}
	if err := db.DropTransfer(ctx, "B", seq); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Transfer(ctx, "B", seq); ok {
		t.Fatal("dropped, still there")
	}
}

// REPL-6 (#23): transfers of rows no longer queued at their place
// (acknowledged, refused, cancelled, re-routed) are listed for the
// worker to abort and forget; a queued row's stays.
func TestStaleTransfersAreListed(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	live := capture(t, db, "laptop", "data/live", OpPut)
	done := capture(t, db, "laptop", "data/done", OpPut)
	for _, s := range []int64{live, done} {
		if err := db.BeginTransfer(ctx, Transfer{Place: "B", Seq: s, UploadID: "u", PartSize: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AckPlacement(ctx, "B", done, "h", db.now()); err != nil {
		t.Fatal(err)
	}
	stale, err := db.StaleTransfers(ctx)
	if err != nil || len(stale) != 1 || stale[0].Seq != done || stale[0].Bucket != "laptop" || stale[0].Key != "data/done" {
		t.Fatalf("%+v %v", stale, err)
	}
}

// #23, folded in from the review of #61: rows refused before #61 told
// the causes apart (cause store, larger than one PUT) become oversize
// in a data migration, so RequeueOversize matches the cause alone and
// a later genuine store refusal of a large object is never re-queued.
func TestOldStoreRefusalsAboveOnePutBecomeOversize(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	quiet := slog.New(slog.DiscardHandler)
	old, err := open(ctx, dir, migrations[:30], quiet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(ctx, `INSERT INTO friendship (peer_key, created_at, address, own_number, peer_number)
		VALUES ('B', 0, 'tcB', ?, ?)`, int64(1<<40), int64(1<<40)); err != nil {
		t.Fatal(err)
	}
	insert := func(key string, size int64, cause string) int64 {
		seqs, err := old.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: key, Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := old.ExecContext(ctx, `UPDATE queue SET state = 'done', size = ?, sha256 = 'h' WHERE seq = ?`, size, seqs[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := old.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, size, cause, detail)
			VALUES (?, 'B', 'laptop', ?, 'put', 'refused', ?, ?, 'refused')`, seqs[0], key, size+ObjectOverhead, cause); err != nil {
			t.Fatal(err)
		}
		return seqs[0]
	}
	big := insert("data/big", 5<<30+1, CauseStore)
	small := insert("data/small", 5, CauseStore)
	limit := insert("data/limit", 5<<30, CauseStore)
	old.Close()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cause := func(seq int64) string {
		var c string
		db.QueryRow(`SELECT cause FROM placement WHERE seq = ?`, seq).Scan(&c)
		return c
	}
	if cause(big) != CauseOversize || cause(small) != CauseStore || cause(limit) != CauseStore {
		t.Fatalf("causes %s %s %s", cause(big), cause(small), cause(limit))
	}
}
