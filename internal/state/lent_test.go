package state

import (
	"context"
	"testing"
	"time"
)

// PEER-8, SEC-16: the lender's mapping table holds each borrower's sealed
// names and nothing else; one borrower's rows are invisible to another.
func TestLentBucketsArePerBorrower(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	for _, r := range []struct{ peer, sealed string }{{"bob", "sbbb"}, {"bob", "saaa"}, {"carol", "sccc"}} {
		if err := db.AddLentBucket(ctx, r.peer, r.sealed, now); err != nil {
			t.Fatal(err)
		}
	}
	// Adding twice keeps the first row (a retried CreateBucket).
	if err := db.AddLentBucket(ctx, "bob", "saaa", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	bs, err := db.LentBuckets(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 2 || bs[0].Sealed != "saaa" || bs[1].Sealed != "sbbb" || !bs[0].CreatedAt.Equal(now) || bs[0].Peer != "bob" {
		t.Fatalf("bob's buckets %+v", bs)
	}
	if ok, err := db.HasLentBucket(ctx, "bob", "sbbb"); err != nil || !ok {
		t.Fatalf("bob's sbbb: %v %v", ok, err)
	}
	if ok, err := db.HasLentBucket(ctx, "carol", "sbbb"); err != nil || ok {
		t.Fatalf("carol sees bob's sbbb: %v %v", ok, err)
	}
	if bs, _ := db.LentBuckets(ctx, "dave"); len(bs) != 0 {
		t.Fatalf("dave has %+v", bs)
	}
}

// LEND-1: each borrower's byte counter moves by what its completed
// writes and deletes change, never below zero, and a reconcile sets it
// from a listing and records when.
func TestLentUsageIsPerBorrowerAndReconciled(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	if n, at, err := db.LentUsage(ctx, "bob"); err != nil || n != 0 || !at.IsZero() {
		t.Fatalf("fresh usage: %d %v %v", n, at, err)
	}
	for _, d := range []int64{100, 50, -30} {
		if err := db.AddLentUsage(ctx, "bob", d); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddLentUsage(ctx, "carol", 7); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := db.LentUsage(ctx, "bob"); n != 120 {
		t.Fatalf("bob uses %d, want 120", n)
	}
	if err := db.AddLentUsage(ctx, "carol", -100); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := db.LentUsage(ctx, "carol"); n != 0 {
		t.Fatalf("carol uses %d, want 0", n)
	}
	now := time.Unix(1_800_000_000, 0)
	if err := db.SetLentUsage(ctx, "bob", 99, now); err != nil {
		t.Fatal(err)
	}
	if n, at, _ := db.LentUsage(ctx, "bob"); n != 99 || !at.Equal(now) {
		t.Fatalf("reconciled: %d at %v", n, at)
	}
	// A later write moves the counter and keeps the reconcile time.
	if err := db.AddLentUsage(ctx, "bob", 1); err != nil {
		t.Fatal(err)
	}
	if n, at, _ := db.LentUsage(ctx, "bob"); n != 100 || !at.Equal(now) {
		t.Fatalf("after a write: %d at %v", n, at)
	}
}

// LEND-4: a borrower's multipart upload keeps the hash it declared until
// it is completed or aborted.
func TestLentUploadKeepsTheDeclaredHash(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	u := LentUpload{UploadID: "up1", Peer: "bob", Sealed: "saaa", Key: "data/x", SHA256: "ab", CreatedAt: time.Unix(1_800_000_000, 0)}
	if err := db.AddLentUpload(ctx, u); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetLentUpload(ctx, "up1")
	if err != nil || got != u {
		t.Fatalf("got %+v %v", got, err)
	}
	if err := db.DropLentUpload(ctx, "up1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetLentUpload(ctx, "up1"); err != ErrNotFound {
		t.Fatalf("dropped upload: %v", err)
	}
}

// LEND-1: settling a part counts what it changes over the part of that
// number counted before, in the upload's count and the borrower's usage
// at once; a re-sent part counts once. An upload's parts can be set
// from a listing, and dropping the upload drops them.
func TestLentPartsSettleOnce(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	if err := db.AddLentUpload(ctx, LentUpload{UploadID: "up", Peer: "bob", CreatedAt: time.Unix(1_800_000_000, 0)}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		part        int32
		size, delta int64
	}{{1, 100, 100}, {1, 100, 0}, {1, 40, -60}, {2, 7, 7}} {
		d, err := db.SettleLentPart(ctx, "bob", "up", s.part, s.size)
		if err != nil || d != s.delta {
			t.Fatalf("part %d of %d: delta %d (%v), want %d", s.part, s.size, d, err, s.delta)
		}
	}
	if u, _ := db.GetLentUpload(ctx, "up"); u.Counted != 47 {
		t.Fatalf("counted %d", u.Counted)
	}
	if n, _, _ := db.LentUsage(ctx, "bob"); n != 47 {
		t.Fatalf("usage %d", n)
	}
	if _, err := db.SettleLentPart(ctx, "bob", "gone", 1, 5); err != ErrNotFound {
		t.Fatalf("a part of an unknown upload: %v", err)
	}
	if err := db.SetLentUploadParts(ctx, "up", map[int32]int64{1: 10, 3: 5}); err != nil {
		t.Fatal(err)
	}
	if u, _ := db.GetLentUpload(ctx, "up"); u.Counted != 15 {
		t.Fatalf("counted after a listing %d", u.Counted)
	}
	if d, _ := db.SettleLentPart(ctx, "bob", "up", 3, 5); d != 0 {
		t.Fatalf("part 3 again: %d", d)
	}
	ups, err := db.LentUploads(ctx, "bob")
	if err != nil || len(ups) != 1 || ups[0].UploadID != "up" {
		t.Fatalf("bob's uploads %+v %v", ups, err)
	}
	if err := db.DropLentUpload(ctx, "up"); err != nil {
		t.Fatal(err)
	}
	if err := db.AddLentUpload(ctx, LentUpload{UploadID: "up", Peer: "bob"}); err != nil {
		t.Fatal(err)
	}
	if d, _ := db.SettleLentPart(ctx, "bob", "up", 1, 10); d != 10 {
		t.Fatalf("a dropped upload's parts are kept: delta %d", d)
	}
}
