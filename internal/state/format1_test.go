package state

import (
	"context"
	"testing"
	"time"
)

// COMPAT-1 (#79): a bucket is marked as holding a format-1 repository
// once, by the first such key; the upgrade's marker ends it for good,
// so a format-1 key seen after it (an import walking old blobs, or the
// blobs Kopia leaves behind) does not mark it again. Deleting the
// bucket deletes the mark with it.
func TestFormat1MarkedOnceUntilUpgraded(t *testing.T) {
	db := openTest(t)
	ctx := context.Background()
	if err := db.AddBucket(ctx, Bucket{Name: "vault", Profile: "kopia", Copies: 2, AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	b, _ := db.GetBucket(ctx, "vault")
	if b.Format1() {
		t.Fatalf("a new bucket is format 1: %+v", b)
	}
	t0 := time.Unix(1_800_000_000, 0)
	if marked, err := db.MarkFormat1(ctx, "vault", "nabc", t0); err != nil || !marked {
		t.Fatalf("first mark: %v %v", marked, err)
	}
	if marked, err := db.MarkFormat1(ctx, "vault", "mdef", t0.Add(time.Hour)); err != nil || marked {
		t.Fatalf("second mark: %v %v", marked, err)
	}
	b, _ = db.GetBucket(ctx, "vault")
	if !b.Format1() || b.Format1Key != "nabc" || !b.Format1At.Equal(t0) {
		t.Fatalf("after marks: %+v", b)
	}
	if marked, err := db.MarkFormat1(ctx, "absent", "nabc", t0); err != nil || marked {
		t.Fatalf("absent bucket: %v %v", marked, err)
	}

	t1 := t0.Add(2 * time.Hour)
	if err := db.MarkUpgraded(ctx, "vault", t1); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkUpgraded(ctx, "vault", t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if marked, err := db.MarkFormat1(ctx, "vault", "nold", t1.Add(2*time.Hour)); err != nil || marked {
		t.Fatalf("mark after upgrade: %v %v", marked, err)
	}
	bs, err := db.Buckets(ctx)
	if err != nil || len(bs) != 1 {
		t.Fatal(bs, err)
	}
	if b := bs[0]; b.Format1() || !b.UpgradedAt.Equal(t1) || b.Format1Key != "nabc" {
		t.Fatalf("after upgrade: %+v", b)
	}

	// A bucket upgraded before any format-1 key was seen is never marked.
	if err := db.AddBucket(ctx, Bucket{Name: "imported", Profile: "kopia", Copies: 2, AccessKey: "AK2", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkUpgraded(ctx, "imported", t0); err != nil {
		t.Fatal(err)
	}
	if marked, _ := db.MarkFormat1(ctx, "imported", "nabc", t1); marked {
		t.Fatal("a bucket whose upgrade marker came first was marked")
	}

	// Deleting a bucket deletes its mark: one added again is new.
	if err := db.DeleteBucket(ctx, "vault"); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, Bucket{Name: "vault", Profile: "kopia", Copies: 2, AccessKey: "AK3", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if b, _ := db.GetBucket(ctx, "vault"); b.Format1() || !b.UpgradedAt.IsZero() || b.Format1Key != "" {
		t.Fatalf("re-added bucket: %+v", b)
	}
}
