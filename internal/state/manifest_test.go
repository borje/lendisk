package state

import (
	"context"
	"testing"
	"time"
)

// ID-4: the manifest's version moves only when its content does, and
// counts up from 1, with the time it changed.
func TestManifestVersionMovesOnlyOnChange(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	t0 := time.Unix(1_800_000_000, 0)
	if v, err := db.ManifestVersion(ctx); err != nil || v.Seq != 0 {
		t.Fatalf("fresh database: %+v %v", v, err)
	}
	v1, err := db.NoteManifest(ctx, "h1", t0)
	if err != nil || v1.Seq != 1 || v1.Hash != "h1" || !v1.UpdatedAt.Equal(t0) {
		t.Fatalf("first version: %+v %v", v1, err)
	}
	same, err := db.NoteManifest(ctx, "h1", t0.Add(time.Hour))
	if err != nil || same != v1 {
		t.Fatalf("unchanged content moved the version: %+v %v", same, err)
	}
	v2, err := db.NoteManifest(ctx, "h2", t0.Add(time.Hour))
	if err != nil || v2.Seq != 2 || !v2.UpdatedAt.Equal(t0.Add(time.Hour)) {
		t.Fatalf("changed content: %+v %v", v2, err)
	}
	if got, _ := db.ManifestVersion(ctx); got != v2 {
		t.Fatalf("stored %+v, want %+v", got, v2)
	}
}

// ID-4: per friend, the version it acknowledged survives a failure of a
// later push, which is recorded beside it until the next success.
func TestManifestDeliveryPerFriend(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.ManifestDelivered(ctx, "bob", 1, t0); err != nil {
		t.Fatal(err)
	}
	if err := db.ManifestPushFailed(ctx, "bob", "offline", t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.ManifestPushFailed(ctx, "carol", "refused", t0); err != nil {
		t.Fatal(err)
	}
	ps, err := db.ManifestPushes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bob, carol := ps["bob"], ps["carol"]
	if bob.Seq != 1 || !bob.DeliveredAt.Equal(t0) || bob.Error != "offline" || !bob.TriedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("bob %+v", bob)
	}
	if carol.Seq != 0 || !carol.DeliveredAt.IsZero() || carol.Error != "refused" {
		t.Fatalf("carol %+v", carol)
	}
	if err := db.ManifestDelivered(ctx, "bob", 2, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	ps, _ = db.ManifestPushes(ctx)
	if b := ps["bob"]; b.Seq != 2 || b.Error != "" {
		t.Fatalf("bob after success %+v", b)
	}
}
