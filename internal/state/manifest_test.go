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
	friend(t, db, "bob")
	friend(t, db, "carol")
	// A former friend's push in flight records nothing (#25).
	if err := db.ManifestDelivered(ctx, "dave", 1, t0); err != nil {
		t.Fatal(err)
	}
	if err := db.ManifestPushFailed(ctx, "dave", "closed", t0); err != nil {
		t.Fatal(err)
	}
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
	if _, ok := ps["dave"]; ok {
		t.Fatalf("a delivery row for a node that is no friend: %+v", ps["dave"])
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

// ID-4 (#28 review): a friendship committed again (the inviter's side of
// a redeem by a friend it kept, #51/#56) or committed new forgets what
// that friend acknowledged, so the manifest is pushed again: the friend
// may have lost its store along with its database.
func TestCommittedFriendshipForgetsManifestDelivery(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	bob := Friendship{Peer: "bob", Address: "tcbob", PeerNumber: 1 << 30, PeerRetention: 24 * time.Hour}
	for _, k := range []string{"ik1", "ik2"} {
		if err := db.AddInvite(ctx, Invite{Key: k, Number: 1 << 30, Retention: 24 * time.Hour, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.RedeemInvite(ctx, "ik1", bob, now); err != nil {
		t.Fatal(err)
	}
	if err := db.ManifestDelivered(ctx, "bob", 4, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RedeemInvite(ctx, "ik2", bob, now); err != nil { // bob redeems again
		t.Fatal(err)
	}
	if ps, _ := db.ManifestPushes(ctx); ps["bob"] != (ManifestPush{}) {
		t.Fatalf("bob's delivery kept after a new redeem: %+v", ps["bob"])
	}
	// A row left from a former friendship goes when it is made again.
	if err := db.ManifestDelivered(ctx, "carol", 4, now); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishRedeem(ctx, "ik3", Friendship{Peer: "carol", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if ps, _ := db.ManifestPushes(ctx); ps["carol"] != (ManifestPush{}) {
		t.Fatalf("carol's delivery kept after a new friendship: %+v", ps["carol"])
	}
}

// ID-4 (#28 review): failed pushes in a row are counted, for the
// pusher's backoff, and a delivery resets the count.
func TestManifestPushFailuresCounted(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	t0 := time.Unix(1_800_000_000, 0)
	friend(t, db, "bob")
	for i := range 3 {
		if err := db.ManifestPushFailed(ctx, "bob", "offline", t0.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if ps, _ := db.ManifestPushes(ctx); ps["bob"].Failures != 3 {
		t.Fatalf("failures %+v", ps["bob"])
	}
	if err := db.ManifestDelivered(ctx, "bob", 1, t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ps, _ := db.ManifestPushes(ctx); ps["bob"].Failures != 0 {
		t.Fatalf("failures after delivery %+v", ps["bob"])
	}
}

// ID-4, ID-5 (#28 review): the version never goes back. A recovered node
// continues from the version it fetched (#29 calls ContinueManifest),
// so its next change is above every copy its friends hold; a lower one
// changes nothing.
func TestManifestVersionContinuesFromFetched(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.ContinueManifest(ctx, 7); err != nil {
		t.Fatal(err)
	}
	v, err := db.NoteManifest(ctx, "rebuilt", t0)
	if err != nil || v.Seq != 8 {
		t.Fatalf("first version after recovery: %+v %v", v, err)
	}
	if err := db.ContinueManifest(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if v, err := db.NoteManifest(ctx, "rebuilt", t0); err != nil || v.Seq != 8 {
		t.Fatalf("a lower fetched version moved it: %+v %v", v, err)
	}
}
