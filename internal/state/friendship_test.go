package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func openPeers(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// PEER-2, SEC-6: redeeming marks the invite used and creates the
// friendship in one transaction, with the invite's terms as this side's;
// a second redemption of the same invite is refused and changes nothing.
func TestRedeemInviteOnceOnly(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	if err := db.AddInvite(ctx, Invite{Key: "ik", Number: 80 << 30, Retention: 30 * 24 * time.Hour, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	f := Friendship{Peer: "bob", Address: "tcbob", PeerNumber: 100 << 30, PeerRetention: 60 * 24 * time.Hour}
	got, err := db.RedeemInvite(ctx, "ik", f, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnNumber != 80<<30 || got.OwnRetention != 30*24*time.Hour || got.Quota() != 80<<30 {
		t.Fatalf("friendship %+v", got)
	}
	stored, err := db.GetFriendship(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Address != "tcbob" || stored.PeerNumber != 100<<30 || stored.OwnNumber != 80<<30 ||
		stored.PeerRetention != 60*24*time.Hour || !stored.CreatedAt.Equal(now) {
		t.Fatalf("stored %+v", stored)
	}
	if r, _ := db.PeerRole(ctx, "ik", now); r != RoleNone {
		t.Fatalf("used invite key still has role %q", r)
	}

	// Replay, as the same friend or another node.
	for _, peer := range []string{"bob", "carol"} {
		f.Peer = peer
		if _, err := db.RedeemInvite(ctx, "ik", f, now); !errors.Is(err, ErrInviteSpent) {
			t.Fatalf("replay as %s: %v", peer, err)
		}
	}
	if _, err := db.GetFriendship(ctx, "carol"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a replayed invite created a friendship")
	}
}

// An expired invite is refused; one redeemed by a node that is already a
// friend stays unused.
func TestRedeemExpiredOrExistingRefused(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	db.AddInvite(ctx, Invite{Key: "old", Number: 1, Retention: time.Hour, ExpiresAt: now})
	if _, err := db.RedeemInvite(ctx, "old", Friendship{Peer: "bob", PeerNumber: 1, PeerRetention: time.Hour}, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("expired invite: %v", err)
	}
	if err := db.AddFriendship(ctx, Friendship{Peer: "bob", OwnNumber: 1, PeerNumber: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(ctx, Friendship{Peer: "bob", OwnNumber: 1, PeerNumber: 1, CreatedAt: now}); !errors.Is(err, ErrExists) {
		t.Fatalf("second friendship row: %v", err)
	}
	db.AddInvite(ctx, Invite{Key: "new", Number: 1, Retention: time.Hour, ExpiresAt: now.Add(time.Hour)})
	if _, err := db.RedeemInvite(ctx, "new", Friendship{Peer: "bob", PeerNumber: 1, PeerRetention: time.Hour}, now); !errors.Is(err, ErrExists) {
		t.Fatalf("redeem by an existing friend: %v", err)
	}
	if r, _ := db.PeerRole(ctx, "new", now); r != RoleInvite {
		t.Fatal("a refused redemption used the invite")
	}
}

// SEC-3, SEC-6: invite rows are kept only while unexpired.
func TestPurgeInvitesDeletesExpired(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	db.AddInvite(ctx, Invite{Key: "live", Number: 1, Retention: time.Hour, ExpiresAt: now.Add(time.Second)})
	db.AddInvite(ctx, Invite{Key: "dead", Number: 1, Retention: time.Hour, ExpiresAt: now})
	n, err := db.PurgeInvites(ctx, now)
	if err != nil || n != 1 {
		t.Fatalf("purged %d, %v", n, err)
	}
	var keys []string
	rows, _ := db.Query(`SELECT invite_key FROM invite`)
	for rows.Next() {
		var k string
		rows.Scan(&k)
		keys = append(keys, k)
	}
	rows.Close()
	if len(keys) != 1 || keys[0] != "live" {
		t.Fatalf("left %q", keys)
	}
}

// A row written before the terms columns existed still lists.
func TestFriendshipsListsOldRows(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	mustExec(t, db, `INSERT INTO friendship (peer_key, created_at) VALUES ('old', 1)`)
	db.AddFriendship(ctx, Friendship{Peer: "new", OwnNumber: 2, PeerNumber: 3, CreatedAt: time.Unix(2, 0)})
	fs, err := db.Friendships(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 2 || fs[0].Peer != "old" || fs[1].Peer != "new" || fs[1].Quota() != 2 {
		t.Fatalf("friendships %+v", fs)
	}
}
