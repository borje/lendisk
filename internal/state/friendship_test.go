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

	// Replay by another node.
	f.Peer = "carol"
	if _, err := db.RedeemInvite(ctx, "ik", f, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("replay as carol: %v", err)
	}
	if _, err := db.GetFriendship(ctx, "carol"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a replayed invite created a friendship")
	}
}

// PEER-2 retry (#51): the node that redeemed an invite may redeem it
// again until it expires, and gets the same terms; its reported number
// and retention are applied. The invite key keeps the invite role for
// that retry only while the friendship stands, and loses it at expiry.
func TestRedeemRetryBySameFriendAnswersSameTerms(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	exp := now.Add(time.Hour)
	if err := db.AddInvite(ctx, Invite{Key: "ik", Number: 80 << 30, Retention: 30 * 24 * time.Hour, ExpiresAt: exp}); err != nil {
		t.Fatal(err)
	}
	f := Friendship{Peer: "bob", Address: "tcbob", PeerNumber: 100 << 30, PeerRetention: 60 * 24 * time.Hour}
	first, err := db.RedeemInvite(ctx, "ik", f, now)
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := db.PeerRole(ctx, "ik", now); r != RoleInvite {
		t.Fatalf("redeemed invite key has role %q; the redeemer cannot retry", r)
	}
	again, err := db.RedeemInvite(ctx, "ik", f, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("retry by the redeemer: %v", err)
	}
	if again.OwnNumber != first.OwnNumber || again.OwnRetention != first.OwnRetention || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("retry answered %+v, first %+v", again, first)
	}
	// A retry carrying another number is the last report: applied.
	f.PeerNumber = 50 << 30
	if got, err := db.RedeemInvite(ctx, "ik", f, now.Add(2*time.Minute)); err != nil || got.PeerNumber != 50<<30 {
		t.Fatalf("retry with a new number: %+v, %v", got, err)
	}
	if fs, _ := db.Friendships(ctx); len(fs) != 1 || fs[0].PeerNumber != 50<<30 {
		t.Fatalf("friendships after retries %+v", fs)
	}
	// Another node is still refused.
	if _, err := db.RedeemInvite(ctx, "ik", Friendship{Peer: "carol", PeerNumber: 1, PeerRetention: time.Hour}, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("replay as carol: %v", err)
	}
	// Expired: refused, and the key has no role.
	if _, err := db.RedeemInvite(ctx, "ik", f, exp); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("retry after expiry: %v", err)
	}
	if r, _ := db.PeerRole(ctx, "ik", exp); r != RoleNone {
		t.Fatalf("expired invite key has role %q", r)
	}
	// Without the friendship the used key admits nothing.
	if _, err := db.ExecContext(ctx, `DELETE FROM friendship`); err != nil {
		t.Fatal(err)
	}
	if r, _ := db.PeerRole(ctx, "ik", now); r != RoleNone {
		t.Fatalf("used invite key of a removed friend has role %q", r)
	}
	if _, err := db.RedeemInvite(ctx, "ik", f, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("retry after the friendship went: %v", err)
	}
}

// PEER-2 (#51): the invited node records its terms before it calls
// redeem, and finishing stores the friendship and drops that record in
// one step.
func TestPendingRedeemKeepsTermsUntilFinished(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	if _, err := db.PendingRedeemOf(ctx, "ik"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no pending redeem yet: %v", err)
	}
	p := PendingRedeem{InviteKey: "ik", Peer: "alice", Number: 100 << 30, Retention: 60 * 24 * time.Hour, StartedAt: now}
	if err := db.StartRedeem(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := db.PendingRedeemOf(ctx, "ik")
	if err != nil || got != p {
		t.Fatalf("pending %+v, %v; want %+v", got, err, p)
	}
	if r, _ := db.PeerRole(ctx, "alice", now); r != RoleNone {
		t.Fatalf("a pending redeem admits the inviter as %q", r)
	}
	f := Friendship{Peer: "alice", Address: "tcalice", OwnNumber: p.Number, PeerNumber: 80 << 30, OwnRetention: p.Retention, CreatedAt: now}
	if err := db.FinishRedeem(ctx, "ik", f); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PendingRedeemOf(ctx, "ik"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pending redeem after finishing: %v", err)
	}
	if _, err := db.GetFriendship(ctx, "alice"); err != nil {
		t.Fatal(err)
	}
	// Abandoning drops the record.
	p.InviteKey = "ik2"
	db.StartRedeem(ctx, p)
	if err := db.AbandonRedeem(ctx, "ik2"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PendingRedeemOf(ctx, "ik2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("abandoned redeem: %v", err)
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
