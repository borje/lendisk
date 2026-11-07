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

// An expired invite is refused, and a friendship row is not added twice.
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
}

// PEER-2 (#56): a friend that lost the answer may redeem any live
// invite, a new one or one another node used, and gets the friendship
// it already has, with its report applied; a new invite is spent by it.
// An expired invite is still refused, and no one else gains anything.
func TestRedeemByFriendAnswersItsFriendship(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	for _, inv := range []Invite{
		{Key: "first", Number: 80 << 30, Retention: 30 * 24 * time.Hour, ExpiresAt: now.Add(time.Hour)},
		{Key: "new", Number: 60 << 30, Retention: time.Hour, ExpiresAt: now.Add(time.Hour)},
		{Key: "carols", Number: 1 << 30, Retention: time.Hour, ExpiresAt: now.Add(time.Hour)},
		{Key: "old", Number: 1 << 30, Retention: time.Hour, ExpiresAt: now},
	} {
		if err := db.AddInvite(ctx, inv); err != nil {
			t.Fatal(err)
		}
	}
	bob := Friendship{Peer: "bob", Address: "tcbob", PeerNumber: 100 << 30, PeerRetention: 60 * 24 * time.Hour}
	first, err := db.RedeemInvite(ctx, "first", bob, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RedeemInvite(ctx, "carols", Friendship{Peer: "carol", PeerNumber: 1, PeerRetention: time.Hour}, now); err != nil {
		t.Fatal(err)
	}

	bob.Address, bob.PeerNumber = "tcbob2", 90<<30
	for _, key := range []string{"new", "carols"} {
		got, err := db.RedeemInvite(ctx, key, bob, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("friend redeeming %s: %v", key, err)
		}
		if got.OwnNumber != first.OwnNumber || got.OwnRetention != first.OwnRetention || !got.CreatedAt.Equal(first.CreatedAt) ||
			got.Address != "tcbob2" || got.PeerNumber != 90<<30 {
			t.Fatalf("friend redeeming %s got %+v, first %+v", key, got, first)
		}
	}
	// The new invite is spent by bob: dave is refused it.
	if _, err := db.RedeemInvite(ctx, "new", Friendship{Peer: "dave", PeerNumber: 1, PeerRetention: time.Hour}, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("dave with the invite bob healed with: %v", err)
	}
	var by string
	db.QueryRow(`SELECT redeemed_by FROM invite WHERE invite_key = 'carols'`).Scan(&by)
	if by != "carol" {
		t.Fatalf("carol's invite now redeemed by %q", by)
	}
	if _, err := db.RedeemInvite(ctx, "old", bob, now); !errors.Is(err, ErrInviteSpent) {
		t.Fatalf("friend with an expired invite: %v", err)
	}
	if fs, _ := db.Friendships(ctx); len(fs) != 2 {
		t.Fatalf("friendships %+v", fs)
	}
}

// PEER-2 (#56): the invited node lists every pending redeem with one
// inviter, and finishing any of them drops them all.
func TestFinishRedeemDropsEveryPendingWithThePeer(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	for i, p := range []PendingRedeem{
		{InviteKey: "ik1", Peer: "alice", Number: 1, Retention: time.Hour},
		{InviteKey: "ik2", Peer: "alice", Number: 2, Retention: time.Hour},
		{InviteKey: "ik3", Peer: "carol", Number: 3, Retention: time.Hour},
	} {
		p.StartedAt = now.Add(time.Duration(i) * time.Minute)
		if err := db.StartRedeem(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	ps, err := db.PendingRedeemsWith(ctx, "alice")
	if err != nil || len(ps) != 2 || ps[0].InviteKey != "ik1" || ps[1].InviteKey != "ik2" {
		t.Fatalf("pending with alice: %+v, %v", ps, err)
	}
	if err := db.FinishRedeem(ctx, "ik2", Friendship{Peer: "alice", OwnNumber: 2, PeerNumber: 1, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if ps, _ := db.PendingRedeemsWith(ctx, "alice"); len(ps) != 0 {
		t.Fatalf("pending with alice after finishing: %+v", ps)
	}
	if _, err := db.PendingRedeemOf(ctx, "ik3"); err != nil {
		t.Fatalf("carol's pending redeem: %v", err)
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

// SEC-10, SEC-12 (#69): a friend's dial key is admitted with exactly the
// friend's rights and names the friend; its identity key still does. An
// invite key names itself. Removing the friendship refuses both keys.
func TestTunnelPeerMapsDialKeyToFriend(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	if err := db.AddFriendship(ctx, Friendship{Peer: "bob", DialKey: "bobdial", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddInvite(ctx, Invite{Key: "ik", Number: 1, Retention: time.Hour, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if f, _ := db.GetFriendship(ctx, "bob"); f.DialKey != "bobdial" {
		t.Fatalf("stored dial key %q", f.DialKey)
	}
	for _, c := range []struct{ key, peer string }{{"bob", "bob"}, {"bobdial", "bob"}} {
		peer, role, err := db.TunnelPeer(ctx, c.key, now)
		if err != nil || peer != c.peer || role != RolePeer {
			t.Fatalf("TunnelPeer(%s) = %q %q %v", c.key, peer, role, err)
		}
	}
	if peer, role, _ := db.TunnelPeer(ctx, "ik", now); peer != "ik" || role != RoleInvite {
		t.Fatalf("invite key: %q %q", peer, role)
	}
	if _, role, _ := db.TunnelPeer(ctx, "stranger", now); role != RoleNone {
		t.Fatalf("stranger: %q", role)
	}
	// The dial key is not an S3 account: PeerRole reads node IDs only.
	if r, _ := db.PeerRole(ctx, "bobdial", now); r != RoleNone {
		t.Fatalf("PeerRole of a dial key = %q", r)
	}
	// A dial key is one friend's: a second friend cannot claim it, nor a
	// friend's node ID.
	for _, f := range []Friendship{{Peer: "carol", DialKey: "bobdial"}, {Peer: "carol", DialKey: "bob"}} {
		if err := db.AddFriendship(ctx, f); !errors.Is(err, ErrDialKeyTaken) {
			t.Fatalf("carol with dial key %s: %v", f.DialKey, err)
		}
	}
	mustExec(t, db, `DELETE FROM friendship WHERE peer_key = 'bob'`)
	for _, k := range []string{"bob", "bobdial"} {
		if _, role, err := db.TunnelPeer(ctx, k, now); err != nil || role != RoleNone {
			t.Fatalf("removed friend's key %s: %q %v", k, role, err)
		}
	}
}

// PEER-2 retry (#51, #69): a redeem again stores the friend's dial key
// as its latest report, and a dial key that is another friend's is
// refused.
func TestRedeemStoresDialKey(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	for _, k := range []string{"ik1", "ik2", "ik3"} {
		if err := db.AddInvite(ctx, Invite{Key: k, Number: 1, Retention: time.Hour, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.RedeemInvite(ctx, "ik1", Friendship{Peer: "bob", DialKey: "d1", PeerNumber: 1, PeerRetention: time.Hour}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RedeemInvite(ctx, "ik2", Friendship{Peer: "bob", DialKey: "d2", PeerNumber: 1, PeerRetention: time.Hour}, now); err != nil {
		t.Fatal(err)
	}
	if f, _ := db.GetFriendship(ctx, "bob"); f.DialKey != "d2" {
		t.Fatalf("dial key after the second redeem %q", f.DialKey)
	}
	if _, err := db.RedeemInvite(ctx, "ik3", Friendship{Peer: "carol", DialKey: "d2", PeerNumber: 1, PeerRetention: time.Hour}, now); !errors.Is(err, ErrDialKeyTaken) {
		t.Fatalf("carol claiming bob's dial key: %v", err)
	}
	if _, err := db.GetFriendship(ctx, "carol"); !errors.Is(err, ErrNotFound) {
		t.Fatal("a refused dial key left a friendship")
	}
}
