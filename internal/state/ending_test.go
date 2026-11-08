package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// PEER-5, SEC-10, SEC-15 (#25): ending a friendship deletes its row, so
// both of the friend's keys are refused at once, and keeps a former
// friend row for the retention window: both keys, the address, and when
// the purge comes (the longer of the two sides' retention, as the
// lender's tombs use). The friend's manifest delivery row goes with the
// friendship (#28). Removing a stranger finds nothing.
func TestRemovingAFriendRefusesBothKeysAndKeepsAFormerFriend(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	if err := p.db.AddFriendship(ctx, Friendship{Peer: "B", Address: "tcB", DialKey: "Bdial", OwnNumber: unit, PeerNumber: unit,
		OwnRetention: 10 * 24 * time.Hour, PeerRetention: 20 * 24 * time.Hour, CreatedAt: p.now}); err != nil {
		t.Fatal(err)
	}
	p.friend("C", 100)
	if err := p.db.ManifestDelivered(ctx, "B", 1, p.now); err != nil {
		t.Fatal(err)
	}
	f, err := p.db.RemoveFriendship(ctx, "B", false, p.now)
	if err != nil {
		t.Fatal(err)
	}
	want := FormerFriend{Peer: "B", DialKey: "Bdial", Address: "tcB", RemovedAt: p.now, PurgeAfter: p.now.Add(20 * 24 * time.Hour)}
	if f != want {
		t.Fatalf("former friend %+v, want %+v", f, want)
	}
	if _, err := p.db.GetFriendship(ctx, "B"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("B's friendship after removal: %v", err)
	}
	for _, key := range []string{"B", "Bdial"} {
		if _, role, err := p.db.TunnelPeer(ctx, key, p.now); err != nil || role != RoleNone {
			t.Errorf("key %s after removal: role %q, %v", key, role, err)
		}
	}
	if _, role, _ := p.db.TunnelPeer(ctx, "C", p.now); role != RolePeer {
		t.Fatalf("C after B's removal: %q", role)
	}
	if pushes, _ := p.db.ManifestPushes(ctx); len(pushes) != 0 {
		t.Fatalf("manifest deliveries after removal: %+v", pushes)
	}
	fs, err := p.db.FormerFriends(ctx)
	if err != nil || len(fs) != 1 || fs[0] != want {
		t.Fatalf("former friends %+v, %v", fs, err)
	}
	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("removing B again: %v", err)
	}
	if _, err := p.db.RemoveFriendship(ctx, "D", false, p.now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("removing a stranger: %v", err)
	}

	// C ends it from its side (its remove call): told already.
	later := p.now.Add(time.Hour)
	f, err = p.db.RemoveFriendship(ctx, "C", true, later)
	if err != nil || !f.ByPeer || !f.NotifiedAt.Equal(later) || !f.PurgeAfter.Equal(later.Add(DefaultRetention)) {
		t.Fatalf("removed by C: %+v, %v", f, err)
	}
	// Telling B is recorded, failures first.
	if err := p.db.NoteFarewell(ctx, "B", "dial failed", later); err != nil {
		t.Fatal(err)
	}
	if err := p.db.NoteFarewell(ctx, "B", "", later); err != nil {
		t.Fatal(err)
	}
	fs, _ = p.db.FormerFriends(ctx)
	if len(fs) != 2 || fs[0].Peer != "B" || !fs[0].NotifiedAt.Equal(later) || fs[0].NotifyError != "" {
		t.Fatalf("former friends after telling B: %+v", fs)
	}
}

// PEER-5 with REPL-11 (#20 review, #25): a removed friend holds no copy
// of this node's data at once, the objects it held are below copies
// until re-placement (#26) and its unsent rows leave its FIFO: a put a
// sender took is abandoned (it may have landed), every other row
// dropped, so none of them holds the metadata barrier or counts as a
// copy the pool will make.
func TestARemovedFriendHoldsNoCopyAndNothingWaitsForIt(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d1)
	if o := p.offsite("laptop", 0); !o.Complete() {
		t.Fatalf("at B and C: %+v", o)
	}
	d2 := p.put("laptop", "data/2", "data", 1)
	d3 := p.put("laptop", "data/3", "data", 1)
	if ok, err := p.db.StartPlacement(ctx, "B", d2, p.now); err != nil || !ok {
		t.Fatalf("taking d2 at B: %v %v", ok, err)
	}
	p.ackRow("C", d2)
	p.ackRow("C", d3)
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	if !p.held(idx) {
		t.Fatal("the index is not held while B's data is queued")
	}
	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); err != nil {
		t.Fatal(err)
	}
	if p.held(idx) {
		t.Fatal("B's unsent data still holds the metadata barrier after B's removal")
	}
	states := map[int64]string{}
	rows, err := p.db.Query(`SELECT seq, state FROM placement WHERE place = 'B'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int64
		var s string
		rows.Scan(&seq, &s)
		states[seq] = s
	}
	rows.Close()
	if states[d1] != PlacementAcked || states[d2] != PlacementAbandoned || states[d3] != PlacementDropped {
		t.Fatalf("B's rows after removal: %v", states)
	}
	o := p.offsite("laptop", 0)
	if o.Complete() || o.Stuck.Gone == 0 {
		t.Fatalf("offsite with B removed: %+v", o)
	}
	below, err := p.db.BelowCopies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := below["laptop"]; got.Objects != 4 {
		t.Fatalf("below copies with B removed: %+v, want the three data objects and the index", got)
	}
	if n, _ := p.db.PoolSize(ctx); n != 1 {
		t.Fatalf("pool after B's removal: %d", n)
	}
	// New data goes to C alone; nothing is placed at B.
	d4 := p.put("laptop", "data/4", "data", 1)
	p.wantPlaces("data/4", d4, "C")
}

// PEER-5 (#25): at the end of the window the former friend's rows go:
// its placement rows and use here, and, on the lender's side, its lent
// buckets, tombs, uploads and usage. Becoming friends again within the
// window cancels the purge, and forgetting then does nothing.
func TestForgettingAFormerFriendDropsItsRowsAndBefriendingCancelsIt(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d1)
	for _, peer := range []string{"B", "C"} {
		if err := p.db.AddLentBucket(ctx, peer, "sealed-"+peer, p.now); err != nil {
			t.Fatal(err)
		}
		if err := p.db.AddLentUsage(ctx, peer, 100); err != nil {
			t.Fatal(err)
		}
		if _, err := p.db.AddTombstone(ctx, Tombstone{Peer: peer, Sealed: "sealed-" + peer, Key: "k", Tomb: "t", SHA256: "h",
			DeletedAt: p.now, PurgeAfter: p.now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if err := p.db.AddLentUpload(ctx, LentUpload{UploadID: "u-" + peer, Peer: peer, Sealed: "sealed-" + peer, Key: "k",
			SHA256: "h", CreatedAt: p.now}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); err != nil {
		t.Fatal(err)
	}
	// Friends again: the purge is off, and forgetting B does nothing.
	p.friend("B", 100)
	if fs, _ := p.db.FormerFriends(ctx); len(fs) != 0 {
		t.Fatalf("former friends after befriending again: %+v", fs)
	}
	if err := p.db.ForgetFormerFriend(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if bs, _ := p.db.LentBuckets(ctx, "B"); len(bs) != 1 {
		t.Fatalf("B's lent buckets forgotten while a friend: %+v", bs)
	}
	p.wantPlaces("data/1", d1, "B", "C")

	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); err != nil {
		t.Fatal(err)
	}
	if err := p.db.ForgetFormerFriend(ctx, "B"); err != nil {
		t.Fatal(err)
	}
	if fs, _ := p.db.FormerFriends(ctx); len(fs) != 0 {
		t.Fatalf("former friends after forgetting B: %+v", fs)
	}
	p.wantPlaces("data/1", d1, "C")
	use, _ := p.db.PlaceUse(ctx)
	if _, ok := use["B"]; ok {
		t.Fatalf("B's use is kept: %v", use)
	}
	for peer, want := range map[string]int{"B": 0, "C": 1} {
		bs, _ := p.db.LentBuckets(ctx, peer)
		n, _, _ := p.db.TombstoneUsage(ctx, peer)
		us, _ := p.db.LentUploads(ctx, peer)
		bytes, _, _ := p.db.LentUsage(ctx, peer)
		if len(bs) != want || n != int64(want) || len(us) != want || (bytes != 0) != (want == 1) {
			t.Errorf("%s after forgetting B: buckets %d, tombs %d, uploads %d, use %d", peer, len(bs), n, len(us), bytes)
		}
	}
}
