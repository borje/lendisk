package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// PEER-5 (#25 review, M2): the purge of a former friend's replica checks
// before each bucket that the peer is still a former friend due for it.
// If it redeems a new invite while the purge runs, the rest of its
// buckets are kept, the purge says so (state.ErrNotFormer), and nothing
// of it is forgotten. A purge that is not due yet deletes nothing.
func TestAPurgeStopsWhenTheFormerFriendIsBefriendedAgain(t *testing.T) {
	ctx := context.Background()
	db, err := state.Open(ctx, t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	peer := id.NodeID().String()
	start := time.Unix(1_800_000_000, 0)
	now := start
	store := t.TempDir()
	l := &Lending{DB: db, StoreDir: store, Log: quiet, Now: func() time.Time { return now }}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: peer, CreatedAt: start}); err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, sealed := range []string{"aaaa", "bbbb"} {
		if err := db.AddLentBucket(ctx, peer, sealed, start); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(store, LentName(id.NodeID(), sealed))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		dirs = append(dirs, dir)
	}
	if _, err := db.RemoveFriendship(ctx, peer, false, start); err != nil {
		t.Fatal(err)
	}
	exists := func(dir string) bool { _, err := os.Stat(dir); return err == nil }

	if err := l.purgeFormer(ctx, peer); !errors.Is(err, state.ErrNotFormer) || !exists(dirs[0]) || !exists(dirs[1]) {
		t.Fatalf("a purge before the window ended: %v, buckets %v %v", err, exists(dirs[0]), exists(dirs[1]))
	}
	now = start.Add(state.DefaultRetention)
	l.afterPurgeBucket = func() {
		// The friend redeems a new invite while the purge runs.
		if err := db.AddFriendship(ctx, state.Friendship{Peer: peer, CreatedAt: now}); err != nil {
			t.Error(err)
		}
	}
	err = l.purgeFormer(ctx, peer)
	if !errors.Is(err, state.ErrNotFormer) {
		t.Fatalf("a purge interrupted by a new friendship: %v", err)
	}
	if exists(dirs[0]) || !exists(dirs[1]) {
		t.Fatalf("after the new friendship: first bucket %v, second %v; want the second kept", exists(dirs[0]), exists(dirs[1]))
	}
	if err := db.ForgetFormerFriend(ctx, peer); err != nil {
		t.Fatal(err)
	}
	if bs, _ := db.LentBuckets(ctx, peer); len(bs) != 2 {
		t.Fatalf("the friend's lent buckets forgotten: %+v", bs)
	}
}
