package control_test

import (
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

// PEER-5 (#25): the remove call is how a friend that ended the
// friendship tells this node. It ends the friendship here too, as ended
// by the friend (and so told already), commits before it answers, calls
// Changed and Ended, and answers for the caller alone: no parameter
// names whom. An invite key cannot call it; a caller with no friendship
// is answered as told, since there is nothing left to end.
func TestRemoveCallEndsTheFriendshipOnThisSide(t *testing.T) {
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bob, carol, invited := newID(t), newID(t), newID(t)
	for _, f := range []string{bob.NodeID().String(), carol.NodeID().String()} {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	var changed, ended atomic.Int32
	now := time.Unix(1_800_000_000, 0)
	l := newLender(t)
	l.srv.DB, l.srv.Now = db, func() time.Time { return now }
	l.srv.Changed = func() { changed.Add(1) }
	l.srv.Ended = func() { ended.Add(1) }

	if err := l.client(t, control.Caller{Peer: invited.NodeID(), Role: state.RoleInvite}).Remove(t.Context()); err == nil {
		t.Fatal("an invite key ended a friendship")
	} else if ce := (*control.CallError)(nil); !errors.As(err, &ce) || ce.Status != http.StatusForbidden {
		t.Fatalf("remove by an invite key: %v", err)
	}
	if err := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer}).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetFriendship(t.Context(), bob.NodeID().String()); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Bob is still a friend: %v", err)
	}
	fs, err := db.FormerFriends(t.Context())
	if err != nil || len(fs) != 1 || fs[0].Peer != bob.NodeID().String() || !fs[0].ByPeer || !fs[0].NotifiedAt.Equal(now) {
		t.Fatalf("former friends: %+v, %v", fs, err)
	}
	if changed.Load() != 1 || ended.Load() != 1 {
		t.Fatalf("Changed %d, Ended %d after the remove call", changed.Load(), ended.Load())
	}
	if _, err := db.GetFriendship(t.Context(), carol.NodeID().String()); err != nil {
		t.Fatalf("Carol after Bob's remove call: %v", err)
	}
	// Again (an answer lost): told already, nothing changes.
	if err := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer}).Remove(t.Context()); err != nil {
		t.Fatalf("Bob's remove call again: %v", err)
	}
	if changed.Load() != 1 || ended.Load() != 1 {
		t.Fatalf("Changed %d, Ended %d after a second remove call", changed.Load(), ended.Load())
	}
}
