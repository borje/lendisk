package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// steps records what a farewell did, in order.
type steps struct {
	mu  sync.Mutex
	got []string
}

func (s *steps) add(f string, args ...any) {
	s.mu.Lock()
	s.got = append(s.got, fmt.Sprintf(f, args...))
	s.mu.Unlock()
}

func (s *steps) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.got
	s.got = nil
	return out
}

// SEC-15 (#25 review, M1): a purge runs on its own goroutine, so one that
// takes minutes (a large replica on a slow disk) never delays revoking a
// removal made meanwhile: the next pass still cuts it. Purges run one at
// a time, and one that finds the peer a friend again (state.ErrNotFormer)
// forgets nothing.
func TestABlockedPurgeDoesNotHoldUpARevocation(t *testing.T) {
	parallel(t, 0)
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	b, c, d := newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID()
	for _, p := range []identity.NodeID{b, c, d} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: p.String(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Unix(1_800_000_000, 0)
	var mu sync.Mutex
	now := start
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	for _, p := range []identity.NodeID{b, d} {
		if _, err := db.RemoveFriendship(ctx, p.String(), true, start); err != nil {
			t.Fatal(err)
		}
	}
	var s steps
	release := make(chan struct{})
	purging := make(chan string, 4)
	fw := &farewell{DB: db, Log: slog.New(slog.DiscardHandler), Now: clock,
		Clear: func() {},
		Cut: func(keys ...identity.NodeID) int {
			for _, k := range keys {
				if k == c {
					s.add("cut C")
				}
			}
			return len(keys)
		},
		Disconnect: func(identity.NodeID) bool { return true },
		Tell:       func(context.Context, state.FormerFriend) error { return nil },
		Purge: func(_ context.Context, peer string) error {
			purging <- peer
			<-release
			if peer == d.String() {
				return state.ErrNotFormer
			}
			return nil
		},
	}
	fw.pass(ctx)
	mu.Lock()
	now = start.Add(state.DefaultRetention)
	mu.Unlock()
	fw.pass(ctx) // starts the purges, the first blocked
	first := <-purging
	if _, err := db.RemoveFriendship(ctx, c.String(), false, now); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { fw.pass(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("a pass waited for a purge")
	}
	if got := s.take(); !slices.Equal(got, []string{"cut C", "cut C"}) { // before and after the disconnect
		close(release)
		t.Fatalf("C's removal during a purge: %q", got)
	}
	select {
	case p := <-purging:
		close(release)
		t.Fatalf("a second purge (%s) ran beside %s", p, first)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-purging // the other one, after the first
	fw.wait()
	fs, _ := db.FormerFriends(ctx)
	var left []string
	for _, f := range fs {
		left = append(left, f.Peer)
	}
	slices.Sort(left)
	want := []string{c.String(), d.String()}
	slices.Sort(want)
	if !slices.Equal(left, want) {
		t.Fatalf("former friends after the purges: %v, want C (not due) and D (purge refused)", left)
	}
}

// SEC-15 in spike #3's order (#25): a removal another process committed
// (`peer remove`) is applied by the daemon's next pass as clear the
// admission cache, cut the connections of both of the friend's keys
// (node ID and dial key), then disconnect each key, and cut again what
// opened in between, once; the friend is then told, retried with a backoff until it answers, and at the end of
// the retention window its replica is purged and the former friend
// forgotten. A friend that ended it itself is not told back.
func TestFarewellRevokesInOrderTellsAndPurges(t *testing.T) {
	parallel(t, 0)
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	b, bDial, c := newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID()
	for _, f := range []state.Friendship{
		{Peer: b.String(), DialKey: bDial.String(), Address: "tcB", CreatedAt: time.Now()},
		{Peer: c.String(), Address: "tcC", CreatedAt: time.Now()},
	} {
		if err := db.AddFriendship(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1_800_000_000, 0)
	var s steps
	tellErr := errors.New("offline")
	mk := func() *farewell {
		return &farewell{DB: db, Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return now },
			Clear: func() {
				_, err := db.GetFriendship(ctx, b.String())
				s.add("clear (B committed: %v)", errors.Is(err, state.ErrNotFound))
			},
			Cut: func(keys ...identity.NodeID) int {
				names := []string{}
				for _, k := range keys {
					names = append(names, map[identity.NodeID]string{b: "B", bDial: "B's dial key", c: "C"}[k])
				}
				s.add("cut %v", names)
				return len(keys)
			},
			Disconnect: func(k identity.NodeID) bool {
				s.add("disconnect %s", map[identity.NodeID]string{b: "B", bDial: "B's dial key", c: "C"}[k])
				return true
			},
			Tell: func(_ context.Context, f state.FormerFriend) error {
				s.add("tell %s", f.Address)
				return tellErr
			},
			Purge: func(_ context.Context, peer string) error {
				s.add("purge %s", map[string]string{b.String(): "B", c.String(): "C"}[peer])
				return nil
			},
		}
	}
	fw := mk()
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); len(got) != 0 {
		t.Fatalf("a pass with no former friend did %v", got)
	}
	if _, err := db.RemoveFriendship(ctx, b.String(), false, now); err != nil {
		t.Fatal(err)
	}
	fw.pass(ctx)
	fw.wait()
	want := []string{"clear (B committed: true)", "cut [B B's dial key]", "disconnect B", "disconnect B's dial key", "cut [B B's dial key]", "tell tcB"}
	if got := s.take(); !slices.Equal(got, want) {
		t.Fatalf("first pass after B's removal:\n got %q\nwant %q", got, want)
	}
	// Cut once; the friend is told again only after the backoff.
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); len(got) != 0 {
		t.Fatalf("a second pass at once did %v", got)
	}
	now = now.Add(time.Minute)
	tellErr = nil
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); !slices.Equal(got, []string{"tell tcB"}) {
		t.Fatalf("after the backoff: %q", got)
	}
	fs, _ := db.FormerFriends(ctx)
	if len(fs) != 1 || !fs[0].NotifiedAt.Equal(now) {
		t.Fatalf("B told: %+v", fs)
	}
	now = now.Add(time.Hour)
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); len(got) != 0 {
		t.Fatalf("B told already, yet: %q", got)
	}

	// C ends it from its side (its remove call): cut, not told back.
	if _, err := db.RemoveFriendship(ctx, c.String(), true, now); err != nil {
		t.Fatal(err)
	}
	fw.Kick()
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); !slices.Equal(got, []string{"clear (B committed: true)", "cut [C]", "disconnect C", "cut [C]"}) {
		t.Fatalf("after C ended it: %q", got)
	}

	// The end of B's window: purged and forgotten; C's comes later.
	now = time.Unix(1_800_000_000, 0).Add(state.DefaultRetention)
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); !slices.Equal(got, []string{"purge B"}) {
		t.Fatalf("at the end of B's window: %q", got)
	}
	if fs, _ := db.FormerFriends(ctx); len(fs) != 1 || fs[0].Peer != c.String() {
		t.Fatalf("former friends after B's purge: %+v", fs)
	}

	// A restarted daemon cuts every former friend again, harmlessly.
	fw2 := mk()
	fw2.pass(ctx)
	fw2.wait()
	if got := s.take(); !slices.Equal(got, []string{"clear (B committed: true)", "cut [C]", "disconnect C", "cut [C]"}) {
		t.Fatalf("after a restart: %q", got)
	}
}
