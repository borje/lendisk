package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
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

// PEER-5 (#84 review): a friend that got the remove call ends the
// friendship and from then refuses this node's key at connect, which
// looks like being unreachable (control.ErrNotConnected). But a request
// written whole (control.ErrNoAnswer) may only have reached the local
// end of the tunnel, and a friend that fell asleep just then looks the
// same. So after such a try and tellRefusals tries in a row that never
// connect, the friend is shown as probably told (the farewell's
// notify_error says so) and is still asked, every tellProbablyEvery
// with jitter, until it answers: 200 or 403 is told at once. Without a
// try sent whole, failing to connect never makes it probable; an answer
// in between starts over.
func TestAFriendThatRefusesAfterTheRemoveCallIsAskedUntilItAnswers(t *testing.T) {
	parallel(t, 0)
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	b, c, d, e := newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID()
	now := time.Unix(1_800_000_000, 0)
	for _, p := range []identity.NodeID{b, c, d, e} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: p.String(), Address: "tc" + p.String(), CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.RemoveFriendship(ctx, p.String(), false, now); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	answers := map[identity.NodeID][]error{}
	tries := map[identity.NodeID]int{}
	wrap := func(stage error) error { return fmt.Errorf("%w: %w", stage, context.DeadlineExceeded) }
	noAnswer, notConnected := wrap(control.ErrNoAnswer), wrap(control.ErrNotConnected)
	failed := &control.CallError{Status: http.StatusInternalServerError, Message: "try again"}
	// B got the call (or only its local tunnel did) and then slept
	// through three tries; it answers when it wakes. C is offline
	// throughout. D answered 500 (it failed to end it) between refusals,
	// so it never becomes probable. E answers 403.
	answers[b] = []error{noAnswer, notConnected, notConnected, notConnected, notConnected, nil}
	answers[d] = []error{noAnswer, notConnected, failed, notConnected, notConnected, notConnected}
	answers[e] = []error{&control.CallError{Status: http.StatusForbidden, Message: state.NoRelationship}}
	fw := &farewell{DB: db, Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return now },
		Rand:  func() float64 { return 0.5 }, // no jitter: exactly tellProbablyEvery
		Clear: func() {}, Cut: func(...identity.NodeID) int { return 0 }, Disconnect: func(identity.NodeID) bool { return false },
		Tell: func(_ context.Context, f state.FormerFriend) error {
			p, _ := identity.ParseNodeID(f.Peer)
			mu.Lock()
			defer mu.Unlock()
			i := tries[p]
			tries[p]++
			if i >= len(answers[p]) {
				return notConnected
			}
			return answers[p][i]
		},
		Purge: func(context.Context, string) error { return nil },
	}
	former := func(p identity.NodeID) state.FormerFriend {
		fs, err := db.FormerFriends(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range fs {
			if f.Peer == p.String() {
				return f
			}
		}
		t.Fatalf("%s is no former friend", p)
		return state.FormerFriend{}
	}
	triesOf := func(p identity.NodeID) int { mu.Lock(); defer mu.Unlock(); return tries[p] }
	probable := func(p identity.NodeID) bool { return strings.Contains(former(p).NotifyError, "probably") }
	pass := func(advance time.Duration) {
		fw.pass(ctx)
		fw.wait()
		now = now.Add(advance)
	}
	// Four passes, each past every backoff: B's lost answer and three
	// refusals.
	for range 4 {
		pass(2 * tellMaxBackoff)
	}
	if n := triesOf(b); n != 4 {
		t.Fatalf("B tried %d times, want 4", n)
	}
	if f := former(b); !f.NotifiedAt.IsZero() || !probable(b) {
		t.Fatalf("B after three refusals: told at %v, %q; want not told, probably", f.NotifiedAt, f.NotifyError)
	}
	if f := former(e); f.NotifiedAt.IsZero() {
		t.Fatal("E answered 403 and is not told")
	}
	for _, p := range []identity.NodeID{c, d} {
		if f := former(p); !f.NotifiedAt.IsZero() || probable(p) || triesOf(p) != 4 {
			t.Fatalf("%s: tried %d times, told at %v, %q", map[identity.NodeID]string{c: "C", d: "D"}[p], triesOf(p),
				f.NotifiedAt, f.NotifyError)
		}
	}
	// B's last try was 2 h ago; it is asked again 6 h after it, not on
	// the backoff.
	pass(3 * time.Hour)    // at 2 h
	pass(50 * time.Minute) // at 5 h
	pass(10 * time.Minute) // at 5h50m
	if n := triesOf(b); n != 4 {
		t.Fatalf("B was asked again %d times before tellProbablyEvery", n-4)
	}
	pass(tellProbablyEvery) // at 6 h: asked, still asleep
	if n := triesOf(b); n != 5 || !probable(b) || !former(b).NotifiedAt.IsZero() {
		t.Fatalf("B at 6 h: %d tries, %+v", n, former(b))
	}
	pass(tellProbablyEvery) // it woke: told
	if n := triesOf(b); n != 6 || former(b).NotifiedAt.IsZero() {
		t.Fatalf("B after it woke: %d tries, %+v", n, former(b))
	}
	pass(tellProbablyEvery)
	if n := triesOf(b); n != 6 {
		t.Fatalf("B told, yet asked again (%d tries)", n)
	}
}

// The jitter of the slow retry stays within a tenth of
// tellProbablyEvery either way.
func TestProbablyToldRetryJitter(t *testing.T) {
	parallel(t, 0)
	for _, r := range []float64{0, 0.5, 0.999} {
		fw := &farewell{Rand: func() float64 { return r }}
		d := fw.probablyEvery()
		if d < tellProbablyEvery*9/10 || d > tellProbablyEvery*11/10 {
			t.Fatalf("r=%v: %v", r, d)
		}
	}
}

// PEER-5 (#83): a friend whose every answer has been the 403 "no
// relationship" (the place's condition `removed`) for longer than the
// friendship's retention window ended it without this node hearing:
// the farewell ends it here too, as ended by the friend (so it raises
// friend_removed and is not told back), and revokes it. A refusal for
// less than the window, a friend unreachable for longer (`retrying`),
// and a refusal older than the friendship (friends again since) end
// nothing.
func TestAFriendRefusingForTheWholeWindowIsEndedHereToo(t *testing.T) {
	parallel(t, 0)
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	start := time.Unix(1_800_000_000, 0)
	b, c, d, e := newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID()
	name := map[identity.NodeID]string{b: "B", c: "C", d: "D", e: "E"}
	created := map[identity.NodeID]time.Time{b: start, c: start, d: start, e: start.Add(time.Hour)}
	for p, at := range created {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: p.String(), Address: "tc" + p.String(), CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	refused := "the friend refuses this node: 403 AccessDenied: lendisk: " + state.NoRelationship
	for p, cond := range map[identity.NodeID]struct {
		c     string
		since time.Time
	}{
		b: {state.ConditionRemoved, start.Add(time.Minute)}, // refusing since just after it began
		c: {state.ConditionRemoved, start.Add(time.Hour)},   // since an hour later: not yet the window
		d: {state.ConditionRetrying, start.Add(time.Minute)},
		e: {state.ConditionRemoved, start.Add(time.Minute)}, // refused the friendship before this one
	} {
		if err := db.SetPlaceCondition(ctx, p.String(), cond.c, refused, cond.since); err != nil {
			t.Fatal(err)
		}
	}
	var s steps
	now := start.Add(time.Minute + state.DefaultRetention)
	alerted := 0
	fw := &farewell{DB: db, Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return now },
		Clear: func() {},
		Cut: func(keys ...identity.NodeID) int {
			for _, k := range keys {
				s.add("cut %s", name[k])
			}
			return len(keys)
		},
		Disconnect: func(identity.NodeID) bool { return true },
		Tell:       func(_ context.Context, f state.FormerFriend) error { s.add("tell %s", f.Address); return nil },
		Purge:      func(context.Context, string) error { return nil },
		Alerted:    func() { alerted++ },
	}
	fw.pass(ctx)
	fw.wait()
	if got := s.take(); !slices.Equal(got, []string{"cut B", "cut B"}) {
		t.Fatalf("the pass did %q; want B revoked and not told", got)
	}
	fs, err := db.FormerFriends(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 || fs[0].Peer != b.String() || !fs[0].ByPeer || !fs[0].RemovedAt.Equal(now) ||
		!fs[0].PurgeAfter.Equal(now.Add(state.DefaultRetention)) {
		t.Fatalf("former friends: %+v; want B, ended by it now, purged a window later", fs)
	}
	if alerted == 0 {
		t.Fatal("ending B raised no alert")
	}
	for _, p := range []identity.NodeID{c, d, e} {
		if _, err := db.GetFriendship(ctx, p.String()); err != nil {
			t.Fatalf("%s: %v", name[p], err)
		}
	}
}
