package manifest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// lenders stands in for the friends' control API: what each received,
// and which are offline.
type lenders struct {
	mu      sync.Mutex
	got     map[string][][]byte
	offline map[string]bool
	pushed  chan string
	// hold, when set, keeps every push in flight until it is closed.
	hold chan struct{}
}

func newLenders() *lenders {
	return &lenders{got: map[string][][]byte{}, offline: map[string]bool{}, pushed: make(chan string, 100)}
}

func (l *lenders) push(_ context.Context, f state.Friendship, blob []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.offline[f.Peer] {
		return errors.New("friend offline")
	}
	l.got[f.Peer] = append(l.got[f.Peer], blob)
	l.pushed <- f.Peer
	if hold := l.hold; hold != nil {
		l.mu.Unlock()
		<-hold
		l.mu.Lock()
	}
	return nil
}

func (l *lenders) count(peer string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.got[peer])
}

func (l *lenders) setOffline(peer string, off bool) {
	l.mu.Lock()
	l.offline[peer] = off
	l.mu.Unlock()
}

type pushRig struct {
	t       *testing.T
	db      *state.DB
	p       *Pusher
	lenders *lenders
	b, c    string
	now     time.Time
}

func newPushRig(t *testing.T) *pushRig {
	t.Helper()
	r := &pushRig{t: t, db: openDB(t), lenders: newLenders(), now: time.Unix(1_800_000_000, 0)}
	self := newID(t)
	r.b, r.c = newID(t).NodeID().String(), newID(t).NodeID().String()
	for _, f := range []string{r.b, r.c} {
		if err := r.db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f[:8], OwnNumber: 1 << 30,
			PeerNumber: 1 << 30, CreatedAt: r.now}); err != nil {
			t.Fatal(err)
		}
	}
	r.p = &Pusher{DB: r.db, Self: self, Push: r.lenders.push, Now: func() time.Time { return r.now },
		Log: slog.New(slog.DiscardHandler)}
	return r
}

// round makes one round and waits for the pushes it started.
func (r *pushRig) round(ctx context.Context) {
	r.p.Round(ctx)
	r.p.Wait()
}

func (r *pushRig) addBucket(name string) {
	r.t.Helper()
	if err := r.db.AddBucket(r.t.Context(), state.Bucket{Name: name, Profile: "restic", Copies: 2, AccessKey: "AK" + name,
		SecretKey: "s", CreatedAt: r.now}); err != nil {
		r.t.Fatal(err)
	}
}

func (r *pushRig) delivered(peer string) state.ManifestPush {
	r.t.Helper()
	ps, err := r.db.ManifestPushes(r.t.Context())
	if err != nil {
		r.t.Fatal(err)
	}
	return ps[peer]
}

// ID-4: every friend gets the manifest once per version: at the first
// round, again after a change, and not when nothing changed. What each
// friend acknowledged is recorded in lendisk.db, and what it got opens
// with the node's identity and lists the buckets.
func TestPushOncePerVersionToEveryFriend(t *testing.T) {
	r := newPushRig(t)
	ctx := t.Context()
	r.addBucket("laptop")
	r.round(ctx)
	if r.lenders.count(r.b) != 1 || r.lenders.count(r.c) != 1 {
		t.Fatalf("first round: B got %d, C got %d", r.lenders.count(r.b), r.lenders.count(r.c))
	}
	if d := r.delivered(r.b); d.Seq != 1 || d.Error != "" || !d.DeliveredAt.Equal(r.now) {
		t.Fatalf("B's delivery %+v", d)
	}
	m, err := Open(r.p.Self, r.lenders.got[r.b][0])
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 1 || len(m.Buckets) != 1 || m.Buckets[0].Sealed != r.p.Self.SealedName("laptop") || len(m.Friends) != 2 {
		t.Fatalf("pushed manifest %+v", m)
	}

	r.now = r.now.Add(time.Minute)
	r.round(ctx)
	if r.lenders.count(r.b) != 1 {
		t.Fatal("an unchanged manifest was pushed again")
	}

	r.addBucket("desktop")
	r.round(ctx)
	if r.lenders.count(r.b) != 2 || r.lenders.count(r.c) != 2 {
		t.Fatal("a change was not pushed to every friend")
	}
	if m, _ := Open(r.p.Self, r.lenders.got[r.c][1]); m.Seq != 2 || len(m.Buckets) != 2 {
		t.Fatalf("second manifest %+v", m)
	}
}

// ID-4: a push a friend missed is recorded with its failure, and made
// when the friend answers again; friends that have the version are not
// pushed to again.
func TestMissedPushIsMadeWhenFriendReturns(t *testing.T) {
	r := newPushRig(t)
	ctx := t.Context()
	r.round(ctx)
	r.lenders.setOffline(r.c, true)
	r.addBucket("laptop")
	r.now = r.now.Add(time.Minute)
	r.round(ctx)
	if d := r.delivered(r.c); d.Seq != 1 || d.Error == "" || !d.TriedAt.Equal(r.now) {
		t.Fatalf("C's delivery after the miss %+v", d)
	}
	if d := r.delivered(r.b); d.Seq != 2 {
		t.Fatalf("B's delivery %+v", d)
	}
	r.lenders.setOffline(r.c, false)
	r.now = r.now.Add(time.Minute)
	r.round(ctx)
	if d := r.delivered(r.c); d.Seq != 2 || d.Error != "" {
		t.Fatalf("C's delivery after its return %+v", d)
	}
	if r.lenders.count(r.b) != 2 || r.lenders.count(r.c) != 2 {
		t.Fatalf("B got %d, C got %d", r.lenders.count(r.b), r.lenders.count(r.c))
	}
}

// ID-4 "on every change": Kick starts a round at once, without waiting
// for the poll.
func TestKickPushesWithoutWaitingForPoll(t *testing.T) {
	r := newPushRig(t)
	r.p.Poll = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { r.p.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	for range 2 { // the first round, at start
		select {
		case <-r.lenders.pushed:
		case <-time.After(10 * time.Second):
			t.Fatal("no first round")
		}
	}
	r.addBucket("laptop")
	r.p.Kick()
	for range 2 {
		select {
		case <-r.lenders.pushed:
		case <-time.After(10 * time.Second):
			t.Fatal("Kick did not push the change")
		}
	}
}

// ID-4 "on every change": a change made while a push to a friend is
// still in flight is pushed when that push ends, not at the next poll.
func TestChangeDuringAPushIsPushedWhenItEnds(t *testing.T) {
	r := newPushRig(t)
	r.p.Poll = time.Hour
	r.lenders.hold = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { r.p.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	for range 2 {
		select {
		case <-r.lenders.pushed:
		case <-time.After(10 * time.Second):
			t.Fatal("no first round")
		}
	}
	r.addBucket("laptop")
	r.p.Kick()
	time.Sleep(200 * time.Millisecond) // let the kicked round find both in flight
	close(r.lenders.hold)
	for range 2 {
		select {
		case <-r.lenders.pushed:
		case <-time.After(10 * time.Second):
			t.Fatal("the change was not pushed after the push in flight ended")
		}
	}
}
