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

// clock is an injected clock the test moves.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// pushes counts calls per friend.
type pushes struct {
	mu sync.Mutex
	n  map[string]int
}

func (p *pushes) count(peer string) int { p.mu.Lock(); defer p.mu.Unlock(); return p.n[peer] }
func (p *pushes) note(peer string) {
	p.mu.Lock()
	if p.n == nil {
		p.n = map[string]int{}
	}
	p.n[peer]++
	p.mu.Unlock()
}

// pusherWith is a Pusher over a fresh database with the given friends
// and push function, on an injected clock.
func pusherWith(t *testing.T, push func(ctx context.Context, f state.Friendship, blob []byte) error, friends ...string) (*Pusher, *clock) {
	t.Helper()
	db := openDB(t)
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	for _, f := range friends {
		if err := db.AddFriendship(context.Background(), state.Friendship{Peer: f, Address: "tc" + f, CreatedAt: c.now()}); err != nil {
			t.Fatal(err)
		}
	}
	p := &Pusher{DB: db, Self: newID(t), Push: push, Now: c.now, Log: slog.New(slog.DiscardHandler)}
	return p, c
}

// round makes one round and waits for the pushes it started.
func round(p *Pusher) {
	p.Round(context.Background())
	p.Wait()
}

// ID-4 (#28 review): a friend whose push fails is tried again after a
// minute, then two, four, … up to an hour, not every round.
func TestPusherBacksOffFromAFailingFriend(t *testing.T) {
	var n pushes
	p, c := pusherWith(t, func(_ context.Context, f state.Friendship, _ []byte) error {
		n.note(f.Peer)
		return errors.New("offline")
	}, "bob")
	round(p)
	if n.count("bob") != 1 {
		t.Fatalf("first round: %d pushes", n.count("bob"))
	}
	want := 1
	for _, wait := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
		16 * time.Minute, 32 * time.Minute, time.Hour, time.Hour, time.Hour} {
		c.add(wait - time.Second)
		round(p)
		if n.count("bob") != want {
			t.Fatalf("tried again %v after a failure, before the backoff of %v", wait-time.Second, wait)
		}
		c.add(time.Second)
		round(p)
		want++
		if n.count("bob") != want {
			t.Fatalf("not tried again after the backoff of %v: %d pushes", wait, n.count("bob"))
		}
	}
}

// ID-4 (#28 review): a friend holding the current version is sent it
// again a day after the last delivery, though nothing changed: it may
// have lost its store.
func TestPusherRepushesDaily(t *testing.T) {
	var n pushes
	p, c := pusherWith(t, func(_ context.Context, f state.Friendship, _ []byte) error {
		n.note(f.Peer)
		return nil
	}, "bob")
	round(p)
	c.add(24*time.Hour - time.Second)
	round(p)
	if n.count("bob") != 1 {
		t.Fatalf("pushed again within a day: %d pushes", n.count("bob"))
	}
	c.add(time.Second)
	round(p)
	if n.count("bob") != 2 {
		t.Fatalf("not pushed again after a day: %d pushes", n.count("bob"))
	}
	ps, _ := p.DB.ManifestPushes(context.Background())
	if !ps["bob"].DeliveredAt.Equal(c.now()) {
		t.Fatalf("the daily push was not recorded: %+v", ps["bob"])
	}
}

// ID-4 (#28 review): a friend whose push hangs holds up no other: the
// round returns, the others are delivered, and the next round does not
// start a second push to the hung one.
func TestOneHungFriendDoesNotHoldUpTheOthers(t *testing.T) {
	release := make(chan struct{})
	var n pushes
	p, _ := pusherWith(t, func(ctx context.Context, f state.Friendship, _ []byte) error {
		n.note(f.Peer)
		if f.Peer == "hung" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return nil
	}, "hung", "quick")
	done := make(chan struct{})
	go func() { p.Round(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("the round waited for the hung friend")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ps, _ := p.DB.ManifestPushes(context.Background())
		if ps["quick"].Seq == 1 {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("the quick friend was not delivered while the other hung")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.Round(context.Background())
	if n.count("hung") != 1 {
		t.Errorf("a second push to the hung friend started: %d", n.count("hung"))
	}
	close(release)
	p.Wait()
	if ps, _ := p.DB.ManifestPushes(context.Background()); ps["hung"].Seq != 1 {
		t.Fatalf("the hung friend's push, once done, was not recorded: %+v", ps["hung"])
	}
}

// ID-4: Kick makes a round now, not at the next poll.
func TestKickMakesARoundNow(t *testing.T) {
	got := make(chan string, 4)
	p, _ := pusherWith(t, func(_ context.Context, f state.Friendship, _ []byte) error {
		got <- f.Peer
		return nil
	})
	p.Poll = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	if err := p.DB.AddFriendship(ctx, state.Friendship{Peer: "bob", CreatedAt: time.Unix(1_800_000_000, 0)}); err != nil {
		t.Fatal(err)
	}
	p.Kick()
	select {
	case peer := <-got:
		if peer != "bob" {
			t.Fatalf("pushed to %s", peer)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no push after Kick")
	}
}

// ID-4, PEER-5 (#25): once a friendship ends the pusher pushes there no
// more, its delivery row is gone with the friendship, and a push to it
// that was in flight records nothing when it ends; the remaining friend
// gets the manifest that no longer names the former one.
func TestPusherStopsAtARemovedFriend(t *testing.T) {
	var n pushes
	release, started := make(chan struct{}), make(chan struct{}, 1)
	p, c := pusherWith(t, func(ctx context.Context, f state.Friendship, _ []byte) error {
		n.note(f.Peer)
		if f.Peer == "bob" && n.count("bob") == 2 {
			started <- struct{}{}
			<-release
			return errors.New("connection closed")
		}
		return nil
	}, "bob", "carol")
	ctx := context.Background()
	round(p)
	if n.count("bob") != 1 || n.count("carol") != 1 {
		t.Fatalf("first round: bob %d, carol %d", n.count("bob"), n.count("carol"))
	}
	// A daily push to Bob is in flight when the friendship ends.
	c.add(24 * time.Hour)
	p.Round(ctx)
	<-started
	if _, err := p.DB.RemoveFriendship(ctx, "bob", false, c.now()); err != nil {
		t.Fatal(err)
	}
	close(release)
	p.Wait()
	round(p)
	if n.count("bob") != 2 {
		t.Fatalf("pushes to Bob after removal: %d, want none past the one in flight", n.count("bob"))
	}
	if n.count("carol") < 3 {
		t.Fatalf("Carol was not sent the manifest without Bob: %d pushes", n.count("carol"))
	}
	ps, _ := p.DB.ManifestPushes(ctx)
	if _, ok := ps["bob"]; ok {
		t.Fatalf("Bob's delivery row is back: %+v", ps["bob"])
	}
}
