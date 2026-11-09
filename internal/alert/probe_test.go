package alert

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// probed is a friend with nothing to receive that answers the
// reachability probe, or not; each failure names its probe, so a test
// sees it recorded.
type probed struct {
	mu     sync.Mutex
	away   bool
	probes int
}

func (p *probed) Put(context.Context, replica.Object) error    { return nil }
func (p *probed) Delete(context.Context, string, string) error { return nil }
func (p *probed) Head(context.Context, string, string) (string, bool, error) {
	return "", false, nil
}

func (p *probed) Probe(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.probes++
	if p.away {
		return fmt.Errorf("dial: i/o timeout (probe %d)", p.probes)
	}
	return nil
}

func (p *probed) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.probes
}

// OPS-2, D6 (#65): an idle friend, one with nothing queued, that goes
// away is alerted as unreachable at alerts.unreachable_after after its
// last answer to the reachability probe, not before; an idle friend
// that keeps answering probes never is. The worker and the alert
// service run on one clock, moved a probe interval at a time.
func TestAnIdleFriendThatGoesAwayIsAlertedAtThreshold(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	friends := map[string]*probed{"B": {}, "C": {}}
	w := &replica.Worker{DB: db, StoreDir: t.TempDir(), Log: quiet, Now: clk.now, Poll: time.Millisecond,
		Connect: func(f state.Friendship) (replica.Replica, error) { return friends[f.Peer], nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	s := &Service{DB: db, Log: quiet, Now: clk.now}

	// step moves the clock one probe interval and waits until each
	// friend's probe is recorded.
	n := 0
	step := func() {
		t.Helper()
		if n > 0 {
			clk.set(clk.now().Add(replica.DefaultProbeEvery))
		}
		n++
		waitFor(t, fmt.Sprintf("probe %d recorded", n), func() bool {
			cs, err := db.PlaceConditions(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for name, f := range friends {
				c := cs[name]
				if f.count() < n {
					return false
				}
				f.mu.Lock()
				away := f.away
				f.mu.Unlock()
				if away && !strings.Contains(c.Detail, fmt.Sprintf("(probe %d)", n)) ||
					!away && !c.LastOK.Equal(clk.now()) {
					return false
				}
			}
			return true
		})
		if err := s.evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 8; i++ {
		step()
	}
	last := clk.now() // B's last answer
	friends["B"].mu.Lock()
	friends["B"].away = true
	friends["B"].mu.Unlock()
	for {
		step()
		as := active(t, db)
		if _, ok := as[KindFriendUnreachable+":C"]; ok {
			t.Fatalf("C, answering every probe, alerted at %v", clk.now().Sub(last))
		}
		a, raised := as[KindFriendUnreachable+":B"]
		if since := clk.now().Sub(last); since < 24*time.Hour {
			if raised {
				t.Fatalf("B alerted %v after its last answer", since)
			}
			continue
		} else if !raised {
			t.Fatalf("B not alerted %v after its last answer", since)
		}
		if !strings.Contains(a.Message, last.UTC().Format(time.RFC3339)) {
			t.Fatalf("message %q does not name the last answer", a.Message)
		}
		break
	}
	// C keeps answering for days more, and is never alerted.
	for i := 0; i < 4*24*2; i++ {
		step()
		if _, ok := active(t, db)[KindFriendUnreachable+":C"]; ok {
			t.Fatalf("C, answering every probe, alerted")
		}
	}
}
