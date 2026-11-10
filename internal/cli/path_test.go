package cli

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// fakePaths answers PathInfo: relayed for a friend's first relayedFor
// probes, then direct; an error for a friend not dialled.
type fakePaths struct {
	transport.Transport
	relayedFor int
	dialled    identity.NodeID

	mu    sync.Mutex
	calls int
}

func (f *fakePaths) PathInfo(_ context.Context, peer identity.NodeID) (transport.Path, error) {
	if peer != f.dialled {
		return transport.Path{}, errors.New("not dialled")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.relayedFor {
		return transport.Path{Relay: "relay.example:443"}, nil
	}
	return transport.Path{Direct: true, Endpoint: "198.51.100.7:41641"}, nil
}

func (f *fakePaths) probes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// NET-4 (#42): the daemon keeps disco-pinging a relayed friend, each
// ping also trying for a direct path, and records each answer: relayed
// with its relay, then direct with the endpoint once a ping comes back
// direct. A direct friend is pinged far less often, and a friend not
// dialled is left alone.
func TestPathKeeperRetriesWhileRelayed(t *testing.T) {
	parallel(t, 0)
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids []identity.NodeID
	for range 2 {
		p, err := identity.NewPhrase()
		if err != nil {
			t.Fatal(err)
		}
		id, err := identity.FromPhrase(p)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id.NodeID())
	}
	for _, id := range ids {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: id.String(), Address: "tc" + id.String(),
			OwnNumber: 1 << 30, PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	tr := &fakePaths{relayedFor: 3, dialled: ids[0]}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		(&pathKeeper{Tr: tr, DB: db, Now: time.Now, Log: slog.New(slog.DiscardHandler),
			Tick: 10 * time.Millisecond, RelayedEvery: 30 * time.Millisecond, DirectEvery: time.Hour}).run(ctx)
		close(done)
	}()
	defer func() { cancel(); <-done }()

	within(t, 2*time.Second, "a relayed path recorded", func() bool {
		f, err := db.GetFriendship(t.Context(), ids[0].String())
		return err == nil && f.Path == "relayed" && f.PathRelay == "relay.example:443" && f.PathEndpoint == "" && !f.PathAt.IsZero()
	})
	within(t, 5*time.Second, "the direct path found by a later ping", func() bool {
		f, err := db.GetFriendship(t.Context(), ids[0].String())
		return err == nil && f.Path == "direct" && f.PathEndpoint == "198.51.100.7:41641" && f.PathRelay == ""
	})
	n := tr.probes()
	time.Sleep(200 * time.Millisecond)
	if got := tr.probes(); got != n || n != tr.relayedFor+1 {
		t.Fatalf("probes: %d relayed then direct should be %d, and none while direct within DirectEvery; got %d, then %d", tr.relayedFor, tr.relayedFor+1, n, got)
	}
	if f, err := db.GetFriendship(t.Context(), ids[1].String()); err != nil || f.Path != "" || !f.PathAt.IsZero() {
		t.Fatalf("a friend not dialled got a path: %+v %v", f, err)
	}
}
