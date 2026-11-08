package control_test

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

// LEND-7, LEND-8 (#24): the quota call carries the caller's number, its
// use of this node's space and its repository size. This node applies
// the number at once, so the quota is the smaller on both sides after
// one call, and answers with its own number, the quota, its use of the
// caller's space, its count as lender of what the caller stores here
// and its repository size, rounded up to a GiB. It answers for the
// caller alone; an invite key and malformed figures are refused.
func TestQuotaCallAppliesTheCallersNumber(t *testing.T) {
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bob, carol, invited := newID(t), newID(t), newID(t)
	for _, f := range []string{bob.NodeID().String(), carol.NodeID().String()} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 80 << 30,
			PeerNumber: 80 << 30, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddLentUsage(ctx, bob.NodeID().String(), 12345); err != nil {
		t.Fatal(err)
	}
	var reported atomic.Int32
	now := time.Unix(1_800_000_000, 0)
	l := newLender(t)
	l.srv.DB, l.srv.Now = db, func() time.Time { return now }
	l.srv.Reported = func(peer string) {
		if peer != bob.NodeID().String() {
			t.Errorf("Reported for %s", peer)
		}
		reported.Add(1)
	}

	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	got, err := asBob.Quota(ctx, control.QuotaRequest{Number: 50 << 30, Use: 777, RepositorySize: 3 << 30})
	if err != nil {
		t.Fatal(err)
	}
	if got.Number != 80<<30 || got.Quota != 50<<30 || got.Lent != 12345 || got.Use != 0 || got.RepositorySize != 0 ||
		got.Protocol != control.Protocol {
		t.Fatalf("answer %+v", got)
	}
	f, err := db.GetFriendship(ctx, bob.NodeID().String())
	if err != nil {
		t.Fatal(err)
	}
	if f.PeerNumber != 50<<30 || f.Quota() != 50<<30 || f.PeerUse != 777 || f.PeerRepositorySize != 3<<30 ||
		!f.SeenAt.Equal(now) || f.PeerProtocol != control.Protocol {
		t.Fatalf("Bob's friendship after his call: %+v", f)
	}
	if reported.Load() != 1 {
		t.Fatalf("Reported %d times", reported.Load())
	}
	if c, _ := db.GetFriendship(ctx, carol.NodeID().String()); c.PeerNumber != 80<<30 {
		t.Fatalf("Bob's call changed Carol's friendship: %+v", c)
	}

	refused := func(what string, c *control.Client, req control.QuotaRequest, status int) {
		t.Helper()
		_, err := c.Quota(ctx, req)
		var ce *control.CallError
		if !errors.As(err, &ce) || ce.Status != status {
			t.Fatalf("%s: %v, want %d", what, err, status)
		}
	}
	asInvite := l.client(t, control.Caller{Peer: invited.NodeID(), Role: state.RoleInvite})
	refused("an invite key", asInvite, control.QuotaRequest{Number: 1 << 30}, http.StatusForbidden)
	for name, req := range map[string]control.QuotaRequest{
		"no number":               {},
		"a negative number":       {Number: -1},
		"a number past the limit": {Number: 1<<62 + 1},
		"a negative use":          {Number: 1 << 30, Use: -1},
		"a negative size":         {Number: 1 << 30, RepositorySize: -1},
		"a size not a whole GiB":  {Number: 1 << 30, RepositorySize: 1<<30 + 1},
		"a size past the limit":   {Number: 1 << 30, RepositorySize: 1 << 62},
		"a use past the limit":    {Number: 1 << 30, Use: 1 << 62},
	} {
		refused(name, asBob, req, http.StatusBadRequest)
	}
	if f, _ := db.GetFriendship(ctx, bob.NodeID().String()); f.PeerNumber != 50<<30 {
		t.Fatalf("a refused call changed the number: %+v", f)
	}
	resp, err := asBob.Do(ctx, http.MethodPost, control.QuotaPath, bytes.NewReader([]byte("not json")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a malformed body: %d", resp.StatusCode)
	}
}

// LEND-7 (#24): the repository size crosses the tunnel rounded up to a
// whole GiB.
func TestRepositorySizeRoundsUpToAGiB(t *testing.T) {
	for in, want := range map[int64]int64{0: 0, 1: 1 << 30, 1 << 30: 1 << 30, 1<<30 + 1: 2 << 30, 5<<30 - 7: 5 << 30} {
		if got := control.RoundSize(in); got != want {
			t.Errorf("RoundSize(%d) = %d, want %d", in, got, want)
		}
	}
}
