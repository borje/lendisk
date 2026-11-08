package verify

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// heldBy reads the fake lender's objects for the real control server.
type heldBy struct{ b *lender }

func (h heldBy) Read(_ context.Context, _ identity.NodeID, _, key string, offset, length int64) (io.ReadCloser, error) {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	o, ok := h.b.held[key]
	if !ok {
		return nil, gateway.ErrNoLentKey
	}
	if length < 0 {
		return io.NopCloser(bytes.NewReader(o)), nil
	}
	return io.NopCloser(bytes.NewReader(o[offset:min(offset+length, int64(len(o)))])), nil
}

// VER-3, LEND-5 (#27 review), with a scaled-down budget: a full check of
// objects whose whole reads leave the friend in debt for longer than
// maxWait still completes, waiting as long as the friend's Retry-After
// says, within what the largest object read so far takes at WholeRate.
// The friend paces whole-object reads apart from the daily ranged
// challenges, so a full check's debt does not stall them.
func TestAFullCheckWaitsOutTheFriendsPaceForLargeObjects(t *testing.T) {
	n := newNode(t, 0)
	for _, k := range []string{"data/x", "data/y", "data/z"} {
		body := make([]byte, 400_000) // 400 s of debt at 1000 B/s: past maxWait
		rand.Read(body)
		n.put(k, body)
	}
	srv := &control.Server{Held: heldBy{n.b}, ChallengeWholeRate: 1000, ChallengeWholeBurst: 1000, Now: n.clock,
		Log: n.v.Log}
	caller := control.Caller{Peer: n.self.NodeID(), Role: state.RolePeer}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(w, r.WithContext(control.WithCaller(r.Context(), caller)))
	}))
	defer ts.Close()
	cl := &control.Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ts.Listener.Addr().String())
		}}}}
	n.v.Call = func(ctx context.Context, _ state.Friendship, req control.ChallengeRequest) (control.ChallengeResponse, error) {
		return cl.Challenge(ctx, req)
	}
	var slept time.Duration
	n.v.Sleep = func(_ context.Context, d time.Duration) error {
		slept += d
		n.advance(d)
		return nil
	}
	n.v.WholeRate = 1000
	if o := n.check(true); o.Outcome != state.VerifyPassed || o.Checked != 3 {
		t.Fatalf("full check: %+v", o)
	}
	if slept < 700*time.Second {
		t.Fatalf("waited %v for the friend's pace, want the debt of two 400 kB reads", slept)
	}
	slept = 0
	if o := n.check(false); o.Outcome != state.VerifyPassed || slept != 0 {
		t.Fatalf("a ranged check after the full one: %+v, waited %v", o, slept)
	}
}
