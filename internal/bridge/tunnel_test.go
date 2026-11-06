package bridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeTunnel stands in for the tunnel on loopback TCP: each client
// connection is registered with the key it "authenticated" with.
type fakeTunnel struct {
	mu   sync.Mutex
	keys map[string]identity.NodeID // client's local address → key
	ln   net.Listener
}

// peerOf waits briefly, since the client registers its key only once its
// dial has returned.
func (f *fakeTunnel) peerOf(c net.Conn) (identity.NodeID, error) {
	for range 50 {
		f.mu.Lock()
		k, ok := f.keys[c.RemoteAddr().String()]
		f.mu.Unlock()
		if ok {
			return k, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return identity.NodeID{}, errors.New("no key")
}

// client returns a control API client whose connections carry key; a
// nil key means a connection without one.
func (f *fakeTunnel) client(key *identity.Identity) *control.Client {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", f.ln.Addr().String())
		if err == nil && key != nil {
			f.mu.Lock()
			f.keys[c.LocalAddr().String()] = key.NodeID()
			f.mu.Unlock()
		}
		return c, err
	}}
	return &control.Client{HTTP: &http.Client{Transport: tr, Timeout: 10 * time.Second}}
}

type lender struct {
	db      *state.DB
	self    *identity.Identity
	tun     *fakeTunnel
	changed atomic.Int32
}

// startLender serves the tunnel handler for a node with a fresh
// database, admitting every connection: the handler alone decides.
func startLender(t *testing.T) *lender {
	t.Helper()
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &lender{db: db, self: newID(t), tun: &fakeTunnel{keys: map[string]identity.NodeID{}, ln: ln}}
	ctl := &control.Server{DB: db, Self: l.self, Changed: func() { l.changed.Add(1) }, Log: quiet}
	roles := func(ctx context.Context, p identity.NodeID) (state.Role, error) {
		return db.PeerRole(ctx, p.String(), time.Now())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- ServeTunnel(ctx, ln, l.tun.peerOf, TunnelHandler(roles, ctl, nil, quiet), quiet) }()
	t.Cleanup(func() { cancel(); <-done })
	return l
}

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (l *lender) addInvite(t *testing.T, key *identity.Identity) {
	t.Helper()
	if err := l.db.AddInvite(t.Context(), state.Invite{Key: key.NodeID().String(), Number: 80 << 30,
		Retention: 30 * 24 * time.Hour, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
}

// every request the tunnel could receive: each control API call by
// name, and S3 calls.
var everyRequest = []struct{ method, path string }{
	{"GET", control.VersionPath},
	{"POST", control.RedeemPath},
	{"POST", control.Prefix + "challenge"},
	{"GET", control.Prefix + "manifest"},
	{"PUT", control.Prefix + "manifest"},
	{"POST", control.Prefix + "quota"},
	{"POST", control.Prefix + "undelete"},
	{"GET", control.Prefix + "list"},
	{"GET", "/"},
	{"PUT", "/sbucket"},
	{"PUT", "/sbucket/key"},
	{"GET", "/sbucket/key"},
	{"DELETE", "/sbucket/key"},
}

func status(t *testing.T, c *control.Client, method, path string) int {
	t.Helper()
	resp, err := c.Do(t.Context(), method, path, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// SEC-11 at the handler: with every connection admitted, an identity
// with no relationship is refused every request, and so is a
// connection without a key (SEC-12).
func TestUnknownIdentityRefusedByMiddleware(t *testing.T) {
	l := startLender(t)
	c := l.tun.client(newID(t))
	for _, r := range everyRequest {
		if got := status(t, c, r.method, r.path); got != http.StatusForbidden {
			t.Errorf("%s %s from a stranger: %d", r.method, r.path, got)
		}
	}
	// A connection without a key is closed before a byte is read.
	if resp, err := l.tun.client(nil).Do(t.Context(), "GET", control.VersionPath, nil); err == nil {
		resp.Body.Close()
		t.Fatalf("a keyless connection was answered: %d", resp.StatusCode)
	}
}

// SEC-13 AC: from a connection holding an unredeemed invite, every call
// but redeem is refused before routing.
func TestInviteKeyMayOnlyRedeem(t *testing.T) {
	l := startLender(t)
	ik := newID(t)
	l.addInvite(t, ik)
	c := l.tun.client(ik)
	for _, r := range everyRequest {
		if r.path == control.RedeemPath {
			continue
		}
		if got := status(t, c, r.method, r.path); got != http.StatusForbidden {
			t.Errorf("%s %s with an invite key: %d, want 403", r.method, r.path, got)
		}
	}
	if got := status(t, c, "GET", control.RedeemPath); got != http.StatusForbidden {
		t.Errorf("GET redeem with an invite key: %d", got)
	}
	if r, _ := l.db.PeerRole(t.Context(), ik.NodeID().String(), time.Now()); r != state.RoleInvite {
		t.Fatal("refused calls touched the invite")
	}
}

// PEER-2 and the identity rule: the friend's key is the one its address
// names, proven by key agreement; a forged proof or another node's
// address is refused, and the friend is never the connection's invite
// key. A replay on the same connection is refused.
func TestRedeemBindsProvenKey(t *testing.T) {
	d := transporttest.Start(t)
	l := startLender(t)
	ik, bob, mallory := newID(t), newID(t), newID(t)
	l.addInvite(t, ik)
	c := l.tun.client(ik)
	bobAddr := transport.AddressOf(bob, d.Region)

	// Mallory holds the invite but claims Bob's address.
	forged, _ := control.RedeemProof(mallory, l.self.NodeID(), ik.NodeID())
	_, err := c.Redeem(t.Context(), control.RedeemRequest{Address: bobAddr, Number: 1 << 30, RetentionSeconds: 3600, Proof: forged})
	var ce *control.CallError
	if !errors.As(err, &ce) || ce.Status != http.StatusForbidden {
		t.Fatalf("forged proof: %v", err)
	}
	// The inviting node's own address.
	selfProof, _ := control.RedeemProof(bob, l.self.NodeID(), ik.NodeID())
	if _, err := c.Redeem(t.Context(), control.RedeemRequest{Address: transport.AddressOf(l.self, d.Region), Number: 1, RetentionSeconds: 1, Proof: selfProof}); err == nil {
		t.Fatal("redeemed with the inviter's own address")
	}

	proof, _ := control.RedeemProof(bob, l.self.NodeID(), ik.NodeID())
	resp, err := c.Redeem(t.Context(), control.RedeemRequest{Address: bobAddr, Number: 100 << 30, RetentionSeconds: 3600, Proof: proof})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Number != 80<<30 || resp.RetentionSeconds != 30*24*3600 {
		t.Fatalf("redeem answer %+v", resp)
	}
	f, err := l.db.GetFriendship(t.Context(), bob.NodeID().String())
	if err != nil || f.Quota() != 80<<30 || f.Address != bobAddr {
		t.Fatalf("friendship %+v, %v", f, err)
	}
	if _, err := l.db.GetFriendship(t.Context(), ik.NodeID().String()); !errors.Is(err, state.ErrNotFound) {
		t.Fatal("the invite key became a friend")
	}
	if l.changed.Load() != 1 {
		t.Fatalf("Changed called %d times", l.changed.Load())
	}
	// Replay: the invite key now has no role, so the middleware refuses.
	if _, err := c.Redeem(t.Context(), control.RedeemRequest{Address: bobAddr, Number: 1, RetentionSeconds: 1, Proof: proof}); !errors.As(err, &ce) || ce.Status != http.StatusForbidden {
		t.Fatalf("replayed redeem: %v", err)
	}
}

// NET-6: version answers; peers one minor apart interoperate, two
// apart or another major are refused with an error that says why.
func TestProtocolVersionInterop(t *testing.T) {
	l := startLender(t)
	friend := newID(t)
	l.db.AddFriendship(t.Context(), state.Friendship{Peer: friend.NodeID().String(), OwnNumber: 1, PeerNumber: 1, CreatedAt: time.Now()})
	c := l.tun.client(friend)
	v, err := c.Version(t.Context())
	if err != nil || v.Protocol != control.Protocol {
		t.Fatalf("version: %+v, %v", v, err)
	}
	for theirs, ok := range map[string]bool{"0.0": true, "0.1": true, "0.2": true, "0.3": false, "1.1": false, "x": false} {
		c.Protocol = theirs
		got := status(t, c, "GET", control.Prefix+"list")
		if ok && got == http.StatusBadRequest || !ok && got != http.StatusBadRequest {
			t.Errorf("protocol %s: status %d", theirs, got)
		}
	}
	if err := control.Compatible("0.1", "0.3"); err == nil || !strings.Contains(err.Error(), "upgrade lendisk on this node") {
		t.Fatalf("compat error: %v", err)
	}
	if err := control.Compatible("0.3", "0.1"); err == nil || !strings.Contains(err.Error(), "upgrade lendisk on the peer") {
		t.Fatalf("compat error: %v", err)
	}
	// The client refuses a peer too far apart, even on a success.
	c.Protocol = "0.3"
	if _, err := c.Version(t.Context()); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("client accepted an incompatible peer: %v", err)
	}
}
