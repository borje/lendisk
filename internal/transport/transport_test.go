package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/tailcat"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func discard(string, ...any) {}

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newTailcat(t *testing.T, id *identity.Identity, d *transporttest.LocalDERP) *Tailcat {
	t.Helper()
	tc, err := NewTailcat(id, d.Region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tc.Close() })
	return tc
}

// listener records what the tunnel port hands out.
type listener struct {
	accepted atomic.Int64
	conns    chan net.Conn
}

func listen(t *testing.T, tc *Tailcat) *listener {
	t.Helper()
	ln, err := tc.Listen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	l := &listener{conns: make(chan net.Conn, 8)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			l.accepted.Add(1)
			l.conns <- c
		}
	}()
	return l
}

func TestTailcatImplementsTransport(t *testing.T) {
	var _ Transport = (*Tailcat)(nil)
}

// NET-8: an implementation with no admission function refuses
// everything; here it refuses to listen at all.
func TestListenWithoutAdmissionRefused(t *testing.T) {
	d := transporttest.Start(t)
	tc := newTailcat(t, newID(t), d)
	if _, err := tc.Listen(t.Context()); !errors.Is(err, ErrNoAdmission) {
		t.Fatalf("Listen without Admit = %v; want ErrNoAdmission", err)
	}
	tc.Admit(nil)
	if _, err := tc.Listen(t.Context()); !errors.Is(err, ErrNoAdmission) {
		t.Fatalf("Listen after Admit(nil) = %v; want ErrNoAdmission", err)
	}
}

func TestNewTailcatNeedsKeyAndRegion(t *testing.T) {
	d := transporttest.Start(t)
	if _, err := NewTailcat(nil, d.Region, quiet); err == nil {
		t.Fatal("NewTailcat accepted no identity")
	}
	if _, err := NewTailcat(newID(t), nil, quiet); err == nil {
		t.Fatal("NewTailcat accepted no region")
	}
}

// SEC-14: the address is a function of the key and the saved region,
// the same before the server starts, while it runs and after a restart,
// and it carries only public values.
func TestAddressIsStableAndCarriesNoSecret(t *testing.T) {
	d := transporttest.Start(t)
	id := newID(t)
	want := AddressOf(id, d.Region)
	if !strings.HasPrefix(want, "tc") {
		t.Fatalf("address %q does not start with tc", want)
	}
	for range 2 {
		tc, err := NewTailcat(id, d.Region, quiet)
		if err != nil {
			t.Fatal(err)
		}
		tc.Admit(func(identity.NodeID) bool { return false })
		if _, err := tc.Listen(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := string(tc.server.TailcatAddr()); got != want {
			t.Fatalf("running server's address differs from AddressOf:\n%s\n%s", got, want)
		}
		if tc.Address() != want {
			t.Fatal("Address() differs from AddressOf")
		}
		tc.Close()
	}
	ci, err := tailcat.ParseAddr(tailcat.Addr(want))
	if err != nil {
		t.Fatal(err)
	}
	if !ci.PresharedKey.IsZero() {
		t.Fatal("address carries a pre-shared key")
	}
	if nodeIDOf(ci.ServerPublic.NodePublic) != id.NodeID() {
		t.Fatal("address does not carry the identity's public key")
	}
	if len(ci.Region) != 1 || len(ci.Region[0].Nodes) != 1 || ci.Region[0].Nodes[0].DERPPort != d.Region.Nodes[0].DERPPort {
		t.Fatalf("address regions %+v", ci.Region)
	}
}

// The identity key is the WireGuard key (ID-1): tailcat derives the
// same public key from it as identity does.
func TestTunnelKeyIsIdentityKey(t *testing.T) {
	id := newID(t)
	k := nodePrivate(id)
	if nodeIDOf(k.Public()) != id.NodeID() {
		t.Fatal("tailcat public key differs from the identity's")
	}
}

func dbAdmission(t *testing.T) (*Admission, *state.DB, *atomic.Int64) {
	t.Helper()
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	var lookups atomic.Int64
	adm := NewAdmission(func(ctx context.Context, peer identity.NodeID) (bool, error) {
		lookups.Add(1)
		r, err := db.PeerRole(ctx, peer.String(), time.Now())
		return r != state.RoleNone, err
	}, quiet)
	return adm, db, &lookups
}

// SEC-10 AC, first state: on a node with zero peers, an unknown identity
// holding only the address is refused before any application data: the
// hook is asked, the stranger never becomes a peer and the tunnel port
// accepts nothing.
func TestUnknownIdentityRefusedOnNodeWithZeroPeers(t *testing.T) {
	d := transporttest.Start(t)
	adm, _, lookups := dbAdmission(t)
	srv := newTailcat(t, newID(t), d)
	srv.Admit(adm.Admit)
	l := listen(t, srv)

	stranger := &tailcat.Client{Server: tailcat.Addr(srv.Address()), Key: nodePrivate(newID(t)), Logf: discard}
	t.Cleanup(func() { stranger.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if c, err := stranger.DialTCPPort(ctx, Port); err == nil {
		c.Write([]byte("should never arrive\n"))
		c.Close()
		t.Fatal("stranger's dial succeeded")
	}
	if lookups.Load() == 0 {
		t.Fatal("admission was never consulted")
	}
	// The client retries about once a second; the negative cache keeps
	// the database out of it.
	if n := lookups.Load(); n != 1 {
		t.Errorf("database looked up %d times; want 1 (negative cache)", n)
	}
	if _, ok := srv.server.Status().Peer[stranger.PublicKey()]; ok {
		t.Fatal("stranger became a WireGuard peer")
	}
	if n := l.accepted.Load(); n != 0 {
		t.Fatalf("tunnel port accepted %d connections; want 0", n)
	}
}

// A friend is admitted, its connection names its key (SEC-12), the
// dialler sees a path, and Disconnect drops it.
func TestFriendAdmittedAndNamed(t *testing.T) {
	d := transporttest.Start(t)
	adm, db, _ := dbAdmission(t)
	srvID, cliID := newID(t), newID(t)
	if _, err := db.Exec(`INSERT INTO friendship (peer_key, created_at) VALUES (?, 0)`, cliID.NodeID().String()); err != nil {
		t.Fatal(err)
	}
	srv := newTailcat(t, srvID, d)
	srv.Admit(adm.Admit)
	l := listen(t, srv)
	cli := newTailcat(t, cliID, d)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// The address must name the key being dialled.
	if _, err := cli.Dial(ctx, cliID.NodeID(), srv.Address()); err == nil {
		t.Fatal("Dial accepted an address for another key")
	}
	cc, err := cli.Dial(ctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	var sc net.Conn
	select {
	case sc = <-l.conns:
	case <-ctx.Done():
		t.Fatal("no connection accepted")
	}
	defer sc.Close()
	got, err := srv.PeerOf(sc)
	if err != nil || got != cliID.NodeID() {
		t.Fatalf("PeerOf = %v, %v; want %v", got, err, cliID.NodeID())
	}
	io.WriteString(cc, "ping\n")
	sc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if line, err := bufio.NewReader(sc).ReadString('\n'); err != nil || line != "ping\n" {
		t.Fatalf("server read %q, %v", line, err)
	}

	p, err := cli.PathInfo(ctx, srvID.NodeID())
	if err != nil {
		t.Fatal(err)
	}
	if !p.Direct && p.Relay == "" {
		t.Fatalf("path names neither an endpoint nor a relay: %+v", p)
	}
	t.Logf("path %+v", p)
	if _, err := cli.PathInfo(ctx, newID(t).NodeID()); err == nil {
		t.Fatal("PathInfo of a peer never dialled succeeded")
	}

	if _, err := srv.PeerOf(&net.TCPConn{}); err == nil {
		t.Fatal("PeerOf a connection that is not the tunnel's succeeded")
	}
	if !srv.Disconnect(cliID.NodeID()) {
		t.Fatal("Disconnect: friend was not connected")
	}
}

// SEC-10: a lookup error or a closed database rejects; only negative
// answers are cached, and Clear forgets them (SEC-15).
func TestAdmissionFailsClosed(t *testing.T) {
	k := newID(t).NodeID()
	var answer bool
	var fail error
	var calls int
	adm := NewAdmission(func(context.Context, identity.NodeID) (bool, error) {
		calls++
		return answer, fail
	}, quiet)

	fail = errors.New("disk on fire")
	answer = true
	if adm.Admit(k) {
		t.Fatal("admitted on a lookup error")
	}
	if adm.Admit(k); calls != 2 {
		t.Fatalf("an error was cached: %d lookups", calls)
	}

	fail, answer = nil, false
	adm.Admit(k)
	adm.Admit(k)
	if calls != 3 {
		t.Fatalf("rejection not cached: %d lookups", calls)
	}
	answer = true
	if adm.Admit(k) {
		t.Fatal("cache forgot the rejection by itself")
	}
	adm.Clear()
	if !adm.Admit(k) || calls != 4 {
		t.Fatalf("after Clear: calls %d", calls)
	}
	adm.Admit(k)
	if calls != 5 {
		t.Fatal("an admission was cached")
	}

	_, db, _ := dbAdmission(t)
	closed := NewAdmission(func(ctx context.Context, p identity.NodeID) (bool, error) {
		r, err := db.PeerRole(ctx, p.String(), time.Now())
		return r != state.RoleNone, err
	}, quiet)
	db.Exec(`INSERT INTO friendship (peer_key, created_at) VALUES (?, 0)`, k.String())
	if !closed.Admit(k) {
		t.Fatal("friend refused")
	}
	db.Close()
	if closed.Admit(k) {
		t.Fatal("closed database admitted a friend")
	}
}

func TestEnsureRegionSavesOnceAndFollowsSetting(t *testing.T) {
	d := transporttest.Start(t)
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	src := &transporttest.Source{D: d}
	ctx := t.Context()

	if r, err := SavedRegion(ctx, db); err != nil || r != nil {
		t.Fatalf("SavedRegion on a new database = %v, %v", r, err)
	}
	r, chosen, err := EnsureRegion(ctx, db, "", src)
	if err != nil || !chosen || r.RegionID != transporttest.RegionID {
		t.Fatalf("EnsureRegion = %v, %v, %v", r, chosen, err)
	}
	// The saved copy wins from then on, even when the relay's map changes.
	port := r.Nodes[0].DERPPort
	d.Region.Nodes[0].DERPPort++
	defer func() { d.Region.Nodes[0].DERPPort-- }()
	for _, want := range []string{"", "900", "LOCAL"} {
		r2, chosen, err := EnsureRegion(ctx, db, want, src)
		if err != nil || chosen || r2.Nodes[0].DERPPort != port {
			t.Fatalf("EnsureRegion(%q) again = %+v, %v, %v", want, r2, chosen, err)
		}
	}
	if n := src.Fetches.Load(); n != 1 {
		t.Fatalf("DERP map fetched %d times; want 1", n)
	}
	if saved, err := SavedRegion(ctx, db); err != nil || saved.Nodes[0].DERPPort != port {
		t.Fatalf("SavedRegion = %+v, %v", saved, err)
	}
	if _, _, err := EnsureRegion(ctx, db, "nowhere", src); err == nil {
		t.Fatal("EnsureRegion accepted a region the map does not have")
	}
}
