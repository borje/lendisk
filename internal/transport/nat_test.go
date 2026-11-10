package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/envknob"
	"tailscale.com/tailcfg"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// natPair is a lender listening and a borrower dialling it, both on
// the relay d, each behind a simulated NAT (transporttest.Symmetric):
// symmetric where asked, else one that maps a node's socket to itself,
// as an endpoint-independent NAT does. Neither advertises a local
// address, so their only endpoints are the ones STUN gave them.
type natPair struct {
	d       *transporttest.LocalDERP
	srvID   *identity.Identity
	srv, dl *Tailcat
	l       *listener
	addr    string // the lender's address as the borrower holds it
}

func newNATPair(t *testing.T, lenderSymmetric, borrowerSymmetric bool, setup func(srv, dl *Tailcat)) *natPair {
	t.Helper()
	envknob.SetenvForTest(t, "TS_DEBUG_OMIT_LOCAL_ADDRS", "true")
	d := transporttest.Start(t)
	sym := transporttest.StartSymmetric(t)
	p := &natPair{d: d, srvID: newID(t)}
	region := d.Region
	if lenderSymmetric {
		region = sym.Behind(region)
	}
	srv, err := NewTailcat(p.srvID, region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	p.srv = srv
	dl, err := NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	p.dl = dl
	if setup != nil {
		setup(srv, dl)
	}
	srv.Admit(func(identity.NodeID) bool { return true })
	p.l = listen(t, srv)
	// The borrower's engine learns its endpoints from the STUN server
	// of the region in the lender's address; the relay is the same.
	p.addr = srv.Address()
	if borrowerSymmetric {
		p.addr = AddressOf(p.srvID, sym.Behind(region))
	}
	return p
}

// dial opens a connection to the lender and returns both ends.
func (p *natPair) dial(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := p.dl.Dial(ctx, p.srvID.NodeID(), p.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-p.l.conns:
		t.Cleanup(func() { s.Close() })
		buf := make([]byte, 2)
		s.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := s.Read(buf); err != nil {
			t.Fatal(err)
		}
		return c, s
	case <-time.After(20 * time.Second):
		t.Fatal("the lender accepted nothing")
	}
	return nil, nil
}

// path disco-pings the lender until the path is direct or the time is
// up, as the daemon's path keeper does, and returns the last answer.
func (p *natPair) path(t *testing.T, within time.Duration) Path {
	t.Helper()
	var last Path
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		pi, err := p.dl.PathInfo(ctx, p.srvID.NodeID())
		cancel()
		if err == nil {
			last = pi
			if pi.Direct {
				return pi
			}
		}
	}
	return last
}

// NET-2 (#42): a borrower behind a symmetric NAT reaches a lender behind
// an endpoint-independent one directly: the borrower's disco pings to
// the lender's STUN address open the lender's side, and the lender
// answers the borrower's real mapping. Once direct, the tunnel no
// longer needs the relay: with the relay gone, new connections still
// open and carry bytes.
func TestNATOneSymmetricGoesDirectAndOutlivesTheRelay(t *testing.T) {
	p := newNATPair(t, false, true, nil)
	c, s := p.dial(t)
	pi := p.path(t, 30*time.Second)
	if !pi.Direct {
		t.Fatalf("one side symmetric, the other not: the path stayed relayed (%+v)", pi)
	}
	t.Logf("direct to %s in %v", pi.Endpoint, pi.Latency)
	echo(t, c, s, "direct")

	p.d.Close()
	c2, s2 := p.dial(t)
	echo(t, c2, s2, "with the relay gone")
	echo(t, c, s, "the first connection too")
}

// NET-2 (#42): two nodes both behind symmetric NATs find no direct path;
// the relay carries them, the path says relayed and names it, and
// without the relay nothing gets through.
func TestNATBothSymmetricFallBackToTheRelay(t *testing.T) {
	p := newNATPair(t, true, true, nil)
	c, s := p.dial(t)
	pi := p.path(t, 10*time.Second)
	if pi.Direct || pi.Relay == "" {
		t.Fatalf("both sides symmetric: want a relayed path naming the relay, got %+v", pi)
	}
	echo(t, c, s, "relayed")

	p.d.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("x")); err == nil {
		buf := make([]byte, 1)
		s.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := s.Read(buf); err == nil {
			t.Fatal("bytes crossed two symmetric NATs with the relay gone")
		}
	}
}

// NET-2, NET-3 (#42): with no relay reachable at all, a tunnel cannot
// come up even to a lender behind no NAT: tailcat admits a client by a
// ping over the relay only (its meow), so a relay is needed once per
// client, and an explicit address does not replace it.
func TestNoReachableRelayNoTunnel(t *testing.T) {
	var port uint16
	p := newNATPair(t, false, false, func(srv, dl *Tailcat) {
		port = freeUDPPort(t)
		srv.SetPort(port)
		dl.SetEndpoints(func(identity.NodeID) []string {
			return []string{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port).String()}
		})
	})
	p.d.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	if c, err := p.dl.Dial(ctx, p.srvID.NodeID(), p.addr); err == nil {
		c.Close()
		t.Fatal("a tunnel came up with no relay reachable")
	}
}

// NET-3 (#42): an explicit address on the peer record is dialled
// directly. Both nodes are behind symmetric NATs, so hole punching
// finds no path (TestNATBothSymmetricFallBackToTheRelay); the lender's
// engine listens on a fixed port (tunnel.port, a port forward), and the
// borrower is given it: the path is direct. The address is a DNS name
// (#99), as a dynamic DNS name would be, looked up at the dial.
func TestExplicitAddressIsDialledDirectly(t *testing.T) {
	var port uint16
	p := newNATPair(t, true, true, func(srv, dl *Tailcat) {
		port = freeUDPPort(t)
		srv.SetPort(port)
		dl.relays.lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			if host == "lender.dyndns.example" {
				return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
			}
			return defaultLookup(ctx, host)
		}
		dl.SetEndpoints(func(peer identity.NodeID) []string {
			return []string{net.JoinHostPort("lender.dyndns.example", strconv.Itoa(int(port)))}
		})
	})
	c, s := p.dial(t)
	pi := p.path(t, 20*time.Second)
	if !pi.Direct || pi.Endpoint != netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port).String() {
		t.Fatalf("want the direct path to the explicit address 127.0.0.1:%d, got %+v", port, pi)
	}
	echo(t, c, s, "explicit")
}

// peerEndpoints are the server's endpoints in the network map c's
// backend holds now.
func peerEndpoints(t *testing.T, c *tailcatClient) []netip.AddrPort {
	t.Helper()
	b, err := backendOf(c.Client)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	nm := *b.nm
	if nm == nil || len(nm.Peers) != 1 {
		t.Fatalf("network map %+v", nm)
	}
	return nm.Peers[0].Endpoints().AsSlice()
}

// #99: the explicit addresses are part of the network map the client's
// backend holds, and when tailcat installs another map without them,
// the next dial gives them again; a dial with them in place leaves the
// map alone.
func TestExplicitAddressesSurviveANetworkMapChange(t *testing.T) {
	ep := netip.MustParseAddrPort("127.0.0.1:9")
	p := newNATPair(t, false, false, func(srv, dl *Tailcat) {
		dl.SetEndpoints(func(identity.NodeID) []string { return []string{ep.String()} })
	})
	p.dial(t)
	p.dl.mu.Lock()
	c := p.dl.clients[p.srvID.NodeID()]
	p.dl.mu.Unlock()
	if got := peerEndpoints(t, c); !slices.Contains(got, ep) {
		t.Fatalf("after the first dial the map's endpoints are %v", got)
	}
	b, err := backendOf(c.Client)
	if err != nil {
		t.Fatal(err)
	}
	mc, err := b.magicsock()
	if err != nil {
		t.Fatal(err)
	}
	// A map as tailcat builds one: the server with no endpoints.
	b.mu.Lock()
	fresh := **b.nm
	peer := fresh.Peers[0].AsStruct()
	peer.Endpoints = nil
	fresh.Peers = []tailcfg.NodeView{peer.View()}
	*b.nm = &fresh
	mc.SetNetworkMap(fresh.SelfNode, fresh.Peers)
	b.mu.Unlock()
	p.dial(t)
	if got := peerEndpoints(t, c); !slices.Contains(got, ep) {
		t.Fatalf("after a network map change the next dial left the endpoints %v", got)
	}
	b.mu.Lock()
	held := *b.nm
	b.mu.Unlock()
	p.dial(t)
	b.mu.Lock()
	again := *b.nm
	b.mu.Unlock()
	if again != held {
		t.Fatal("a dial with the addresses in place installed another map")
	}
}

// #99: a dial whose explicit addresses are in place does not wait for
// another dial to the same friend that is pinging its client before
// giving it new ones.
func TestDialDoesNotWaitForAnotherDialsPing(t *testing.T) {
	tc, err := NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	pinging, release := make(chan struct{}), make(chan struct{})
	tc.ping = func(ctx context.Context, _ *tailcatClient) error {
		close(pinging)
		<-release
		return errors.New("no answer")
	}
	old := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:41641")}
	c := &tailcatClient{Client: &tailcat.Client{}, t: tc, eps: old}
	go tc.direct(t.Context(), c, []netip.AddrPort{netip.MustParseAddrPort("192.0.2.2:41641")})
	<-pinging
	done := make(chan struct{})
	go func() { tc.direct(t.Context(), c, old); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a dial with its addresses in place waited for another dial's ping")
	}
	close(release)
}

// #99: an explicit address is ip:port or name:port; a name is looked
// up at each dial, every IP it gives is an endpoint, and one that does
// not resolve is left out.
func TestExplicitAddressNamesResolveAtEachDial(t *testing.T) {
	for in, want := range map[string]string{
		"203.0.113.5:41641":           "203.0.113.5:41641",
		"[::ffff:203.0.113.5]:41641":  "203.0.113.5:41641",
		" Home.DynDNS.Example.:41641": "home.dyndns.example:41641",
		"[2001:db8::5]:1":             "[2001:db8::5]:1",
	} {
		if got, err := ParseExplicitAddress(in); err != nil || got != want {
			t.Errorf("%q: %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"home.example", "home.example:0", "home.example:70000", "0.0.0.0:1", "[ff02::1]:1",
		"-home.example:1", "home_example:1", "1.2.3:41641", "", ":41641"} {
		if got, err := ParseExplicitAddress(bad); err == nil {
			t.Errorf("%q accepted as %q", bad, got)
		}
	}
	tc, err := NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	ip := "198.51.100.1"
	tc.relays.lookup = func(_ context.Context, host string) ([]netip.Addr, error) {
		if host == "home.dyndns.example" {
			return []netip.Addr{netip.MustParseAddr(ip), netip.MustParseAddr("2001:db8::1")}, nil
		}
		return nil, errors.New("no such host")
	}
	addrs := []string{"home.dyndns.example:41641", "gone.example:41641", "203.0.113.5:41641"}
	want := []netip.AddrPort{netip.MustParseAddrPort("198.51.100.1:41641"), netip.MustParseAddrPort("203.0.113.5:41641"),
		netip.MustParseAddrPort("[2001:db8::1]:41641")}
	if got := tc.endpointsOf(t.Context(), "friend", addrs); !slices.Equal(got, want) {
		t.Fatalf("endpoints %v, want %v", got, want)
	}
	ip = "198.51.100.2" // the dynamic address moved
	if got := tc.endpointsOf(t.Context(), "friend", addrs); got[0] != netip.MustParseAddrPort("198.51.100.2:41641") {
		t.Fatalf("a moved name was not looked up again: %v", got)
	}
}

func freeUDPPort(t *testing.T) uint16 {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	return uint16(pc.LocalAddr().(*net.UDPAddr).Port)
}
