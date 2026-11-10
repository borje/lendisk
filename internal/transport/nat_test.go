package transport

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"tailscale.com/envknob"

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
		dl.SetEndpoints(func(identity.NodeID) []netip.AddrPort {
			return []netip.AddrPort{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}
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
// borrower is given it: the path is direct.
func TestExplicitAddressIsDialledDirectly(t *testing.T) {
	var port uint16
	p := newNATPair(t, true, true, func(srv, dl *Tailcat) {
		port = freeUDPPort(t)
		srv.SetPort(port)
		dl.SetEndpoints(func(peer identity.NodeID) []netip.AddrPort {
			return []netip.AddrPort{netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)}
		})
	})
	c, s := p.dial(t)
	pi := p.path(t, 20*time.Second)
	if !pi.Direct || pi.Endpoint != netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port).String() {
		t.Fatalf("want the direct path to the explicit address 127.0.0.1:%d, got %+v", port, pi)
	}
	echo(t, c, s, "explicit")
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
