package transport

import (
	"context"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// addresses lists the tunnel addresses on a started client's stack.
func addresses(t *testing.T, c *tailcatClient) []netip.Addr {
	t.Helper()
	st, _, err := clientStack(c.Client)
	if err != nil || st == nil {
		t.Fatalf("no stack for the client: %v", err)
	}
	var as []netip.Addr
	for _, addrs := range st.AllAddresses() {
		for _, pa := range addrs {
			if a, ok := netip.AddrFromSlice(pa.AddressWithPrefix.Address.AsSlice()); ok && tunnelULA.Contains(a) {
				as = append(as, a)
			}
		}
	}
	return as
}

// strayAddress puts addr on a started client's stack as tailscale's
// netstack does for a stray inbound segment addressed to it
// (wrapTCPProtocolHandler, addSubnetAddress).
func strayAddress(t *testing.T, c *tailcatClient, addr netip.Addr) {
	t.Helper()
	st, _, err := clientStack(c.Client)
	if err != nil || st == nil {
		t.Fatalf("no stack for the client: %v", err)
	}
	pa := tcpip.ProtocolAddress{
		Protocol:          ipv6.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix(),
	}
	if err := st.AddProtocolAddress(1, pa, stack.AddressProperties{}); err != nil {
		t.Fatalf("adding %v: %s", addr, err)
	}
}

// NET-1, NET-8 (#111): a stray segment addressed to the friend's
// tunnel address leaves that address on the borrower's client stack,
// and every new dial's SYN then leaves from the friend's own address
// and is dropped by the friend's WireGuard, while the open connections
// carry on. The transport removes the stray address before each dial:
// the next dial reaches the friend through the same client, with no
// wait for its connections to go quiet, and the transfer in flight
// keeps moving.
func TestADialAfterAStrayFriendAddressReachesTheFriend(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(identity.NodeID) bool { return true })
	l := listen(t, srv)
	var logs syncBuffer
	dl, err := NewDialer(me, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.quietFor = time.Hour // no replacement can be what cures it
	peer := srvID.NodeID()
	a, sa := dialed(t, dl, srv, peer, l)
	shared := dl.client(peer)
	echo(t, a, sa, "before")
	friend := netip.MustParseAddrPort(a.RemoteAddr().String()).Addr()
	if slices.Contains(addresses(t, shared), friend) {
		t.Fatal("the client holds the friend's address before any stray segment")
	}

	strayAddress(t, shared, friend)
	// The stuck state: a dial through the client, bypassing the
	// transport, finds no SYN-ACK while the open connection carries on.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	if c, err := shared.DialTCPPort(ctx, Port); err == nil {
		c.Close()
		t.Fatal("a dial from the friend's own address reached the friend: the stray address does not reproduce #111")
	}
	cancel()
	echo(t, a, sa, "stuck")

	start := time.Now()
	b, sb := dialed(t, dl, srv, peer, l)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the dial after the stray address took %v", took)
	}
	if dl.client(peer) != shared {
		t.Fatal("the client was replaced; its transfers were cut")
	}
	if slices.Contains(addresses(t, shared), friend) {
		t.Fatal("the stray address is still on the client")
	}
	echo(t, b, sb, "b")
	echo(t, a, sa, "a")
	if !strings.Contains(logs.String(), "removed a stray address") || !strings.Contains(logs.String(), friend.String()) {
		t.Fatalf("the removal was not logged:\n%s", logs.String())
	}
}

// NET-1, NET-8 (#111): a friend that restarts while the borrower's
// client holds its stray address is reached again: the address is
// removed before the transport's checks too, the checks then fail on
// the session the friend forgot, and the client is replaced as for any
// restart (#18, #52), whatever dials through it (replication, the
// reachability probe, the quota exchange, the manifest push).
func TestARestartedFriendIsReachedDespiteAStrayAddress(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	serve := func() (*Tailcat, *listener) {
		srv := newTailcat(t, srvID, d)
		srv.Admit(func(identity.NodeID) bool { return true })
		return srv, listen(t, srv)
	}
	srv, l := serve()
	dl, err := NewDialer(me, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.probeTimeout = 2 * time.Second
	peer := srvID.NodeID()
	a, _ := dialed(t, dl, srv, peer, l)
	friend := netip.MustParseAddrPort(a.RemoteAddr().String()).Addr()
	a.Close()
	strayAddress(t, dl.client(peer), friend)
	srv.Close()
	srv, l = serve()
	start := time.Now()
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		c, err := dl.Dial(ctx, peer, srv.Address())
		cancel()
		if err == nil {
			c.Close()
			break
		}
		if time.Since(start) > 30*time.Second {
			t.Fatal("the restarted friend is never reached")
		}
	}
	select {
	case c := <-l.conns:
		c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("no connection accepted after the restart")
	}
}
