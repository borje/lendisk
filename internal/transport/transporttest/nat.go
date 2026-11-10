package transporttest

import (
	"net"
	"net/netip"
	"testing"

	"tailscale.com/net/stun"
	"tailscale.com/tailcfg"
)

// Symmetric is a STUN server that answers as the far side of a
// symmetric NAT would let a node behind it learn: every binding
// request gets a mapped address of its own, 127.0.0.1 and a port that
// was free a moment ago and that nothing listens on. A node that learns
// its endpoints from it (and, with TS_DEBUG_OMIT_LOCAL_ADDRS, from
// nothing else) advertises only addresses that do not reach it, as a
// node behind a symmetric NAT advertises a mapping made for the STUN
// server, which its NAT keeps for that destination alone. The node's
// real socket is still reached by a peer that the node itself sent to
// first, which is what the NAT's mapping for that peer does.
//
// What it does not simulate: filtering of unsolicited packets (a peer
// that learnt the node's real port some other way would get through),
// mapping timeouts, hairpinning and port preservation. The tests that
// use it never hand a peer the real port except where a test means to
// (an explicit address).
type Symmetric struct {
	Port int // the STUN server's UDP port on 127.0.0.1
}

// StartSymmetric starts a symmetric-NAT STUN server, stopped when the
// test ends.
func StartSymmetric(t testing.TB) *Symmetric {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			tx, err := stun.ParseBindingRequest(buf[:n])
			if err != nil {
				continue
			}
			pc.WriteTo(stun.Response(tx, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), freePort())), from)
		}
	}()
	return &Symmetric{Port: pc.LocalAddr().(*net.UDPAddr).Port}
}

// freePort is a UDP port nothing listens on, just now.
func freePort() uint16 {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return 9 // discard; nothing answers there either
	}
	defer pc.Close()
	return uint16(pc.LocalAddr().(*net.UDPAddr).Port)
}

// Behind is r with every node's STUN port moved to s: a node whose
// region it is learns its endpoints as behind a symmetric NAT. The
// DERP servers, and so the relay, are r's.
func (s *Symmetric) Behind(r *tailcfg.DERPRegion) *tailcfg.DERPRegion {
	c := *r
	c.Nodes = nil
	for _, n := range r.Nodes {
		m := *n
		m.STUNPort = s.Port
		c.Nodes = append(c.Nodes, &m)
	}
	return &c
}
