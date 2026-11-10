package transport

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"

	"github.com/borje/lendisk/internal/identity"
)

var mineCert = "sha256-raw:" + strings.Repeat("aa", 32)

// hintRefused is a relay policy over a set holding one self-hosted
// relay, peer hints refused, and the names its lookup was asked for.
func hintRefused(host string) (*relays, *[]string) {
	asked := &[]string{}
	p := &relays{now: time.Now, set: &RelaySet{List: []RelayServer{{Host: host, Cert: mineCert}}},
		lookup: func(_ context.Context, h string) ([]netip.Addr, error) {
			*asked = append(*asked, h)
			return []netip.Addr{netip.MustParseAddr("192.0.2.10")}, nil
		}}
	return p, asked
}

// dialNodes are the relay nodes of the address relayFor gives to dial
// for a friend whose address has nodes.
func dialNodes(t *testing.T, p *relays, id *identity.Identity, nodes ...*tailcfg.DERPNode) ([]*tailcfg.DERPNode, error) {
	t.Helper()
	addr := AddressOf(id, &tailcfg.DERPRegion{RegionID: 1, Nodes: nodes})
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		t.Fatal(err)
	}
	_, dial, err := p.relayFor(t.Context(), addr, ci)
	if err != nil {
		return nil, err
	}
	dci, err := tailcat.ParseAddr(tailcat.Addr(dial))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := KeyOf(dial); got != id.NodeID() {
		t.Fatalf("the address dialled names %s, not the friend %s", got, id.NodeID())
	}
	return dci.Region[0].Nodes, nil
}

// #42 review H1, NET-5a: with peer hints refused, a friend's address
// naming a host of the set with an IP and a certificate pin of its own
// is dialled at the set's relay as the set gives it: its host, no
// other IP, the set's pin. The address's IP and pin are never used.
func TestHintRefusedSetHostWithForeignIPAndPinDialsTheSet(t *testing.T) {
	p, _ := hintRefused("relay.mine.example")
	nodes, err := dialNodes(t, p, newID(t), &tailcfg.DERPNode{HostName: "relay.mine.example", IPv4: "203.0.113.66", IPv6: "none",
		CertName: "sha256-raw:" + strings.Repeat("bb", 32)})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].HostName != "relay.mine.example" || nodes[0].IPv4 != "" || nodes[0].CertName != mineCert {
		t.Fatalf("dialled %+v, want the set's node alone", nodes)
	}
}

// #42 review H1, NET-5a: with peer hints refused, a region mixing a
// foreign node, first, with a node of the set is dialled at the set's
// node alone: tailcat would take the first node that answers.
func TestHintRefusedMixedRegionDialsOnlyTheSetNode(t *testing.T) {
	p, _ := hintRefused("relay.mine.example")
	nodes, err := dialNodes(t, p, newID(t),
		&tailcfg.DERPNode{HostName: "evil.example"}, &tailcfg.DERPNode{HostName: "relay.mine.example"})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if n.HostName != "relay.mine.example" || n.CertName != mineCert {
			t.Fatalf("dialled a node outside the set: %+v", n)
		}
	}
}

// #42 review H1, L1, NET-5a: with peer hints refused, a friend's host
// name is never resolved, so one that resolves to a set relay's IP now
// (and anywhere at dial time: DNS rebinding) is refused, not dialled.
func TestHintRefusedNameResolvingToASetIPIsRefused(t *testing.T) {
	p, asked := hintRefused("192.0.2.10")
	if _, err := dialNodes(t, p, newID(t), &tailcfg.DERPNode{HostName: "evil.example"}); !errors.Is(err, ErrRelayNotAllowed) {
		t.Fatalf("a name resolving to a set IP: %v, want ErrRelayNotAllowed", err)
	}
	if len(*asked) > 0 {
		t.Fatalf("names were resolved with hints refused: %v", *asked)
	}
}
