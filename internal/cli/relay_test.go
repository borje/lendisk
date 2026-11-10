package cli

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tailscale/tailcat"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// runRelay runs `lendisk relay` for n in this process until the test
// ends and returns the relays.list entry it reports.
func runRelay(t *testing.T, n *node) string {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stun := pc.LocalAddr().(*net.UDPAddr).Port
	pc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	entries := make(chan string, 1)
	var out, errOut syncBuffer
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx,
		serving: func(e string) { entries <- e }}
	done := make(chan int, 1)
	go func() {
		done <- a.run([]string{"relay", "--config", n.config, "--data-dir", n.data, "--relay-hostname", "127.0.0.1",
			"--relay-listen", "127.0.0.1:0", "--relay-stun-port", strconv.Itoa(stun)})
	}()
	t.Cleanup(func() {
		cancel()
		if code := <-done; code != 0 {
			t.Errorf("relay exit %d: %s%s", code, out.String(), errOut.String())
		}
	})
	select {
	case e := <-entries:
		if !strings.Contains(out.String(), "relays.list") {
			t.Errorf("the relay did not log its entry:\n%s", out.String())
		}
		return e
	case code := <-done:
		t.Fatalf("relay exited %d: %s%s", code, out.String(), errOut.String())
	case <-time.After(30 * time.Second):
		t.Fatal("relay did not start")
	}
	return ""
}

// transportRelayOf names the DERP servers an address embeds.
func transportRelayOf(t *testing.T, addr string) string {
	t.Helper()
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		t.Fatal(err)
	}
	var hosts []string
	for _, r := range ci.Region {
		for _, n := range r.Nodes {
			hosts = append(hosts, net.JoinHostPort(n.HostName, strconv.Itoa(n.DERPPort)))
		}
	}
	return strings.Join(hosts, " ")
}

var reEntry = regexp.MustCompile(`^host=127\.0\.0\.1,port=\d+,stun_port=\d+,cert=sha256-raw:[0-9a-f]{64}$`)

// NET-5 (#42): `lendisk relay` serves a self-hosted relay, needing no
// identity, and reports the relays.list entry nodes use it by; its
// certificate key is private. A relay without a host name refuses to
// start, as a usage error.
func TestRelayCommandReportsItsEntry(t *testing.T) {
	parallel(t, 1)
	n := newNode(t)
	e := runRelay(t, n)
	if !reEntry.MatchString(e) {
		t.Fatalf("entry %q", e)
	}
	if fi, err := os.Stat(filepath.Join(n.data, "relay", "relay.key")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("relay key: %v %v", fi, err)
	}
	if fi, err := os.Stat(filepath.Join(n.data, "relay")); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("relay dir: %v %v", fi, err)
	}
	if code, _, e := n.run("", "", "relay", "--relay-listen", "127.0.0.1:0"); code != 2 || !strings.Contains(e, "relay.hostname") {
		t.Fatalf("relay without a host name: %d %s", code, e)
	}
}

// NET-5 AC, with a single self-hosted relay (#42): two nodes whose only
// relay is a `lendisk relay`, listed by its entry with the hosted
// relays off, choose it for their addresses and become friends through
// it. relays.list replaces the project's map, and with it the hosted
// relays are left out unless asked for (#42 review M1).
func TestFriendsThroughASelfHostedRelayOnly(t *testing.T) {
	serving(t, 1)
	entry := runRelay(t, newNode(t))
	only := []string{"--relays-list", entry}
	an, bn := newNode(t), newNode(t)
	for _, n := range []*node{an, bn} {
		n.regions = nil // the relay set the flags name, not a test source
		n.flags = only
		p, err := identity.NewPhrase()
		if err != nil {
			t.Fatal(err)
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	port := strings.Split(strings.Split(entry, "port=")[1], ",")[0]
	for _, n := range []*node{an, bn} {
		addr := n.status().Node.Address
		if !strings.Contains(transportRelayOf(t, addr), "127.0.0.1:"+port) {
			t.Fatalf("the address names %s, not the self-hosted relay at port %s", transportRelayOf(t, addr), port)
		}
	}
	stop := an.serveLogged(only...)
	defer stop()
	if code, out, e := bn.run("\n", "", "peer", "add", an.invite("--quota", "10G")); code != 0 || !strings.Contains(out, "Now friends") {
		t.Fatalf("peer add through the self-hosted relay: %d %s%s", code, out, e)
	}
}

// NET-5a (#42): a node whose relay set is its own relay alone refuses an
// invite whose address names another relay, saying how to allow it, and
// accepts it with relays.accept_peer_hints = true.
func TestPeerAddRefusesAPeerHintRelayUnlessAccepted(t *testing.T) {
	serving(t, 0)
	an, bn := friendNode(t), newNode(t)
	bn.regions = &transporttest.Source{D: transporttest.Start(t), Alone: true}
	bn.init()
	stop := an.serveLogged()
	defer stop()
	inv := an.invite("--quota", "10G")
	code, out, e := bn.run("\n", "", "peer", "add", inv)
	if code != 1 || !strings.Contains(e, "relays.accept_peer_hints") || !strings.Contains(e, "not one of this node's relays") {
		t.Fatalf("peer add through a relay outside the set: %d %s%s", code, out, e)
	}
	if len(bn.peers()) != 0 {
		t.Fatal("a refused peer add stored a friendship")
	}
	if code, out, e := bn.run("\n", "", "peer", "add", inv, "--relays-accept-peer-hints", "true"); code != 0 {
		t.Fatalf("peer add with peer hints accepted: %d %s%s", code, out, e)
	}
}

// NET-3 (#42): `peer address` gives a friend explicit addresses, shown
// by peer list and replaced or cleared by the next call; it refuses
// what is not a dialable ip:port, and a node that is no friend.
func TestPeerAddressSetsExplicitAddresses(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	other := friendNode(t).identity().NodeID().String()
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(t.Context(), state.Friendship{Peer: other, Address: "tc" + other, OwnNumber: 1 << 30,
		PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	code, out, e := n.run("", "", "peer", "address", other, "203.0.113.5:41641", "[2001:db8::5]:41641")
	if code != 0 || !strings.Contains(out, "dialled directly at 203.0.113.5:41641, [2001:db8::5]:41641") {
		t.Fatalf("peer address: %d %s%s", code, out, e)
	}
	if p := n.peerOf(other); strings.Join(p.ExplicitAddresses, " ") != "203.0.113.5:41641 [2001:db8::5]:41641" {
		t.Fatalf("peer list: %+v", p.ExplicitAddresses)
	}
	if code, _, e := n.run("", "", "peer", "address", other, "198.51.100.7:9"); code != 0 {
		t.Fatal(e)
	}
	if p := n.peerOf(other); strings.Join(p.ExplicitAddresses, " ") != "198.51.100.7:9" {
		t.Fatalf("not replaced: %+v", p.ExplicitAddresses)
	}
	for _, bad := range [][]string{
		{other},
		{other, "relay.example:41641"},
		{other, "203.0.113.5"},
		{other, "0.0.0.0:41641"},
		{other, "203.0.113.5:0"},
		{other, "--clear", "203.0.113.5:41641"},
		{"not-a-node", "203.0.113.5:41641"},
	} {
		if code, _, _ := n.run("", "", append([]string{"peer", "address"}, bad...)...); code != 2 {
			t.Errorf("peer address %v: exit %d, want 2", bad, code)
		}
	}
	stranger := friendNode(t).identity().NodeID().String()
	if code, _, e := n.run("", "", "peer", "address", stranger, "203.0.113.5:41641"); code != 1 || !strings.Contains(e, "not a friend") {
		t.Fatalf("a stranger: %d %s", code, e)
	}
	if code, _, e := n.run("", "", "peer", "address", other, "--clear"); code != 0 {
		t.Fatal(e)
	}
	if p := n.peerOf(other); len(p.ExplicitAddresses) != 0 {
		t.Fatalf("not cleared: %+v", p.ExplicitAddresses)
	}
}

// NET-4 (#42): with both daemons running, each friend's path, from the
// daemon's disco ping, shows in status and peer list as direct or
// relayed, with when it was checked; both nodes are on 127.0.0.1, so
// it is direct.
func TestStatusAndPeerListShowThePath(t *testing.T) {
	parallel(t, 2)
	an, bn := friendNode(t), friendNode(t)
	a, b := newProcNode(t, an), newProcNode(t, bn)
	a.start()
	b.start()
	if code, _, e := bn.run("\n", "", "peer", "add", an.invite("--quota", "10G")); code != 0 {
		t.Fatalf("peer add: %s", e)
	}
	aKey, bKey := an.identity().NodeID().String(), bn.identity().NodeID().String()
	for _, side := range []struct {
		n    *node
		peer string
	}{{an, bKey}, {bn, aKey}} {
		within(t, 90*time.Second, "the path to "+side.peer, func() bool {
			p := side.n.peerOf(side.peer)
			return p.PathCheckedAt != nil && p.Reachability == "direct"
		})
		if p := side.n.peerOf(side.peer); p.PathEndpoint == "" || p.PathRelay != "" {
			t.Errorf("a direct path names its endpoint and no relay: %+v", p)
		}
		var place StatusPlace
		for _, pl := range side.n.status().Places {
			if pl.Place == side.peer {
				place = pl
			}
		}
		if place.Path != "direct" || place.PathEndpoint == "" || place.PathCheckedAt == "" {
			t.Errorf("status place: %+v", place)
		}
		_, text, _ := side.n.run("", "", "status")
		if !strings.Contains(text, side.peer+" (friend): ") || !strings.Contains(text, ", direct") {
			t.Errorf("status text shows no path:\n%s", text)
		}
		_, list, _ := side.n.run("", "", "peer", "list")
		if !strings.Contains(list, "direct") {
			t.Errorf("peer list text shows no path:\n%s", list)
		}
	}
}
