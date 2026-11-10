package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// servedMaps serves DERP maps by URL to a RelaySet, counting fetches.
type servedMaps map[string]*tailcfg.DERPMap

func (m servedMaps) set(s *RelaySet) *RelaySet {
	s.fetch = func(_ context.Context, url string) (*tailcfg.DERPMap, error) {
		if dm, ok := m[url]; ok {
			return dm, nil
		}
		return nil, errors.New("unreachable")
	}
	return s
}

func regionIDs(m *tailcfg.DERPMap) []int {
	var ids []int
	for id := range m.Regions {
		ids = append(ids, int(id))
	}
	slices.Sort(ids)
	return ids
}

func oneRegion(id tailcfg.DERPRegionID, host string) *tailcfg.DERPRegion {
	return &tailcfg.DERPRegion{RegionID: id, RegionCode: host, Nodes: []*tailcfg.DERPNode{{Name: host, RegionID: id, HostName: host}}}
}

// NET-5 (#42): relays.map_url and relays.list each replace the project's
// map, together their union; the hosted relays come in only with
// relays.tailscale_bootstrap, and never over an ID of the node's own.
func TestRelaySetReplacesTheProjectMap(t *testing.T) {
	served := servedMaps{
		"https://relays.example/map.json": {Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
			901: oneRegion(901, "a.relays.example"), 902: oneRegion(902, "b.relays.example")}},
		bootstrapURL: {Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{
			1: oneRegion(1, "derp1.hosted.example"), 902: oneRegion(902, "clash.hosted.example")}},
	}
	project, err := ProjectMap()
	if err != nil {
		t.Fatal(err)
	}
	list := []RelayServer{{Host: "vps.example", Port: 8443, Cert: "sha256-raw:00ff"}, {Host: "192.0.2.7"}}
	for _, tc := range []struct {
		name string
		set  RelaySet
		want []int
	}{
		{"default: the project's map", RelaySet{}, regionIDs(project)},
		{"map_url", RelaySet{MapURL: "https://relays.example/map.json"}, []int{901, 902}},
		{"list", RelaySet{List: list}, []int{900, 901}},
		{"both", RelaySet{MapURL: "https://relays.example/map.json", List: list}, []int{901, 902, 903, 904}},
		{"bootstrap beside the list", RelaySet{List: list, Bootstrap: true}, []int{1, 900, 901, 902}},
		{"bootstrap never over an own ID", RelaySet{MapURL: "https://relays.example/map.json", Bootstrap: true}, []int{1, 901, 902}},
	} {
		m, err := served.set(&tc.set).DERPMap(t.Context())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := regionIDs(m); !slices.Equal(got, tc.want) {
			t.Errorf("%s: regions %v, want %v", tc.name, got, tc.want)
		}
		if tc.name == "bootstrap never over an own ID" && m.Regions[902].Nodes[0].HostName != "b.relays.example" {
			t.Errorf("a hosted region replaced the node's own region 902: %+v", m.Regions[902].Nodes[0])
		}
	}

	m, _ := served.set(&RelaySet{List: list}).DERPMap(t.Context())
	n := m.Regions[900].Nodes[0]
	if m.Regions[900].RegionCode != "vps.example" || n.HostName != "vps.example" || n.DERPPort != 8443 || n.CertName != "sha256-raw:00ff" {
		t.Errorf("a list entry's region: %+v, node %+v", m.Regions[900], n)
	}
	if ip := m.Regions[901].Nodes[0]; ip.IPv4 != "192.0.2.7" || ip.IPv6 != "none" {
		t.Errorf("an IP entry gives its IP and no other family: %+v", ip)
	}
	if _, err := served.set(&RelaySet{MapURL: "https://down.example/map.json", Bootstrap: true}).DERPMap(t.Context()); err == nil {
		t.Error("an unreachable relays.map_url was taken for an empty set")
	}
}

// NET-5 (#42): the address's region is picked from the node's own
// relays; a hosted region is picked only when none of them answers.
func TestPickPrefersOwnRelaysOverTheBootstrap(t *testing.T) {
	own, hosted := transporttest.Start(t), transporttest.Start(t)
	hostedRegion := *hosted.Region
	hostedNode := *hosted.Region.Nodes[0]
	hostedRegion.RegionID, hostedNode.RegionID, hostedNode.Name = 1, 1, "1a"
	hostedRegion.Nodes = []*tailcfg.DERPNode{&hostedNode}
	served := servedMaps{bootstrapURL: {Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{1: &hostedRegion}}}
	list := []RelayServer{{Host: "127.0.0.1", Port: own.Region.Nodes[0].DERPPort, STUNPort: own.Region.Nodes[0].STUNPort}}
	s := served.set(&RelaySet{List: list, Bootstrap: true})
	m, err := s.DERPMap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The list entry's node needs the test relay's self-signed
	// certificate accepted, as a pinned one would be.
	m.Regions[900].Nodes[0].InsecureForTests = true
	if id, err := s.Pick(t.Context(), m); err != nil || id != 900 {
		t.Fatalf("picked region %d (%v), want the node's own 900", id, err)
	}
	own.Close()
	if id, err := s.Pick(t.Context(), m); err != nil || id != 1 {
		t.Fatalf("with the own relay down, picked region %d (%v), want the hosted 1", id, err)
	}
	if _, err := served.set(&RelaySet{List: list}).Pick(t.Context(), &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{900: m.Regions[900]}}); err == nil {
		t.Fatal("without the bootstrap, a down relay set picked a region")
	}
}

// NET-5a (#42): a friend whose address names a relay outside the node's
// relay set is not dialled, and no client is built for it, unless
// relays.accept_peer_hints allows it; a friend on one of the node's
// relays is dialled either way.
func TestPeerHintRelayRefusedUnlessAccepted(t *testing.T) {
	mine, theirs := transporttest.Start(t), transporttest.Start(t)
	serve := func(d *transporttest.LocalDERP) (identity.NodeID, *Tailcat, *listener) {
		id := newID(t)
		srv := newTailcat(t, id, d)
		srv.Admit(func(identity.NodeID) bool { return true })
		return id.NodeID(), srv, listen(t, srv)
	}
	onMine, srvMine, lMine := serve(mine)
	onTheirs, srvTheirs, lTheirs := serve(theirs)
	set := &transporttest.Source{D: mine, Alone: true}

	dl, err := NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.SetRelays(set, false)
	dialed(t, dl, srvMine, onMine, lMine)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = dl.Dial(ctx, onTheirs, srvTheirs.Address())
	if !errors.Is(err, ErrRelayNotAllowed) {
		t.Fatalf("dialling a friend on a relay outside the set: %v, want ErrRelayNotAllowed", err)
	}
	if dl.client(onTheirs) != nil {
		t.Fatal("a client was built for a relay outside the set")
	}
	t.Logf("refused: %v", err)

	dl.SetRelays(set, true)
	dialed(t, dl, srvTheirs, onTheirs, lTheirs)
}

// #60 review: Listen holds no lock of the transport while tailcat
// starts, listens or, on an error, closes: the admission hook and
// Close take that lock. A failed Listen leaves the transport able to
// listen again.
func TestListenHoldsNoLockAcrossTailcat(t *testing.T) {
	d := transporttest.Start(t)
	srv := newTailcat(t, newID(t), d)
	srv.Admit(func(identity.NodeID) bool { return true })
	var held, closes atomic.Int64
	free := func() {
		if srv.mu.TryLock() {
			srv.mu.Unlock()
		} else {
			held.Add(1)
		}
	}
	start := srv.listenTailcat
	srv.listenTailcat = func(ctx context.Context, s *tailcat.Server) (net.Listener, error) {
		free()
		s.UDPIdleTimeout = -1 // tailcat refuses to start
		return start(ctx, s)
	}
	srv.closeTailcat = func(c io.Closer) error {
		free()
		closes.Add(1)
		return c.Close()
	}
	if _, err := srv.Listen(t.Context()); err == nil {
		t.Fatal("Listen succeeded with a server tailcat refuses")
	}
	if closes.Load() == 0 {
		t.Fatal("the failed server was not closed")
	}
	if held.Load() > 0 {
		t.Fatalf("Listen held the transport's lock across %d tailcat calls", held.Load())
	}
	srv.listenTailcat = start
	if _, err := srv.Listen(t.Context()); err != nil {
		t.Fatalf("Listen after a failed one: %v", err)
	}
}
