// Package transporttest runs a DERP relay and STUN server on 127.0.0.1
// so tests of the peer transport never touch the real relays. It is for
// tests only; nothing outside _test.go files may import it.
package transporttest

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"tailscale.com/derp/derpserver"
	"tailscale.com/net/stunserver"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
)

// RegionID is the local relay's region ID, outside the public map's range.
const RegionID = 900

// LocalDERP is a DERP relay and STUN server on 127.0.0.1, the shape of
// tailscale.com/tstest/integration.RunDERPAndSTUN.
type LocalDERP struct {
	Region *tailcfg.DERPRegion
	close  func()
}

// Start starts a local relay and stops it when the test ends.
func Start(t testing.TB) *LocalDERP {
	return StartNamed(t, "")
}

// StartNamed starts a local relay whose node gives host as its host
// name and no IPv4 address, so it is reached by resolving host; ""
// gives 127.0.0.1 as both.
func StartNamed(t testing.TB, host string) *LocalDERP {
	t.Helper()
	d, err := start(logger.Discard, host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}

func start(logf logger.Logf, host string) (*LocalDERP, error) {
	const ip = "127.0.0.1"
	v4 := ip
	if host == "" {
		host = ip
	} else {
		v4 = ""
	}
	d := derpserver.New(key.NewNode(), logf)
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		return nil, err
	}
	hs := httptest.NewUnstartedServer(derpserver.Handler(d))
	hs.Listener.Close()
	hs.Listener = ln
	hs.Config.ErrorLog = logger.StdLogger(logf)
	hs.Config.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler))
	hs.StartTLS()

	ctx, cancel := context.WithCancel(context.Background())
	stun := stunserver.New(ctx)
	if err := stun.Listen(net.JoinHostPort(ip, "0")); err != nil {
		cancel()
		hs.Close()
		d.Close()
		return nil, err
	}
	go stun.Serve()

	reg := &tailcfg.DERPRegion{
		RegionID:   RegionID,
		RegionCode: "local",
		RegionName: "Local test relay",
		Nodes: []*tailcfg.DERPNode{{
			Name:             fmt.Sprintf("%da", RegionID),
			RegionID:         RegionID,
			HostName:         host,
			IPv4:             v4,
			IPv6:             "none",
			STUNPort:         stun.LocalAddr().(*net.UDPAddr).Port,
			DERPPort:         ln.Addr().(*net.TCPAddr).Port,
			InsecureForTests: true, // self-signed httptest certificate
			STUNTestIP:       ip,
		}},
	}
	var once sync.Once
	l := &LocalDERP{Region: reg}
	l.close = func() {
		once.Do(func() {
			live.Lock()
			delete(live.m, l)
			live.Unlock()
			cancel()
			hs.CloseClientConnections()
			hs.Close()
			d.Close()
		})
	}
	live.Lock()
	if live.m == nil {
		live.m = map[*LocalDERP]bool{}
	}
	live.m[l] = true
	live.Unlock()
	return l, nil
}

// live are the process's running local relays, which a Source offers
// beside its own as a project map lists every relay.
var live struct {
	sync.Mutex
	m map[*LocalDERP]bool
}

// Close stops the relay and its STUN server before the test ends, as a
// relay that goes away does; the test's end does nothing more.
func (l *LocalDERP) Close() { l.close() }

// Map is a DERP map holding only this relay's region.
func (l *LocalDERP) Map() *tailcfg.DERPMap {
	return &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{RegionID: l.Region}}
}

// Source is a transport.RegionSource that picks this relay without
// measuring anything. Its map is the node's relay set (NET-5): this
// relay as RegionID and, unless Alone, every other local relay running
// in the process, as regions 901 and up, as a project map lists every
// relay, so test nodes on relays of their own may dial each other
// (NET-5a). Fetches counts DERPMap calls.
type Source struct {
	D       *LocalDERP
	Alone   bool
	Fetches atomic.Int64
}

// DERPMap returns the local map.
func (s *Source) DERPMap(context.Context) (*tailcfg.DERPMap, error) {
	s.Fetches.Add(1)
	m := s.D.Map()
	if s.Alone {
		return m, nil
	}
	live.Lock()
	defer live.Unlock()
	id := tailcfg.DERPRegionID(RegionID)
	for l := range live.m {
		if l == s.D {
			continue
		}
		id++
		r := *l.Region
		r.RegionID, r.RegionCode = id, fmt.Sprintf("local-%d", id)
		r.Nodes = nil
		for _, n := range l.Region.Nodes {
			c := *n
			c.RegionID, c.Name = id, fmt.Sprintf("%da", id)
			r.Nodes = append(r.Nodes, &c)
		}
		m.Regions[id] = &r
	}
	return m, nil
}

// Pick returns the local region.
func (s *Source) Pick(context.Context, *tailcfg.DERPMap) (tailcfg.DERPRegionID, error) {
	return RegionID, nil
}
