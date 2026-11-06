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
	t.Helper()
	d, err := start(logger.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.close)
	return d
}

func start(logf logger.Logf) (*LocalDERP, error) {
	const ip = "127.0.0.1"
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
			HostName:         ip,
			IPv4:             ip,
			IPv6:             "none",
			STUNPort:         stun.LocalAddr().(*net.UDPAddr).Port,
			DERPPort:         ln.Addr().(*net.TCPAddr).Port,
			InsecureForTests: true, // self-signed httptest certificate
			STUNTestIP:       ip,
		}},
	}
	return &LocalDERP{Region: reg, close: func() {
		cancel()
		hs.CloseClientConnections()
		hs.Close()
		d.Close()
	}}, nil
}

// Map is a DERP map holding only this relay's region.
func (l *LocalDERP) Map() *tailcfg.DERPMap {
	return &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{RegionID: l.Region}}
}

// Source is a transport.RegionSource that offers only this relay and
// picks it without measuring anything. Fetches counts DERPMap calls.
type Source struct {
	D       *LocalDERP
	Fetches atomic.Int64
}

// DERPMap returns the local map.
func (s *Source) DERPMap(context.Context) (*tailcfg.DERPMap, error) {
	s.Fetches.Add(1)
	return s.D.Map(), nil
}

// Pick returns the local region.
func (s *Source) Pick(context.Context, *tailcfg.DERPMap) (tailcfg.DERPRegionID, error) {
	return RegionID, nil
}
