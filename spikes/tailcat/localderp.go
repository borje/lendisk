package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"

	"tailscale.com/derp/derpserver"
	"tailscale.com/net/stunserver"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
)

// LocalDERP is a DERP relay and STUN server on 127.0.0.1, the same
// shape as tailscale.com/tstest/integration.RunDERPAndSTUN, for runs
// that must not touch the real relays.
type LocalDERP struct {
	Region *tailcfg.DERPRegion
	close  func()
}

func StartLocalDERP(logf logger.Logf) (*LocalDERP, error) {
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
		RegionID:   900,
		RegionCode: "local",
		Nodes: []*tailcfg.DERPNode{{
			Name:             "900a",
			RegionID:         900,
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

func (l *LocalDERP) Close() { l.close() }
