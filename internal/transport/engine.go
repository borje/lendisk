package transport

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"slices"
	"sync"
	"unsafe"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/tsd"
	"tailscale.com/types/netmap"
	"tailscale.com/wgengine/magicsock"
)

// tailcat exposes neither a friend's explicit addresses nor a fixed UDP
// port for its engine: it has no control plane, and its engines bind any
// port. Both are reached in tailcat's unexported backend, as tailcat
// itself reaches gVisor's (closeProxyConn) and Reset does here. Every
// field is checked for its type first, so a tailcat whose internals
// changed gives an error, which the caller logs, never a crash: the
// friend is then reached by hole punching and the relay as before.

// errInternals means tailcat's internals are not what this code knows.
var errInternals = errors.New("tailcat's internals have changed; explicit addresses and tunnel.port are not applied")

// field returns v's field name as an addressable value of type T, or
// nil if v has no such field of that type.
func field[T any](v reflect.Value, name string) *T {
	f := v.FieldByName(name)
	if !f.IsValid() || f.Type() != reflect.TypeFor[T]() || !f.CanAddr() {
		return nil
	}
	return (*T)(unsafe.Pointer(f.UnsafeAddr()))
}

// backend is the locoBackend behind a tailcat Server or Client: its
// subsystems, network map and the lock guarding the map.
type backend struct {
	sys *tsd.System
	nm  **netmap.NetworkMap
	mu  *sync.Mutex
}

func backendOf(owner any) (backend, error) {
	v := reflect.ValueOf(owner)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return backend{}, errInternals
	}
	lb := v.Elem().FieldByName("lb")
	if !lb.IsValid() || lb.Kind() != reflect.Pointer || lb.IsNil() {
		return backend{}, errInternals
	}
	be := lb.Elem()
	b := backend{sys: field[tsd.System](be, "sys"), nm: field[*netmap.NetworkMap](be, "nm"), mu: field[sync.Mutex](be, "mu")}
	if b.sys == nil || b.nm == nil || b.mu == nil {
		return backend{}, errInternals
	}
	return b, nil
}

func (b backend) magicsock() (*magicsock.Conn, error) {
	mc, ok := b.sys.MagicSock.GetOK()
	if !ok || mc == nil {
		return nil, errInternals
	}
	return mc, nil
}

// setEnginePort binds a started server's engine to a UDP port.
func setEnginePort(s *tailcat.Server, port uint16) error {
	b, err := backendOf(s)
	if err != nil {
		return err
	}
	mc, err := b.magicsock()
	if err != nil {
		return err
	}
	mc.SetPreferredPort(port)
	return nil
}

// setPeerEndpoints gives a started client's engine eps as the server's
// UDP addresses, as a control plane's network map would: magicsock
// disco-pings each, and the first to answer becomes the direct path.
func setPeerEndpoints(c *tailcat.Client, eps []netip.AddrPort) error {
	b, err := backendOf(c)
	if err != nil {
		return err
	}
	mc, err := b.magicsock()
	if err != nil {
		return err
	}
	b.mu.Lock()
	nm := *b.nm
	b.mu.Unlock()
	if nm == nil || len(nm.Peers) != 1 {
		return errInternals // a client's map holds its server alone
	}
	peer := nm.Peers[0].AsStruct()
	peer.Endpoints = slices.Clone(eps)
	mc.SetNetworkMap(nm.SelfNode, []tailcfg.NodeView{peer.View()})
	return nil
}

// direct gives c's engine the friend's explicit addresses (NET-3) when
// they changed since it was last given them. The engine exists once the
// client has started, so a client not yet up is started first, with
// tailcat's meow ping, which the dial does anyway.
func (t *Tailcat) direct(ctx context.Context, c *tailcatClient, eps []netip.AddrPort) {
	eps = slices.Clone(eps)
	slices.SortFunc(eps, func(a, b netip.AddrPort) int { return a.Compare(b) })
	eps = slices.Compact(eps)
	c.epsMu.Lock()
	defer c.epsMu.Unlock()
	if slices.Equal(c.eps, eps) {
		return
	}
	if _, err := c.Ping(ctx); err != nil {
		return // the dial fails the same way, and the next one tries again
	}
	if err := setPeerEndpoints(c.Client, eps); err != nil {
		t.log.Warn("transport: a friend's explicit addresses could not be given to the tunnel; it is reached by hole punching and the relay",
			"peer", c.peer.String(), "err", err)
		return
	}
	t.logf("tunnel to %s: explicit addresses %v", c.peer, eps)
	c.eps = eps
}
