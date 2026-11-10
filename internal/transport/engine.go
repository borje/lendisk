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
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return backend{}, errInternals
	}
	lb := v.Elem().FieldByName("lb")
	if !lb.IsValid() || lb.Kind() != reflect.Pointer || lb.IsNil() || lb.Elem().Kind() != reflect.Struct {
		return backend{}, errInternals // FieldByName panics on a non-struct
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
// The map it installs is also the backend's own, so tailcat reads the
// endpoints back, and it returns that map: when tailcat installs
// another, the endpoints are gone and must be given again (applied).
func setPeerEndpoints(c *tailcat.Client, eps []netip.AddrPort) (*netmap.NetworkMap, error) {
	b, err := backendOf(c)
	if err != nil {
		return nil, err
	}
	mc, err := b.magicsock()
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	nm := *b.nm
	if nm == nil || len(nm.Peers) != 1 {
		return nil, errInternals // a client's map holds its server alone
	}
	peer := nm.Peers[0].AsStruct()
	peer.Endpoints = slices.Clone(eps)
	next := *nm
	next.Peers = []tailcfg.NodeView{peer.View()}
	*b.nm = &next
	// Under the backend's lock, as tailcat's own setNetworkMapLocked
	// does, so no map tailcat installs meanwhile is overwritten.
	mc.SetNetworkMap(next.SelfNode, next.Peers)
	return &next, nil
}

// applied reports whether c's backend still holds nm, the map that
// carried its explicit addresses; a client whose internals cannot be
// read counts as holding it, since nothing can be given to it again.
func applied(c *tailcat.Client, nm *netmap.NetworkMap) bool {
	b, err := backendOf(c)
	if err != nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return *b.nm == nm
}

// direct gives c's engine the friend's explicit addresses (NET-3) when
// they changed since it was last given them, or when the client's
// network map was replaced since, which drops them (#99). The engine
// exists once the client has started, so a client not yet up is
// started first, with tailcat's meow ping, which the dial does anyway.
// The ping is a network round trip, so it runs without c.epsMu: a
// concurrent dial to the same friend whose addresses are in place does
// not wait for it (#99).
func (t *Tailcat) direct(ctx context.Context, c *tailcatClient, eps []netip.AddrPort) {
	current := func() bool {
		return slices.Equal(c.eps, eps) && (len(eps) == 0 || applied(c.Client, c.nm))
	}
	c.epsMu.Lock()
	done := current()
	c.epsMu.Unlock()
	if done {
		return
	}
	if err := t.ping(ctx, c); err != nil {
		return // the dial fails the same way, and the next one tries again
	}
	c.epsMu.Lock()
	defer c.epsMu.Unlock()
	if current() {
		return // a concurrent dial gave them
	}
	nm, err := setPeerEndpoints(c.Client, eps)
	if err != nil {
		t.log.Warn("transport: a friend's explicit addresses could not be given to the tunnel; it is reached by hole punching and the relay",
			"peer", c.peer.String(), "err", err)
		return
	}
	t.logf("tunnel to %s: explicit addresses %v", c.peer, eps)
	c.eps, c.nm = eps, nm
}
