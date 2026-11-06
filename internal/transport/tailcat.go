package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
	"tailscale.com/wgengine/filter"

	"github.com/borje/lendisk/internal/identity"
)

// Tailcat is the Transport over tailcat (D5): a tailcat Server on the
// identity's key, with no pre-shared key and the saved region, serving
// Port; and one tailcat Client per dialled friend, with the same key.
type Tailcat struct {
	key    key.NodePrivate
	region *tailcfg.DERPRegion
	logf   logger.Logf

	mu      sync.Mutex
	admit   func(identity.NodeID) bool
	server  *tailcat.Server // set by Listen
	clients map[identity.NodeID]*tailcatClient
	retired map[*tailcatClient]bool // replaced, closed when their last connection is
	closed  bool
}

// tailcatClient is the client shared by every dial to one friend. Its
// fields after Client are guarded by Tailcat.mu.
type tailcatClient struct {
	addr string
	*tailcat.Client
	t       *Tailcat
	refs    int  // open connections and dials in flight through it
	retired bool // replaced: no new dials; closed once refs is 0
	probing bool // a check of the client is running (see Dial)
	closed  bool
}

// probeTimeout bounds the transport's own check of a client after a
// dial through it ran out of its caller's time.
const probeTimeout = 10 * time.Second

// NewTailcat returns a transport for id with the saved region. It
// neither listens nor dials until asked.
func NewTailcat(id *identity.Identity, region *tailcfg.DERPRegion, log *slog.Logger) (*Tailcat, error) {
	if id == nil {
		return nil, errors.New("transport: no identity key")
	}
	if region == nil || region.RegionID == 0 {
		return nil, errors.New("transport: no saved tunnel region")
	}
	return &Tailcat{
		key:    nodePrivate(id),
		region: region,
		logf: func(format string, args ...any) {
			log.Debug(strings.TrimSpace(fmt.Sprintf(format, args...)), "component", "tailcat")
		},
		clients: map[identity.NodeID]*tailcatClient{},
		retired: map[*tailcatClient]bool{},
	}, nil
}

// NewDialer returns a transport that only dials, with a key that need
// not be a node's identity: an invited node dials the inviting node with
// the invite key (PEER-1). It has no region, so Listen refuses.
func NewDialer(id *identity.Identity, log *slog.Logger) (*Tailcat, error) {
	if id == nil {
		return nil, errors.New("transport: no key")
	}
	t, err := NewTailcat(id, &tailcfg.DERPRegion{RegionID: -1}, log)
	if err != nil {
		return nil, err
	}
	t.region = nil
	return t, nil
}

// KeyOf is the node ID an address names.
func KeyOf(addr string) (identity.NodeID, error) {
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		return identity.NodeID{}, fmt.Errorf("not a tunnel address: %w", err)
	}
	return nodeIDOf(ci.ServerPublic.NodePublic), nil
}

// AddressOf is the address a node with id and the saved region has
// (SEC-14): the identity's public key, the disco key derived from it and
// the region's relay nodes, and no pre-shared key. It is what the
// running server reports, computed without starting one.
func AddressOf(id *identity.Identity, region *tailcfg.DERPRegion) string {
	return addressOf(nodePrivate(id), region)
}

func addressOf(k key.NodePrivate, region *tailcfg.DERPRegion) string {
	ci := tailcat.ConnInfo{
		ServerPublic:      tailcat.NodePublic{NodePublic: k.Public()},
		ServerDiscoPublic: tailcat.DiscoPublicForNode(k),
		Region:            []*tailcfg.DERPRegion{region},
	}
	return string(ci.Addr())
}

// Address is this node's address.
// A dialer has none and returns "".
func (t *Tailcat) Address() string {
	if t.region == nil {
		return ""
	}
	return addressOf(t.key, t.region)
}

// Admit sets the admission function.
func (t *Tailcat) Admit(fn func(identity.NodeID) bool) {
	t.mu.Lock()
	t.admit = fn
	t.mu.Unlock()
}

// allow is the hook tailcat calls. It is never nil, so tailcat's
// admit-everyone default never applies, and it refuses while no
// admission function is set.
func (t *Tailcat) allow(k key.NodePublic) bool {
	t.mu.Lock()
	fn := t.admit
	t.mu.Unlock()
	return fn != nil && fn(nodeIDOf(k))
}

// Listen starts the tailcat server and listens on Port.
func (t *Tailcat) Listen(ctx context.Context) (net.Listener, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.admit == nil {
		return nil, ErrNoAdmission
	}
	if t.closed {
		return nil, net.ErrClosed
	}
	if t.server != nil {
		return nil, errors.New("transport: already listening")
	}
	if t.region == nil {
		return nil, errors.New("transport: a dialer does not listen")
	}
	s := &tailcat.Server{
		Key:                 t.key,
		DisablePresharedKey: true, // SEC-14: the address carries no secret
		Region:              t.region,
		AllowClient:         t.allow,
		Logf:                t.logf,
		// The packet filter admits only the tunnel port.
		ServedTCPPorts: []filter.PortRange{{First: Port, Last: Port}},
		ServedUDPPorts: []filter.PortRange{},
	}
	if err := s.StartContext(ctx); err != nil {
		return nil, fmt.Errorf("starting the tunnel: %w", err)
	}
	ln, err := s.Listen(ctx, "tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("listening on the tunnel port: %w", err)
	}
	t.server = s
	return ln, nil
}

// Dial connects to peer's tunnel port at addr, through one client per
// peer shared by every caller (NET-1, NET-8).
//
// tailcat admits a client as a WireGuard peer at its first ping only,
// so a client whose dials fail may be talking to a server that
// restarted and forgot it, and must be replaced. But a failed dial says
// little about the client: its caller's context may have ended, or the
// friend hiccupped. So the transport checks the client with a dial of
// its own, bounded by probeTimeout, and replaces it only if that fails
// too. Such a client is closed at once: none of its connections can be
// working, and tailcat's server keeps one WireGuard session per key, so
// a live predecessor would fight its successor for it. A client
// replaced because the friend's address changed takes no new dials but
// stays up until its last connection closes.
func (t *Tailcat) Dial(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error) {
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		return nil, fmt.Errorf("not a tunnel address: %w", err)
	}
	if got := nodeIDOf(ci.ServerPublic.NodePublic); got != peer {
		return nil, fmt.Errorf("address belongs to node %s, not %s", got, peer)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, net.ErrClosed
	}
	var stale *tailcatClient
	c := t.clients[peer]
	if c != nil && c.addr != addr {
		stale = t.retireLocked(peer, c) // the friend's address changed
		c = nil
	}
	if c == nil {
		c = &tailcatClient{addr: addr, t: t, Client: &tailcat.Client{Server: tailcat.Addr(addr), Key: t.key, Logf: t.logf}}
		t.clients[peer] = c
	}
	c.refs++
	t.mu.Unlock()
	stale.shut()

	conn, err := c.DialTCPPort(ctx, Port)
	if err == nil {
		return &tunnelConn{Conn: conn, c: c}, nil // the connection keeps the dial's reference
	}
	t.mu.Lock()
	if !c.probing && !c.retired && !c.closed {
		c.probing = true
		c.refs++
		go t.probe(peer, c)
	}
	last := t.releaseLocked(c)
	t.mu.Unlock()
	last.shut()
	return nil, err
}

// probe checks c, a dial through which failed, with a dial of the
// transport's own, and closes and replaces c if that fails too. It holds
// a reference to c, which it releases.
func (t *Tailcat) probe(peer identity.NodeID, c *tailcatClient) {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	conn, err := c.DialTCPPort(ctx, Port)
	cancel()
	if err == nil {
		conn.Close()
	} else {
		t.logf("tunnel to %s: a check of the client failed, replacing it: %v", peer, err)
	}
	t.mu.Lock()
	c.probing = false
	var dead *tailcatClient
	if err != nil && !c.closed {
		if t.clients[peer] == c {
			delete(t.clients, peer)
		}
		delete(t.retired, c)
		c.retired, c.closed = true, true
		dead = c
	}
	last := t.releaseLocked(c)
	t.mu.Unlock()
	dead.shut()
	last.shut()
}

// retireLocked takes c out of service: no new dial uses it, and it is
// closed when its last connection is. It returns c if nothing uses it
// and it should be shut now.
func (t *Tailcat) retireLocked(peer identity.NodeID, c *tailcatClient) *tailcatClient {
	if t.clients[peer] == c {
		delete(t.clients, peer)
	}
	if c.retired || c.closed {
		return nil
	}
	c.retired = true
	t.retired[c] = true
	return t.idleLocked(c)
}

// releaseLocked drops a reference to c, returning c if it should now be
// shut.
func (t *Tailcat) releaseLocked(c *tailcatClient) *tailcatClient {
	c.refs--
	return t.idleLocked(c)
}

func (t *Tailcat) idleLocked(c *tailcatClient) *tailcatClient {
	if !c.retired || c.refs > 0 || c.closed {
		return nil
	}
	c.closed = true
	delete(t.retired, c)
	return c
}

// shut closes a client marked closed; nil is a no-op. It runs outside
// Tailcat.mu, since closing tailcat's engine can take a while.
func (c *tailcatClient) shut() {
	if c != nil {
		c.Client.Close()
	}
}

// tunnelConn is a dialled connection; closing it releases its client.
type tunnelConn struct {
	net.Conn
	c    *tailcatClient
	once sync.Once
}

func (tc *tunnelConn) Close() error {
	err := tc.Conn.Close()
	tc.once.Do(func() {
		t := tc.c.t
		t.mu.Lock()
		last := t.releaseLocked(tc.c)
		t.mu.Unlock()
		last.shut()
	})
	return err
}

// PeerOf returns the key of the client that opened an accepted connection.
func (t *Tailcat) PeerOf(c net.Conn) (identity.NodeID, error) {
	t.mu.Lock()
	s := t.server
	t.mu.Unlock()
	if s == nil {
		return identity.NodeID{}, errors.New("transport: not listening")
	}
	k, ok := s.PeerKey(c.RemoteAddr())
	if !ok {
		return identity.NodeID{}, fmt.Errorf("no tunnel peer for %v", c.RemoteAddr())
	}
	return nodeIDOf(k), nil
}

// Disconnect drops peer's tunnel. tailcat's disconnect resets no
// connection and cuts only the server-to-client direction (Q-Q): close
// the peer's connections first.
func (t *Tailcat) Disconnect(peer identity.NodeID) bool {
	t.mu.Lock()
	s := t.server
	t.mu.Unlock()
	return s != nil && s.DisconnectClient(nodePublic(peer))
}

// PathInfo probes the path to a dialled peer with a disco ping, whose
// answer names an endpoint only when it came over a direct path.
func (t *Tailcat) PathInfo(ctx context.Context, peer identity.NodeID) (Path, error) {
	t.mu.Lock()
	c := t.clients[peer]
	t.mu.Unlock()
	if c == nil {
		return Path{}, fmt.Errorf("node %s has not been dialled", peer)
	}
	r, err := c.DiscoPing(ctx)
	if err != nil {
		return Path{}, err
	}
	return Path{
		Direct:   r.Endpoint != "",
		Endpoint: r.Endpoint,
		Relay:    r.DERPRegionCode,
		Latency:  time.Duration(r.LatencySeconds * float64(time.Second)),
	}, nil
}

// Close stops the server and every client.
func (t *Tailcat) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	var errs []error
	for _, c := range t.clients {
		c.closed = true
		errs = append(errs, c.Client.Close())
	}
	clear(t.clients)
	for c := range t.retired {
		c.closed = true
		errs = append(errs, c.Client.Close())
	}
	clear(t.retired)
	if t.server != nil {
		errs = append(errs, t.server.Close())
	}
	return errors.Join(errs...)
}

// nodePrivate is the identity's X25519 key as tailcat's WireGuard key;
// they are the same key (ID-1).
func nodePrivate(id *identity.Identity) key.NodePrivate {
	var k key.NodePrivate
	if err := k.UnmarshalText([]byte("privkey:" + hex.EncodeToString(id.PrivateKey().Bytes()))); err != nil {
		panic(err) // a 32-byte key always parses
	}
	return k
}

func nodeIDOf(k key.NodePublic) identity.NodeID {
	var id identity.NodeID
	copy(id[:], k.AppendTo(nil))
	return id
}

func nodePublic(id identity.NodeID) key.NodePublic {
	var k key.NodePublic
	if err := k.UnmarshalText([]byte("nodekey:" + hex.EncodeToString(id[:]))); err != nil {
		panic(err)
	}
	return k
}
