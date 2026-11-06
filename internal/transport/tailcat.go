package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	// probeTimeout bounds the transport's own check of a client after
	// a dial through it failed; quietFor is how long a client's open
	// connections must have moved no bytes before a failed check
	// closes it. check is the check itself. Tests set all three.
	probeTimeout time.Duration
	quietFor     time.Duration
	check        func(context.Context, *tailcatClient) error

	mu       sync.Mutex
	admit    func(identity.NodeID) bool
	server   *tailcat.Server // set by Listen
	clients  map[identity.NodeID]*tailcatClient
	retired  map[*tailcatClient]bool // replaced, closed when their last connection is
	shutting map[*tailcatClient]bool // marked closed, Client.Close not yet returned
	done     chan struct{}           // closed by Close
	closed   bool
}

// tailcatClient is the client shared by every dial to one friend at one
// relay. Its fields after relay are guarded by Tailcat.mu.
type tailcatClient struct {
	*tailcat.Client
	t      *Tailcat
	peer   identity.NodeID
	relay  string        // relayOf the address it was built for
	down   chan struct{} // closed once Client.Close has returned
	active atomic.Int64  // unix nanoseconds a connection last moved bytes

	refs    int  // open connections, dials in flight and a running check
	conns   int  // open connections
	retired bool // replaced: no new dials; closed once refs is 0
	// stopCheck, set while a check of the client runs (see Dial), ends
	// it when the client is retired or closed.
	stopCheck context.CancelFunc
	closed    bool
}

// The defaults of Tailcat.probeTimeout and Tailcat.quietFor. quietFor
// outlasts a relay restart or a Wi-Fi drop of 15 s, after which the
// transfer's TCP resumes.
const (
	probeTimeout = 10 * time.Second
	quietFor     = 30 * time.Second
)

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
		probeTimeout: probeTimeout,
		quietFor:     quietFor,
		check:        dialCheck,
		clients:      map[identity.NodeID]*tailcatClient{},
		retired:      map[*tailcatClient]bool{},
		shutting:     map[*tailcatClient]bool{},
		done:         make(chan struct{}),
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
// tailcat's server keeps one WireGuard session per client key, and a
// second client with the same key at the same server stalls the first.
// So the transport never runs two clients for one friend at one relay:
// an address that changes but names the same relay reuses the client; a
// client replaced because the address moved to another relay takes no
// new dials but stays up until its last connection closes, and comes
// back if the address returns to its relay; and a successor is not
// dialled until a closed client of the same relay has finished closing.
//
// tailcat admits a client as a WireGuard peer at its first ping only,
// so a client whose dials fail may be talking to a server that
// restarted and forgot it, and must be replaced. But a failed dial says
// little about the client: its caller's context may have ended, or the
// relay blipped. So the transport checks the client with a dial of its
// own, bounded by probeTimeout, at most one at a time. A failed check
// closes the client only once its open connections have moved no bytes
// for quietFor; until then it checks again, so a transfer that pauses
// through a short outage is not cut.
func (t *Tailcat) Dial(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error) {
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		return nil, fmt.Errorf("not a tunnel address: %w", err)
	}
	if got := nodeIDOf(ci.ServerPublic.NodePublic); got != peer {
		return nil, fmt.Errorf("address belongs to node %s, not %s", got, peer)
	}
	relay := relayOf(ci)
	c, err := t.clientFor(ctx, peer, relay, addr)
	if err != nil {
		return nil, err
	}

	conn, err := c.DialTCPPort(ctx, Port)
	if err == nil {
		t.mu.Lock()
		c.conns++ // the connection keeps the dial's reference
		t.mu.Unlock()
		c.touch()
		return &tunnelConn{Conn: conn, c: c}, nil
	}
	t.mu.Lock()
	if c.stopCheck == nil && !c.retired && !c.closed {
		var check context.Context
		check, c.stopCheck = context.WithCancel(context.Background())
		c.refs++
		go t.probe(check, c)
	}
	last := t.releaseLocked(c)
	t.mu.Unlock()
	last.shut()
	return nil, err
}

// clientFor returns peer's client for relay, with a reference taken
// for the caller, building it from addr if there is none.
func (t *Tailcat) clientFor(ctx context.Context, peer identity.NodeID, relay, addr string) (*tailcatClient, error) {
	t.mu.Lock()
	for {
		if t.closed {
			t.mu.Unlock()
			return nil, net.ErrClosed
		}
		var stale *tailcatClient
		c := t.clients[peer]
		if c != nil && c.relay != relay {
			stale = t.retireLocked(c) // the friend moved to another relay
			c = nil
		}
		if c == nil {
			c = t.reviveLocked(peer, relay)
		}
		if c == nil {
			if down := t.shuttingLocked(peer, relay); down != nil {
				t.mu.Unlock()
				stale.shut()
				select {
				case <-down:
				case <-t.done:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				t.mu.Lock()
				continue
			}
			c = &tailcatClient{
				Client: &tailcat.Client{Server: tailcat.Addr(addr), Key: t.key, Logf: t.logf},
				t:      t,
				peer:   peer,
				relay:  relay,
				down:   make(chan struct{}),
			}
			t.clients[peer] = c
		}
		c.refs++
		t.mu.Unlock()
		stale.shut()
		return c, nil
	}
}

// probe checks c, a dial through which failed, with a dial of the
// transport's own. A failed check closes and drops c once its
// connections are quiet (see Dial). stop ends when c is retired or
// closed. It holds a reference to c, which it releases.
func (t *Tailcat) probe(stop context.Context, c *tailcatClient) {
	var dead *tailcatClient
	for {
		ctx, cancel := context.WithTimeout(stop, t.probeTimeout)
		err := t.check(ctx, c)
		cancel()
		t.mu.Lock()
		if err == nil || stop.Err() != nil || c.retired || c.closed || t.closed {
			break
		}
		if quiet := time.Since(c.lastActive()); c.conns > 0 && quiet < t.quietFor {
			t.mu.Unlock()
			t.logf("tunnel to %s: a check of the client failed, but a connection moved bytes %v ago; checking again: %v", c.peer, quiet.Round(time.Second), err)
			wait := time.NewTimer(t.quietFor - quiet)
			select {
			case <-wait.C:
			case <-stop.Done():
				wait.Stop()
			case <-t.done:
				wait.Stop()
			}
			continue
		}
		t.logf("tunnel to %s: a check of the client failed, replacing it: %v", c.peer, err)
		if t.clients[c.peer] == c {
			delete(t.clients, c.peer)
		}
		dead = t.closeLocked(c)
		break
	}
	c.stopCheck()
	c.stopCheck = nil
	last := t.releaseLocked(c)
	t.mu.Unlock()
	dead.shut()
	last.shut()
}

// dialCheck is the default check: a dial through c.
func dialCheck(ctx context.Context, c *tailcatClient) error {
	conn, err := c.DialTCPPort(ctx, Port)
	if err == nil {
		conn.Close()
	}
	return err
}

// relayOf names the relay an address reaches its server through: a
// public region by its ID, or the DERP servers (host and port) of the
// region it carries. The rest of the address (node IPs, STUN ports,
// region names) can change without moving the friend to another
// relay. The server key is the peer's and checked by Dial. An address
// carries no region or node names (tailcat strips them), so a local
// relay is told apart from another by its host and port.
func relayOf(ci tailcat.ConnInfo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d", ci.RegionID)
	for _, r := range ci.Region {
		b.WriteString("/")
		for _, n := range r.Nodes {
			fmt.Fprintf(&b, ",%s", net.JoinHostPort(n.HostName, strconv.Itoa(n.DERPPort)))
		}
	}
	return b.String()
}

// retireLocked takes c out of service: no new dial uses it, and it is
// closed when its last connection is. It returns c if nothing uses it
// and it should be shut now.
func (t *Tailcat) retireLocked(c *tailcatClient) *tailcatClient {
	if t.clients[c.peer] == c {
		delete(t.clients, c.peer)
	}
	if c.retired || c.closed {
		return nil
	}
	c.retired = true
	t.retired[c] = true
	if c.stopCheck != nil {
		c.stopCheck() // its check would only keep it up
	}
	return t.idleLocked(c)
}

// reviveLocked puts back in service a retired client of peer at relay
// whose connections are still open, and returns it, or nil if there is
// none.
func (t *Tailcat) reviveLocked(peer identity.NodeID, relay string) *tailcatClient {
	for c := range t.retired {
		if c.peer == peer && c.relay == relay && !c.closed {
			delete(t.retired, c)
			c.retired = false
			t.clients[peer] = c
			return c
		}
	}
	return nil
}

// shuttingLocked returns the down channel of a client of peer at relay
// that is still closing, or nil if there is none.
func (t *Tailcat) shuttingLocked(peer identity.NodeID, relay string) chan struct{} {
	for c := range t.shutting {
		if c.peer == peer && c.relay == relay {
			return c.down
		}
	}
	return nil
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
	return t.closeLocked(c)
}

// closeLocked marks c closed and returns it for its caller to shut
// once Tailcat.mu is released. Until then it is in shutting, and no
// successor at its relay is dialled. c must not be closed yet.
func (t *Tailcat) closeLocked(c *tailcatClient) *tailcatClient {
	c.closed = true
	delete(t.retired, c)
	t.shutting[c] = true
	if c.stopCheck != nil {
		c.stopCheck()
	}
	return c
}

// shut closes a client marked closed; nil is a no-op. It runs outside
// Tailcat.mu, since closing tailcat's engine can take a while.
func (c *tailcatClient) shut() {
	if c == nil {
		return
	}
	c.Client.Close()
	c.t.mu.Lock()
	delete(c.t.shutting, c)
	c.t.mu.Unlock()
	close(c.down)
}

// touch records that a connection through c moved bytes.
func (c *tailcatClient) touch() { c.active.Store(time.Now().UnixNano()) }

func (c *tailcatClient) lastActive() time.Time { return time.Unix(0, c.active.Load()) }

// tunnelConn is a dialled connection; closing it releases its client.
// Its reads and writes record when it last moved bytes.
type tunnelConn struct {
	net.Conn
	c    *tailcatClient
	once sync.Once
}

func (tc *tunnelConn) Read(b []byte) (int, error) {
	n, err := tc.Conn.Read(b)
	if n > 0 {
		tc.c.touch()
	}
	return n, err
}

func (tc *tunnelConn) Write(b []byte) (int, error) {
	n, err := tc.Conn.Write(b)
	if n > 0 {
		tc.c.touch()
	}
	return n, err
}

func (tc *tunnelConn) Close() error {
	err := tc.Conn.Close()
	tc.once.Do(func() {
		t := tc.c.t
		t.mu.Lock()
		tc.c.conns--
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
	close(t.done)
	var errs []error
	closeNow := func(c *tailcatClient) {
		c.closed = true
		if c.stopCheck != nil {
			c.stopCheck()
		}
		errs = append(errs, c.Client.Close())
		close(c.down)
	}
	for _, c := range t.clients {
		closeNow(c)
	}
	clear(t.clients)
	for c := range t.retired {
		closeNow(c)
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
