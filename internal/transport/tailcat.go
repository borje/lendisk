package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/tailscale/tailcat"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
	"tailscale.com/types/netmap"
	"tailscale.com/wgengine/filter"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/version"
)

// Version is the linked tailcat module version, for `status` (EP-12).
func Version() string { return version.Module("github.com/tailscale/tailcat") }

// Tailcat is the Transport over tailcat (D5): a tailcat Server on the
// identity's key, with no pre-shared key and the saved region, serving
// Port; and one tailcat Client per dialled friend, with the dial key
// for that friend (identity.DialKeyFor, #69). Every engine has a key of
// its own because a relay delivers a key's packets to whichever of its
// connections wrote last: with one key, the node's own client would
// take the packets meant for its server, or for its client to another
// friend (DESIGN §3.7).
type Tailcat struct {
	key        key.NodePrivate                       // the server's: the identity key
	dialKeyFor func(identity.NodeID) key.NodePrivate // a client's, by friend
	region     *tailcfg.DERPRegion
	logf       logger.Logf
	log        *slog.Logger // what the transport itself reports

	// probeTimeout bounds the transport's own check of a client after
	// a dial through it failed; quietFor is how long a client's open
	// connections must have moved no bytes before a failed check
	// closes it. check is the check itself. Tests set all three.
	probeTimeout time.Duration
	quietFor     time.Duration
	check        func(context.Context, *tailcatClient) error

	// elapsed is the transport's clock: the time since it was made,
	// read on the monotonic clock, so a step of the wall clock (an NTP
	// correction on a board without a real-time clock) moves no
	// activity stamp. dial opens a connection through a client, and
	// closeTailcat closes a tailcat client or server. Tests wrap all
	// three.
	elapsed      func() time.Duration
	dial         func(context.Context, *tailcatClient) (net.Conn, error)
	closeTailcat func(io.Closer) error
	// listenTailcat starts the server and listens on the tunnel port;
	// tests wrap it.
	listenTailcat func(context.Context, *tailcat.Server) (net.Listener, error)

	// relays is the relay policy (NET-5a) and the relays of the
	// addresses dialled (see relay).
	relays relays

	strayOnce sync.Once // warns once that dropStray cannot work

	mu sync.Mutex
	// endpoints gives a friend's explicit addresses (NET-3), dialled
	// directly; nil for none. port is the UDP port the server's engine
	// binds, 0 for any (SetPort).
	endpoints func(identity.NodeID) []string
	port      uint16
	// ping starts a client with tailcat's meow ping before its engine
	// is given explicit addresses; tests wrap it.
	ping func(context.Context, *tailcatClient) error

	admit    func(identity.NodeID) bool
	server   *tailcat.Server // set by Listen
	clients  map[identity.NodeID]*tailcatClient
	retired  map[*tailcatClient]bool // replaced, closed when their last connection is
	shutting map[*tailcatClient]bool // marked closed, Client.Close not yet returned
	done     chan struct{}           // closed by Close
	closed   bool
	starting bool // Listen is starting the server, without mu
}

// tailcatClient is the client shared by every dial to one friend at one
// relay. Its fields after relay are guarded by Tailcat.mu.
type tailcatClient struct {
	*tailcat.Client
	t      *Tailcat
	peer   identity.NodeID
	relay  relay         // relayOf the address it was built for
	down   chan struct{} // closed once Client.Close has returned
	active atomic.Int64  // Tailcat.elapsed when a connection last moved bytes

	// eps are the explicit addresses given to the client's engine
	// (NET-3), and nm the network map that carried them; guarded by
	// epsMu.
	epsMu sync.Mutex
	eps   []netip.AddrPort
	nm    *netmap.NetworkMap

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

// NewTailcat returns a transport for id with the saved region: it
// serves with id and dials each friend with id.DialKeyFor(friend). It
// neither listens nor dials until asked.
func NewTailcat(id *identity.Identity, region *tailcfg.DERPRegion, log *slog.Logger) (*Tailcat, error) {
	if id == nil {
		return nil, errors.New("transport: no identity key")
	}
	if region == nil || region.RegionID == 0 {
		return nil, errors.New("transport: no saved tunnel region")
	}
	epoch := time.Now()
	t := &Tailcat{
		key:    nodePrivate(id),
		region: region,
		log:    log,
		logf: func(format string, args ...any) {
			log.Debug(strings.TrimSpace(fmt.Sprintf(format, args...)), "component", "tailcat")
		},
		probeTimeout: probeTimeout,
		quietFor:     quietFor,
		elapsed:      func() time.Duration { return time.Since(epoch) },
		dial: func(ctx context.Context, c *tailcatClient) (net.Conn, error) {
			return c.DialTCPPort(ctx, Port)
		},
		closeTailcat: io.Closer.Close,
		relays:       relays{lookup: defaultLookup, now: time.Now},
		clients:      map[identity.NodeID]*tailcatClient{},
		retired:      map[*tailcatClient]bool{},
		shutting:     map[*tailcatClient]bool{},
		done:         make(chan struct{}),
	}
	t.ping = func(ctx context.Context, c *tailcatClient) error {
		_, err := c.Ping(ctx)
		return err
	}
	t.check = t.dialCheck
	t.listenTailcat = t.startServer
	t.dialKeyFor = func(friend identity.NodeID) key.NodePrivate { return nodePrivate(id.DialKeyFor(friend)) }
	return t, nil
}

// NewDialer returns a transport that only dials, with the key it is
// given, not one derived from it: an invited node dials the inviting
// node with the invite key (PEER-1). It has no region, so Listen
// refuses.
func NewDialer(id *identity.Identity, log *slog.Logger) (*Tailcat, error) {
	if id == nil {
		return nil, errors.New("transport: no key")
	}
	t, err := NewTailcat(id, &tailcfg.DERPRegion{RegionID: -1}, log)
	if err != nil {
		return nil, err
	}
	t.region = nil
	t.dialKeyFor = func(identity.NodeID) key.NodePrivate { return t.key }
	return t, nil
}

// NewFriendDialer returns a transport that only dials, each friend with
// id.DialKeyFor(friend) as the daemon's transport does (#69): what a
// node dials its friends with when no daemon runs, as recovery does
// (ID-5, manifest.Fetch). It has no region, so Listen refuses.
func NewFriendDialer(id *identity.Identity, log *slog.Logger) (*Tailcat, error) {
	t, err := NewDialer(id, log)
	if err != nil {
		return nil, err
	}
	t.dialKeyFor = func(friend identity.NodeID) key.NodePrivate { return nodePrivate(id.DialKeyFor(friend)) }
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

// Listen starts the tailcat server and listens on Port. It does not
// hold Tailcat.mu while tailcat starts, listens or closes (#60): the
// admission hook takes that lock.
func (t *Tailcat) Listen(ctx context.Context) (net.Listener, error) {
	t.mu.Lock()
	var refuse error
	switch {
	case t.admit == nil:
		refuse = ErrNoAdmission
	case t.closed:
		refuse = net.ErrClosed
	case t.server != nil || t.starting:
		refuse = errors.New("transport: already listening")
	case t.region == nil:
		refuse = errors.New("transport: a dialer does not listen")
	}
	if refuse != nil {
		t.mu.Unlock()
		return nil, refuse
	}
	t.starting = true
	t.mu.Unlock()

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
	ln, err := t.listenTailcat(ctx, s)
	t.mu.Lock()
	t.starting = false
	closed := t.closed
	if err == nil && !closed {
		t.server = s
	}
	t.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if closed { // Close ran meanwhile and did not see the server
		ln.Close()
		t.closeTailcat(s)
		return nil, net.ErrClosed
	}
	return ln, nil
}

// startServer is the default listenTailcat: it starts s, binds its
// engine to the configured UDP port, and listens on Port; on an error
// it closes s.
func (t *Tailcat) startServer(ctx context.Context, s *tailcat.Server) (net.Listener, error) {
	if err := s.StartContext(ctx); err != nil {
		t.closeTailcat(s)
		return nil, fmt.Errorf("starting the tunnel: %w", err)
	}
	t.mu.Lock()
	port := t.port
	t.mu.Unlock()
	if port != 0 {
		if err := setEnginePort(s, port); err != nil {
			t.log.Warn("transport: the tunnel's UDP port could not be set, so it uses one of its own; an explicit address naming tunnel.port does not reach it",
				"port", port, "err", err)
		}
	}
	ln, err := s.Listen(ctx, "tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		t.closeTailcat(s)
		return nil, fmt.Errorf("listening on the tunnel port: %w", err)
	}
	return ln, nil
}

// SetRelays sets the node's relay set (NET-5) and whether a friend's
// address may name a relay outside it (NET-5a, relays.accept_peer_hints).
// Without a set, any relay an address names is dialled. Set it before
// the first Dial.
func (t *Tailcat) SetRelays(set RegionSource, acceptHints bool) {
	t.relays.mu.Lock()
	t.relays.set, t.relays.acceptHints = set, acceptHints
	t.relays.cache, t.relays.mapAt = nil, time.Time{}
	t.relays.mu.Unlock()
}

// SetEndpoints sets where a friend's explicit addresses come from
// (NET-3): each is given to the friend's client as an address the
// friend's engine listens on, so the client tries it directly at once.
// fn is asked at each dial; it gives ip:port or name:port
// (ParseExplicitAddress), a name being looked up at each dial (#99).
func (t *Tailcat) SetEndpoints(fn func(identity.NodeID) []string) {
	t.mu.Lock()
	t.endpoints = fn
	t.mu.Unlock()
}

// SetPort binds the server's engine to a fixed UDP port (tunnel.port),
// so a port forward can name it and a friend can give it as an
// explicit address (NET-3); 0 is any port. Set it before Listen.
func (t *Tailcat) SetPort(port uint16) {
	t.mu.Lock()
	t.port = port
	t.mu.Unlock()
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
// through a short outage is not cut. A dial that succeeds through a
// client closed meanwhile closes its connection and fails.
func (t *Tailcat) Dial(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error) {
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		return nil, fmt.Errorf("not a tunnel address: %w", err)
	}
	if got := nodeIDOf(ci.ServerPublic.NodePublic); got != peer {
		return nil, fmt.Errorf("address belongs to node %s, not %s", got, peer)
	}
	// With peer hints refused, the address dialled is the friend's key
	// at the set's own relay, never the address's relay data (NET-5a).
	relay, dialAddr, err := t.relays.relayFor(ctx, addr, ci)
	if err != nil {
		return nil, err
	}
	c, err := t.clientFor(ctx, peer, relay, dialAddr)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	endpoints := t.endpoints
	t.mu.Unlock()
	if endpoints != nil {
		t.direct(ctx, c, t.endpointsOf(ctx, peer.String(), endpoints(peer)))
	}

	t.dropStray(c)
	conn, err := t.dial(ctx, c)
	if err == nil {
		t.mu.Lock()
		if c.closed {
			t.releaseLocked(c) // a closed client is already shut or shutting
			t.mu.Unlock()
			conn.Close()
			return nil, fmt.Errorf("tunnel to %s: the client closed during the dial; dial again: %w", peer, net.ErrClosed)
		}
		c.conns++ // the connection keeps the dial's reference
		c.touch()
		t.mu.Unlock()
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
func (t *Tailcat) clientFor(ctx context.Context, peer identity.NodeID, relay relay, addr string) (*tailcatClient, error) {
	t.mu.Lock()
	for {
		if t.closed {
			t.mu.Unlock()
			return nil, net.ErrClosed
		}
		var stale *tailcatClient
		c := t.clients[peer]
		if c != nil && !c.relay.overlaps(relay) {
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
				Client: &tailcat.Client{Server: tailcat.Addr(addr), Key: t.dialKeyFor(peer), Logf: t.logf},
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
		t.dropStray(c)
		ctx, cancel := context.WithTimeout(stop, t.probeTimeout)
		err := t.check(ctx, c)
		cancel()
		t.mu.Lock()
		if err == nil || stop.Err() != nil || c.retired || c.closed || t.closed {
			break
		}
		if quiet := c.quiet(); c.conns > 0 && quiet < t.quietFor {
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

// dropStray removes from c's stack any tunnel address that is not its
// own (#111, dropStrayAddresses), before each dial and each check: a
// stray address takes every new dial's SYNs, which the friend drops.
// The dial that met it fails at its timeout and the next one goes
// through, without replacing the client, so the transfers it carries
// go on. A tailcat whose internals changed is warned of once; the
// client is then replaced by its failed checks as before.
func (t *Tailcat) dropStray(c *tailcatClient) {
	dropped, err := dropStrayAddresses(c.Client)
	if err != nil {
		t.strayOnce.Do(func() {
			t.log.Warn("transport: stray tunnel addresses cannot be removed from a client; a friend's dials that meet one time out until the client is replaced",
				"peer", c.peer.String(), "err", err)
		})
	}
	for _, a := range dropped {
		t.log.Info("transport: removed a stray address from the tunnel to a friend; it would have taken the friend's dials",
			"peer", c.peer.String(), "addr", a.String())
	}
}

// dialCheck is the default check: a dial through c.
func (t *Tailcat) dialCheck(ctx context.Context, c *tailcatClient) error {
	conn, err := t.dial(ctx, c)
	if err == nil {
		conn.Close()
	}
	return err
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
func (t *Tailcat) reviveLocked(peer identity.NodeID, relay relay) *tailcatClient {
	for c := range t.retired {
		if c.peer == peer && c.relay.overlaps(relay) && !c.closed {
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
func (t *Tailcat) shuttingLocked(peer identity.NodeID, relay relay) chan struct{} {
	for c := range t.shutting {
		if c.peer == peer && c.relay.overlaps(relay) {
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
	c.t.closeTailcat(c.Client)
	c.t.mu.Lock()
	delete(c.t.shutting, c)
	c.t.mu.Unlock()
	close(c.down)
}

// touch records that a connection through c moved bytes.
func (c *tailcatClient) touch() { c.active.Store(int64(c.t.elapsed())) }

// quiet is how long since a connection through c last moved bytes, on
// the transport's clock. It is never negative, so a wait for the rest
// of quietFor never exceeds quietFor.
func (c *tailcatClient) quiet() time.Duration {
	return max(c.t.elapsed()-time.Duration(c.active.Load()), 0)
}

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

// CloseWrite half-closes the connection: the peer reads EOF while
// this side still reads its answer. Embedding does not promote it, so
// a splice (`peer forward`) passes a client's half-close on through it
// (#94). A connection that cannot half-close is not closed; the error
// says so and the caller decides.
func (tc *tunnelConn) CloseWrite() error {
	if cw, ok := tc.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return fmt.Errorf("transport: a %T cannot half-close", tc.Conn)
}

// Reset closes the connection with a TCP reset (linger 0), discarding
// what it has not sent: the peer's next read fails at once, instead of
// reading on what the tunnel still holds and then a FIN behind it. A
// sender abandoning a request body uses it, so the friend ends that
// request, and gives back what it reserved for it, at once (#89). A
// connection that is not gVisor's, or whose internals have changed, is
// closed as usual, and that is logged as a warning (#92): the friend
// then holds what it reserved until its stall bound ends the body.
func (tc *tunnelConn) Reset() error {
	if c, ok := tc.Conn.(*gonet.TCPConn); ok {
		if ep := gonetEndpoint(c); ep != nil {
			ep.SocketOptions().SetLinger(tcpip.LingerOption{Enabled: true, Timeout: 0})
			return tc.Close()
		}
	}
	tc.c.t.log.Warn("transport: a tunnel connection could not be reset and was closed instead; "+
		"the friend reads on what the tunnel holds of the abandoned send until its stall bound ends it",
		"peer", tc.c.peer.String(), "conn", fmt.Sprintf("%T", tc.Conn))
	return tc.Close()
}

// gonetEndpoint returns c's unexported gVisor endpoint, nil if gonet no
// longer has it (tailcat reaches it the same way, closeProxyConn).
func gonetEndpoint(c *gonet.TCPConn) tcpip.Endpoint {
	f := reflect.ValueOf(c).Elem().FieldByName("ep")
	if !f.IsValid() || f.Type() != reflect.TypeFor[tcpip.Endpoint]() {
		return nil
	}
	ep, _ := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface().(tcpip.Endpoint)
	return ep
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
	p := Path{
		Direct:   r.Endpoint != "",
		Endpoint: r.Endpoint,
		Latency:  time.Duration(r.LatencySeconds * float64(time.Second)),
	}
	if !p.Direct {
		// tailcat strips region codes from addresses: name the relay's
		// servers instead.
		if ci, err := tailcat.ParseAddr(c.Server); err == nil {
			p.Relay = describeRelay(ci)
		}
	}
	return p, nil
}

// Close stops the server and every client. It marks them closed under
// Tailcat.mu and closes them after releasing it, since tailcat's
// shutdown may wait on a goroutine in the admission hook, which takes
// that lock. A client already shutting is left to its own shut.
func (t *Tailcat) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.done)
	var shut []*tailcatClient
	mark := func(c *tailcatClient) {
		c.closed = true
		if c.stopCheck != nil {
			c.stopCheck()
		}
		shut = append(shut, c)
	}
	for _, c := range t.clients {
		mark(c)
	}
	clear(t.clients)
	for c := range t.retired {
		mark(c)
	}
	clear(t.retired)
	s := t.server
	t.mu.Unlock()

	var errs []error
	for _, c := range shut {
		errs = append(errs, t.closeTailcat(c.Client))
		close(c.down)
	}
	if s != nil {
		errs = append(errs, t.closeTailcat(s))
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
