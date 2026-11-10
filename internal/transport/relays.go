package transport

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/envknob"
	"tailscale.com/tailcfg"
)

// projectMapJSON is the project's DERP map (NET-5), shipped in the
// binary so a node needs no fetch, and no host but the relays, to know
// it. It lists no region while the project runs no relay of its own;
// relays.tailscale_bootstrap then supplies the only regions.
//
//go:embed projectrelays.json
var projectMapJSON []byte

// ProjectMap is the project's DERP map, the default relay set (NET-5).
func ProjectMap() (*tailcfg.DERPMap, error) {
	var m tailcfg.DERPMap
	if err := json.Unmarshal(projectMapJSON, &m); err != nil {
		return nil, fmt.Errorf("the built-in relay map is not valid JSON: %w", err)
	}
	return &m, nil
}

// RelayServer is one self-hosted relay of relays.list: a DERP server
// as `lendisk relay` prints it.
type RelayServer struct {
	Host     string // DNS name or IP address
	Port     int    // DERP port; 0 is 443
	STUNPort int    // 0 is 3478, -1 none
	Cert     string // "sha256-raw:<hex>" pins a self-signed certificate; "" verifies against the system roots
}

// listRegionBase is the first region ID given to relays.list entries,
// the range derper's documentation keeps for custom regions.
const listRegionBase = 900

// RelaySet is the node's own relays (NET-5, NET-5a): the regions it may
// pick its address's region from, and the only relays it dials a
// friend through unless relays.accept_peer_hints allows a friend's.
// MapURL and List each replace the project's map (both: their union);
// Bootstrap adds tailcat's hosted map (Tailscale's relays) as a last
// resort, picked only when no region of the rest answers.
type RelaySet struct {
	MapURL    string
	List      []RelayServer
	Bootstrap bool
	// Cache keeps fetched maps, so a node restarted while a map's host
	// is down still knows its relays; nil uses tailcat's in-memory one.
	Cache tailcat.DERPMapCache
	// fetch reads a DERP map from a URL; tests replace it.
	fetch func(ctx context.Context, url string) (*tailcfg.DERPMap, error)
}

// bootstrapURL is where the hosted relays' map is read.
const bootstrapURL = tailcat.DefaultDERPMapURL

func (s *RelaySet) get(ctx context.Context, url string) (*tailcfg.DERPMap, error) {
	if s.fetch != nil {
		return s.fetch(ctx, url)
	}
	opts := []any{tailcat.DERPMapURL(url), tailcat.ExpandForServer}
	if s.Cache != nil {
		opts = append(opts, s.Cache)
	}
	return tailcat.FetchDERPMap(ctx, opts...)
}

// own is the set without the bootstrap regions.
func (s *RelaySet) own(ctx context.Context) (*tailcfg.DERPMap, error) {
	m := &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{}}
	if s.MapURL == "" && len(s.List) == 0 {
		return ProjectMap()
	}
	if s.MapURL != "" {
		fetched, err := s.get(ctx, s.MapURL)
		if err != nil {
			return nil, fmt.Errorf("reading relays.map_url %s: %w", s.MapURL, err)
		}
		for id, r := range fetched.Regions {
			if r != nil {
				m.Regions[id] = r
			}
		}
	}
	next := tailcfg.DERPRegionID(listRegionBase)
	for id := range m.Regions {
		next = max(next, id+1)
	}
	for _, e := range s.List {
		m.Regions[next] = e.region(next)
		next++
	}
	return m, nil
}

// region is the one-node region of a relays.list entry; its code is the
// host, so tunnel.region can name it.
func (e RelayServer) region(id tailcfg.DERPRegionID) *tailcfg.DERPRegion {
	n := &tailcfg.DERPNode{Name: fmt.Sprintf("%da", id), RegionID: id, HostName: e.Host,
		DERPPort: e.Port, STUNPort: e.STUNPort, CertName: e.Cert}
	if a, err := netip.ParseAddr(e.Host); err == nil {
		if a.Is4() {
			n.IPv4, n.IPv6 = a.String(), "none"
		} else {
			n.IPv6, n.IPv4 = a.String(), "none"
		}
	}
	return &tailcfg.DERPRegion{RegionID: id, RegionCode: strings.ToLower(e.Host), RegionName: e.Host, Nodes: []*tailcfg.DERPNode{n}}
}

// DERPMap is the whole set: the node's own regions and, with Bootstrap,
// the hosted ones whose IDs the own ones leave free.
func (s *RelaySet) DERPMap(ctx context.Context) (*tailcfg.DERPMap, error) {
	m, err := s.own(ctx)
	if err != nil {
		return nil, err
	}
	if !s.Bootstrap {
		return m, nil
	}
	boot, err := s.get(ctx, bootstrapURL)
	if err != nil {
		if len(m.Regions) > 0 {
			return m, nil // the bootstrap is a last resort; the own set stands
		}
		return nil, fmt.Errorf("reading the hosted relays' map (relays.tailscale_bootstrap): %w", err)
	}
	for id, r := range boot.Regions {
		if _, taken := m.Regions[id]; !taken && r != nil {
			m.Regions[id] = r
		}
	}
	return m, nil
}

// Pick chooses the nearest of the node's own regions in m, and only
// when none answers, the nearest bootstrap region.
func (s *RelaySet) Pick(ctx context.Context, m *tailcfg.DERPMap) (tailcfg.DERPRegionID, error) {
	own, err := s.own(ctx)
	if err != nil {
		return 0, err
	}
	mine := &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{}}
	rest := &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{}}
	for id, r := range m.Regions {
		if _, ok := own.Regions[id]; ok {
			mine.Regions[id] = r
		} else {
			rest.Regions[id] = r
		}
	}
	var firstErr error
	for _, cand := range []*tailcfg.DERPMap{mine, rest} {
		if len(cand.Regions) == 0 {
			continue
		}
		id, err := PublicRelays.Pick(ctx, cand)
		if err == nil {
			return id, nil
		}
		firstErr = cmpErr(firstErr, err)
	}
	if firstErr == nil {
		firstErr = errors.New("no relay is configured: set relays.list or relays.map_url, or relays.tailscale_bootstrap = true")
	}
	return 0, firstErr
}

func cmpErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

// ErrRelayNotAllowed means a friend's address names a relay outside the
// node's relay set, and relays.accept_peer_hints is false (NET-5a).
var ErrRelayNotAllowed = errors.New("the friend's relay is not one of this node's relays")

// relay names the DERP servers a client of an address may connect to,
// as keys: "host:port" for each server by host name, by each IP it
// gives and by each IP its host name resolves to; "region:N" for each
// region of the node's own relay set the address shares a server with;
// and "byid:N" for an address naming a hosted region by ID only.
// tailcat's client connects to the first node of the address's region
// whose TCP connect succeeds, in order (derphttp's dialRegion, 1.5 s a
// node), and does not report which, so the client may be on any of
// them. Two addresses are one relay when their keys overlap.
type relay []string

// inSet reports whether r shares a region with the node's relay set.
func (r relay) inSet() bool {
	return slices.ContainsFunc(r, func(k string) bool { return strings.HasPrefix(k, "region:") })
}

// derpPort is a node's DERP port: its own, or the scheme's default,
// which is what derphttp dials (443 over HTTPS, 3340 over plain HTTP
// with TS_DEBUG_USE_DERP_HTTP).
func derpPort(n *tailcfg.DERPNode) int {
	switch {
	case n.DERPPort != 0:
		return n.DERPPort
	case envknob.Bool("TS_DEBUG_USE_DERP_HTTP"):
		return 3340
	}
	return 443
}

// nodeKeys are the keys of one DERP node: by host name and each IP it
// gives, and by each IP resolve gives for its host name (nil: none).
func nodeKeys(n *tailcfg.DERPNode, resolve func(host string) []netip.Addr) []string {
	port := strconv.Itoa(derpPort(n))
	host := strings.TrimSuffix(strings.ToLower(n.HostName), ".")
	var keys []string
	add := func(h string) {
		if a, err := netip.ParseAddr(h); err == nil {
			h = a.Unmap().String()
		}
		if h != "" {
			keys = append(keys, net.JoinHostPort(h, port))
		}
	}
	add(host)
	for _, ip := range []string{n.IPv4, n.IPv6} {
		if a, err := netip.ParseAddr(ip); err == nil {
			add(a.String())
		}
	}
	if _, err := netip.ParseAddr(host); err != nil && host != "" && resolve != nil {
		for _, a := range resolve(host) {
			add(a.String())
		}
	}
	return keys
}

// relayOf is the relay of an address. An address carries no region ID,
// code or node names (tailcat strips them), and the rest of it (STUN
// ports, node order, which nodes of a region it lists) can change
// without moving the friend to another relay. So a node of the address
// that is a node of a region of known, the node's own relay set, gives
// that region's key, and two addresses with disjoint nodes of one
// meshed region are one relay. The server key is the peer's and checked
// by Dial.
func relayOf(ci tailcat.ConnInfo, known *tailcfg.DERPMap, resolve func(host string) []netip.Addr) relay {
	var r relay
	for _, reg := range ci.Region {
		for _, n := range reg.Nodes {
			if !n.STUNOnly {
				r = append(r, nodeKeys(n, resolve)...)
			}
		}
	}
	if len(r) == 0 && ci.RegionID != 0 {
		r = relay{fmt.Sprintf("byid:%d", ci.RegionID)}
	}
	slices.Sort(r)
	r = slices.Compact(r)
	if known != nil {
		for id, reg := range known.Regions {
			if reg == nil {
				continue
			}
			for _, n := range reg.Nodes {
				if n.STUNOnly {
					continue
				}
				if slices.ContainsFunc(nodeKeys(n, nil), func(k string) bool { _, ok := slices.BinarySearch(r, k); return ok }) {
					r = append(r, fmt.Sprintf("region:%d", id))
					break
				}
			}
		}
		slices.Sort(r)
		r = slices.Compact(r)
	}
	return r
}

// overlaps reports whether a client of one address and a client of the
// other may connect to the same DERP server, and so count as one relay.
// It errs towards yes: a client taken for another relay fails its
// check and is replaced, while two clients with one key at one relay
// stall each other (#52).
func (r relay) overlaps(o relay) bool {
	for _, e := range r {
		if _, ok := slices.BinarySearch(o, e); ok {
			return true
		}
	}
	return false
}

// resolveTimeout bounds the DNS lookups of one address's relay hosts;
// relayTTL is how long an address's relay is reused before it is
// worked out again (a host's IPs, or the relay set, may change).
const (
	resolveTimeout = 2 * time.Second
	relayTTL       = 10 * time.Minute
)

// relays is the transport's relay policy and the cache of the relays
// of the addresses it dialled.
type relays struct {
	mu          sync.Mutex
	set         RegionSource // nil: no policy, no regions known
	acceptHints bool
	lookup      func(ctx context.Context, host string) ([]netip.Addr, error)
	cache       map[string]cachedRelay
	now         func() time.Time
	// The set's map as last read (setMap).
	known  *tailcfg.DERPMap
	mapErr error
	mapAt  time.Time
}

type cachedRelay struct {
	r  relay
	at time.Time
}

// How long the relay set's map is reused before it is read again, and
// a failure to read it remembered, so a map host that is down costs a
// dial its fetch timeout at most once a minute.
const (
	setMapTTL    = 5 * time.Minute
	setMapErrTTL = time.Minute
)

// setMap is set's map, read at most once per setMapTTL unless fresh.
func (p *relays) setMap(ctx context.Context, set RegionSource, fresh bool) (*tailcfg.DERPMap, error) {
	p.mu.Lock()
	if !fresh && !p.mapAt.IsZero() {
		ttl := setMapTTL
		if p.mapErr != nil {
			ttl = setMapErrTTL
		}
		if p.now().Sub(p.mapAt) < ttl {
			m, err := p.known, p.mapErr
			p.mu.Unlock()
			return m, err
		}
	}
	p.mu.Unlock()
	m, err := set.DERPMap(ctx)
	if ctx.Err() != nil {
		return m, err // the caller's deadline, not the set's answer
	}
	p.mu.Lock()
	p.known, p.mapErr, p.mapAt = m, err, p.now()
	p.mu.Unlock()
	return m, err
}

func defaultLookup(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

// relayFor is the relay of addr, refused with ErrRelayNotAllowed when a
// relay set is given, acceptHints is false and the address's relay is
// not in it (NET-5a).
func (p *relays) relayFor(ctx context.Context, addr string, ci tailcat.ConnInfo) (relay, error) {
	p.mu.Lock()
	set, accept, lookup := p.set, p.acceptHints, p.lookup
	if c, ok := p.cache[addr]; ok && p.now().Sub(c.at) < relayTTL {
		p.mu.Unlock()
		return c.r, nil
	}
	p.mu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	resolve := func(host string) []netip.Addr {
		if lookup == nil {
			return nil
		}
		as, _ := lookup(rctx, host) // a name that does not resolve keeps its host key
		return as
	}
	var r relay
	// A relay outside the set as last read is checked against the set
	// read afresh before it is refused: the set may have grown.
	for _, fresh := range []bool{false, true} {
		var known *tailcfg.DERPMap
		if set != nil {
			m, err := p.setMap(ctx, set, fresh)
			switch {
			case err == nil:
				known = m
			case !accept:
				return nil, fmt.Errorf("reading this node's relays to check the friend's: %w", err)
			}
			// With peer hints accepted, the set only tells meshed
			// regions apart; without it the relay is known by its
			// servers alone.
		}
		r = relayOf(ci, known, resolve)
		if set == nil || accept || r.inSet() {
			break
		}
		if fresh {
			return nil, fmt.Errorf("%w: it is %s; add it to relays.list or relays.map_url, or set relays.accept_peer_hints = true to use the relay a friend's address names (NET-5a)",
				ErrRelayNotAllowed, describeRelay(ci))
		}
	}
	p.mu.Lock()
	if p.cache == nil || len(p.cache) > 1024 {
		p.cache = map[string]cachedRelay{}
	}
	p.cache[addr] = cachedRelay{r: r, at: p.now()}
	p.mu.Unlock()
	return r, nil
}

// describeRelay names an address's relay for a message.
func describeRelay(ci tailcat.ConnInfo) string {
	var hosts []string
	for _, reg := range ci.Region {
		for _, n := range reg.Nodes {
			if !n.STUNOnly {
				hosts = append(hosts, net.JoinHostPort(cmpStr(n.HostName, n.IPv4), strconv.Itoa(derpPort(n))))
			}
		}
	}
	if len(hosts) == 0 {
		return fmt.Sprintf("hosted region %d", ci.RegionID)
	}
	return strings.Join(hosts, ", ")
}

func cmpStr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// StoreMapCache keeps fetched DERP maps in lendisk.db's node table
// (tailcat.DERPMapCache), so a node restarted while a map's host is down
// still knows its relays: tailcat uses a stored map of any age when a
// fetch fails, and one younger than an hour without fetching.
type StoreMapCache struct {
	Store NodeStore
}

type storedMap struct {
	Data     []byte    `json:"data"`
	ETag     string    `json:"etag"`
	StoredAt time.Time `json:"stored_at"`
}

const mapCacheKey = "relays.map:"

// Get returns the map stored for url.
func (c StoreMapCache) Get(url string) (data []byte, etag string, storedAt time.Time, ok bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, found, err := c.Store.GetNode(ctx, mapCacheKey+url)
	if err != nil || !found {
		return nil, "", time.Time{}, false
	}
	var m storedMap
	if json.Unmarshal([]byte(v), &m) != nil || len(m.Data) == 0 {
		return nil, "", time.Time{}, false
	}
	return m.Data, m.ETag, m.StoredAt, true
}

// Put stores the map fetched from url.
func (c StoreMapCache) Put(url string, data []byte, etag string) error {
	b, err := json.Marshal(storedMap{Data: data, ETag: etag, StoredAt: time.Now()})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Store.SetNode(ctx, mapCacheKey+url, string(b))
}
