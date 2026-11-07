package bridge

import (
	"net"
	"sync"

	"github.com/borje/lendisk/internal/identity"
)

// Defaults for Caps. A borrower's replica client moves one object per
// bucket at a time over a couple of connections (§3.4); these leave it
// room and stop one friend from holding the tunnel port.
const (
	DefaultConns    = 32
	DefaultRequests = 16
)

// Caps bounds what one friend may hold open on the tunnel port (SEC-7,
// #15), so one borrower cannot exhaust the connections and requests the
// gateway behind it serves for everyone: fasthttp's MaxConnections and
// MaxRequests at gw.sock are shared by every listener. Conns bounds the
// open connections per connection key (a friend has two: its node ID and
// its dial key); a connection over it is closed unread at accept.
// Requests bounds the requests in flight per friend, counted on the
// resolved node ID; one over it is answered 503 SlowDown. Loopback and
// LAN listeners are not counted. Zero fields take the defaults: there is
// no unlimited setting. One Caps is shared by ServeTunnel and
// TunnelHandler.
type Caps struct {
	Conns    int
	Requests int

	mu    sync.Mutex
	conns map[identity.NodeID]int
	reqs  map[identity.NodeID]int
}

func (c *Caps) limits() (conns, reqs int) {
	conns, reqs = c.Conns, c.Requests
	if conns <= 0 {
		conns = DefaultConns
	}
	if reqs <= 0 {
		reqs = DefaultRequests
	}
	return conns, reqs
}

// take counts one more for key in m, unless it already holds max.
func (c *Caps) take(m *map[identity.NodeID]int, key identity.NodeID, max int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if *m == nil {
		*m = map[identity.NodeID]int{}
	}
	if (*m)[key] >= max {
		return false
	}
	(*m)[key]++
	return true
}

func (c *Caps) give(m map[identity.NodeID]int, key identity.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m[key] <= 1 {
		delete(m, key)
		return
	}
	m[key]--
}

// conn counts an accepted connection on key; false means key is at its cap.
func (c *Caps) conn(key identity.NodeID) bool {
	n, _ := c.limits()
	return c.take(&c.conns, key, n)
}

func (c *Caps) connDone(key identity.NodeID) { c.give(c.conns, key) }

// request counts a request in flight for peer; false means peer is at its cap.
func (c *Caps) request(peer identity.NodeID) bool {
	_, n := c.limits()
	return c.take(&c.reqs, peer, n)
}

func (c *Caps) requestDone(peer identity.NodeID) { c.give(c.reqs, peer) }

// peerConn is an accepted tunnel connection with its key. It gives its
// place under Caps back once, when it is closed.
type peerConn struct {
	net.Conn
	peer identity.NodeID
	once sync.Once
	done func()
}

func (c *peerConn) Close() error {
	c.once.Do(c.done)
	return c.Conn.Close()
}
