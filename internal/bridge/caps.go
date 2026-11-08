package bridge

import (
	"context"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
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
// open connections per friend, counted on the friend Of resolves the
// connection's key to, so its node ID and its dial key share one budget;
// a key Of does not resolve counts on its own. A connection over it is
// closed unread at accept.
// Requests bounds the requests in flight per friend, counted on the
// resolved node ID; one over it is answered 503 SlowDown. Loopback and
// LAN listeners are not counted. Zero fields take the defaults: there is
// no unlimited setting. One Caps is shared by ServeTunnel and
// TunnelHandler.
type Caps struct {
	Conns    int
	Requests int
	// Of resolves a connection's key to the friend it speaks for
	// (RolesIn); nil counts each key on its own.
	Of RoleOf

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

// friendOf is the friend key speaks for, or key itself when Of cannot
// say (no relationship, or a lookup error: TunnelHandler refuses those
// requests anyway).
func (c *Caps) friendOf(key identity.NodeID) identity.NodeID {
	if c.Of == nil {
		return key
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, r, err := c.Of(ctx, key)
	if err != nil || r == state.RoleNone {
		return key
	}
	return p
}

// conn counts an accepted connection for friend; false means friend is
// at its cap.
func (c *Caps) conn(friend identity.NodeID) bool {
	n, _ := c.limits()
	return c.take(&c.conns, friend, n)
}

func (c *Caps) connDone(friend identity.NodeID) { c.give(c.conns, friend) }

// request counts a request in flight for peer; false means peer is at its cap.
func (c *Caps) request(peer identity.NodeID) bool {
	_, n := c.limits()
	return c.take(&c.reqs, peer, n)
}

func (c *Caps) requestDone(peer identity.NodeID) { c.give(c.reqs, peer) }
