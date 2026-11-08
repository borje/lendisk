package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// ErrCut is the cause of a tunnel request's context when its connection
// was cut because the key's relationship ended (SEC-15).
var ErrCut = errors.New("the relationship with this key ended")

// DefaultGrace bounds how long Cut lets a busy connection's cancelled
// requests answer before it closes the connection anyway.
const DefaultGrace = 2 * time.Second

// Conns tracks the tunnel's open connections by the key each
// authenticated with (SEC-15, #25), so that ending a friendship can cut
// them: the bridge's half of spike #3's order. The caller commits the
// change and clears admission first, calls Cut, and only then has the
// transport disconnect the keys: a connection closed after the
// disconnect never tells the client. One Conns is given to ServeTunnel;
// nil tracks nothing.
type Conns struct {
	// Grace bounds the wait for a busy connection's requests to answer
	// (DefaultGrace when zero).
	Grace time.Duration

	mu   sync.Mutex
	open map[identity.NodeID]map[*peerConn]struct{}
}

func (c *Conns) add(pc *peerConn) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.open == nil {
		c.open = map[identity.NodeID]map[*peerConn]struct{}{}
	}
	if c.open[pc.peer] == nil {
		c.open[pc.peer] = map[*peerConn]struct{}{}
	}
	c.open[pc.peer][pc] = struct{}{}
}

func (c *Conns) remove(pc *peerConn) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.open[pc.peer], pc)
	if len(c.open[pc.peer]) == 0 {
		delete(c.open, pc.peer)
	}
}

// count is how many connections key holds open.
func (c *Conns) count(key identity.NodeID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.open[key])
}

// Cut ends every open connection that authenticated with one of keys
// and returns how many: it cancels each one's requests in flight (their
// contexts end with ErrCut, which ends the proxied request to gw.sock
// too, and a request whose answer has not started gets the removed
// answer), lets a busy connection answer for at most Grace, and closes
// it. The caller has committed the change and cleared admission, so
// anything the client sends meanwhile is refused (SEC-11).
func (c *Conns) Cut(keys ...identity.NodeID) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	var cut []*peerConn
	for _, k := range keys {
		for pc := range c.open[k] {
			cut = append(cut, pc)
		}
	}
	c.mu.Unlock()
	grace := c.Grace
	if grace <= 0 {
		grace = DefaultGrace
	}
	var busy []*peerConn
	for _, pc := range cut {
		if pc.cutNow() {
			busy = append(busy, pc)
		} else {
			pc.Close()
		}
	}
	deadline := time.Now().Add(grace)
	for _, pc := range busy {
		for pc.active.Load() > 0 && !pc.isClosed() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		// The answer is written once the handler returned; the server
		// closes the connection after it (Connection: close).
		for !pc.isClosed() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		pc.Close()
	}
	return len(cut)
}

// peerConn is an accepted tunnel connection with its key. It gives its
// place under Caps back once, when it is closed, and leaves Conns.
type peerConn struct {
	net.Conn
	peer  identity.NodeID
	once  sync.Once
	done  func()
	conns *Conns

	active atomic.Int32 // requests being served on it

	mu     sync.Mutex
	cancel context.CancelCauseFunc // set by ConnContext
	cut    bool
	closed bool
}

func (c *peerConn) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.conns.remove(c)
		c.done()
	})
	return c.Conn.Close()
}

func (c *peerConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// cutNow marks the connection cut, ends every read from it (a request
// body being proxied, and the drain net/http does after a handler) and
// cancels its requests in flight; it reports whether one was being
// served.
func (c *peerConn) cutNow() bool {
	c.mu.Lock()
	c.cut = true
	cancel := c.cancel
	c.mu.Unlock()
	c.Conn.SetReadDeadline(readEnded)
	if cancel != nil {
		cancel(ErrCut)
	}
	return c.active.Load() > 0
}

// readEnded is the read deadline of a cut connection: long past.
var readEnded = time.Unix(1, 0)

// SetReadDeadline keeps a cut connection's reads ended: the body bound
// (boundedBody) and net/http move the deadline as they go.
func (c *peerConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	if c.cut {
		t = readEnded
	}
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

// SetDeadline is SetReadDeadline and the write deadline at once.
func (c *peerConn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetWriteDeadline(t); err != nil {
		return err
	}
	return c.SetReadDeadline(t)
}

// context is the connection's context for ConnContext: cancelled with
// ErrCut by Cut, already if Cut came first.
func (c *peerConn) context(parent context.Context) context.Context {
	ctx, cancel := context.WithCancelCause(parent)
	c.mu.Lock()
	c.cancel = cancel
	cut := c.cut
	c.mu.Unlock()
	if cut {
		cancel(ErrCut)
	}
	return ctx
}

type connKey struct{}

// counted counts each request on its connection while it is served, so
// Cut knows to let it answer.
func counted(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pc, ok := r.Context().Value(connKey{}).(*peerConn); ok {
			pc.active.Add(1)
			defer pc.active.Add(-1)
		}
		h.ServeHTTP(w, r)
	})
}

// wasCut reports whether r's connection was cut (ErrCut).
func wasCut(r *http.Request) bool {
	return errors.Is(context.Cause(r.Context()), ErrCut)
}

// writeCut answers a request whose connection was cut with the removed
// answer (PEER-5): the tunnel's refusal for a key with no relationship,
// with its length, and then closes the connection itself, so the client
// reads the answer and the end at once instead of waiting for a body it
// is still sending to drain.
func writeCut(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if isControlPath(r) {
		body, _ = json.Marshal(map[string]string{"error": state.NoRelationship})
		w.Header().Set("Content-Type", "application/json")
	} else {
		body = s3err.APIError{Code: "AccessDenied", Description: "lendisk: " + state.NoRelationship}.XMLBody("", "")
		w.Header().Set("Content-Type", "application/xml")
	}
	w.Header().Set("Connection", "close")
	w.Header().Set(control.ProtocolHeader, control.Protocol)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusForbidden)
	w.Write(body)
	rc := http.NewResponseController(w)
	rc.Flush()
	c, _, err := rc.Hijack()
	if err == nil {
		c.Close()
	}
}
