package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

// DefaultLinger bounds how long a connection whose last answer is
// written waits for the client to close it (Conns.Linger). It is the
// remove call's control.DefaultAnswerWait: the two bound the same wait,
// the handler's from its side and Cut's from its own, and Cut must not
// close the remove call's connection while its handler still waits.
const DefaultLinger = control.DefaultAnswerWait

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
	// Linger bounds the wait, from when a cut connection's last answer
	// was written (its handler hijacked it), for the client to read it
	// and close the connection (DefaultLinger when zero).
	Linger time.Duration

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
//
// A connection is only closed once its answer reached the client: an
// answer written and flushed is still in the tunnel's send buffer, and
// the disconnect that follows Cut drops it (#84). So the removed
// answer (writeCut), like the remove call's own (control.Server), is
// written with Connection: close, and its handler then hijacks the
// connection and reads it until the client closes it, as a client does
// after such an answer, for Linger at the most. A client still sending
// a request body keeps sending (Go's client returns the answer only
// once its write ends or fails), so the drain keeps it from a reset
// that would discard the answer, and it gets the answer as the
// connection closes after Linger. A hijacked connection
// keeps its reads, since no net/http read is left on it to end, and
// Cut waits for its handler to close it: for Grace from the cut, or
// Linger from the hijack (when the answer was written), whichever ends
// later.
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
	linger := c.linger()
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
		// The answer is written once the handler returned, and the
		// server closes the connection after it (Connection: close); or
		// the handler hijacked it and closes it once the client did.
		for !pc.isClosed() {
			d := deadline
			if at, ok := pc.hijackedAt(); ok && at.Add(linger).After(d) {
				d = at.Add(linger)
			}
			if !time.Now().Before(d) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		pc.Close()
	}
	return len(cut)
}

func (c *Conns) linger() time.Duration {
	if c == nil || c.Linger <= 0 {
		return DefaultLinger
	}
	return c.Linger
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

	mu       sync.Mutex
	cancel   context.CancelCauseFunc // set by ConnContext
	cut      bool
	closed   bool
	hijacked time.Time // when its handler took it over (http.StateHijacked)
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

// hijack records that a handler took the connection over from net/http.
func (c *peerConn) hijack() {
	c.mu.Lock()
	c.hijacked = time.Now()
	c.mu.Unlock()
}

// hijackedAt is when its handler took the connection over, if it did.
func (c *peerConn) hijackedAt() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hijacked, !c.hijacked.IsZero()
}

// cutNow marks the connection cut, ends every read from it (a request
// body being proxied, and the drain net/http does after a handler)
// unless a handler hijacked it, and cancels its requests in flight; it
// reports whether one was being served.
//
// The deadline is set under mu, as SetReadDeadline and hijack take it,
// so a hijack comes wholly before the cut (which then leaves the
// connection's reads alone) or after it (and the handler's own deadline
// is the last one set, #84 review). The mark is set with the deadline,
// so a read the deadline ends is already on a connection wasCut sees
// as cut, though the cancel comes after.
func (c *peerConn) cutNow() bool {
	c.mu.Lock()
	c.cut = true
	cancel := c.cancel
	if c.hijacked.IsZero() {
		c.Conn.SetReadDeadline(readEnded)
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel(ErrCut)
	}
	return c.active.Load() > 0
}

// readEnded is the read deadline of a cut connection: long past.
var readEnded = time.Unix(1, 0)

// SetReadDeadline keeps a cut connection's reads ended: the body bound
// (boundedBody) and net/http move the deadline as they go. A handler
// that hijacked the connection sets its own.
func (c *peerConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cut && c.hijacked.IsZero() {
		t = readEnded
	}
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

// wasCut reports whether r's connection was cut (ErrCut). It asks the
// connection first: cutNow marks it cut and ends its reads before it
// cancels the context, so a body read can fail on the cut's deadline
// before the context says why, and the request must still get the
// removed answer, not the stalled one.
func wasCut(r *http.Request) bool {
	if pc, ok := r.Context().Value(connKey{}).(*peerConn); ok && pc.wasCut() {
		return true
	}
	return errors.Is(context.Cause(r.Context()), ErrCut)
}

// wasCut reports whether Cut marked the connection cut.
func (c *peerConn) wasCut() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cut
}

// writeCut answers a request whose connection was cut with the removed
// answer (PEER-5): the tunnel's refusal for a key with no relationship,
// with its length and Connection: close. It then hijacks the
// connection, so net/http does not wait for a body the client is still
// sending, and reads it until the client closes it after the answer,
// for the Linger of its Conns at the most, before closing it: closed at
// once, the answer could still be in the tunnel's send buffer when the
// disconnect after Cut drops the tunnel (#84).
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
	c, brw, err := rc.Hijack()
	if err != nil {
		return
	}
	defer c.Close()
	var conns *Conns
	if pc, ok := r.Context().Value(connKey{}).(*peerConn); ok {
		conns = pc.conns
	}
	c.SetReadDeadline(time.Now().Add(conns.linger()))
	io.Copy(io.Discard, brw)
}
