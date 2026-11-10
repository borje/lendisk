package replica

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

// answerFirst is a friend's tunnel connection on which a write that
// fails because the friend reset the connection (ECONNRESET, EPIPE)
// waits, before it fails, until the HTTP transport closes the
// connection, for wait at the most (LEND-2, #109).
//
// A lender answers a write it refuses (507 QuotaExceeded, 503
// SlowDown) before it has read the body, and its server closes the
// connection half a second later with the body unread, which resets
// it. The answer is in this node's receive buffer by then, and stays
// readable after the reset, but Go's transport returns the body
// write's error as soon as it sees it, and drops the answer when its
// reading goroutine has not yet run: on a loaded machine the borrower
// got "connection reset by peer", a network failure, in place of busy
// or full. Held back, the failed write lets the read deliver the
// answer; the transport then closes the connection, which ends the
// write. If the friend reset without an answer, the read fails and
// closes it too. The bridge does the same on the lender's leg to
// gw.sock (#100).
type answerFirst struct {
	net.Conn
	wait   time.Duration
	once   sync.Once
	closed chan struct{}
}

func newAnswerFirst(c net.Conn, wait time.Duration) *answerFirst {
	return &answerFirst{Conn: c, wait: wait, closed: make(chan struct{})}
}

func (a *answerFirst) Write(p []byte) (int, error) {
	n, err := a.Conn.Write(p)
	if resetByFriend(err) {
		t := time.NewTimer(a.wait)
		defer t.Stop()
		select {
		case <-a.closed:
		case <-t.C:
		}
	}
	return n, err
}

// resetByFriend reports whether a write failed because the friend
// reset the connection. The tunnel's connections are gVisor's, whose
// errors carry gVisor's message, not an errno (gonet.TCPConn). A
// write on a connection this node closed itself ends at once anyway:
// Close has released the hold.
func resetByFriend(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Err != nil {
		switch oe.Err.Error() {
		case "connection reset by peer", "connection aborted",
			// After a read has already reported the reset.
			"endpoint is closed for send":
			return true
		}
	}
	return false
}

func (a *answerFirst) Close() error {
	a.once.Do(func() { close(a.closed) })
	return a.Conn.Close()
}

// Reset resets the connection when it can (the tunnel's, abandoning);
// otherwise it closes it.
func (a *answerFirst) Reset() error {
	a.once.Do(func() { close(a.closed) })
	if r, ok := a.Conn.(resetter); ok {
		return r.Reset()
	}
	return a.Conn.Close()
}
