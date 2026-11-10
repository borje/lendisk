package bridge

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"time"
)

// answerWait bounds how long a write to gw.sock that failed because the
// gateway stopped reading waits for the connection to close
// (answerFirst). The transport closes it as soon as the answer is read
// and relayed, or found missing, so this only bounds a leak.
const answerWait = time.Minute

// answerFirst is a connection to gw.sock on which a write that fails
// because the gateway stopped reading (EPIPE, ECONNRESET) waits, before
// it fails, until the transport closes the connection (#100, LEND-2).
//
// The gateway answers a write it refuses (507 QuotaExceeded, 503
// SlowDown) before reading the body, and closes the connection. The
// transport reads that answer while it writes the body, as it does any
// early answer, but if the write fails first it drops the answer and
// the borrower would get this bridge's "gateway unavailable", a network
// failure, instead of busy or full. Held back, the failed write lets
// the read deliver the answer: the transport relays it, then closes the
// connection, which ends the write. If the gateway closed without an
// answer, the read fails and closes it too. Nothing of the body is read
// from the borrower meanwhile. A unix socket keeps what the gateway
// wrote readable after it closed (seed.answered is the same on the
// import path, #36).
type answerFirst struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func newAnswerFirst(c net.Conn) *answerFirst {
	return &answerFirst{Conn: c, closed: make(chan struct{})}
}

func (a *answerFirst) Write(p []byte) (int, error) {
	n, err := a.Conn.Write(p)
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		t := time.NewTimer(answerWait)
		defer t.Stop()
		select {
		case <-a.closed:
		case <-t.C:
		}
	}
	return n, err
}

func (a *answerFirst) Close() error {
	a.once.Do(func() { close(a.closed) })
	return a.Conn.Close()
}
