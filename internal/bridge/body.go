package bridge

import (
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/versity/versitygw/s3err"
)

// Defaults for the tunnel's request body bound (LEND-2, SEC-5): the
// borrower's own stall watchdog is 2 minutes (§3.4).
const (
	DefaultStall       = 2 * time.Minute
	DefaultMinProgress = 64 << 10
)

// errStalled answers a request whose body brought too little for too
// long; the borrower sends it again.
var errStalled = s3err.APIError{Code: "RequestTimeout", HTTPStatusCode: http.StatusBadRequest,
	Description: "lendisk: the request body brought too little for too long; send it again"}

func (b *Bridge) stall() time.Duration {
	if b.Stall > 0 {
		return b.Stall
	}
	return DefaultStall
}

func (b *Bridge) minProgress() int64 {
	if b.MinProgress > 0 {
		return b.MinProgress
	}
	return DefaultMinProgress
}

// boundedBody bounds a request body on the tunnel with the connection's
// read deadline: set to a stall period before the body starts, and
// pushed a stall period on each time MinProgress more bytes arrive, so
// a body that stops, trickles, or never starts though its headers
// promised one fails its read, and the proxy drops the request to the
// gateway, which ends there too. The read happens on the proxy's
// goroutine, so every deadline change is under mu and none after the
// handler has returned.
type boundedBody struct {
	io.ReadCloser
	rc      *http.ResponseController
	stall   time.Duration
	min     int64
	mu      sync.Mutex
	win     int64
	done    bool
	stalled bool
}

// bound wraps r's body, if it has one, and returns the function the
// handler calls when it returns.
func (b *Bridge) bound(w http.ResponseWriter, r *http.Request) (finish func()) {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return func() {}
	}
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(b.stall())); err != nil {
		b.Log.Warn("tunnel: cannot bound a request body; serving it unbounded", "err", err)
		return func() {}
	}
	bb := &boundedBody{ReadCloser: r.Body, rc: rc, stall: b.stall(), min: b.minProgress()}
	r.Body = bb
	return bb.finish
}

func (bb *boundedBody) Read(p []byte) (int, error) {
	n, err := bb.ReadCloser.Read(p)
	bb.mu.Lock()
	defer bb.mu.Unlock()
	if bb.done {
		return n, err
	}
	var ne net.Error
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &ne) && ne.Timeout():
		bb.stalled = true
	case err == io.EOF:
		bb.rc.SetReadDeadline(time.Time{})
	default:
		bb.win += int64(n)
		if bb.win >= bb.min {
			bb.win = 0
			bb.rc.SetReadDeadline(time.Now().Add(bb.stall))
		}
	}
	return n, err
}

// finish clears the deadline; no change is made after it.
func (bb *boundedBody) finish() {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	if !bb.done {
		bb.done = true
		bb.rc.SetReadDeadline(time.Time{})
	}
}

// stalledBody reports whether r's body ended for the stall bound.
func stalledBody(r *http.Request) bool {
	bb, ok := r.Body.(*boundedBody)
	if !ok {
		return false
	}
	bb.mu.Lock()
	defer bb.mu.Unlock()
	return bb.stalled
}
