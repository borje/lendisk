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

// Defaults for the tunnel's request body bounds (LEND-2, SEC-5, #76).
//
// The stall bound: a body must bring MinProgress bytes in every Stall
// period, the borrower's own stall watchdog of 2 minutes (§3.4). It
// ends a body that stops; alone it lets one trickle at 64 KiB per 2
// minutes and hold its request slot for as long as its length takes.
//
// The floor rate: a body must also bring FloorRate bytes a second,
// averaged over each FloorWindow from its start, or the request ends.
// 8 KiB/s is 64 kbit/s: a borrower using every one of its 16 request
// slots (DefaultRequests) needs 1 Mbit/s of uplink to keep them all
// above it, and an honest replica client uses far fewer (§3.4). The
// window is long enough that a brief stop (a relay switch, a busy
// link) is averaged out. So a slot is held for at most a body's length
// at the floor rate, plus one window.
const (
	DefaultStall       = 2 * time.Minute
	DefaultMinProgress = 64 << 10
	DefaultFloorRate   = 8 << 10
	DefaultFloorWindow = 10 * time.Minute
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

func (b *Bridge) floor() (rate int64, window time.Duration) {
	rate, window = b.FloorRate, b.FloorWindow
	if rate <= 0 {
		rate = DefaultFloorRate
	}
	if window <= 0 {
		window = DefaultFloorWindow
	}
	return rate, window
}

// boundedBody bounds a request body on the tunnel with the connection's
// read deadline: set to a stall period before the body starts, and
// pushed a stall period on each time MinProgress more bytes arrive, so
// a body that stops, trickles, or never starts though its headers
// promised one fails its read, and the proxy drops the request to the
// gateway, which ends there too. The read happens on the proxy's
// goroutine, so every deadline change is under mu and none after the
// handler has returned.
//
// It also keeps the floor rate: the first read once a floor window has
// passed since the window began fails with errBelowFloor when the
// window brought fewer than rate bytes a second, and starts the next
// window otherwise. A body that brings nothing at all is ended by the
// deadline, as before.
type boundedBody struct {
	io.ReadCloser
	rc       *http.ResponseController
	stall    time.Duration
	min      int64
	rate     int64
	window   time.Duration
	mu       sync.Mutex
	win      int64
	deadline time.Time // the stall deadline
	since    time.Time // the floor window's start
	got      int64     // bytes in the floor window
	done     bool
	stalled  bool
	slow     bool   // ended for the floor rate
	release  func() // ends the body's share of the lending limit
}

// errBelowFloor ends a body's read when it brought too little over a
// floor window.
var errBelowFloor = errors.New("the request body arrives below the floor rate")

// bound wraps r's body, if it has one, and returns the function the
// handler calls when it returns. With a Pace, the body is read at the
// borrower's lending limit (LEND-5), and the time the limit holds it
// back, up to the body's fair share of the limit, moves its floor
// window and its stall deadline on, so the lender's own pacing never
// ends a borrower's body as slow, and a body trickling behind another
// of the borrower's that keeps the limit busy is not excused for that
// one's bytes (#87).
func (b *Bridge) bound(w http.ResponseWriter, r *http.Request, peer string) (finish func()) {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return func() {}
	}
	rc := http.NewResponseController(w)
	deadline := time.Now().Add(b.stall())
	if err := rc.SetReadDeadline(deadline); err != nil {
		b.Log.Warn("tunnel: cannot bound a request body; serving it unbounded", "err", err)
		return func() {}
	}
	rate, window := b.floor()
	bb := &boundedBody{ReadCloser: r.Body, rc: rc, stall: b.stall(), min: b.minProgress(),
		rate: rate, window: window, since: time.Now(), deadline: deadline, release: func() {}}
	if b.Pace != nil {
		paced, done := b.Pace.LendIn(r.Context(), peer, r.Body, bb.excuse)
		bb.ReadCloser = struct {
			io.Reader
			io.Closer
		}{paced, r.Body}
		bb.release = done
	}
	r.Body = bb
	return bb.finish
}

// excuse moves the floor window and the stall deadline on by d, the
// part of a wait the lender's own limit made that the body's own bytes
// account for (LEND-5, pace.Pacer.LendIn). Only MinProgress more bytes
// set the stall deadline a whole stall period ahead (Read).
func (bb *boundedBody) excuse(d time.Duration) {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	if bb.done || bb.stalled {
		return
	}
	bb.since = bb.since.Add(d)
	bb.deadline = bb.deadline.Add(d)
	bb.rc.SetReadDeadline(bb.deadline)
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
			bb.deadline = time.Now().Add(bb.stall)
			bb.rc.SetReadDeadline(bb.deadline)
		}
		bb.got += int64(n)
		if el := time.Since(bb.since); el >= bb.window {
			if float64(bb.got) < float64(bb.rate)*el.Seconds() {
				// Every later read fails at once too, as a stalled
				// one's does: net/http would otherwise drain the rest
				// of the body at the trickle's pace.
				bb.stalled, bb.slow = true, true
				bb.rc.SetReadDeadline(readEnded)
				return 0, errBelowFloor
			}
			bb.since, bb.got = time.Now(), 0
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
		bb.release()
	}
}

// stalledBody reports whether r's body ended for the stall bound or
// the floor rate, and whether for the floor rate.
func stalledBody(r *http.Request) (stalled, slow bool) {
	bb, ok := r.Body.(*boundedBody)
	if !ok {
		return false, false
	}
	bb.mu.Lock()
	defer bb.mu.Unlock()
	return bb.stalled, bb.slow
}
