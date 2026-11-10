package bridge

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/pace"
)

// pace.MinRate, the least a limit may be, keeps a paced body above the
// floor rate with room to spare (#76, #23).
func TestTheLeastLimitIsTwiceTheFloorRate(t *testing.T) {
	if pace.MinRate < 2*DefaultFloorRate {
		t.Fatalf("pace.MinRate %d is below twice the floor rate %d", pace.MinRate, DefaultFloorRate)
	}
}

// LEND-5 (#23): a lender limits what one borrower sends it and what it
// answers that borrower, each at its own rate, and leaves another
// borrower alone. The lender's own pacing is not the borrower's
// slowness: a body it holds back is not ended for the floor rate, here
// set far above the limit to show it.
func TestALenderLimitsOneBorrowerWithoutEndingItsBodies(t *testing.T) {
	br := New(streamingGateway(t), slog.New(slog.DiscardHandler))
	br.Stall, br.MinProgress = time.Minute, 1
	br.FloorRate, br.FloorWindow = 1<<20, 300*time.Millisecond
	bob, carol := newID(t), newID(t)
	p := pace.New(nil)
	p.Set(pace.Settings{Lend: map[string]pace.Rates{bob.NodeID().String(): {Up: 128 << 10, Down: 128 << 10}}})
	br.Pace = p
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{})
	l.befriend(t, bob, carol)
	const size = 384 << 10
	put := func(who string, c *http.Client, key string) time.Duration {
		t.Helper()
		r, _ := http.NewRequestWithContext(t.Context(), "PUT", "http://lender:9000/sbucket/"+key, bytes.NewReader(make([]byte, size)))
		r.ContentLength = size
		r.Header.Set("Authorization", sigv4(who))
		start := time.Now()
		resp, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s's put: %d %s", who, resp.StatusCode, b)
		}
		return time.Since(start)
	}
	get := func(who string, c *http.Client) time.Duration {
		t.Helper()
		r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://lender:9000/sbucket/big", nil)
		r.Header.Set("Authorization", sigv4(who))
		start := time.Now()
		resp, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if n, err := io.CopyN(io.Discard, resp.Body, size); err != nil || n != size {
			t.Fatalf("%s's get: %d %v", who, n, err)
		}
		return time.Since(start)
	}
	// The first burst is free: (384 − 64) KiB at 128 KiB/s is 2.5 s at
	// the least. Lower bounds only: the race detector slows everything.
	least := time.Duration(float64(size-pace.Burst) / float64(128<<10) * float64(time.Second))
	bc, cc := l.tun.untimed(bob), l.tun.untimed(carol)
	if d := put(bob.NodeID().String(), bc, "in"); d < least {
		t.Fatalf("bob's %d bytes arrived in %v under a 128 KiB/s limit", size, d)
	}
	if d := get(bob.NodeID().String(), bc); d < least {
		t.Fatalf("bob read %d bytes in %v under a 128 KiB/s limit", size, d)
	}
	if d := put(carol.NodeID().String(), cc, "in"); d >= least {
		t.Logf("carol, unlimited, took %v (a slow machine?)", d)
	}
}

// zeroStream sends zeros until stop is closed, then fails the body.
type zeroStream struct{ stop chan struct{} }

func (z zeroStream) Read(p []byte) (int, error) {
	select {
	case <-z.stop:
		return 0, io.ErrUnexpectedEOF
	default:
	}
	clear(p)
	return len(p), nil
}

// trickle sends a byte every gap until stop is closed.
type trickle struct {
	gap  time.Duration
	stop chan struct{}
}

func (tr trickle) Read(p []byte) (int, error) {
	select {
	case <-tr.stop:
		return 0, io.ErrUnexpectedEOF
	case <-time.After(tr.gap):
	}
	p[0] = 't'
	return 1, nil
}

// lendPut sends a PUT of size bytes from body as who and returns the
// answer's status and body.
func lendPut(t *testing.T, c *http.Client, who, key string, body io.Reader, size int64) (int, string, error) {
	r, _ := http.NewRequestWithContext(t.Context(), "PUT", "http://lender:9000/sbucket/"+key, body)
	r.ContentLength = size
	r.Header.Set("Authorization", sigv4(who))
	resp, err := c.Do(r)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

// trickledBesideABusyBody runs one of bob's bodies at his whole lending
// limit and another trickling a byte every 20 ms beside it, and returns
// how the trickling one was answered, failing if it was not answered
// within give while the busy one still flowed.
func trickledBesideABusyBody(t *testing.T, br *Bridge, give time.Duration) (int, string) {
	t.Helper()
	bob := newID(t)
	p := pace.New(nil)
	p.Set(pace.Settings{Lend: map[string]pace.Rates{bob.NodeID().String(): {Down: 128 << 10}}})
	br.Pace = p
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{})
	l.befriend(t, bob)
	c := l.tun.untimed(bob)
	who := bob.NodeID().String()
	stop := make(chan struct{})
	busyDone := make(chan struct{})
	go func() {
		defer close(busyDone)
		lendPut(t, c, who, "busy", zeroStream{stop}, 1<<30)
	}()
	defer func() { close(stop); <-busyDone }()
	time.Sleep(300 * time.Millisecond) // the busy body holds the limit
	type answer struct {
		code int
		body string
		err  error
	}
	got := make(chan answer, 1)
	start := time.Now()
	go func() {
		code, body, err := lendPut(t, c, who, "trickle", trickle{20 * time.Millisecond, stop}, 1<<20)
		got <- answer{code, body, err}
	}()
	select {
	case a := <-got:
		t.Logf("the trickling body was answered after %v", time.Since(start))
		if a.err != nil {
			t.Fatalf("the trickling put: %v", a.err)
		}
		select {
		case <-busyDone:
			t.Fatal("the busy body ended before the trickling one was answered")
		default:
		}
		return a.code, a.body
	case <-time.After(give):
		t.Fatalf("a body trickling beside a busy one of the same borrower still holds its slot after %v", give)
	}
	return 0, ""
}

// LEND-2, LEND-5 (#87): a body is excused the lender's pacing only up
// to its own fair share of the borrower's limit. A borrower keeping one
// body at its limit cannot keep another trickling a byte at a time
// beside it: that one's waits behind the busy body are not its own,
// and the floor rate ends it.
func TestABodyTricklingBesideABusyOneIsEndedByTheFloorRate(t *testing.T) {
	br := New(streamingGateway(t), quiet)
	br.Stall, br.MinProgress = time.Minute, 1 // the stall bound never ends it
	br.FloorRate, br.FloorWindow = 16<<10, 300*time.Millisecond
	if code, body := trickledBesideABusyBody(t, br, 4*time.Second); code != http.StatusBadRequest || !strings.Contains(body, "RequestTimeout") {
		t.Fatalf("the trickling body: %d %s; want 400 RequestTimeout", code, body)
	}
}

// LEND-2, SEC-5 (#87): the lender's pacing moves a body's stall deadline
// only by the body's fair share; only MinProgress bytes set it a whole
// stall period ahead. A body trickling beside a busy one of the same
// borrower is ended by the stall bound.
func TestABodyTricklingBesideABusyOneIsEndedByTheStallBound(t *testing.T) {
	br := New(streamingGateway(t), quiet)
	br.Stall, br.MinProgress = time.Second, 64<<10
	br.FloorRate, br.FloorWindow = 1, time.Hour // the floor rate never ends it
	if code, body := trickledBesideABusyBody(t, br, 4*time.Second); code != http.StatusBadRequest || !strings.Contains(body, "RequestTimeout") {
		t.Fatalf("the trickling body: %d %s; want 400 RequestTimeout", code, body)
	}
}

// LEND-5 (#87): three honest bodies of one borrower share its limit of
// 128 KiB/s, each below the floor rate of 64 KiB/s by its share, and
// each is excused its share of the waits: none is ended.
func TestBodiesSharingABorrowersLimitAreAllServed(t *testing.T) {
	br := New(streamingGateway(t), quiet)
	br.Stall, br.MinProgress = 2*time.Second, 64<<10
	br.FloorRate, br.FloorWindow = 64<<10, time.Second
	bob := newID(t)
	p := pace.New(nil)
	p.Set(pace.Settings{Lend: map[string]pace.Rates{bob.NodeID().String(): {Down: 128 << 10}}})
	br.Pace = p
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{})
	l.befriend(t, bob)
	c := l.tun.untimed(bob)
	const size = 256 << 10
	errs := make(chan error, 3)
	for _, key := range []string{"one", "two", "three"} {
		go func() {
			code, body, err := lendPut(t, c, bob.NodeID().String(), key, bytes.NewReader(make([]byte, size)), size)
			if err == nil && code != http.StatusOK {
				err = fmt.Errorf("put %s: %d %s", key, code, body)
			}
			errs <- err
		}()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
