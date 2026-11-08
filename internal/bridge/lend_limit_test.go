package bridge

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
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
