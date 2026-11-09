package pace

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// clock is a fake clock whose sleeps advance it: a paced transfer's
// time is what its sleeps add up to, with no wall clock involved.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return ctx.Err()
}

func paced(c *clock, s Settings) *Pacer {
	p := New(c.now)
	p.Sleep = c.sleep
	p.Set(s)
	return p
}

func drain(t *testing.T, r io.Reader) int64 {
	t.Helper()
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func zeros(n int) io.Reader { return bytes.NewReader(make([]byte, n)) }

// REPL-7: a global upload limit holds to within one burst: 1 MiB at
// 256 KiB/s takes 4 s less the first burst, which is free.
func TestAnUploadLimitHolds(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Up: 256 << 10})
	start := c.now()
	if n := drain(t, p.Send(t.Context(), "B", zeros(1<<20))); n != 1<<20 {
		t.Fatal(n)
	}
	el := c.now().Sub(start)
	if el < 3750*time.Millisecond || el > 4*time.Second {
		t.Fatalf("1 MiB at 256 KiB/s took %v", el)
	}
}

// REPL-7: a friend's own limit and the global one both hold; the
// smaller wins. Downloads have their own buckets.
func TestPeerAndGlobalLimitsBothHold(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Up: 1 << 20, Down: 64 << 10, Peers: map[string]Rates{"B": {Up: 128 << 10}}})
	start := c.now()
	drain(t, p.Send(t.Context(), "B", zeros(1<<20)))
	if el := c.now().Sub(start); el < 7*time.Second || el > 8*time.Second {
		t.Fatalf("B's 1 MiB at its 128 KiB/s took %v", el)
	}
	start = c.now()
	drain(t, p.Send(t.Context(), "C", zeros(1<<20)))
	if el := c.now().Sub(start); el > time.Second {
		t.Fatalf("C's 1 MiB at the global 1 MiB/s took %v", el)
	}
	start = c.now()
	drain(t, p.Fetch(t.Context(), "C", zeros(256<<10)))
	if el := c.now().Sub(start); el < 3*time.Second || el > 4*time.Second {
		t.Fatalf("256 KiB down at 64 KiB/s took %v", el)
	}
}

// REPL-7: a limit changed while a transfer runs applies from its next
// read (Q-Y.4), and removing it lifts it.
func TestALimitChangedMidTransferAppliesAtOnce(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Up: 64 << 10})
	r := p.Send(t.Context(), "B", zeros(4<<20))
	buf := make([]byte, 1<<20)
	io.ReadFull(r, buf[:256<<10]) // 4 bursts at 64 KiB/s: 3 s
	p.Set(Settings{Up: 1 << 20})
	start := c.now()
	io.ReadFull(r, buf) // 1 MiB at 1 MiB/s
	if el := c.now().Sub(start); el > 1100*time.Millisecond {
		t.Fatalf("1 MiB after raising the limit to 1 MiB/s took %v", el)
	}
	p.Set(Settings{})
	start = c.now()
	drain(t, r)
	if el := c.now().Sub(start); el != 0 {
		t.Fatalf("unlimited, the rest took %v", el)
	}
}

// REPL-7, #76: a global upload limit admits only as many request bodies
// at once as it can keep at MinRate each, so pacing never trickles one
// body below a lender's floor rate; raising the limit admits more.
func TestUploadSlotsKeepEveryBodyAboveTheFloor(t *testing.T) {
	p := New(nil)
	p.Set(Settings{Up: 3*MinRate + MinRate/2})
	var rel []func()
	for range 3 {
		r, err := p.Slot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		rel = append(rel, r)
	}
	got := make(chan func(), 1)
	go func() {
		r, err := p.Slot(t.Context())
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	select {
	case <-got:
		t.Fatal("a fourth body admitted at 3.5 × MinRate")
	case <-time.After(50 * time.Millisecond):
	}
	rel[0]()
	(<-got)()
	// Raising the limit admits more at once; unlimited admits any.
	p.Set(Settings{Up: 5 * MinRate})
	r, err := p.Slot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r()
	p.Set(Settings{})
	for range 10 {
		if _, err := p.Slot(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	// A cancelled wait returns its context's error.
	p.Set(Settings{Up: MinRate})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.Slot(ctx); err == nil {
		t.Fatal("a cancelled wait admitted")
	}
}

// LEND-5: a lender's limit on one borrower holds for what it receives
// and what it sends, leaves another borrower alone, and reports each
// wait, which the bridge does not count against the borrower's floor.
func TestALenderLimitsEachBorrowerIndependently(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Up: MinRate, Down: MinRate, Lend: map[string]Rates{"B": {Up: 64 << 10, Down: 128 << 10}}})
	var waited time.Duration
	start := c.now()
	drain(t, p.LendIn(t.Context(), "B", zeros(1<<20), func(d time.Duration) { waited += d }))
	el := c.now().Sub(start)
	if el < 7*time.Second || el > 8*time.Second || waited != el {
		t.Fatalf("1 MiB in from B at 128 KiB/s took %v, waits reported %v", el, waited)
	}
	start = c.now()
	var out bytes.Buffer
	if _, err := io.Copy(p.LendOut(t.Context(), "B", &out), zeros(256<<10)); err != nil || out.Len() != 256<<10 {
		t.Fatal(err, out.Len())
	}
	if el := c.now().Sub(start); el < 3*time.Second || el > 4*time.Second {
		t.Fatalf("256 KiB out to B at 64 KiB/s took %v", el)
	}
	// C has no limit, and the node's own replication limits do not
	// apply to what it lends.
	start = c.now()
	drain(t, p.LendIn(t.Context(), "C", zeros(4<<20), nil))
	io.Copy(p.LendOut(t.Context(), "C", io.Discard), zeros(4<<20))
	if el := c.now().Sub(start); el != 0 {
		t.Fatalf("C unlimited took %v", el)
	}
}

// A nil Pacer paces nothing and is always open.
func TestANilPacerPassesThrough(t *testing.T) {
	var p *Pacer
	if drain(t, p.Send(t.Context(), "B", zeros(10))) != 10 || drain(t, p.Fetch(t.Context(), "B", zeros(10))) != 10 ||
		drain(t, p.LendIn(t.Context(), "B", zeros(10), nil)) != 10 || !p.Open() {
		t.Fatal("nil pacer")
	}
	r, err := p.Slot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r()
}

// REPL-7: the schedule is local time-of-day windows; one ending before
// it starts wraps past midnight; empty is always open.
func TestScheduleWindows(t *testing.T) {
	s, err := ParseSchedule("01:00-07:00, 22:30-23:00")
	if err != nil {
		t.Fatal(err)
	}
	at := func(h, m int) time.Time { return time.Date(2026, 11, 8, h, m, 0, 0, time.UTC) }
	for _, c := range []struct {
		h, m int
		open bool
	}{{0, 59, false}, {1, 0, true}, {6, 59, true}, {7, 0, false}, {22, 29, false}, {22, 30, true}, {23, 0, false}} {
		if s.Open(at(c.h, c.m)) != c.open {
			t.Errorf("%02d:%02d open %v", c.h, c.m, !c.open)
		}
	}
	if next := s.Next(at(7, 0)); !next.Equal(at(22, 30)) {
		t.Errorf("next opening after 07:00: %v", next)
	}
	if next := s.Next(at(23, 30)); !next.Equal(at(1, 0).AddDate(0, 0, 1)) {
		t.Errorf("next opening after 23:30: %v", next)
	}
	wrap, _ := ParseSchedule("23:00-02:00")
	if !wrap.Open(at(23, 30)) || !wrap.Open(at(1, 59)) || wrap.Open(at(2, 0)) || wrap.Open(at(12, 0)) {
		t.Error("23:00-02:00 does not wrap")
	}
	var none Schedule
	if !none.Open(at(3, 0)) {
		t.Error("an empty schedule is closed")
	}
	for _, bad := range []string{"1-7", "01:00", "01:00-01:00", "25:00-26:00", "01:00-07:00,"} {
		if _, err := ParseSchedule(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if s.String() != "01:00-07:00,22:30-23:00" {
		t.Errorf("String %q", s.String())
	}
}

// REPL-7: the schedule decides with the pacer's clock.
func TestThePacerIsOpenByItsSchedule(t *testing.T) {
	c := newClock() // 12:00
	s, _ := ParseSchedule("01:00-07:00")
	p := paced(c, Settings{Schedule: s})
	if p.Open() {
		t.Fatal("open at noon")
	}
	c.sleep(t.Context(), 14*time.Hour) // 02:00
	if !p.Open() {
		t.Fatal("closed at 02:00")
	}
}

// Rates below MinRate are refused: they would make a lender end the
// body (#76).
func TestRatesBelowTheFloorAreRefused(t *testing.T) {
	if err := CheckRate(MinRate - 1); err == nil || !strings.Contains(err.Error(), "16K") {
		t.Fatalf("%v", err)
	}
	if CheckRate(0) != nil || CheckRate(MinRate) != nil {
		t.Fatal("0 or MinRate refused")
	}
}

// VER-5 (#31): the bit-rot sampler's disk reads hold to their own limit
// (store.bitrot_read_limit), apart from every network limit, and a
// changed limit applies from the next read.
func TestBitrotReadsHoldToTheirLimit(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Down: 64 << 10, Scrub: 512 << 10})
	start := c.now()
	if n := drain(t, p.Scrub(t.Context(), zeros(2<<20))); n != 2<<20 {
		t.Fatal(n)
	}
	if el := c.now().Sub(start); el < 3750*time.Millisecond || el > 4*time.Second {
		t.Fatalf("2 MiB at 512 KiB/s took %v", el)
	}
	p.Set(Settings{Scrub: 1 << 20})
	start = c.now()
	drain(t, p.Scrub(t.Context(), zeros(2<<20)))
	if el := c.now().Sub(start); el < 1900*time.Millisecond || el > 2*time.Second {
		t.Fatalf("2 MiB at 1 MiB/s took %v", el)
	}
	p.Set(Settings{})
	start = c.now()
	drain(t, p.Scrub(t.Context(), zeros(4<<20)))
	if el := c.now().Sub(start); el != 0 {
		t.Fatalf("no limit waited %v", el)
	}
	var none *Pacer
	if n := drain(t, none.Scrub(t.Context(), zeros(10))); n != 10 {
		t.Fatal(n)
	}
}
