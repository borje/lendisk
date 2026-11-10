package pace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
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
	drain(t, lendIn(p, t.Context(), "B", zeros(1<<20), func(d time.Duration) { waited += d }))
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
	drain(t, lendIn(p, t.Context(), "C", zeros(4<<20), nil))
	io.Copy(p.LendOut(t.Context(), "C", io.Discard), zeros(4<<20))
	if el := c.now().Sub(start); el != 0 {
		t.Fatalf("C unlimited took %v", el)
	}
}

// A nil Pacer paces nothing and is always open.
func TestANilPacerPassesThrough(t *testing.T) {
	var p *Pacer
	if drain(t, p.Send(t.Context(), "B", zeros(10))) != 10 || drain(t, p.Fetch(t.Context(), "B", zeros(10))) != 10 ||
		drain(t, lendIn(p, t.Context(), "B", zeros(10), nil)) != 10 || !p.Open() {
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

// LEND-5 (#87): a wait is credited to a body only up to its fair share
// of the borrower's limit: the bytes it brought at the limit, times the
// borrower's bodies under way. A body trickling a byte at a time behind
// another that keeps the limit busy waits as long as that one, but is
// credited a byte's share, so the floor rate and the stall bound still
// end it; the busy body is credited its whole wait.
func TestALendWaitIsCreditedOnlyUpToTheBodysFairShare(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	p := New(func() time.Time { return now }) // waits do not move the clock: every reservation contends
	p.Sleep = func(context.Context, time.Duration) error { return nil }
	p.Set(Settings{Lend: map[string]Rates{"B": {Down: 64 << 10}}})
	var busyWaits, trickleWaits []time.Duration
	busy, doneBusy := p.LendIn(t.Context(), "B", zeros(1<<20), func(d time.Duration) { busyWaits = append(busyWaits, d) })
	trickle, doneTrickle := p.LendIn(t.Context(), "B", zeros(10), func(d time.Duration) { trickleWaits = append(trickleWaits, d) })
	defer doneTrickle()
	buf := make([]byte, Burst)
	io.ReadFull(busy, buf) // the burst: no wait
	io.ReadFull(busy, buf) // 1 s behind it
	if len(busyWaits) != 1 || busyWaits[0] != time.Second {
		t.Fatalf("the busy body was credited %v, want its whole 1 s wait", busyWaits)
	}
	if n, _ := trickle.Read(buf[:1]); n != 1 {
		t.Fatal(n)
	}
	// It waited about 1 s, behind the busy body's reservation; its share
	// is 1 byte at 64 KiB/s for each of 2 bodies.
	share := 2 * time.Second / (64 << 10)
	if len(trickleWaits) != 1 || trickleWaits[0] > share+time.Microsecond {
		t.Fatalf("a 1-byte read behind a busy body was credited %v, want at most %v", trickleWaits, share)
	}
	// With the busy body ended, the trickling one is alone.
	doneBusy()
	doneBusy() // done is idempotent
	trickle.Read(buf[:1])
	if len(trickleWaits) != 2 || trickleWaits[1] > share/2+time.Microsecond {
		t.Fatalf("alone, a 1-byte read was credited %v, want at most %v", trickleWaits, share/2)
	}
}

// LEND-5 (#107): an abandoned body still under way (its handler waits
// on the store, so it reads nothing, and its done is not yet called)
// counts among the borrower's bodies, which raises an honest body's
// fair share and never lowers it: each of the honest body's waits at
// the limit is credited whole, beside it and once it is done. So the
// floor rate and the stall bound see none of the lender's pacing.
func TestAnHonestBodyBesideAnAbandonedOneIsCreditedItsWholeWait(t *testing.T) {
	c := newClock()
	p := paced(c, Settings{Lend: map[string]Rates{"B": {Down: Burst}}}) // a burst a second
	_, doneAbandoned := p.LendIn(t.Context(), "B", zeros(1<<20), nil)
	var waits []time.Duration
	honest, doneHonest := p.LendIn(t.Context(), "B", zeros(8*Burst), func(d time.Duration) { waits = append(waits, d) })
	defer doneHonest()
	buf := make([]byte, Burst)
	for i := range 4 {
		if i == 2 {
			doneAbandoned() // the abandoned body ends mid-way
		}
		start := c.now()
		if _, err := io.ReadFull(honest, buf); err != nil {
			t.Fatal(err)
		}
		slept := c.now().Sub(start)
		if i == 0 {
			continue // the burst: no wait
		}
		if slept != time.Second {
			t.Fatalf("read %d waited %v; want 1 s at a burst a second", i, slept)
		}
	}
	if len(waits) != 3 {
		t.Fatalf("credited %v; want 3 waits", waits)
	}
	for i, d := range waits {
		if d != time.Second {
			t.Fatalf("wait %d was credited %v of its 1 s; want all of it", i+1, d)
		}
	}
}

// #87: a friend's limiter made on its first transfer has the rates of
// the last Set, though the Set raced with that transfer's start: the
// settings are read where the limiter is made. Racy by nature; before
// the fix a round now and then made the limiter at the old rate.
func TestALimiterMadeDuringASetHasTheNewRate(t *testing.T) {
	p := New(nil)
	old := Settings{Lend: map[string]Rates{}, Peers: map[string]Rates{}}
	cur := Settings{Lend: map[string]Rates{}, Peers: map[string]Rates{}}
	const rounds = 500
	for i := range rounds {
		peer := fmt.Sprintf("P%d", i)
		old.Lend[peer], old.Peers[peer] = Rates{Up: MinRate, Down: MinRate}, Rates{Up: MinRate, Down: MinRate}
		cur.Lend[peer], cur.Peers[peer] = Rates{Up: 4 * MinRate, Down: 4 * MinRate}, Rates{Up: 4 * MinRate, Down: 4 * MinRate}
	}
	want := rate.Limit(4 * MinRate)
	for i := range rounds {
		peer := fmt.Sprintf("P%d", i)
		p.Set(old)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); p.Set(cur) }()
		go func() {
			defer wg.Done()
			p.Send(t.Context(), peer, zeros(0))
			_, done := p.LendIn(t.Context(), peer, zeros(0), nil)
			done()
		}()
		wg.Wait()
		p.mu.Lock()
		up, in := p.peers[peer].up.Limit(), p.lend[peer].down.Limit()
		p.mu.Unlock()
		if up != want || in != want {
			t.Fatalf("round %d: limiters made beside a Set at %v and %v, want %v", i, up, in, want)
		}
	}
}

// lendIn is LendIn's reader alone, for a body that ends with the test.
func lendIn(p *Pacer, ctx context.Context, peer string, r io.Reader, waited func(time.Duration)) io.Reader {
	pr, _ := p.LendIn(ctx, peer, r, waited)
	return pr
}

// REPL-7 (#87): like uploads, a download limit admits only as many
// fetches at once as it can keep at MinRate each (at least one), the
// global limit and the friend's alike, so many read-throughs sharing
// it do not slow each below what a lender's write deadline allows.
func TestFetchSlotsKeepEveryFetchAboveMinRate(t *testing.T) {
	p := New(nil)
	p.Set(Settings{Down: 2*MinRate + MinRate/2, Peers: map[string]Rates{"B": {Down: MinRate}}})
	admitted := func(peer string) (func(), bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		r, err := p.FetchSlot(ctx, peer)
		return r, err == nil
	}
	b1, ok := admitted("B")
	if !ok {
		t.Fatal("B's first fetch waits")
	}
	if _, ok := admitted("B"); ok {
		t.Fatal("a second fetch from B admitted at B's limit of MinRate")
	}
	c1, ok := admitted("C")
	if !ok {
		t.Fatal("C's fetch waits behind B's own limit")
	}
	if _, ok := admitted("D"); ok {
		t.Fatal("a third fetch admitted at 2.5 × MinRate")
	}
	got := make(chan func(), 1)
	go func() {
		r, err := p.FetchSlot(t.Context(), "D")
		if err != nil {
			t.Error(err)
		}
		got <- r
	}()
	c1()
	(<-got)()
	b1()
	b1() // release is idempotent
	p.Set(Settings{})
	for range 10 {
		if _, ok := admitted("B"); !ok {
			t.Fatal("unlimited, a fetch waits")
		}
	}
	var none *Pacer
	if r, err := none.FetchSlot(t.Context(), "B"); err != nil {
		t.Fatal(err)
	} else {
		r()
	}
}
