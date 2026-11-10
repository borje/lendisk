// Package pace paces the bytes this node moves to and from its friends
// (REPL-7, LEND-5; DESIGN §3.4 REPL-7): token-bucket rate limits on what
// it sends and fetches as a borrower, globally and per friend; the
// replication schedule (time-of-day windows); the upload slots that keep
// every request body above a lender's floor rate (#76); and, as a
// lender, a limit per borrower on what it receives from and sends to
// that borrower; and the disk reads of the bit-rot sampler (VER-5,
// #31), which move no bytes over the network but compete with backups
// for a Pi's disk.
//
// Rules a caller must keep:
//   - Pace by concurrency, never by trickling one body: take a Slot for
//     each request body sent (a single PUT or one part) and release it
//     when the body ends. A global upload limit admits only as many
//     bodies at once as it can keep at MinRate each, and every limit is
//     at least MinRate (CheckRate), so a paced body never arrives at a
//     lender below its floor rate. Likewise take a FetchSlot for each
//     fetch and release it when its body is closed (#87).
//   - Call LendIn's done when the body ends: until then the body counts
//     among the borrower's, whose waits are credited by fair share.
//   - Each read through a paced reader is cut to Burst before it waits,
//     so a changed limit applies from the next read (Set, Q-Y.4).
//   - The clock and the sleep are fields (Now, Sleep): tests set them.
//   - A nil *Pacer paces nothing and is always open.
package pace

import (
	"context"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Burst is each token bucket's size and the most one paced read or
// write moves before it waits (Q-Y.4).
const Burst = 64 << 10

// MinRate is the lowest limit a setting may have, and what a global
// upload limit keeps each body above: twice the lender's floor rate
// (bridge.DefaultFloorRate, 8 KiB/s over 10 minutes, #76), so a body
// paced here is never ended there for being slow.
const MinRate = 16 << 10

// CheckRate refuses a limit above zero and below MinRate.
func CheckRate(bytesPerSecond int64) error {
	if bytesPerSecond != 0 && bytesPerSecond < MinRate {
		return fmt.Errorf("%d bytes/s is below 16K/s, the least a friend accepts a request body at "+
			"(twice its floor rate of 8K/s); give at least 16K, or nothing for no limit", bytesPerSecond)
	}
	if bytesPerSecond < 0 {
		return fmt.Errorf("a negative rate")
	}
	return nil
}

// Rates are a pair of limits in bytes a second; zero is no limit.
type Rates struct {
	Up   int64 // what this node sends
	Down int64 // what it receives
}

// Settings are the limits and the schedule, from the configuration.
type Settings struct {
	// Up and Down limit this node's own transfers as a borrower, all
	// friends together: replica sends, and read-through fetches.
	Up, Down int64
	// Peers limits them per friend (node ID).
	Peers map[string]Rates
	// Lend limits, per borrower (node ID), what this node receives from
	// it (Down: its puts and parts) and sends it (Up: answers to its
	// reads), LEND-5. The global limits do not apply to lending.
	Lend map[string]Rates
	// Schedule is when replication sends; empty is always.
	Schedule Schedule
	// PartSize is the size of each part of an object sent in parts, and
	// the largest object sent in one PUT (REPL-6); 0 is DefaultPartSize.
	PartSize int64
	// Scrub limits the bit-rot sampler's reads of the local store
	// (VER-5, store.bitrot_read_limit); zero is no limit.
	Scrub int64
}

// DefaultPartSize is replication.part_size's default (REPL-6).
const DefaultPartSize = 16 << 20

// PartSize is the part size in force (REPL-6).
func (p *Pacer) PartSize() int64 {
	if s := p.Settings(); s.PartSize > 0 {
		return s.PartSize
	}
	return DefaultPartSize
}

// Pacer holds the token buckets for one daemon. Set changes them while
// transfers run.
type Pacer struct {
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Sleep waits d or until ctx ends; nil means a timer.
	Sleep func(ctx context.Context, d time.Duration) error

	mu       sync.Mutex
	s        Settings
	up, down *rate.Limiter
	scrub    *rate.Limiter
	peers    map[string]*pair
	lend     map[string]*pair
	slots    int           // bodies holding an upload slot
	fetches  int           // fetches holding a fetch slot
	freed    chan struct{} // closed when a slot frees or the limit changes
}

// pair is one friend's limiters. bodies counts what is under way on
// them: as a lender, the borrower's request bodies being received
// (LendIn); as a borrower, the fetches from the friend (FetchSlot).
type pair struct {
	up, down *rate.Limiter
	bodies   int
}

// New returns a pacer with no limits, open always.
func New(now func() time.Time) *Pacer {
	return &Pacer{Now: now, up: unlimited(), down: unlimited(), scrub: unlimited(), peers: map[string]*pair{}, lend: map[string]*pair{},
		freed: make(chan struct{})}
}

func unlimited() *rate.Limiter { return rate.NewLimiter(rate.Inf, Burst) }

func limitOf(n int64) rate.Limit {
	if n <= 0 {
		return rate.Inf
	}
	return rate.Limit(n)
}

func (p *Pacer) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Pacer) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil // no wait: a body is never failed by its context here
	}
	if p.Sleep != nil {
		return p.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Set applies s from each transfer's next read, and admits waiting
// bodies a raised upload limit has room for.
func (p *Pacer) Set(s Settings) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	s.Peers, s.Lend = maps.Clone(s.Peers), maps.Clone(s.Lend)
	p.s = s
	p.up.SetLimitAt(now, limitOf(s.Up))
	p.down.SetLimitAt(now, limitOf(s.Down))
	p.scrub.SetLimitAt(now, limitOf(s.Scrub))
	set := func(m map[string]*pair, rs map[string]Rates) {
		for peer, l := range m {
			r := rs[peer]
			l.up.SetLimitAt(now, limitOf(r.Up))
			l.down.SetLimitAt(now, limitOf(r.Down))
		}
	}
	set(p.peers, s.Peers)
	set(p.lend, s.Lend)
	close(p.freed)
	p.freed = make(chan struct{})
}

// Settings returns what Set last applied.
func (p *Pacer) Settings() Settings {
	if p == nil {
		return Settings{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.s
}

// of returns peer's pair, as a lender's borrower (lend) or as a
// friend this node sends to and fetches from, made on first use with
// the rates in force. The rates are read here, under the lock Set
// holds, so a Set beside the first use cannot leave the old rate.
func (p *Pacer) of(lend bool, peer string) *pair {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ofLocked(lend, peer)
}

func (p *Pacer) ofLocked(lend bool, peer string) *pair {
	m, rs := p.peers, p.s.Peers
	if lend {
		m, rs = p.lend, p.s.Lend
	}
	l := m[peer]
	if l == nil {
		r := rs[peer]
		l = &pair{up: rate.NewLimiter(limitOf(r.Up), Burst), down: rate.NewLimiter(limitOf(r.Down), Burst)}
		m[peer] = l
	}
	return l
}

// Open reports whether the schedule lets replication send now.
func (p *Pacer) Open() bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	s := p.s.Schedule
	p.mu.Unlock()
	return s.Open(p.now())
}

// Slot admits one request body to be sent, waiting while the global
// upload limit already carries as many bodies as it can keep at MinRate
// each (at least one). The caller releases it when the body ends.
func (p *Pacer) Slot(ctx context.Context) (release func(), err error) {
	if p == nil {
		return func() {}, nil
	}
	return p.admit(ctx, func() (func(), bool) {
		if !room(p.s.Up, p.slots) {
			return nil, false
		}
		p.slots++
		return func() { p.slots-- }, true
	})
}

// FetchSlot admits one fetch from peer (the read-through, REPL-7),
// waiting while the global download limit, or peer's own, already
// carries as many fetches as it can keep at MinRate each (at least one
// for each). So many reads sharing a limit do not slow each past what
// the lender's per-write deadline allows (§3.9 *Per-friend caps*). The
// caller releases it when the body is closed.
func (p *Pacer) FetchSlot(ctx context.Context, peer string) (release func(), err error) {
	if p == nil {
		return func() {}, nil
	}
	return p.admit(ctx, func() (func(), bool) {
		pr := p.ofLocked(false, peer)
		if !room(p.s.Down, p.fetches) || !room(p.s.Peers[peer].Down, pr.bodies) {
			return nil, false
		}
		p.fetches++
		pr.bodies++
		return func() { p.fetches--; pr.bodies-- }, true
	})
}

// room reports whether a limit of limit bytes a second keeps one more
// body than the n it carries at MinRate (always, with no limit).
func room(limit int64, n int) bool {
	return limit <= 0 || n < max(1, int(limit/MinRate))
}

// admit waits until take, called under p.mu, takes a slot; the release
// it returns is called under p.mu too, once, and wakes the waiters.
func (p *Pacer) admit(ctx context.Context, take func() (func(), bool)) (release func(), err error) {
	for {
		p.mu.Lock()
		if give, ok := take(); ok {
			p.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					p.mu.Lock()
					give()
					close(p.freed)
					p.freed = make(chan struct{})
					p.mu.Unlock()
				})
			}, nil
		}
		freed := p.freed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-freed:
		}
	}
}

// Send paces r, a body this node sends to peer as a borrower: peer's
// limit and the global one.
func (p *Pacer) Send(ctx context.Context, peer string, r io.Reader) io.Reader {
	if p == nil {
		return r
	}
	return &reader{ctx: ctx, r: r, p: p, lims: []*rate.Limiter{p.of(false, peer).up, p.up}}
}

// Fetch paces r, a body this node fetches from peer as a borrower (the
// read-through): peer's download limit and the global one.
func (p *Pacer) Fetch(ctx context.Context, peer string, r io.Reader) io.Reader {
	if p == nil {
		return r
	}
	return &reader{ctx: ctx, r: r, p: p, lims: []*rate.Limiter{p.of(false, peer).down, p.down}}
}

// Scrub paces r, the bit-rot sampler's read of an object in the local
// store (VER-5), by the scrub limit alone.
func (p *Pacer) Scrub(ctx context.Context, r io.Reader) io.Reader {
	if p == nil {
		return r
	}
	return &reader{ctx: ctx, r: r, p: p, lims: []*rate.Limiter{p.scrub}}
}

// LendIn paces r, a request body borrower peer sends this node, by its
// lending limit (LEND-5). The caller calls done when the body ends:
// until then it is one of the borrower's bodies sharing the limit.
// waited, if set, is told the part of each wait that is this node's
// doing and not the borrower's slowness: the wait, but at most the
// body's fair share: what the bytes the read brought take at the
// limit divided among the borrower's bodies under way (#87). A body
// trickling behind another that keeps the limit busy waits as long as
// that one, but is credited only what its own bytes needed.
func (p *Pacer) LendIn(ctx context.Context, peer string, r io.Reader, waited func(time.Duration)) (paced io.Reader, done func()) {
	if p == nil {
		return r, func() {}
	}
	p.mu.Lock()
	pr := p.ofLocked(true, peer)
	pr.bodies++
	p.mu.Unlock()
	rd := &reader{ctx: ctx, r: r, p: p, lims: []*rate.Limiter{pr.down}}
	if waited != nil {
		rd.waited = func(n int, d time.Duration) {
			p.mu.Lock()
			k := max(1, pr.bodies)
			p.mu.Unlock()
			if lim := pr.down.Limit(); lim != rate.Inf && lim > 0 {
				// As rate computes a reservation's delay.
				d = min(d, time.Duration(float64(time.Second)*(float64(n*k)/float64(lim))))
			}
			waited(d)
		}
	}
	var once sync.Once
	return rd, func() {
		once.Do(func() {
			p.mu.Lock()
			pr.bodies--
			p.mu.Unlock()
		})
	}
}

// LendOut paces w, an answer this node sends borrower peer, by its
// lending limit (LEND-5).
func (p *Pacer) LendOut(ctx context.Context, peer string, w io.Writer) io.Writer {
	if p == nil {
		return w
	}
	return &writer{ctx: ctx, w: w, p: p, lim: p.of(true, peer).up}
}

// wait takes n bytes from every limiter in lims at once and sleeps for
// the longest delay; it returns how long it slept.
func (p *Pacer) wait(ctx context.Context, lims []*rate.Limiter, n int) (time.Duration, error) {
	now := p.now()
	var d time.Duration
	for _, l := range lims {
		r := l.ReserveN(now, n)
		if !r.OK() {
			return 0, fmt.Errorf("pacing %d bytes: more than a burst", n)
		}
		d = max(d, r.DelayFrom(now))
	}
	return d, p.sleep(ctx, d)
}

type reader struct {
	ctx    context.Context
	r      io.Reader
	p      *Pacer
	lims   []*rate.Limiter
	waited func(n int, d time.Duration) // told each read's wait
}

func (r *reader) Read(b []byte) (int, error) {
	if len(b) > Burst {
		b = b[:Burst]
	}
	n, err := r.r.Read(b)
	if n > 0 {
		d, werr := r.p.wait(r.ctx, r.lims, n)
		if d > 0 && r.waited != nil {
			r.waited(n, d)
		}
		if werr != nil {
			return n, werr
		}
	}
	return n, err
}

type writer struct {
	ctx context.Context
	w   io.Writer
	p   *Pacer
	lim *rate.Limiter
}

func (w *writer) Write(b []byte) (int, error) {
	var done int
	for len(b) > 0 {
		c := b[:min(len(b), Burst)]
		if _, err := w.p.wait(w.ctx, []*rate.Limiter{w.lim}, len(c)); err != nil {
			return done, err
		}
		n, err := w.w.Write(c)
		done += n
		if err != nil {
			return done, err
		}
		b = b[n:]
	}
	return done, nil
}
