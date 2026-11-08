// Package quota exchanges each friendship's numbers with the friend
// (LEND-7, LEND-8, PEER-6, SEC-15; DESIGN §3.2): the quota call, made by
// the daemon to every friend.
//
// The Exchanger calls a friend at once when this node's own number for
// the friendship is not yet told (`peer quota` changed it, in this
// process or another), on Due (the friend answered a write with 507),
// and otherwise every Refresh, so each side keeps the other's number,
// use and repository size, and this node's count of its use there is
// reconciled from the friend's own (#68). A call that fails is repeated
// after a backoff from MinBackoff doubling to MaxBackoff until the
// friend answers. Each pass also compares every friendship's quota with
// the one it last saw and passes a change to Changed, whatever made it
// (this node's number, the friend's call, the friend's answer).
//
// Rules a caller must keep:
//   - Changed runs on the pass's goroutine and must not block.
//   - The friend's figures are its word: they are stored as reported,
//     after control.QuotaResponse.Check, and never change this node's
//     own number.
package quota

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

const (
	defaultPoll       = 10 * time.Second
	defaultRefresh    = time.Hour
	defaultMinBackoff = 10 * time.Second
	defaultMaxBackoff = 5 * time.Minute
	defaultTimeout    = 30 * time.Second
)

// DialFunc opens a connection to a friend's tunnel port
// (transport.Transport.Dial).
type DialFunc func(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error)

// Exchanger makes the quota call to every friend (package comment).
type Exchanger struct {
	DB *state.DB
	// Dial reaches a friend's tunnel port; Call uses it when nil.
	Dial DialFunc
	// Call makes one quota call to f; nil means the control API over
	// Dial. Tests set it.
	Call func(ctx context.Context, f state.Friendship, req control.QuotaRequest) (control.QuotaResponse, error)
	// Path, if set, says how an answered call reached the friend
	// ("direct" or "relayed"; "" unknown).
	Path func(ctx context.Context, peer string) string
	// Changed, if set, is called with a friendship's quota before and
	// after, each time a pass finds it changed, and with before = -1
	// when a pass first sees the friendship: a quota lowered before
	// then (a friendship made and lowered within one Poll) must still
	// reach the writes in flight.
	Changed func(peer string, before, after int64)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Poll is the time between passes (default 10 s): a number `peer
	// quota` changed in another process is told within it.
	Poll time.Duration
	// Refresh is how long after an answer a friend is called again
	// (default an hour).
	Refresh time.Duration
	// MinBackoff and MaxBackoff bound the wait after failed calls in a
	// row (defaults 10 s and 5 minutes).
	MinBackoff, MaxBackoff time.Duration
	// Timeout bounds one call (default 30 s).
	Timeout time.Duration
	Log     *slog.Logger

	once     sync.Once
	kick     chan struct{}
	mu       sync.Mutex
	quotas   map[string]int64         // peer -> quota last seen
	answered map[string]time.Time     // peer -> last answer
	next     map[string]time.Time     // peer -> not before, after a failure
	backoff  map[string]time.Duration // peer -> the wait after its next failure
	due      map[string]bool          // peer -> asked for by Due
	inflight map[string]bool
	wg       sync.WaitGroup
}

func or(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func (x *Exchanger) init() {
	x.once.Do(func() {
		x.kick = make(chan struct{}, 1)
		x.quotas, x.answered, x.next = map[string]int64{}, map[string]time.Time{}, map[string]time.Time{}
		x.backoff, x.due, x.inflight = map[string]time.Duration{}, map[string]bool{}, map[string]bool{}
	})
}

func (x *Exchanger) now() time.Time {
	if x.Now == nil {
		return time.Now()
	}
	return x.Now()
}

// Kick asks for a pass now. It never blocks.
func (x *Exchanger) Kick() {
	x.init()
	select {
	case x.kick <- struct{}{}:
	default:
	}
}

// Due asks for a call to peer at the next pass, whatever its backoff
// (a 507 from it, #68), and kicks a pass.
func (x *Exchanger) Due(peer string) {
	x.init()
	x.mu.Lock()
	x.due[peer] = true
	delete(x.next, peer)
	x.mu.Unlock()
	x.Kick()
}

// Wait waits for every call in flight.
func (x *Exchanger) Wait() { x.wg.Wait() }

// Run makes passes until ctx ends: at once, every Poll and on Kick.
func (x *Exchanger) Run(ctx context.Context) {
	x.init()
	defer x.Wait()
	t := time.NewTicker(or(x.Poll, defaultPoll))
	defer t.Stop()
	for {
		x.Pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-x.kick:
		}
	}
}

// Pass makes one pass: it passes changed quotas to Changed and starts a
// call to every friend due one. It does not wait for the calls.
func (x *Exchanger) Pass(ctx context.Context) {
	x.init()
	fs, err := x.DB.Friendships(ctx)
	if err != nil {
		x.logErr(ctx, "quota: reading friendships failed", err)
		return
	}
	now := x.now()
	type changed struct {
		peer          string
		before, after int64
	}
	var changes []changed
	var todo []state.Friendship
	x.mu.Lock()
	seen := map[string]bool{}
	for _, f := range fs {
		seen[f.Peer] = true
		q := f.Quota()
		if old, ok := x.quotas[f.Peer]; !ok {
			changes = append(changes, changed{f.Peer, -1, q})
		} else if old != q {
			changes = append(changes, changed{f.Peer, old, q})
		}
		x.quotas[f.Peer] = q
		if f.Address == "" || x.inflight[f.Peer] || now.Before(x.next[f.Peer]) {
			continue
		}
		last, ok := x.answered[f.Peer]
		if f.OwnNumber != f.ToldNumber || x.due[f.Peer] || !ok || now.Sub(last) >= or(x.Refresh, defaultRefresh) {
			delete(x.due, f.Peer)
			x.inflight[f.Peer] = true
			todo = append(todo, f)
		}
	}
	for p := range x.quotas {
		if !seen[p] { // removed
			delete(x.quotas, p)
			delete(x.answered, p)
			delete(x.next, p)
			delete(x.backoff, p)
			delete(x.due, p)
		}
	}
	x.mu.Unlock()
	for _, c := range changes {
		if c.before >= 0 {
			x.Log.Info("quota: the friendship's quota changed", "peer", c.peer, "before", c.before, "after", c.after)
		}
		if x.Changed != nil {
			x.Changed(c.peer, c.before, c.after)
		}
	}
	if len(todo) == 0 {
		return
	}
	use, err := x.DB.PlaceUse(ctx)
	if err == nil {
		var size int64
		if size, err = x.DB.RepositorySize(ctx); err == nil {
			for _, f := range todo {
				req := control.QuotaRequest{Number: f.OwnNumber, Use: min(max(use[f.Peer], 0), control.MaxNumber-1),
					RepositorySize: control.RoundSize(size)}
				x.wg.Add(1)
				go x.exchange(ctx, f, req)
			}
			return
		}
	}
	x.logErr(ctx, "quota: reading this node's use failed", err)
	x.mu.Lock()
	for _, f := range todo {
		delete(x.inflight, f.Peer)
	}
	x.mu.Unlock()
}

// exchange makes one call to f and records its outcome.
func (x *Exchanger) exchange(ctx context.Context, f state.Friendship, req control.QuotaRequest) {
	defer x.wg.Done()
	cctx, cancel := context.WithTimeout(ctx, or(x.Timeout, defaultTimeout))
	resp, err := x.call(cctx, f, req)
	cancel()
	if ctx.Err() != nil {
		x.mu.Lock()
		delete(x.inflight, f.Peer)
		x.mu.Unlock()
		return
	}
	now := x.now()
	if err != nil {
		x.mu.Lock()
		delete(x.inflight, f.Peer)
		b := or(x.backoff[f.Peer], or(x.MinBackoff, defaultMinBackoff))
		x.next[f.Peer] = now.Add(b)
		x.backoff[f.Peer] = min(2*b, or(x.MaxBackoff, defaultMaxBackoff))
		x.mu.Unlock()
		lvl := slog.LevelDebug
		if f.ExchangeError == "" || f.OwnNumber != f.ToldNumber {
			lvl = slog.LevelWarn
		}
		x.Log.Log(ctx, lvl, "quota: the call to the friend failed; retrying until it answers", "peer", f.Peer,
			"number", req.Number, "retry_in", b, "err", err)
		if err := x.DB.QuotaFailed(ctx, f.Peer, err.Error(), now); err != nil {
			x.logErr(ctx, "quota: recording a failed call failed", err)
		}
		return
	}
	path := ""
	if x.Path != nil {
		path = x.Path(ctx, f.Peer)
	}
	_, err = x.DB.QuotaAnswered(ctx, f.Peer, req.Number, state.QuotaAnswer{Number: resp.Number, Use: resp.Use, Lent: resp.Lent,
		RepositorySize: resp.RepositorySize, Protocol: resp.Protocol, Path: path}, now)
	x.mu.Lock()
	delete(x.inflight, f.Peer)
	if err == nil {
		x.answered[f.Peer] = now
		delete(x.backoff, f.Peer)
		delete(x.next, f.Peer)
	}
	x.mu.Unlock()
	if err != nil {
		x.logErr(ctx, "quota: recording the friend's answer failed", err)
		return
	}
	if f.OwnNumber != f.ToldNumber {
		x.Log.Info("quota: the friend was told this node's number", "peer", f.Peer, "number", req.Number,
			"their_number", resp.Number, "quota", resp.Quota)
	}
	x.Kick() // a changed quota reaches Changed
}

func (x *Exchanger) call(ctx context.Context, f state.Friendship, req control.QuotaRequest) (control.QuotaResponse, error) {
	if x.Call != nil {
		return x.Call(ctx, f, req)
	}
	peer, err := identity.ParseNodeID(f.Peer)
	if err != nil {
		return control.QuotaResponse{}, err
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return x.Dial(ctx, peer, f.Address)
	}}
	defer tr.CloseIdleConnections()
	return (&control.Client{HTTP: &http.Client{Transport: tr}}).Quota(ctx, req)
}

func (x *Exchanger) logErr(ctx context.Context, msg string, err error) {
	if ctx.Err() == nil {
		x.Log.Error(msg, "err", err)
	}
}
