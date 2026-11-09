package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

// Worker drains every place's FIFO (REPL-1, REPL-3): one sender per
// friend, each sending its queued placement rows in write-sequence
// order, one object at a time, the places in parallel. A metadata row
// at the head of a FIFO waits until every earlier data object of its
// bucket is acknowledged wherever it is placed (the barrier,
// state.DB.Held), unsent data at a friend that stopped answering is
// re-routed (state.DB.Reroute), and copies are re-placed when the pool
// changes (state.DB.Replace, REPL-13): a copy re-placement deletes is a
// delete in its place's FIFO. An object is read
// from the store as it is sent, once per place, with its capture hash
// (REPL-10); the place's acknowledgement records the hash. The queue is
// lendisk.db, so a restart continues at the first unacknowledged row;
// an object larger than the part size goes in parts, recorded there as
// they complete, so a restart continues at the next part (REPL-6). Every
// body sent is paced by Pace (REPL-7), and nothing is sent outside its
// schedule. A sender with nothing to send probes its place every
// ProbeEvery (Prober, OPS-2, D6, #65), so an idle friend's condition
// and last answer stay current.
type Worker struct {
	DB       *state.DB
	StoreDir string // the posix store: an object is <StoreDir>/<bucket>/<key>
	Self     *identity.Identity
	Dial     DialFunc
	Client   Options
	Log      *slog.Logger

	// Connect builds a friend's client; nil means NewFriend over Dial.
	Connect func(f state.Friendship) (Replica, error)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Poll is how often an idle sender looks at its FIFO (default 1 s);
	// friendships are re-read every five polls.
	Poll time.Duration
	// MinBackoff and MaxBackoff bound the retry delay after an error
	// worth retrying (defaults 1 s and 5 minutes).
	MinBackoff, MaxBackoff time.Duration
	// RerouteEvery is how often unsent data at friends that stopped
	// answering is re-routed (state.DB.Reroute, §3.4; default 1 minute).
	RerouteEvery time.Duration
	// ReplaceEvery is how often re-placement runs when the pool has not
	// changed (state.DB.Replace, REPL-13; default an hour). A change to
	// the pool (state.DB.PoolFingerprint) runs it within five polls.
	ReplaceEvery time.Duration
	// RewriteRetry is how long a rewrite the place refuses without a
	// version to name instead is retried before it is refused for good
	// (default DefaultRewriteRetry, #74).
	RewriteRetry time.Duration
	// PauseRetry is how long a place paused for quota or removed waits
	// before trying again (default an hour, LEND-2).
	PauseRetry time.Duration
	// ProbeEvery is how long a sender with nothing to send (its FIFO
	// empty, its head held at the barrier, or the schedule closed) waits
	// after its last send or probe before it probes its place (Prober,
	// #65; default DefaultProbeEvery). A sender probes once when it
	// starts idle.
	ProbeEvery time.Duration
	// StoreStall is how long one call into the store (opening and
	// stating an object, stating its bucket's directory, one read while
	// it is sent or re-hashed) may take before it fails as the store's
	// failure (default 1 minute: half a friend's stall timeout, so a hung
	// disk is not blamed on the friend, #61, #63).
	StoreStall time.Duration
	// Alerted, if set, is called after the worker records a condition
	// that raises an alert (a refusal, a store failure), so the alert
	// service reads it at once (alert.Service.Check, #13). It must not
	// block.
	Alerted func()
	// Fill, if set, puts an own bucket's object the store lacks into it
	// from a friend that holds it, verified (the gateway's read-through,
	// EP-10): filled is false when no friend is recorded holding it, an
	// error means it may pass. A recovered node's store starts empty, and
	// its copies must still be re-placed (REPL-13, #26 review).
	Fill func(ctx context.Context, bucket, key string) (filled bool, err error)
	// Full, if set, is called after a place answered 507 (LEND-2), so
	// the daemon asks it for its own count of this node's use (the quota
	// call, #68). It must not block.
	Full func(place string)
	// OnRead, if set, is called after each read of a body sent to a
	// place, with the bytes of the object read so far. Tests only.
	OnRead func(place string, seq, read int64)
	// Pace paces every body sent and holds the schedule and the part
	// size (REPL-6, REPL-7); nil sends unpaced, at any time, in parts of
	// pace.DefaultPartSize.
	Pace *pace.Pacer

	// friendships reads the friendships; nil means DB.Friendships. Tests
	// set it to fail.
	friendships func(context.Context) ([]state.Friendship, error)
	// open opens a file in the store; nil means os.Open. Tests set it to
	// inject open, stat and read failures.
	open func(name string) (fs.File, error)
	// stat stats a bucket's directory in the store; nil means os.Stat.
	// Tests set it to hang.
	stat func(name string) (fs.FileInfo, error)

	// flight guards sending: the row each sender has taken from its FIFO
	// and not yet recorded the outcome of, which re-routing skips (a send
	// that lands must find its row, §3.4). A sender takes its row and a
	// re-routing pass reads the set under it, so neither misses the other.
	flight  sync.Mutex
	sending map[string]int64 // place -> seq
	// wakes are the senders' wake-ups from a pause (Resume), per place,
	// under flight.
	wakes map[string]chan struct{}
	// rewriteSince is, per place and row, when a refused rewrite with no
	// version to name instead was first seen (under flight).
	rewriteSince map[string]map[int64]time.Time
}

// next takes place's next queued row and marks it being sent.
func (w *Worker) next(ctx context.Context, place string) (state.Placement, bool, error) {
	w.flight.Lock()
	defer w.flight.Unlock()
	p, ok, err := w.DB.NextPlacement(ctx, place)
	if ok && err == nil {
		if w.sending == nil {
			w.sending = map[string]int64{}
		}
		w.sending[place] = p.Seq
	}
	return p, ok, err
}

// Resume wakes place's sender if it is paused for quota or removal, so
// it tries its next row at once (LEND-8: the quota rose). It never
// blocks.
func (w *Worker) Resume(place string) {
	select {
	case w.wake(place) <- struct{}{}:
	default:
	}
}

// wake is place's wake-up channel.
func (w *Worker) wake(place string) chan struct{} {
	w.flight.Lock()
	defer w.flight.Unlock()
	if w.wakes == nil {
		w.wakes = map[string]chan struct{}{}
	}
	c := w.wakes[place]
	if c == nil {
		c = make(chan struct{}, 1)
		w.wakes[place] = c
	}
	return c
}

// done marks place's row as no longer being sent.
func (w *Worker) done(place string) {
	w.flight.Lock()
	defer w.flight.Unlock()
	delete(w.sending, place)
}

func (w *Worker) openFile(name string) (fs.File, error) {
	if w.open != nil {
		return w.open(name)
	}
	return os.Open(name)
}

func (w *Worker) alerted() {
	if w.Alerted != nil {
		w.Alerted()
	}
}

func (w *Worker) statFile(name string) (fs.FileInfo, error) {
	if w.stat != nil {
		return w.stat(name)
	}
	return os.Stat(name)
}

func (w *Worker) storeStall() time.Duration { return orDefault(w.StoreStall, time.Minute) }

// DefaultProbeEvery is how often an idle friend is probed (#65): often
// enough that its last answer (place.last_ok) is never much older than
// this while it answers, so OPS-2's alert counts from when it really
// stopped, and rare enough to cost nothing (one small request).
const DefaultProbeEvery = 15 * time.Minute

func (w *Worker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// errSuperseded: a later write or delete of the key replaced the row's
// bytes in the store.
var errSuperseded = errors.New("superseded by a later write of the key")

// errLocal: the store definitely cannot supply the object the row
// captured (it is gone, its size differs, it is a directory, its bytes
// no longer match the capture hash, its key escapes the bucket, it is
// too large for one PUT). Unless a later capture of the key explains
// it, retrying cannot help, so the row is refused for every place,
// against its bucket (#50, #57).
var errLocal = errors.New("the store cannot supply the captured object")

// errOversize: the object is larger than the place can be sent (one
// PUT, or MaxObjectSize in parts). A local error refused with cause
// oversize, which the worker re-queues at start when it can send it
// (state.RequeueOversize, #61, #23).
var errOversize = fmt.Errorf("%w: larger than an object can be sent", errLocal)

// errWindowClosed: the schedule closed (REPL-7). The row stays queued
// and its parts recorded; it continues once the schedule opens.
var errWindowClosed = errors.New("outside the replication schedule")

// errGone: the store holds no file for the object. A local error, unless
// Worker.Fill can put it there from a friend (after recover, #26 review).
var errGone = fmt.Errorf("%w: it is gone", errLocal)

// errStore: the store cannot be read now: the bucket's directory is
// missing (a disk not mounted), or opening, stating or reading the
// object failed in a way that may pass (EMFILE, EACCES, EIO…). Nothing
// is refused; the row waits with backoff (#57).
var errStore = errors.New("the store cannot be read")

// Run replicates until ctx ends.
func (w *Worker) Run(ctx context.Context) {
	type sender struct {
		addr   string
		cancel context.CancelFunc
		done   chan struct{}
	}
	senders := map[string]*sender{}
	unbuilt := map[string]bool{} // friends whose client failed to build, recorded as retrying
	stop := func(s *sender) { s.cancel(); <-s.done }
	defer func() {
		for _, s := range senders {
			stop(s)
		}
	}()
	poll := orDefault(w.Poll, time.Second)
	friendships := w.friendships
	if friendships == nil {
		friendships = w.DB.Friendships
	}
	w.start(ctx)
	var rerouted, replaced time.Time
	var pool string // the pool's fingerprint at the last re-placement pass
	again := false  // the last pass left a row in flight: look again
	for {
		now := w.now()
		if now.Sub(rerouted) >= orDefault(w.RerouteEvery, time.Minute) {
			rerouted = now
			w.reroute(ctx)
			w.dropStale(ctx)
			if again {
				replaced = time.Time{}
			}
		}
		// Re-placement (REPL-13, #26): after every change to the pool,
		// and hourly.
		if fp, err := w.DB.PoolFingerprint(ctx); err != nil {
			if ctx.Err() == nil {
				w.Log.Error("replication: reading the pool failed", "err", err)
			}
		} else if fp != pool || now.Sub(replaced) >= orDefault(w.ReplaceEvery, time.Hour) {
			pool, replaced = fp, now
			again = w.replace(ctx)
		}
		fs, err := friendships(ctx)
		if err != nil {
			// Keep the senders as they are: a failed read says nothing
			// about who the friends are (#53).
			if ctx.Err() == nil {
				w.Log.Error("replication: reading friendships failed; keeping the current senders", "err", err)
			}
			if !sleep(ctx, 5*poll) {
				return
			}
			continue
		}
		seen := map[string]bool{}
		for _, f := range fs {
			if f.Address == "" {
				continue
			}
			seen[f.Peer] = true
			if s := senders[f.Peer]; s != nil {
				if s.addr == f.Address {
					continue
				}
				stop(s) // the friend's address changed
				delete(senders, f.Peer)
			}
			r, err := w.connect(f)
			if err != nil {
				// No sender, so nothing else would ever record its
				// condition: it is retrying, with the reason, and so
				// unreachable and alerted like a friend that does not
				// answer (#65). Recorded once per failing run; tried again
				// at every pass.
				if !unbuilt[f.Peer] {
					w.Log.Error("replication: cannot build the friend's client; retrying", "peer", f.Peer, "err", err)
					if err := w.DB.SetPlaceCondition(context.Background(), f.Peer, state.ConditionRetrying,
						"cannot build the friend's client: "+err.Error(), w.now()); err != nil {
						w.Log.Error("replication: recording the place's condition failed", "peer", f.Peer, "err", err)
					} else {
						unbuilt[f.Peer] = true
					}
				}
				continue
			}
			delete(unbuilt, f.Peer)
			sctx, cancel := context.WithCancel(ctx)
			s := &sender{addr: f.Address, cancel: cancel, done: make(chan struct{})}
			senders[f.Peer] = s
			go func() { defer close(s.done); w.drain(sctx, f.Peer, r) }()
		}
		for p, s := range senders {
			if !seen[p] {
				stop(s)
				delete(senders, p)
			}
		}
		for p := range unbuilt {
			if !seen[p] {
				delete(unbuilt, p)
			}
		}
		if !sleep(ctx, 5*poll) {
			return
		}
	}
}

// start runs once when the worker starts: rows refused only as too
// large for one PUT are put back in their FIFOs, as they now go in parts
// (state.RequeueOversize, #61, #23), and the multipart uploads of rows
// that left their FIFO while the daemon was down are forgotten.
func (w *Worker) start(ctx context.Context) {
	if n, err := w.DB.RequeueOversize(ctx, MaxObjectSize); err != nil {
		if ctx.Err() == nil {
			w.Log.Error("replication: re-queueing objects too large for one PUT failed", "err", err)
		}
	} else if n > 0 {
		w.Log.Info("replication: objects refused as too large for one PUT are queued again, to be sent in parts", "rows", n)
	}
	w.dropStale(ctx)
}

// dropStale forgets the uploads of rows no longer queued at their place
// (acknowledged, refused, re-routed). The place's own upload is left to
// it: a lender aborts an upload with no part for a week (§3.5 LEND-1).
func (w *Worker) dropStale(ctx context.Context) {
	stale, err := w.DB.StaleTransfers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			w.Log.Error("replication: reading unfinished uploads failed", "err", err)
		}
		return
	}
	for _, t := range stale {
		if err := w.DB.DropTransfer(ctx, t.Place, t.Seq); err != nil && ctx.Err() == nil {
			w.Log.Error("replication: forgetting an unfinished upload failed", "place", t.Place, "seq", t.Seq, "err", err)
		}
	}
}

// reroute moves unsent data from friends failing for longer than
// reroute_after, or full by their last answer, to friends with room
// (§3.4), so the metadata barrier does not wait on them.
func (w *Worker) reroute(ctx context.Context) {
	// Called with w.flight held, as Reroute applies its moves.
	sending := func(place string, seq int64) bool {
		s, ok := w.sending[place]
		return ok && s == seq
	}
	moved, dropped, err := w.DB.Reroute(ctx, sending, &w.flight)
	if dropped > 0 {
		w.alerted() // alert.KindCopiesDropped
	}
	// A pass can both move rows and fail on others: say both.
	if moved > 0 || dropped > 0 {
		w.Log.Info("replication: re-routed unsent data from friends that stopped answering or are full",
			"moved", moved, "dropped_below_copies", dropped)
	}
	if err != nil && ctx.Err() == nil {
		w.Log.Error("replication: re-routing unsent data failed", "err", err)
	}
}

// replace runs a re-placement pass (REPL-13, state.DB.Replace): copies
// of a pool that changed are placed elsewhere, surplus copies removed.
// It reports whether the pass left a copy alone because it was being
// sent, or failed, so the next re-routing tick looks again.
func (w *Worker) replace(ctx context.Context) bool {
	// Called with w.flight held, as Replace applies its batches.
	sending := func(place string, seq int64) bool {
		s, ok := w.sending[place]
		return ok && s == seq
	}
	st, err := w.DB.Replace(ctx, sending, &w.flight)
	if st.Below > 0 || st.Placed > 0 || st.Dropping > 0 || st.Cancelled > 0 {
		w.alerted() // alert.KindBelowCopies reads what the pass recorded
	}
	switch {
	case err != nil:
		if ctx.Err() == nil {
			w.Log.Error("replication: re-placing copies failed", "err", err)
		}
	case st.Placed > 0 || st.Dropping > 0 || st.Cancelled > 0:
		w.Log.Info("replication: re-placed copies after the pool changed",
			"placed", st.Placed, "deleting", st.Dropping, "cancelled", st.Cancelled, "below_copies", st.Below)
	}
	return st.Skipped > 0 || err != nil // a failed batch is tried again at the next tick
}

func (w *Worker) connect(f state.Friendship) (Replica, error) {
	if w.Connect != nil {
		return w.Connect(f)
	}
	peer, err := identity.ParseNodeID(f.Peer)
	if err != nil {
		return nil, err
	}
	return NewFriend(w.Self, peer, f.Address, w.Dial, w.Client)
}

// sleep waits d or until ctx ends; false means ctx ended.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// pauseFor waits d, or until ctx ends or wake fires (Resume).
func pauseFor(ctx context.Context, d time.Duration, wake <-chan struct{}) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	case <-wake:
	}
}

// drain sends place's FIFO until ctx ends.
func (w *Worker) drain(ctx context.Context, place string, r Replica) {
	poll := orDefault(w.Poll, time.Second)
	minB, maxB := orDefault(w.MinBackoff, time.Second), orDefault(w.MaxBackoff, 5*time.Minute)
	pause := orDefault(w.PauseRetry, time.Hour)
	backoff := minB
	// failing is the kind of failure the last outcome was ("store",
	// "network"), or "" for any other outcome: the first failure of a
	// kind after anything else is alerted and backs off from minB, so a
	// store failure is alerted whatever the network did before (#61).
	failing := ""
	closed := false // the schedule was closed at the last look
	log := w.Log.With("place", place)
	// stale: the place may be recorded as waiting on the store, so an
	// empty FIFO resets it (#57). True at start for a condition left by
	// an earlier run. Only the store condition is reset: the others are
	// the place's own answer (#61).
	stale := true
	condition := func(c, detail string) {
		if err := w.DB.SetPlaceCondition(context.Background(), place, c, detail, w.now()); err != nil {
			log.Error("replication: recording the place's condition failed", "err", err)
		}
		stale = c == state.ConditionStore
	}
	defer w.done(place)
	wake := w.wake(place)
	// The reachability probe (#65): when the sender has nothing to send
	// and nothing reached the place for ProbeEvery (no send, no probe;
	// at once when the sender starts idle). It runs on this goroutine,
	// so never beside one of its sends.
	every := orDefault(w.ProbeEvery, DefaultProbeEvery)
	var contacted time.Time // the last send or probe
	probeFailed := false    // the last probe failed, and was logged
	probe := func() {
		pr, ok := r.(Prober)
		if !ok || !contacted.IsZero() && w.now().Sub(contacted) < every {
			return
		}
		err := pr.Probe(ctx)
		if ctx.Err() != nil {
			return
		}
		contacted = w.now()
		switch {
		case err == nil:
			// A sign of life, not an acknowledgement: last_ok advances,
			// retrying and removed end, a 507 or a wait on the store
			// stands (state.ProbeAnswered).
			if err := w.DB.ProbeAnswered(context.Background(), place, contacted); err != nil {
				log.Error("replication: recording the probe's answer failed", "err", err)
			}
			if probeFailed {
				log.Info("replication: the friend answers the reachability probe again")
			}
			probeFailed = false
		case errors.Is(err, ErrRemoved):
			level := slog.LevelDebug
			if !probeFailed {
				level = slog.LevelWarn
			}
			log.Log(ctx, level, "replication: the friend refuses this node (removed?) at the reachability probe", "err", err)
			condition(state.ConditionRemoved, err.Error())
			probeFailed = true
		case stale && w.waitingOnStore(place):
			// Waiting on this node's store: that condition, and its
			// alert, stand until a row goes (#57).
			log.Debug("replication: the reachability probe failed", "err", err)
		default:
			level := slog.LevelDebug
			if !probeFailed {
				level = slog.LevelWarn
			}
			log.Log(ctx, level, "replication: the friend does not answer the reachability probe; probing again later",
				"err", err, "retry_in", every)
			condition(state.ConditionRetrying, err.Error())
			probeFailed = true
		}
	}
	for ctx.Err() == nil {
		w.done(place) // the previous row's outcome is recorded
		p, ok, err := w.next(ctx, place)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("replication: reading the queue failed", "err", err)
			}
			sleep(ctx, poll)
			continue
		}
		if !ok {
			// Its rows went for another reason than its answer (refused
			// for the store by another sender): the store no longer holds
			// it up.
			if stale {
				if err := w.DB.ResetPlaceCondition(context.Background(), place, w.now()); err != nil {
					log.Error("replication: resetting the place's condition failed", "err", err)
				} else {
					stale = false
				}
			}
			failing = ""
			probe()
			sleep(ctx, poll)
			continue
		}
		if p.Class == "metadata" || p.Class == "snapshot" || p.Op == state.OpDelete {
			// The barrier (REPL-1): metadata not before every earlier data object
			// of the bucket is acknowledged wherever it is placed; a data
			// delete not before the metadata before it (#21).
			held, err := w.DB.Held(ctx, p.Seq)
			if err != nil || held {
				if err != nil && ctx.Err() == nil {
					log.Error("replication: reading the metadata barrier failed", "err", err)
				}
				if held {
					probe() // nothing goes there while it holds
				}
				sleep(ctx, poll)
				continue
			}
		}
		// A put a later write or delete replaced is cancelled before it
		// is taken, so only a row once sent stays the place's row of the
		// key for a later delete (#21).
		if p.Op == state.OpPut {
			if sup, err := w.DB.Superseded(ctx, p.Seq); err != nil || sup {
				if err == nil {
					err = w.DB.CancelPlacement(context.Background(), place, p.Seq)
				}
				if err != nil && ctx.Err() == nil {
					log.Error("replication: cancelling a superseded row failed", "seq", p.Seq, "err", err)
					sleep(ctx, poll)
				}
				continue
			}
		}
		// The schedule (REPL-7): nothing is taken while it is closed.
		if !w.Pace.Open() {
			if !closed {
				log.Info("replication: outside the schedule; waiting for it to open", "schedule", w.Pace.Settings().Schedule.String())
				closed = true
			}
			probe() // carries no object, so not held to the schedule
			sleep(ctx, poll)
			continue
		}
		closed = false
		// Taken: from here the send may land, so a delete of the key
		// follows the row instead of cancelling it. A row a delete has
		// cancelled meanwhile is not sent (REPL-2, #21).
		if ok, err := w.DB.StartPlacement(ctx, place, p.Seq, w.now()); err != nil || !ok {
			if err != nil && ctx.Err() == nil {
				log.Error("replication: recording a send's start failed", "err", err)
				sleep(ctx, poll)
			}
			continue
		}
		err = w.send(ctx, place, r, p)
		if ctx.Err() != nil {
			return // stopping; the row stays queued
		}
		contacted = w.now() // its outcome is the place's answer, or its condition
		if err == nil {
			probeFailed = false
		}
		if err != nil && !errors.Is(err, errSuperseded) && !errors.Is(err, errLocal) &&
			!errors.Is(err, ErrImmutable) && !errors.Is(err, ErrRejected) {
			// Not sent, and the row stays queued: it may be re-routed
			// while this sender waits to try again.
			w.done(place)
		}
		prev := failing
		failing = ""
		row := []any{"bucket", p.Bucket, "key", p.Key, "seq", p.Seq, "op", p.Op}
		bg := context.Background()
		switch {
		case err == nil:
			hash := ""
			if p.Op == state.OpPut {
				hash = p.SHA256
			}
			if err := w.DB.AckPlacement(bg, place, p.Seq, hash, w.now()); err != nil {
				log.Error("replication: recording an acknowledgement failed", append(row, "err", err)...)
				sleep(ctx, poll)
				continue
			}
			condition(state.ConditionOK, "")
			log.Debug("replicated", row...)
		case errors.Is(err, errSuperseded):
			w.forget(place, r, p)
			if err := w.DB.CancelPlacement(bg, place, p.Seq); err != nil {
				log.Error("replication: cancelling a superseded row failed", append(row, "err", err)...)
				sleep(ctx, poll)
			}
		case errors.Is(err, errWindowClosed):
			// Stopped at the end of a part; the rest goes once the schedule
			// opens again (REPL-7).
			log.Info("replication: the schedule closed; the object continues at its next part when it opens", row...)
			sleep(ctx, poll)
		case errors.Is(err, ErrImmutable) && p.Rewritable:
			w.forget(place, r, p)
			w.refuse(log, place, p, row, err, "replication alert: the friend keeps refusing a rewrite of this rewritable key; not replicated there")
			condition(state.ConditionOK, "")
		case errors.Is(err, ErrImmutable):
			// EP-11 (#22): the same condition as a change held back at
			// capture, alerted as key_changed rather than place_refused.
			var ce *changedError
			held := ""
			if errors.As(err, &ce) {
				held = ce.held
			}
			w.forget(place, r, p)
			log.Error("immutability alert: the friend holds this key with another hash; not replicated there",
				append(row, "sha256", p.SHA256, "held", held, "alert", true, "err", err)...)
			if err := w.DB.RefuseChanged(bg, place, p.Seq, held, err.Error()); err != nil {
				log.Error("replication: recording a refusal failed", append(row, "err", err)...)
			}
			w.alerted()
			condition(state.ConditionOK, "")
		case errors.Is(err, errLocal):
			if w.rewriting(ctx, p) {
				// A later write of the key is in flight; its capture will
				// supersede this row once done (or it is dropped and the
				// object is as captured again). Not the place's failure.
				log.Debug("replication: waiting for a later write of the key", append(row, "err", err)...)
				sleep(ctx, poll)
				continue
			}
			// Nothing will make the store supply the captured bytes, so
			// waiting would stall every place's FIFO for good (#48, #50).
			w.forget(place, r, p)
			w.refuseObject(log, p, row, err)
		case errors.Is(err, ErrRejected):
			// send re-hashed the store's copy and it matches the capture:
			// the place is at fault, and only its copy is refused (#57).
			w.forget(place, r, p)
			w.refuse(log, place, p, row, err, "replication alert: the place rejects bytes that match their capture hash; not replicated there")
			condition(state.ConditionOK, "") // the place answered
		case errors.Is(err, errStore):
			// Retried for as long as it lasts, never refused: what may pass
			// (EMFILE, EACCES, a disk dropping out) must not lose an
			// object for good. Alerted on the first store failure after
			// any other outcome, whatever the network backoff was, and
			// shown as the place's condition (#57, #61).
			failing = "store"
			if prev != failing {
				backoff = minB
			}
			attrs := append(row, "err", err, "retry_in", backoff)
			if prev != failing {
				log.Error("replication alert: this node's store cannot be read; waiting", append(attrs, "alert", true)...)
			} else {
				log.Debug("replication: this node's store cannot be read; waiting", attrs...)
			}
			condition(state.ConditionStore, err.Error())
			if prev != failing {
				w.alerted() // the condition is recorded: alert.KindStoreUnreadable
			}
			sleep(ctx, backoff)
			backoff = min(2*backoff, maxB)
		case errors.Is(err, ErrQuota):
			log.Warn("replication paused for quota: the friend refuses new objects; retrying later", append(row, "err", err, "retry_in", pause)...)
			condition(state.ConditionQuota, err.Error())
			if w.Full != nil {
				w.Full(place)
			}
			w.alerted() // alert.KindQuotaExhausted
			pauseFor(ctx, pause, wake)
		case errors.Is(err, ErrRemoved):
			log.Warn("replication stopped: the friend refuses this node (removed?); retrying later", append(row, "err", err, "retry_in", pause)...)
			condition(state.ConditionRemoved, err.Error())
			pauseFor(ctx, pause, wake)
		default:
			failing = "network"
			level := slog.LevelDebug
			if prev != failing {
				backoff = minB
				level = slog.LevelWarn // the first failure in a row
			}
			log.Log(ctx, level, "replication: sending failed; retrying", append(row, "err", err, "retry_in", backoff)...)
			condition(state.ConditionRetrying, err.Error())
			sleep(ctx, backoff)
			backoff = min(2*backoff, maxB)
		}
	}
}

// waitingOnStore reports whether place's recorded condition is
// ConditionStore; false if it cannot be read.
func (w *Worker) waitingOnStore(place string) bool {
	cs, err := w.DB.PlaceConditions(context.Background())
	return err == nil && cs[place].Condition == state.ConditionStore
}

// refuse takes a row out of the FIFO for good and raises its alert: in
// the log, and as alert.KindPlaceRefused once recorded (#13).
func (w *Worker) refuse(log *slog.Logger, place string, p state.Placement, row []any, err error, why string) {
	log.Error(why, append(row, "sha256", p.SHA256, "alert", true, "err", err)...)
	if err := w.DB.RefusePlacement(context.Background(), place, p.Seq, err.Error()); err != nil {
		log.Error("replication: recording a refusal failed", append(row, "err", err)...)
	}
	w.alerted()
}

// refuseObject takes a row whose object the store cannot supply out of
// every place's FIFO, against its bucket, and raises its alert (in the
// log, and as alert.KindObjectsNotReplicated once recorded, #13) once:
// when another place's sender refused it first, this one only logs at
// debug level.
func (w *Worker) refuseObject(log *slog.Logger, p state.Placement, row []any, err error) {
	cause := state.CauseStore
	if errors.Is(err, errOversize) {
		cause = state.CauseOversize // multipart replication re-queues it (#23)
	}
	refused, rerr := w.DB.RefuseObject(context.Background(), p.Seq, cause, err.Error())
	if rerr != nil {
		log.Error("replication: recording a refusal failed", append(row, "err", rerr)...)
	}
	if !refused && rerr == nil {
		log.Debug("replication: another place's sender refused the object first", append(row, "err", err)...)
		return
	}
	log.Error("replication alert: the store cannot supply the object as captured; not replicated",
		append(row, "sha256", p.SHA256, "alert", true, "err", err)...)
	w.alerted()
}

// rewriting reports whether a later write of the key is in flight or
// done, which explains a hash mismatch without the store having rotted.
func (w *Worker) rewriting(ctx context.Context, p state.Placement) bool {
	later, err := w.DB.LaterCapture(ctx, p.Seq)
	return err != nil || later
}

// send sends one row to the place.
//
// A put of a key the bucket's profile names rewritable (EP-7, #74) is
// sent as a rewrite of the version the place acknowledged, when that is
// another (Object.Replaces). If the place refuses it all the same, it
// holds a version the index does not show it acknowledged: a rewrite
// stored there whose answer was lost before a later write superseded it,
// or, after recovery, another friend's version. The worker then asks the
// place which version it holds and sends the rewrite once more, marked
// with that hash; a place that holds the row's bytes already has it.
// Only the key's latest capture is ever sent (a superseded row is
// cancelled first), so a rewrite never replaces a newer version.
func (w *Worker) send(ctx context.Context, place string, r Replica, p state.Placement) error {
	if p.Op == state.OpDelete {
		return r.Delete(ctx, p.Bucket, p.Key)
	}
	if sup, err := w.DB.Superseded(ctx, p.Seq); err != nil {
		return err
	} else if sup {
		return errSuperseded
	}
	replaces := ""
	if !p.Rewritable {
		// Deleted at the place and written again: a new object to this
		// node, but the lender keeps the deleted version in a tomb and
		// stores other bytes over it only marked as replacing it (LEND-4,
		// RET-1; #22 review).
		h, ok, err := w.DB.ReplacedHash(ctx, place, p.Bucket, p.Key, p.Seq)
		if err != nil {
			return err
		}
		if ok && h != p.SHA256 {
			replaces = h
		}
	} else {
		// The last version the place acknowledged, deleted since or not:
		// a deleted one is in its lender's tomb, and a rewrite naming it
		// is stored beside the tomb.
		h, ok, err := w.DB.AckedPutHash(ctx, place, p.Bucket, p.Key)
		if err != nil {
			return err
		}
		if ok && h != p.SHA256 {
			replaces = h
		}
	}
	err := w.put(ctx, place, r, p, replaces)
	if !p.Rewritable && errors.Is(err, ErrImmutable) && ctx.Err() == nil {
		// A key that is not rewritable, held by the place with another
		// hash the index does not show (EP-11, #22): ask which, for the
		// alert and status. A place already holding the row's bytes has
		// it. A failed Head is retried with the network's backoff (the
		// put is sent again first): the hash is what status names.
		held, ok, herr := r.Head(ctx, p.Bucket, p.Key)
		switch {
		case herr != nil:
			return fmt.Errorf("the place refused %s as holding other bytes, and asking which failed: %w", p.Key, herr)
		case ok && held == p.SHA256:
			return nil
		case !ok:
			held = ""
		}
		return &changedError{held: held, err: err}
	}
	if !p.Rewritable || !errors.Is(err, ErrImmutable) || ctx.Err() != nil {
		w.rewriteDone(place, p.Seq)
		return err
	}
	held, ok, herr := r.Head(ctx, p.Bucket, p.Key)
	switch {
	case herr != nil:
		return herr // retried with backoff
	case ok && held == p.SHA256:
		return nil
	case ok && held != replaces:
		return w.put(ctx, place, r, p, held)
	}
	// The place holds nothing live at the key (a tomb of another version
	// than the one named, or the Head fell inside the lender's own
	// rewrite of it), or holds exactly the version the refused rewrite
	// named (it changed under the request, or the lender is too old to
	// read the mark). That may pass: the row waits with backoff, as for a
	// network error, for Worker.RewriteRetry; past it, it is refused for
	// good, so the place's FIFO moves on (#74 reviews).
	if since := w.rewriteWaiting(place, p.Seq); w.now().Sub(since) >= orDefault(w.RewriteRetry, DefaultRewriteRetry) {
		w.rewriteDone(place, p.Seq)
		return fmt.Errorf("%w: the friend kept refusing the rewrite of %s for %v while it holds %s "+
			"(another version's tomb, kept until its retention window ends, or a lendisk too old to accept rewrites): %v",
			ErrImmutable, p.Key, w.now().Sub(since).Round(time.Second), heldDesc(held, ok), err)
	}
	return fmt.Errorf("the place refused a rewrite of %s (%v) but holds %s; trying again", p.Key, err, heldDesc(held, ok))
}

// changedError is a place's immutability refusal (412) of a key that is
// not rewritable, with the hash the place said it holds (empty if it
// did not say). It wraps ErrImmutable.
type changedError struct {
	held string
	err  error
}

func (e *changedError) Error() string { return e.err.Error() }
func (e *changedError) Unwrap() error { return e.err }

// DefaultRewriteRetry is how long a rewrite a place refuses without a
// version to name instead is retried before it is refused for good
// (#74).
const DefaultRewriteRetry = time.Hour

// rewriteWaiting returns when the worker first saw place refuse row seq's
// rewrite with no version to name instead, recording now if this is the
// first time. It lives in memory: a restart starts the wait again.
func (w *Worker) rewriteWaiting(place string, seq int64) time.Time {
	w.flight.Lock()
	defer w.flight.Unlock()
	if w.rewriteSince == nil {
		w.rewriteSince = map[string]map[int64]time.Time{}
	}
	if w.rewriteSince[place] == nil {
		w.rewriteSince[place] = map[int64]time.Time{}
	}
	t, ok := w.rewriteSince[place][seq]
	if !ok {
		t = w.now()
		w.rewriteSince[place][seq] = t
	}
	return t
}

// rewriteDone forgets place's wait on row seq, and on any earlier row
// (the FIFO has moved past them).
func (w *Worker) rewriteDone(place string, seq int64) {
	w.flight.Lock()
	defer w.flight.Unlock()
	for s := range w.rewriteSince[place] {
		if s <= seq {
			delete(w.rewriteSince[place], s)
		}
	}
}

func heldDesc(held string, ok bool) string {
	if !ok {
		return "nothing live there"
	}
	return "the version it replaces (" + held + ")"
}

// put sends the row's object from the store, marked as a rewrite of the
// version with hash replaces if that is set: in one PUT, or in parts
// when it is larger than the part size or one PUT, or when an upload of
// it is already under way (REPL-6). Each body is paced and holds an
// upload slot while it is sent (REPL-7).
func (w *Worker) put(ctx context.Context, place string, r Replica, p state.Placement, replaces string) error {
	mp, multi := r.(Multipart)
	limit := int64(MaxPutSize)
	if multi {
		limit = MaxObjectSize
	}
	if p.Size > limit {
		return fmt.Errorf("%w: %d bytes is more than %d", errOversize, p.Size, limit)
	}
	var err error
	if multi {
		tr, has, terr := w.DB.Transfer(ctx, place, p.Seq)
		if terr != nil {
			return terr
		}
		if p.Size > w.Pace.PartSize() || p.Size > MaxPutSize || has {
			err = w.putParts(ctx, place, mp, p, replaces, tr, has)
		} else {
			err = w.putOnce(ctx, place, r, p, replaces)
		}
	} else {
		err = w.putOnce(ctx, place, r, p, replaces)
	}
	if ctx.Err() != nil || err == nil {
		return err
	}
	if errors.Is(err, ErrRejected) {
		// The place says the bytes do not match the hash. Only the
		// store's copy decides whose fault that is (#57): a friend must
		// not be able to stop replication to the others.
		if lerr := w.verify(ctx, p); lerr != nil {
			return lerr
		}
		return fmt.Errorf("%w (the store's copy matches its capture hash)", err)
	}
	return err
}

// putOnce sends the object in one PUT; an interrupted one is sent again
// whole.
func (w *Worker) putOnce(ctx context.Context, place string, r Replica, p state.Placement, replaces string) error {
	body, done, err := w.openFilled(ctx, p)
	if err != nil {
		return err
	}
	defer done()
	release, err := w.Pace.Slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	// A store read that does not return ends the body with an error, so
	// the put fails as the store's failure before the client's stall
	// timeout blames the place (#61).
	sr := w.storeReader(body, p.Size)
	defer sr.Close()
	body = w.Pace.Send(ctx, place, sr)
	if w.OnRead != nil {
		body = &observed{r: body, fn: func(n int64) { w.OnRead(place, p.Seq, n) }}
	}
	err = r.Put(ctx, Object{Bucket: p.Bucket, Key: p.Key, Size: p.Size, SHA256: p.SHA256, Replaces: replaces, Body: body})
	if ctx.Err() != nil || err == nil {
		return err
	}
	if rerr := sr.failed(); rerr != nil {
		// The place did not fail: reading the store did.
		return w.unreadable(ctx, p, "reading", rerr)
	}
	return err
}

// partSizeFor is the part size for an object of size bytes: the
// configured one, larger (in whole MiB) when the object would need more
// than MaxParts.
func partSizeFor(size, configured int64) int64 {
	if need := (size + MaxParts - 1) / MaxParts; need > configured {
		const mib = 1 << 20
		return (need + mib - 1) / mib * mib
	}
	return configured
}

// putParts sends the object in parts (REPL-6, Q-Y.1). The upload ID and
// each part the place acknowledges are recorded as they complete
// (state.Transfer); an upload recorded before is checked against the
// place's listing, the parts it holds with the recorded ETag are kept,
// and the rest are sent, each read from its offset in the store. An
// upload the place no longer has starts the object over, as does one
// begun with another mark than the row now needs. The schedule is
// checked before each part, so a closing window stops the object at the
// end of the part in flight (REPL-7).
func (w *Worker) putParts(ctx context.Context, place string, mp Multipart, p state.Placement, replaces string, tr state.Transfer, has bool) error {
	body, done, err := w.openFilled(ctx, p)
	if err != nil {
		return err
	}
	defer done()
	ra, ok := body.(io.ReaderAt)
	if !ok {
		return fmt.Errorf("%w: %s/%s cannot be read from an offset", errStore, p.Bucket, p.Key)
	}
	if has && tr.Replaces != replaces {
		// Begun as a rewrite of another version, or of none: the place
		// would store it so, so it starts over (#74).
		w.abort(mp, p, tr)
		has = false
	}
	kept := map[int]state.TransferPart{}
	if has {
		listed, err := mp.Parts(ctx, p.Bucket, p.Key, tr.UploadID)
		switch {
		case errors.Is(err, ErrNoSuchUpload):
			w.dropTransfer(place, p.Seq)
			has = false
		case err != nil:
			return err
		}
		for _, part := range tr.Parts {
			if e, ok := listed[part.Number]; ok && trimETag(e) == trimETag(part.ETag) {
				kept[part.Number] = part
			}
		}
	}
	if !has {
		if !w.Pace.Open() {
			return errWindowClosed
		}
		size := partSizeFor(p.Size, w.Pace.PartSize())
		id, err := mp.Create(ctx, Object{Bucket: p.Bucket, Key: p.Key, Size: p.Size, SHA256: p.SHA256, Replaces: replaces})
		if err != nil {
			return err
		}
		tr = state.Transfer{Place: place, Seq: p.Seq, UploadID: id, PartSize: size, Replaces: replaces}
		if err := w.DB.BeginTransfer(context.Background(), tr); err != nil {
			w.abort(mp, p, tr)
			return err
		}
	}
	n := int((p.Size + tr.PartSize - 1) / tr.PartSize)
	if len(kept) > 0 {
		w.Log.Info("replication: resuming an upload at its next part", "place", place, "bucket", p.Bucket, "key", p.Key,
			"seq", p.Seq, "parts_kept", len(kept), "parts", n)
	}
	for i := 1; i <= n; i++ {
		if _, ok := kept[i]; ok {
			continue
		}
		if !w.Pace.Open() {
			return errWindowClosed
		}
		if sup, err := w.DB.Superseded(ctx, p.Seq); err != nil {
			return err
		} else if sup {
			return errSuperseded
		}
		off := int64(i-1) * tr.PartSize
		part, err := w.putPart(ctx, place, mp, p, tr.UploadID, i, io.NewSectionReader(ra, off, min(tr.PartSize, p.Size-off)), off)
		if errors.Is(err, ErrNoSuchUpload) {
			w.dropTransfer(place, p.Seq)
			return fmt.Errorf("the place no longer has the upload of %s; starting the object over: %w", p.Key, err)
		}
		if err != nil {
			return err
		}
		kept[i] = part
		if err := w.DB.AddTransferPart(context.Background(), place, p.Seq, part); err != nil {
			return err
		}
	}
	parts := make([]Part, 0, n)
	for i := 1; i <= n; i++ {
		k := kept[i]
		parts = append(parts, Part{Number: k.Number, ETag: k.ETag, SHA256: k.SHA256, Size: k.Size})
	}
	err = mp.Complete(ctx, p.Bucket, p.Key, tr.UploadID, parts)
	switch {
	case err == nil, errors.Is(err, ErrRejected):
		// Stored, or refused and removed by the place with the upload.
		w.dropTransfer(place, p.Seq)
	case errors.Is(err, ErrNoSuchUpload):
		w.dropTransfer(place, p.Seq)
		return fmt.Errorf("the place no longer has the upload of %s; starting the object over: %w", p.Key, err)
	}
	return err
}

// putPart sends part n, read from the store through section; off is
// its offset in the object.
func (w *Worker) putPart(ctx context.Context, place string, mp Multipart, p state.Placement, id string, n int, section *io.SectionReader, off int64) (state.TransferPart, error) {
	release, err := w.Pace.Slot(ctx)
	if err != nil {
		return state.TransferPart{}, err
	}
	defer release()
	size := section.Size()
	sr := w.storeReader(section, size)
	defer sr.Close()
	body := w.Pace.Send(ctx, place, sr)
	if w.OnRead != nil {
		body = &observed{r: body, n: off, fn: func(n int64) { w.OnRead(place, p.Seq, n) }}
	}
	part, err := mp.PutPart(ctx, p.Bucket, p.Key, id, n, body, size)
	if ctx.Err() != nil {
		return state.TransferPart{}, ctx.Err()
	}
	if err != nil {
		if rerr := sr.failed(); rerr != nil {
			return state.TransferPart{}, w.unreadable(ctx, p, "reading", rerr)
		}
		return state.TransferPart{}, err
	}
	return state.TransferPart{Number: n, ETag: part.ETag, SHA256: part.SHA256, Size: size}, nil
}

// forget drops the row's upload at place, if one is under way: the row
// left the FIFO for another reason than its acknowledgement.
func (w *Worker) forget(place string, r Replica, p state.Placement) {
	tr, has, err := w.DB.Transfer(context.Background(), place, p.Seq)
	if err != nil || !has {
		return
	}
	if mp, ok := r.(Multipart); ok {
		w.abort(mp, p, tr)
		return
	}
	w.dropTransfer(place, p.Seq)
}

// abort drops the upload at its place, as far as the place answers, and
// forgets it here. A place that does not answer aborts it itself once
// it has had no part for a week (§3.5 LEND-1).
func (w *Worker) abort(mp Multipart, p state.Placement, tr state.Transfer) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := mp.Abort(ctx, p.Bucket, p.Key, tr.UploadID); err != nil && !errors.Is(err, ErrNoSuchUpload) {
		w.Log.Debug("replication: aborting an unfinished upload at the place failed; it lapses there", "place", tr.Place,
			"seq", tr.Seq, "err", err)
	}
	w.dropTransfer(tr.Place, tr.Seq)
}

func (w *Worker) dropTransfer(place string, seq int64) {
	if err := w.DB.DropTransfer(context.Background(), place, seq); err != nil {
		w.Log.Error("replication: forgetting an upload failed", "place", place, "seq", seq, "err", err)
	}
}

func trimETag(e string) string { return strings.Trim(e, `"`) }

// openFilled is openCaptured, but an object the store holds no file for
// is first put there by Fill, when set, from a friend that holds it:
// after `recover` the store is empty, and re-placement must still send
// copies (REPL-13, ID-5, #26 review). A fetch that fails may pass, so
// the row waits as for an unreadable store; with no friend holding it,
// its absence is the store's local error.
func (w *Worker) openFilled(ctx context.Context, p state.Placement) (io.Reader, func(), error) {
	body, done, err := w.openCaptured(ctx, p)
	if !errors.Is(err, errGone) || w.Fill == nil {
		return body, done, err
	}
	filled, ferr := w.Fill(ctx, p.Bucket, p.Key)
	switch {
	case ferr != nil:
		return nil, nil, fmt.Errorf("%w: %s/%s is not in this node's store, and fetching it from a friend that holds it failed: %w",
			errStore, p.Bucket, p.Key, ferr)
	case !filled:
		return nil, nil, err
	}
	return w.openCaptured(ctx, p)
}

// openCaptured opens the row's object in the store, checked against
// the capture: a body of p.Size bytes (an *os.File, readable from an
// offset, but for a directory marker) and the function that closes it.
// The error is errLocal only for what is definitely permanent (gone, a
// directory or special file, another size, an escaping key), errStore for anything that may pass (#57), including an open or
// stat that does not return within Worker.StoreStall (#63).
func (w *Worker) openCaptured(ctx context.Context, p state.Placement) (io.Reader, func(), error) {
	name := filepath.FromSlash(p.Key)
	if !filepath.IsLocal(name) {
		return nil, nil, fmt.Errorf("%w: key %q does not name a file in the store", errLocal, p.Key)
	}
	// local blames the object, unless the bucket's directory is
	// unreadable too, which is the store's fault, not the object's.
	local := func(format string, a ...any) error {
		if err := w.storeReadable(ctx, p.Bucket); err != nil {
			return err
		}
		return fmt.Errorf("%w: "+format, append([]any{errLocal}, a...)...)
	}
	// Opening and stating run on their own goroutine (storeCall); a stat
	// that fails closes the file there, not here, where the close could
	// hang on the same disk.
	type opened struct {
		f     fs.File
		fi    fs.FileInfo
		doing string // what failed: "opening" or "stating"
	}
	o, err := storeCall(ctx, w.storeStall(), "opening and stating "+p.Bucket+"/"+p.Key, func() (opened, error) {
		f, err := w.openFile(filepath.Join(w.StoreDir, p.Bucket, name))
		if err != nil {
			return opened{doing: "opening"}, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return opened{doing: "stating"}, err
		}
		return opened{f: f, fi: fi}, nil
	}, func(o opened) { o.f.Close() })
	switch {
	case ctx.Err() != nil:
		if err == nil {
			o.f.Close() // opened just as the sender was cancelled
		}
		return nil, nil, ctx.Err()
	case err != nil && o.doing == "":
		return nil, nil, fmt.Errorf("%w: %w", errStore, err) // it stalled
	case o.doing == "opening" && (errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)):
		if err := w.storeReadable(ctx, p.Bucket); err != nil {
			return nil, nil, err
		}
		return nil, nil, errGone
	case err != nil:
		return nil, nil, w.unreadable(ctx, p, o.doing, err)
	}
	f, fi := o.f, o.fi
	var body io.Reader = f
	switch {
	case fi.IsDir() && strings.HasSuffix(p.Key, "/") && p.Size == 0:
		// A directory marker, which the posix store keeps as a
		// directory: an empty object.
		body = strings.NewReader("")
	case !fi.Mode().IsRegular():
		f.Close()
		return nil, nil, local("the store holds a %s there, not an object file", fileKind(fi))
	case fi.Size() != p.Size:
		// Usually a later write under way, whose capture will supersede
		// this row; drain tells the two apart.
		f.Close()
		return nil, nil, local("the store holds %d bytes, the capture %d", fi.Size(), p.Size)
	}
	return body, func() { f.Close() }, nil
}

// verify re-hashes the store's copy of the row's object: nil when it
// matches the capture hash, errLocal when it does not (it rotted, or a
// write outside lendisk changed it: VER-5), errStore when it cannot be
// read now.
func (w *Worker) verify(ctx context.Context, p state.Placement) error {
	body, done, err := w.openFilled(ctx, p)
	if err != nil {
		return err
	}
	defer done()
	sr := w.storeReader(body, p.Size) // a hung or short re-hash is the store's failure too
	defer sr.Close()
	h := sha256.New()
	if _, err := io.Copy(h, &ctxReader{ctx: ctx, r: sr}); err != nil {
		if ctx.Err() != nil {
			return err
		}
		return w.unreadable(ctx, p, "re-hashing", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != p.SHA256 {
		return fmt.Errorf("%w: the store's copy no longer matches its capture hash (it hashes to %s; bit rot, or changed outside lendisk)",
			errLocal, got)
	}
	return nil
}

// unreadable is errStore for a failure that may pass: opening, stating
// or reading the object failed for another reason than its absence.
func (w *Worker) unreadable(ctx context.Context, p state.Placement, doing string, err error) error {
	if serr := w.storeReadable(ctx, p.Bucket); serr != nil {
		return serr
	}
	return fmt.Errorf("%w: %s %s/%s: %w", errStore, doing, p.Bucket, p.Key, err)
}

// ctxReader stops reading when ctx ends.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// storeReadable checks that bucket's directory is in the store, so a
// missing or unreadable object is the object's fault and not, say, a
// store disk that is not mounted. A stat that does not return within
// Worker.StoreStall is the store's failure too (#63).
func (w *Worker) storeReadable(ctx context.Context, bucket string) error {
	fi, err := storeCall(ctx, w.storeStall(), "stating it", func() (fs.FileInfo, error) {
		return w.statFile(filepath.Join(w.StoreDir, bucket))
	}, nil)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil && !fi.IsDir() {
		err = errors.New("not a directory")
	}
	if err != nil {
		return fmt.Errorf("%w: bucket %s: %w", errStore, bucket, err)
	}
	return nil
}

func fileKind(fi os.FileInfo) string {
	if fi.IsDir() {
		return "directory"
	}
	return "special file (" + fi.Mode().Type().String() + ")"
}

// storeCall runs call, which touches the store, on its own goroutine, so
// a call that does not return within stall fails at once, with an error
// saying what did not return, and one still blocked when ctx ends does
// not hold up the sender's stop (#63). The goroutine stuck in such a
// call is left behind until the disk answers; drop, if set, then
// releases what it returned.
func storeCall[T any](ctx context.Context, stall time.Duration, what string, call func() (T, error), drop func(T)) (T, error) {
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := call()
		ch <- result{v, err}
	}()
	t := time.NewTimer(stall)
	defer t.Stop()
	var err error
	select {
	case r := <-ch:
		return r.v, r.err
	case <-t.C:
		err = fmt.Errorf("%s did not return for %v", what, stall)
	case <-ctx.Done():
		err = ctx.Err()
	}
	go func() {
		if r := <-ch; r.err == nil && drop != nil {
			drop(r.v)
		}
	}()
	var zero T
	return zero, err
}

// storeReader reads body, size bytes, from the store on its own
// goroutine and hands the bytes on through a pipe, so a store read that
// does not return within Worker.StoreStall ends the body with an error
// at once (#61). A body that ends before size bytes (the file was
// truncated under the sender) ends with the store's error too, not with
// the friend's (#63).
// Cancelling a put's context is not enough: net/http does not return
// while the request body's Read is blocked. The goroutine stuck in such
// a read is left behind until the disk answers; its bytes are dropped.
func (w *Worker) storeReader(body io.Reader, size int64) *localReader {
	stall := w.storeStall()
	pr, pw := io.Pipe()
	l := &localReader{pr: pr}
	go func() {
		buf := make([]byte, 32<<10)
		var read int64
		for {
			t := time.AfterFunc(stall, func() {
				err := fmt.Errorf("a read did not return for %v", stall)
				l.fail(err)
				pw.CloseWithError(err)
			})
			n, err := body.Read(buf)
			if !t.Stop() {
				return // the stall ended the body
			}
			read += int64(n)
			if err == io.EOF && read < size {
				err = fmt.Errorf("the object ended after %d of %d bytes: %w", read, size, io.ErrUnexpectedEOF)
			}
			if n > 0 {
				if _, werr := pw.Write(buf[:n]); werr != nil {
					return // the reader closed: the put is over
				}
			}
			if err == io.EOF {
				pw.Close()
				return
			}
			if err != nil {
				l.fail(err)
				pw.CloseWithError(err)
				return
			}
		}
	}()
	return l
}

// localReader is the body storeReader returns. It remembers a read
// error from the store, a stall included, which the client would
// otherwise report as its own failure. Close stops the reading
// goroutine at its next write.
type localReader struct {
	pr  *io.PipeReader
	mu  sync.Mutex
	err error
}

func (l *localReader) Read(p []byte) (int, error) { return l.pr.Read(p) }

func (l *localReader) Close() error { return l.pr.Close() }

// fail keeps the first error: a hung read that fails once the file is
// closed is still the hang.
func (l *localReader) fail(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil {
		l.err = err
	}
}

func (l *localReader) failed() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

type observed struct {
	r  io.Reader
	n  int64
	fn func(int64)
}

func (o *observed) Read(p []byte) (int, error) {
	n, err := o.r.Read(p)
	o.n += int64(n)
	o.fn(o.n)
	return n, err
}
