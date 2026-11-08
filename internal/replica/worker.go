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
	"github.com/borje/lendisk/internal/state"
)

// Worker drains every place's FIFO (REPL-1, REPL-3): one sender per
// friend, each sending its queued placement rows in write-sequence
// order, one object at a time, the places in parallel. A metadata row
// at the head of a FIFO waits until every earlier data object of its
// bucket is acknowledged wherever it is placed (the barrier,
// state.DB.Held), and unsent data at a friend that stopped answering is
// re-routed (state.DB.Reroute). An object is read
// from the store as it is sent, once per place, with its capture hash
// (REPL-10); the place's acknowledgement records the hash. The queue is
// lendisk.db, so a restart continues at the first unacknowledged row.
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
	// RewriteRetry is how long a rewrite the place refuses without a
	// version to name instead is retried before it is refused for good
	// (default DefaultRewriteRetry, #74).
	RewriteRetry time.Duration
	// PauseRetry is how long a place paused for quota or removed waits
	// before trying again (default an hour, LEND-2).
	PauseRetry time.Duration
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
	// OnRead, if set, is called after each read of a body sent to a
	// place, with the bytes read so far. Tests only.
	OnRead func(place string, seq, read int64)

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

// errOversize: the object is larger than one PUT carries. A local error
// refused with cause oversize, which multipart replication (#23)
// re-queues (state.RequeueOversize).
var errOversize = fmt.Errorf("%w: larger than one PUT carries", errLocal)

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
	var rerouted time.Time
	for {
		if now := w.now(); now.Sub(rerouted) >= orDefault(w.RerouteEvery, time.Minute) {
			rerouted = now
			w.reroute(ctx)
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
				w.Log.Error("replication: cannot build the friend's client", "peer", f.Peer, "err", err)
				continue
			}
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
		if !sleep(ctx, 5*poll) {
			return
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
	switch {
	case err != nil:
		if ctx.Err() == nil {
			w.Log.Error("replication: re-routing unsent data failed", "err", err)
		}
	case moved > 0 || dropped > 0:
		w.Log.Info("replication: re-routed unsent data from friends that stopped answering or are full",
			"moved", moved, "dropped_below_copies", dropped)
	}
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
			sleep(ctx, poll)
			continue
		}
		if p.Class == "metadata" || p.Class == "snapshot" {
			// The barrier (REPL-1): not before every earlier data object
			// of the bucket is acknowledged wherever it is placed.
			held, err := w.DB.Held(ctx, p.Seq)
			if err != nil || held {
				if err != nil && ctx.Err() == nil {
					log.Error("replication: reading the metadata barrier failed", "err", err)
				}
				sleep(ctx, poll)
				continue
			}
		}
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
			if err := w.DB.CancelPlacement(bg, place, p.Seq); err != nil {
				log.Error("replication: cancelling a superseded row failed", append(row, "err", err)...)
				sleep(ctx, poll)
			}
		case errors.Is(err, ErrImmutable) && p.Rewritable:
			w.refuse(log, place, p, row, err, "replication alert: the friend keeps refusing a rewrite of this rewritable key; not replicated there")
			condition(state.ConditionOK, "")
		case errors.Is(err, ErrImmutable):
			w.refuse(log, place, p, row, err, "immutability alert: the friend holds this key with another hash; not replicated")
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
			w.refuseObject(log, p, row, err)
		case errors.Is(err, ErrRejected):
			// send re-hashed the store's copy and it matches the capture:
			// the place is at fault, and only its copy is refused (#57).
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
			sleep(ctx, pause)
		case errors.Is(err, ErrRemoved):
			log.Warn("replication stopped: the friend refuses this node (removed?); retrying later", append(row, "err", err, "retry_in", pause)...)
			condition(state.ConditionRemoved, err.Error())
			sleep(ctx, pause)
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
	if p.Rewritable {
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
// version with hash replaces if that is set.
func (w *Worker) put(ctx context.Context, place string, r Replica, p state.Placement, replaces string) error {
	body, done, err := w.openCaptured(ctx, p)
	if err != nil {
		return err
	}
	defer done()
	// A store read that does not return ends the body with an error, so
	// the put fails as the store's failure before the client's stall
	// timeout blames the place (#61).
	sr := w.storeReader(body, p.Size)
	defer sr.Close()
	body = sr
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

// openCaptured opens the row's object in the store, checked against
// the capture: a body of p.Size bytes and the function that closes it.
// The error is errLocal only for what is definitely permanent (gone, a
// directory or special file, another size, an escaping key, too large to
// send), errStore for anything that may pass (#57), including an open or
// stat that does not return within Worker.StoreStall (#63).
func (w *Worker) openCaptured(ctx context.Context, p state.Placement) (io.Reader, func(), error) {
	name := filepath.FromSlash(p.Key)
	if !filepath.IsLocal(name) {
		return nil, nil, fmt.Errorf("%w: key %q does not name a file in the store", errLocal, p.Key)
	}
	if p.Size > MaxPutSize {
		return nil, nil, fmt.Errorf("%w: %d bytes is more than %d and multipart replication is not built yet (#23)",
			errOversize, p.Size, int64(MaxPutSize))
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
		return nil, nil, local("it is gone")
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
	body, done, err := w.openCaptured(ctx, p)
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
