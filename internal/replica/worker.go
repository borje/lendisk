package replica

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Worker drains every place's FIFO (REPL-1, REPL-3): one sender per
// friend, each sending its queued placement rows in write-sequence
// order, one object at a time, the places in parallel. An object is read
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
	// PauseRetry is how long a place paused for quota or removed waits
	// before trying again (default an hour, LEND-2).
	PauseRetry time.Duration
	// OnRead, if set, is called after each read of a body sent to a
	// place, with the bytes read so far. Tests only.
	OnRead func(place string, seq, read int64)
}

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
	for {
		fs, err := w.DB.Friendships(ctx)
		if err != nil && ctx.Err() == nil {
			w.Log.Error("replication: reading friendships failed", "err", err)
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
	log := w.Log.With("place", place)
	condition := func(c, detail string) {
		if err := w.DB.SetPlaceCondition(context.Background(), place, c, detail, w.now()); err != nil {
			log.Error("replication: recording the place's condition failed", "err", err)
		}
	}
	for ctx.Err() == nil {
		p, ok, err := w.DB.NextPlacement(ctx, place)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("replication: reading the queue failed", "err", err)
			}
			sleep(ctx, poll)
			continue
		}
		if !ok {
			sleep(ctx, poll)
			continue
		}
		err = w.send(ctx, place, r, p)
		if ctx.Err() != nil {
			return // stopping; the row stays queued
		}
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
			backoff = minB
			log.Debug("replicated", row...)
		case errors.Is(err, errSuperseded):
			if err := w.DB.CancelPlacement(bg, place, p.Seq); err != nil {
				log.Error("replication: cancelling a superseded row failed", append(row, "err", err)...)
				sleep(ctx, poll)
			}
		case errors.Is(err, ErrImmutable):
			w.refuse(log, place, p, row, err, "immutability alert: the friend holds this key with another hash; not replicated")
			condition(state.ConditionOK, "")
		case errors.Is(err, ErrRejected) && !w.rewriting(ctx, p):
			w.refuse(log, place, p, row, err, "the store's copy no longer matches the hash taken at capture; not replicated")
			condition(state.ConditionOK, "")
		case errors.Is(err, ErrQuota):
			log.Warn("replication paused for quota: the friend refuses new objects; retrying later", append(row, "err", err, "retry_in", pause)...)
			condition(state.ConditionQuota, err.Error())
			sleep(ctx, pause)
		case errors.Is(err, ErrRemoved):
			log.Warn("replication stopped: the friend refuses this node (removed?); retrying later", append(row, "err", err, "retry_in", pause)...)
			condition(state.ConditionRemoved, err.Error())
			sleep(ctx, pause)
		default:
			level := slog.LevelDebug
			if backoff == minB {
				level = slog.LevelWarn // the first failure in a row
			}
			log.Log(ctx, level, "replication: sending failed; retrying", append(row, "err", err, "retry_in", backoff)...)
			condition(state.ConditionRetrying, err.Error())
			sleep(ctx, backoff)
			backoff = min(2*backoff, maxB)
		}
	}
}

// refuse takes a row out of the FIFO for good and raises its alert in
// the log (alert delivery is #13).
func (w *Worker) refuse(log *slog.Logger, place string, p state.Placement, row []any, err error, why string) {
	log.Error(why, append(row, "sha256", p.SHA256, "alert", true, "err", err)...)
	if err := w.DB.RefusePlacement(context.Background(), place, p.Seq, err.Error()); err != nil {
		log.Error("replication: recording a refusal failed", append(row, "err", err)...)
	}
}

// rewriting reports whether a later write of the key is in flight or
// done, which explains a hash mismatch without the store having rotted.
func (w *Worker) rewriting(ctx context.Context, p state.Placement) bool {
	later, err := w.DB.LaterCapture(ctx, p.Seq)
	return err != nil || later
}

// send sends one row to the place.
func (w *Worker) send(ctx context.Context, place string, r Replica, p state.Placement) error {
	if p.Op == state.OpDelete {
		return r.Delete(ctx, p.Bucket, p.Key)
	}
	if sup, err := w.DB.Superseded(ctx, p.Seq); err != nil {
		return err
	} else if sup {
		return errSuperseded
	}
	name := filepath.FromSlash(p.Key)
	if !filepath.IsLocal(name) {
		return fmt.Errorf("key %q does not name a file in the store", p.Key)
	}
	f, err := os.Open(filepath.Join(w.StoreDir, p.Bucket, name))
	if err != nil {
		return fmt.Errorf("reading the object from the store: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != p.Size {
		// A later write is under way; its capture will supersede this row.
		return fmt.Errorf("the store holds %d bytes, the capture %d", fi.Size(), p.Size)
	}
	var body io.Reader = f
	if w.OnRead != nil {
		body = &observed{r: f, fn: func(n int64) { w.OnRead(place, p.Seq, n) }}
	}
	return r.Put(ctx, Object{Bucket: p.Bucket, Key: p.Key, Size: p.Size, SHA256: p.SHA256, Body: body})
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
