package replica

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	// friendships reads the friendships; nil means DB.Friendships. Tests
	// set it to fail.
	friendships func(context.Context) ([]state.Friendship, error)
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

// errLocal: the store cannot supply the object the row captured (it is
// gone, its size differs, it is a directory, it cannot be read, it is
// too large for one PUT). Unless a later capture of the key explains
// it, retrying cannot help, so the row is refused against its bucket.
var errLocal = errors.New("the store cannot supply the captured object")

// errStore: the bucket's directory in the store cannot be read at all
// (a disk not mounted): nothing about one object, so rows wait.
var errStore = errors.New("the store is unreadable")

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
	for {
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
		case errors.Is(err, ErrRejected) || errors.Is(err, errLocal):
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
			if errors.Is(err, ErrRejected) {
				condition(state.ConditionOK, "") // the place answered
			}
			backoff = minB
		case errors.Is(err, errStore):
			level := slog.LevelDebug
			if backoff == minB {
				level = slog.LevelError
			}
			log.Log(ctx, level, "replication: the store cannot be read; waiting", append(row, "err", err, "retry_in", backoff)...)
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

// refuseObject takes a row whose object the store cannot supply out of
// every place's FIFO, against its bucket, and raises its alert in the
// log (alert delivery is #13).
func (w *Worker) refuseObject(log *slog.Logger, p state.Placement, row []any, err error) {
	log.Error("replication alert: the store cannot supply the object as captured; not replicated",
		append(row, "sha256", p.SHA256, "alert", true, "err", err)...)
	if err := w.DB.RefuseObject(context.Background(), p.Seq, err.Error()); err != nil {
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
		return fmt.Errorf("%w: key %q does not name a file in the store", errLocal, p.Key)
	}
	if p.Size > MaxPutSize {
		return fmt.Errorf("%w: %d bytes is more than one PUT carries (%d) and multipart replication is not built yet (#23)",
			errLocal, p.Size, int64(MaxPutSize))
	}
	// local blames the object for err, unless the bucket's directory is
	// unreadable too, which is the store's fault, not the object's.
	local := func(format string, a ...any) error {
		if err := w.storeReadable(p.Bucket); err != nil {
			return err
		}
		return fmt.Errorf("%w: "+format, append([]any{errLocal}, a...)...)
	}
	f, err := os.Open(filepath.Join(w.StoreDir, p.Bucket, name))
	if errors.Is(err, os.ErrNotExist) {
		return local("it is gone")
	}
	if err != nil {
		return local("%w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return local("%w", err)
	}
	var body io.Reader = f
	switch {
	case fi.IsDir() && strings.HasSuffix(p.Key, "/") && p.Size == 0:
		// A directory marker, which the posix store keeps as a
		// directory: an empty object.
		body = strings.NewReader("")
	case !fi.Mode().IsRegular():
		return local("the store holds a %s there, not an object file", fileKind(fi))
	case fi.Size() != p.Size:
		// Usually a later write under way, whose capture will supersede
		// this row; drain tells the two apart.
		return local("the store holds %d bytes, the capture %d", fi.Size(), p.Size)
	}
	lr := &localReader{r: body}
	body = lr
	if w.OnRead != nil {
		body = &observed{r: body, fn: func(n int64) { w.OnRead(place, p.Seq, n) }}
	}
	err = r.Put(ctx, Object{Bucket: p.Bucket, Key: p.Key, Size: p.Size, SHA256: p.SHA256, Body: body})
	if rerr := lr.failed(); err != nil && rerr != nil && ctx.Err() == nil {
		// The place did not fail: reading the store did.
		return local("reading it: %w", rerr)
	}
	return err
}

// storeReadable checks that bucket's directory is in the store, so a
// missing or unreadable object is the object's fault and not, say, a
// store disk that is not mounted.
func (w *Worker) storeReadable(bucket string) error {
	fi, err := os.Stat(filepath.Join(w.StoreDir, bucket))
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

// localReader remembers a read error from the store, which the client
// would otherwise report as its own failure. The HTTP transport may
// still be reading when Put returns, hence the lock.
type localReader struct {
	r   io.Reader
	mu  sync.Mutex
	err error
}

func (l *localReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	if err != nil && err != io.EOF {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
	}
	return n, err
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
