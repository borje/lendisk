package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// place is an in-memory place standing in for a friend's node: the
// network is tested in friend_test.go and internal/cli.
type place struct {
	name string
	mu   sync.Mutex
	objs map[string][]byte // bucket/key
	log  []string          // op bucket/key, in order
	// fail, if set, answers instead of storing.
	fail func(op, key string) error
	// hold, if set, blocks every request until closed.
	hold                  chan struct{}
	inFlight, maxInFlight int
	bytesRead             int64
}

func newPlace(name string) *place { return &place{name: name, objs: map[string][]byte{}} }

func (p *place) enter() func() {
	p.mu.Lock()
	p.inFlight++
	p.maxInFlight = max(p.maxInFlight, p.inFlight)
	hold := p.hold
	p.mu.Unlock()
	if hold != nil {
		<-hold
	}
	return func() { p.mu.Lock(); p.inFlight--; p.mu.Unlock() }
}

func (p *place) Put(ctx context.Context, o Object) error {
	defer p.enter()()
	// Like the HTTP transport, Put does not return while a read of the
	// body is blocked, whatever ctx does.
	b, err := io.ReadAll(o.Body)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bytesRead += int64(len(b))
	if p.fail != nil {
		if err := p.fail("put", o.Key); err != nil {
			return err
		}
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != o.SHA256 || int64(len(b)) != o.Size {
		return ErrRejected
	}
	p.objs[o.Bucket+"/"+o.Key] = b
	p.log = append(p.log, "put "+o.Bucket+"/"+o.Key)
	return nil
}

func (p *place) Delete(ctx context.Context, bucket, key string) error {
	defer p.enter()()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		if err := p.fail("delete", key); err != nil {
			return err
		}
	}
	delete(p.objs, bucket+"/"+key)
	p.log = append(p.log, "delete "+bucket+"/"+key)
	return nil
}

func (p *place) snapshot() (log []string, objs int, maxInFlight int, read int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.log...), len(p.objs), p.maxInFlight, p.bytesRead
}

// fixture is a node's store and lendisk.db with friends whose places
// are in memory.
type fixture struct {
	t      *testing.T
	db     *state.DB
	store  string
	places map[string]*place
	w      *Worker
}

func newFixture(t *testing.T, friends ...string) *fixture {
	t.Helper()
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	fx := &fixture{t: t, db: db, store: t.TempDir(), places: map[string]*place{}}
	for _, f := range friends {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		fx.places[f] = newPlace(f)
	}
	fx.w = &Worker{DB: db, StoreDir: fx.store, Log: quiet,
		Connect: func(f state.Friendship) (Replica, error) { return fx.places[f.Peer], nil },
		Poll:    10 * time.Millisecond, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
		PauseRetry: 50 * time.Millisecond}
	return fx
}

// write stores an object and captures it, as the decorator does.
func (fx *fixture) write(bucket, key, content string) int64 {
	fx.t.Helper()
	p := filepath.Join(fx.store, bucket, filepath.FromSlash(key))
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		fx.t.Fatal(err)
	}
	return fx.capture(bucket, key, int64(len(content)), content)
}

// capture records a completed put of key, size bytes hashed from
// content, without touching the store.
func (fx *fixture) capture(bucket, key string, size int64, content string) int64 {
	fx.t.Helper()
	seqs, err := fx.db.BeginCaptures(fx.t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: "data"}})
	if err != nil {
		fx.t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(content))
	if _, err := fx.db.FinishPut(fx.t.Context(), seqs[0], size, hex.EncodeToString(sum[:])); err != nil {
		fx.t.Fatal(err)
	}
	return seqs[0]
}

func (fx *fixture) remove(bucket, key string) {
	fx.t.Helper()
	os.Remove(filepath.Join(fx.store, bucket, filepath.FromSlash(key)))
	seqs, err := fx.db.BeginCaptures(fx.t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpDelete, Class: "data"}})
	if err != nil {
		fx.t.Fatal(err)
	}
	if err := fx.db.FinishDelete(fx.t.Context(), seqs...); err != nil {
		fx.t.Fatal(err)
	}
}

// run starts the worker until the test ends.
func (fx *fixture) run() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); fx.w.Run(ctx) }()
	fx.t.Cleanup(func() { cancel(); <-done })
}

// drained waits until place's FIFO is empty.
func (fx *fixture) drained(place string) {
	fx.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok, err := fx.db.NextPlacement(fx.t.Context(), place); err == nil && !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	fx.t.Fatalf("%s not drained", place)
}

func (fx *fixture) condition(place string) state.PlaceCondition {
	cs, err := fx.db.PlaceConditions(fx.t.Context())
	if err != nil {
		fx.t.Fatal(err)
	}
	return cs[place]
}

// refused returns how many of bucket's objects were refused because the
// store could not supply them as captured.
func (fx *fixture) refused(bucket string) int64 {
	fx.t.Helper()
	r, err := fx.db.RefusedObjects(fx.t.Context())
	if err != nil {
		fx.t.Fatal(err)
	}
	return r[bucket]
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// REPL-1, REPL-10 (#18): each place receives its FIFO in write order,
// one object at a time, places in parallel; each object is read once
// per place, and the place's acknowledged hash is recorded.
func TestEachPlaceGetsItsFIFOInWriteOrder(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.places["C"].hold = make(chan struct{}) // C is slow: B must not wait for it
	var want []string
	var size int64
	for _, k := range []string{"data/1", "data/2", "index/1", "data/3", "snapshots/1"} {
		fx.write("laptop", k, "content of "+k)
		want = append(want, "put laptop/"+k)
		size += int64(len("content of " + k))
	}
	fx.run()
	fx.drained("B")
	if log, _, _, _ := fx.places["C"].snapshot(); len(log) != 0 {
		t.Fatalf("C stored while held: %v", log)
	}
	close(fx.places["C"].hold)
	fx.drained("C")
	for _, name := range []string{"B", "C"} {
		log, objs, maxIn, read := fx.places[name].snapshot()
		if len(log) != len(want) || objs != len(want) {
			t.Fatalf("%s: %v", name, log)
		}
		for i := range want {
			if log[i] != want[i] {
				t.Fatalf("%s order %v, want %v", name, log, want)
			}
		}
		if maxIn != 1 {
			t.Errorf("%s had %d objects in flight", name, maxIn)
		}
		if read != size {
			t.Errorf("%s read %d bytes for %d", name, read, size)
		}
	}
	sum := sha256.Sum256([]byte("content of index/1"))
	if h, ok, _ := fx.db.AckedHash(t.Context(), "B", "laptop", "index/1"); !ok || h != hex.EncodeToString(sum[:]) {
		t.Fatalf("acked hash %q %v", h, ok)
	}
	if c := fx.condition("B"); c.Condition != state.ConditionOK || c.LastOK.IsZero() {
		t.Fatalf("condition %+v", c)
	}
}

// A put whose key was written again or deleted before it was sent is
// cancelled (the store no longer has its bytes); the delete is sent.
func TestOverwrittenOrDeletedPutIsNotSent(t *testing.T) {
	fx := newFixture(t, "B")
	fx.write("laptop", "data/1", "first")
	fx.write("laptop", "data/1", "second")
	fx.write("laptop", "data/2", "gone soon")
	fx.remove("laptop", "data/2")
	fx.run()
	fx.drained("B")
	log, _, _, _ := fx.places["B"].snapshot()
	want := []string{"put laptop/data/1", "delete laptop/data/2"}
	if len(log) != 2 || log[0] != want[0] || log[1] != want[1] {
		t.Fatalf("sent %v, want %v", log, want)
	}
	if b := fx.places["B"].objs["laptop/data/1"]; string(b) != "second" {
		t.Fatalf("B holds %q", b)
	}
}

// §3.4 Errors, LEND-2, PEER-5: quota and removal refusals pause the
// place with that condition and keep the row queued; when the friend
// accepts again the FIFO drains.
func TestQuotaAndRemovalPauseThePlace(t *testing.T) {
	for _, kind := range []struct {
		err  error
		cond string
	}{{ErrQuota, state.ConditionQuota}, {ErrRemoved, state.ConditionRemoved}, {errors.New("connection refused"), state.ConditionRetrying}} {
		t.Run(kind.cond, func(t *testing.T) {
			fx := newFixture(t, "B")
			var mu sync.Mutex
			refuse := true
			fx.places["B"].fail = func(string, string) error {
				mu.Lock()
				defer mu.Unlock()
				if refuse {
					return kind.err
				}
				return nil
			}
			fx.write("laptop", "data/1", "x")
			fx.write("laptop", "data/2", "y")
			fx.run()
			waitFor(t, kind.cond, func() bool { return fx.condition("B").Condition == kind.cond })
			if p, ok, _ := fx.db.NextPlacement(t.Context(), "B"); !ok || p.Key != "data/1" {
				t.Fatalf("head %+v %v", p, ok)
			}
			mu.Lock()
			refuse = false
			mu.Unlock()
			fx.drained("B")
			if c := fx.condition("B"); c.Condition != state.ConditionOK {
				t.Fatalf("condition %+v", c)
			}
		})
	}
}

// #53: a transient error reading the friendships keeps the senders
// running; it is not read as "no friends".
func TestFriendshipReadErrorKeepsSenders(t *testing.T) {
	fx := newFixture(t, "B")
	var failing atomic.Bool
	var connects atomic.Int32
	fx.w.friendships = func(ctx context.Context) ([]state.Friendship, error) {
		if failing.Load() {
			return nil, errors.New("database is locked")
		}
		return fx.db.Friendships(ctx)
	}
	connect := fx.w.Connect
	fx.w.Connect = func(f state.Friendship) (Replica, error) { connects.Add(1); return connect(f) }
	fx.write("laptop", "data/1", "x")
	fx.run()
	fx.drained("B")
	failing.Store(true)
	time.Sleep(20 * fx.w.Poll) // several friendship reads fail
	fx.write("laptop", "data/2", "y")
	fx.drained("B")
	failing.Store(false)
	time.Sleep(10 * fx.w.Poll)
	if n := connects.Load(); n != 1 {
		t.Fatalf("the sender was rebuilt: %d connects", n)
	}
}

// EP-11, REPL-10: an immutability refusal, or a hash the place rejects
// because the local copy changed after capture, is not retried: the row
// leaves the FIFO as refused and the next one is sent.
func TestImmutabilityAndRejectedHashAreRefused(t *testing.T) {
	fx := newFixture(t, "B")
	fx.places["B"].fail = func(_, key string) error {
		if key == "data/immutable" {
			return ErrImmutable
		}
		return nil
	}
	fx.write("laptop", "data/immutable", "a")
	fx.write("laptop", "data/rotted", "original")
	// The store's copy rots after capture, keeping its size.
	os.WriteFile(filepath.Join(fx.store, "laptop", "data", "rotted"), []byte("0riginal"), 0o600)
	fx.write("laptop", "data/fine", "fine")
	fx.run()
	fx.drained("B")
	log, _, _, _ := fx.places["B"].snapshot()
	if len(log) != 1 || log[0] != "put laptop/data/fine" {
		t.Fatalf("sent %v", log)
	}
	// The friend refused the immutable key; the rotted copy is the
	// bucket's fault, not the friend's (#50).
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 1 || fx.refused("laptop") != 1 {
		t.Fatalf("stats %+v, bucket refused %d", st["B"], fx.refused("laptop"))
	}
}

// REPL-1, REPL-10 (#48): a row whose object changed size or vanished
// under it, with no later capture of the key to supersede it, is refused
// rather than retried for ever; one whose key a capture in flight is
// rewriting waits for that capture and is then cancelled.
func TestChangedObjectDoesNotStallTheFIFO(t *testing.T) {
	fx := newFixture(t, "B")
	ctx := t.Context()
	fx.write("laptop", "data/grown", "short")
	os.WriteFile(filepath.Join(fx.store, "laptop", "data", "grown"), []byte("longer now"), 0o600)
	fx.write("laptop", "data/vanished", "v")
	os.Remove(filepath.Join(fx.store, "laptop", "data", "vanished"))
	fx.write("laptop", "data/fine", "fine")
	fx.write("laptop", "data/rewritten", "old")
	// A rewrite of the key is in flight: stored, its row still pending.
	later, err := fx.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/rewritten", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(fx.store, "laptop", "data", "rewritten"), []byte("newer"), 0o600)
	fx.run()
	waitFor(t, "data/fine sent", func() bool {
		log, _, _, _ := fx.places["B"].snapshot()
		return len(log) == 1
	})
	if st, _ := fx.db.PlaceStats(ctx, time.Now()); fx.refused("laptop") != 2 || st["B"].Refused != 0 || st["B"].Queued != 1 {
		t.Fatalf("stats %+v, bucket refused %d: want two refused against the bucket and the rewritten row waiting",
			st["B"], fx.refused("laptop"))
	}
	// Waiting for the rewrite is not the friend's failure (#50).
	if c := fx.condition("B"); c.Condition != state.ConditionOK {
		t.Fatalf("condition %+v while the rewrite is in flight", c)
	}
	sum := sha256.Sum256([]byte("newer"))
	if _, err := fx.db.FinishPut(ctx, later[0], 5, hex.EncodeToString(sum[:])); err != nil {
		t.Fatal(err)
	}
	fx.drained("B")
	log, _, _, _ := fx.places["B"].snapshot()
	if len(log) != 2 || log[0] != "put laptop/data/fine" || log[1] != "put laptop/data/rewritten" {
		t.Fatalf("sent %v", log)
	}
	if st, _ := fx.db.PlaceStats(ctx, time.Now()); st["B"].Refused != 0 || fx.refused("laptop") != 2 {
		t.Fatalf("stats %+v", st["B"])
	}
}

// REPL-1, REPL-3, VER-5 (#50): a row the store can never supply as
// captured is refused against its bucket, once for every place, with an
// alert, and each place's FIFO moves on: a directory where an object
// should be, a truncated object, a missing file, an object larger than
// one PUT carries (until #23), a key that names no file in the store. A
// directory marker (photos/ in a generic bucket, which the posix store
// keeps as a directory) is an empty object and is sent as one.
func TestPermanentLocalErrorsDoNotStallTheFIFO(t *testing.T) {
	fx := newFixture(t, "B", "C")
	bucket := filepath.Join(fx.store, "laptop")
	os.MkdirAll(filepath.Join(bucket, "photos"), 0o700)
	os.WriteFile(filepath.Join(bucket, "photos", "cat.jpg"), []byte("meow"), 0o600)
	fx.capture("laptop", "photos/", 0, "")
	os.MkdirAll(filepath.Join(bucket, "album"), 0o700)
	fx.capture("laptop", "album", 5, "album")
	fx.write("laptop", "data/truncated", "whole object")
	os.Truncate(filepath.Join(bucket, "data", "truncated"), 3)
	fx.write("laptop", "data/missing", "m")
	os.Remove(filepath.Join(bucket, "data", "missing"))
	f, err := os.Create(filepath.Join(bucket, "data", "huge"))
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(MaxPutSize + 1) // sparse
	f.Close()
	fx.capture("laptop", "data/huge", MaxPutSize+1, "huge")
	fx.capture("laptop", "a/../../escape", 1, "e")
	fx.write("laptop", "data/fine", "fine")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	for _, p := range []string{"B", "C"} {
		log, _, _, _ := fx.places[p].snapshot()
		if len(log) != 2 || log[0] != "put laptop/photos/" || log[1] != "put laptop/data/fine" {
			t.Fatalf("%s got %v", p, log)
		}
		if c := fx.condition(p); c.Condition != state.ConditionOK {
			t.Fatalf("%s condition %+v", p, c)
		}
	}
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 0 || st["C"].Refused != 0 || fx.refused("laptop") != 5 {
		t.Fatalf("places %+v, bucket refused %d; want 5 against the bucket", st, fx.refused("laptop"))
	}
	// Only the huge object is refused as oversize, so multipart
	// replication (#23) re-queues it and nothing else (#61).
	if n, err := fx.db.RequeueOversize(t.Context(), MaxPutSize); err != nil || n != 2 {
		t.Fatalf("re-queued %d rows (%v), want data/huge at B and C", n, err)
	}
}

// REPL-3 (#50, #57): when the bucket's directory in the store cannot be
// read at all (a store disk not mounted), nothing is refused and the
// friend is not blamed: the rows wait until the store is back.
func TestUnreadableStoreWaitsWithoutRefusing(t *testing.T) {
	fx := newFixture(t, "B")
	fx.write("laptop", "data/1", "one")
	bucket := filepath.Join(fx.store, "laptop")
	aside := filepath.Join(fx.store, "aside")
	if err := os.Rename(bucket, aside); err != nil {
		t.Fatal(err)
	}
	fx.run()
	time.Sleep(200 * time.Millisecond)
	if st, _ := fx.db.PlaceStats(t.Context(), time.Now()); st["B"].Queued != 1 || fx.refused("laptop") != 0 {
		t.Fatalf("stats %+v, refused %d", st["B"], fx.refused("laptop"))
	}
	// The place shows the store as the cause, not the friend (#57).
	if c := fx.condition("B"); c.Condition != state.ConditionStore || !strings.Contains(c.Detail, "bucket laptop") {
		t.Fatalf("condition %+v", c)
	}
	if err := os.Rename(aside, bucket); err != nil {
		t.Fatal(err)
	}
	fx.drained("B")
}

// REPL-3: the FIFO is in lendisk.db, so a worker stopped mid-way and
// started again continues where it was, sending nothing twice that was
// acknowledged.
func TestWorkerRestartContinues(t *testing.T) {
	fx := newFixture(t, "B")
	for _, k := range []string{"data/1", "data/2", "data/3"} {
		fx.write("laptop", k, k)
	}
	b := fx.places["B"]
	stopAfter := make(chan struct{})
	var once sync.Once
	b.fail = func(_, key string) error {
		if key == "data/2" {
			once.Do(func() { close(stopAfter) })
			return errors.New("connection reset")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); fx.w.Run(ctx) }()
	<-stopAfter
	cancel()
	<-done
	b.mu.Lock()
	b.fail = nil
	b.mu.Unlock()
	fx.run()
	fx.drained("B")
	log, _, _, _ := b.snapshot()
	if len(log) != 3 || log[0] != "put laptop/data/1" || log[1] != "put laptop/data/2" || log[2] != "put laptop/data/3" {
		t.Fatalf("sent %v", log)
	}
}

// logBuffer collects a worker's log as JSON lines.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

// alerts returns the log records with alert=true whose message
// contains msg.
func (l *logBuffer) alerts(msg string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(l.b.String(), "\n") {
		var r map[string]any
		if json.Unmarshal([]byte(line), &r) == nil && r["alert"] == true && strings.Contains(fmt.Sprint(r["msg"]), msg) {
			out = append(out, r)
		}
	}
	return out
}

func (fx *fixture) logTo() *logBuffer {
	l := &logBuffer{}
	fx.w.Log = slog.New(slog.NewJSONHandler(l, nil))
	return l
}

// failingFile is a store file whose reads fail after the first byte.
type failingFile struct {
	fs.File
	err  error
	read bool
}

func (f *failingFile) Read(p []byte) (int, error) {
	if f.read {
		return 0, f.err
	}
	f.read = true
	return f.File.Read(p[:1])
}

// REPL-1, REPL-3 (#57): an error opening or reading an object that may
// pass (EMFILE under load, EACCES until ownership is fixed, EIO from a
// disk dropping out) is retried with backoff and alerted, never
// refused; the place shows the store as the cause; once the store can
// be read the object is sent.
func TestTemporaryStoreErrorsAreRetriedNotRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(f fs.File) (fs.File, error)
	}{
		{"EMFILE at open", func(f fs.File) (fs.File, error) {
			f.Close()
			return nil, &fs.PathError{Op: "open", Path: "x", Err: syscall.EMFILE}
		}},
		{"EACCES at open", func(f fs.File) (fs.File, error) {
			f.Close()
			return nil, &fs.PathError{Op: "open", Path: "x", Err: syscall.EACCES}
		}},
		{"EIO reading", func(f fs.File) (fs.File, error) {
			return &failingFile{File: f, err: &fs.PathError{Op: "read", Path: "x", Err: syscall.EIO}}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, "B", "C")
			logs := fx.logTo()
			fx.write("laptop", "data/1", "one")
			fx.write("laptop", "data/2", "two")
			var failing atomic.Bool
			failing.Store(true)
			fx.w.open = func(name string) (fs.File, error) {
				f, err := os.Open(name)
				if err != nil || !failing.Load() || filepath.Base(name) != "1" {
					return f, err
				}
				return tc.fail(f)
			}
			fx.run()
			waitFor(t, "both places waiting on the store", func() bool {
				return fx.condition("B").Condition == state.ConditionStore && fx.condition("C").Condition == state.ConditionStore
			})
			time.Sleep(100 * time.Millisecond) // several retries
			if st, _ := fx.db.PlaceStats(t.Context(), time.Now()); st["B"].Queued != 2 || fx.refused("laptop") != 0 {
				t.Fatalf("stats %+v, refused %d", st["B"], fx.refused("laptop"))
			}
			if c := fx.condition("B"); c.Detail == "" {
				t.Fatalf("no cause shown: %+v", c)
			}
			if len(logs.alerts("store")) == 0 {
				t.Fatal("not alerted")
			}
			failing.Store(false)
			fx.drained("B")
			fx.drained("C")
			for _, p := range []string{"B", "C"} {
				if log, _, _, _ := fx.places[p].snapshot(); len(log) != 2 {
					t.Fatalf("%s got %v", p, log)
				}
				if c := fx.condition(p); c.Condition != state.ConditionOK {
					t.Fatalf("%s condition %+v", p, c)
				}
			}
		})
	}
}

// SEC-7, REPL-1 (#57): a friend that answers BadDigest to everything,
// while the store's copy matches its capture hash, has each row refused
// at that friend only, with an alert; replication to another friend is
// untouched and nothing counts against the bucket.
func TestFriendRejectingEverythingDoesNotStopTheOthers(t *testing.T) {
	fx := newFixture(t, "B", "C")
	logs := fx.logTo()
	fx.places["B"].fail = func(string, string) error {
		return fmt.Errorf("%w: BadDigest", ErrRejected)
	}
	for _, k := range []string{"data/1", "data/2", "index/1"} {
		fx.write("laptop", k, "content of "+k)
	}
	fx.run()
	fx.drained("B")
	fx.drained("C")
	if log, _, _, _ := fx.places["C"].snapshot(); len(log) != 3 {
		t.Fatalf("C got %v", log)
	}
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 3 || st["C"].Refused != 0 || fx.refused("laptop") != 0 {
		t.Fatalf("places %+v, bucket refused %d", st, fx.refused("laptop"))
	}
	if n := len(logs.alerts("")); n != 3 {
		t.Fatalf("%d alerts, want 3", n)
	}
}

// VER-5, REPL-10 (#57): when a place answers BadDigest and the store's
// copy no longer matches its capture hash, the copy rotted: the row is
// refused for every place, against the bucket, with an alert.
func TestRottedObjectIsRefusedForEveryPlaceWithAnAlert(t *testing.T) {
	fx := newFixture(t, "B", "C")
	logs := fx.logTo()
	fx.write("laptop", "data/rotted", "original")
	os.WriteFile(filepath.Join(fx.store, "laptop", "data", "rotted"), []byte("0riginal"), 0o600)
	fx.write("laptop", "data/fine", "fine")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	for _, p := range []string{"B", "C"} {
		if log, _, _, _ := fx.places[p].snapshot(); len(log) != 1 || log[0] != "put laptop/data/fine" {
			t.Fatalf("%s got %v", p, log)
		}
	}
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 0 || st["C"].Refused != 0 || fx.refused("laptop") != 1 {
		t.Fatalf("places %+v, bucket refused %d", st, fx.refused("laptop"))
	}
	as := logs.alerts("")
	if len(as) != 1 || !strings.Contains(fmt.Sprint(as[0]["err"]), "capture hash") {
		t.Fatalf("alerts %v", as)
	}
}

// REPL-1 (#57): re-uploading the same bytes after a refusal, by a
// friend or because the store's copy rotted, replicates the object, and
// the refusal no longer counts.
func TestReuploadAfterRefusalReplicates(t *testing.T) {
	fx := newFixture(t, "B", "C")
	var mu sync.Mutex
	hostile := true
	fx.places["B"].fail = func(_, key string) error {
		mu.Lock()
		defer mu.Unlock()
		if hostile && key == "data/1" {
			return ErrRejected
		}
		return nil
	}
	fx.write("laptop", "data/1", "one")
	fx.write("laptop", "data/2", "two")
	os.WriteFile(filepath.Join(fx.store, "laptop", "data", "2"), []byte("tw0"), 0o600)
	fx.run()
	fx.drained("B")
	fx.drained("C")
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 1 || fx.refused("laptop") != 1 {
		t.Fatalf("places %+v, bucket refused %d", st, fx.refused("laptop"))
	}
	mu.Lock()
	hostile = false
	mu.Unlock()
	fx.write("laptop", "data/1", "one")
	fx.write("laptop", "data/2", "two")
	fx.drained("B")
	fx.drained("C")
	for _, p := range []string{"B", "C"} {
		pl := fx.places[p]
		pl.mu.Lock()
		one, two := string(pl.objs["laptop/data/1"]), string(pl.objs["laptop/data/2"])
		pl.mu.Unlock()
		if one != "one" || two != "two" {
			t.Fatalf("%s holds %q, %q", p, one, two)
		}
	}
	st, _ = fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 0 || fx.refused("laptop") != 0 {
		t.Fatalf("places %+v, bucket refused %d after healing", st, fx.refused("laptop"))
	}
}

// #57, #61: a place whose FIFO empties because its rows were refused for
// the store goes from store back to ok; a place's own answer (retrying,
// removed) stands until it answers again, also at daemon start.
func TestEmptyFIFOKeepsThePlacesOwnAnswer(t *testing.T) {
	t.Run("store resets", func(t *testing.T) {
		fx := newFixture(t, "B")
		seq := fx.write("laptop", "data/1", "one")
		fx.w.open = func(name string) (fs.File, error) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.EIO}
		}
		fx.run()
		waitFor(t, "store", func() bool { return fx.condition("B").Condition == state.ConditionStore })
		if _, err := fx.db.RefuseObject(t.Context(), seq, state.CauseStore, "rotted"); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
		if c := fx.condition("B"); c.Detail != "" || !c.LastOK.IsZero() {
			t.Fatalf("condition %+v", c)
		}
	})
	t.Run("retrying stays until the place answers", func(t *testing.T) {
		fx := newFixture(t, "B")
		var offline atomic.Bool
		offline.Store(true)
		fx.places["B"].fail = func(string, string) error {
			if offline.Load() {
				return errors.New("connection refused")
			}
			return nil
		}
		seq := fx.write("laptop", "data/1", "one")
		fx.run()
		waitFor(t, "retrying", func() bool { return fx.condition("B").Condition == state.ConditionRetrying })
		// Another place's sender finds the store's copy rotted.
		if _, err := fx.db.RefuseObject(t.Context(), seq, state.CauseStore, "rotted"); err != nil {
			t.Fatal(err)
		}
		fx.drained("B")
		time.Sleep(20 * fx.w.Poll) // the sender polls its empty FIFO
		if c := fx.condition("B"); c.Condition != state.ConditionRetrying {
			t.Fatalf("an unreachable place shows %+v", c)
		}
		offline.Store(false)
		fx.write("laptop", "data/2", "two")
		fx.drained("B")
		if c := fx.condition("B"); c.Condition != state.ConditionOK || c.LastOK.IsZero() {
			t.Fatalf("condition %+v after the place answered", c)
		}
	})
	t.Run("at daemon start", func(t *testing.T) {
		fx := newFixture(t, "B", "C")
		now := time.Now()
		if err := fx.db.SetPlaceCondition(t.Context(), "B", state.ConditionRemoved, "AccessDenied", now); err != nil {
			t.Fatal(err)
		}
		if err := fx.db.SetPlaceCondition(t.Context(), "C", state.ConditionStore, "EIO", now); err != nil {
			t.Fatal(err)
		}
		fx.run()
		waitFor(t, "C ok", func() bool { return fx.condition("C").Condition == state.ConditionOK })
		time.Sleep(10 * fx.w.Poll)
		if c := fx.condition("B"); c.Condition != state.ConditionRemoved {
			t.Fatalf("a friend that removed this node shows %+v", c)
		}
	})
}

// REPL-3, OPS-1 (#61): a store failure is alerted when it follows any
// other outcome, even with the network backoff at its maximum because
// the friend was offline; later failures of the same run are not
// alerted again.
func TestStoreFailureAlertsAtMaximumNetworkBackoff(t *testing.T) {
	fx := newFixture(t, "B")
	logs := fx.logTo()
	var attempts atomic.Int32
	fx.places["B"].fail = func(string, string) error {
		attempts.Add(1)
		return errors.New("connection refused")
	}
	fx.write("laptop", "data/1", "one")
	var failing atomic.Bool
	fx.w.open = func(name string) (fs.File, error) {
		if failing.Load() {
			return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.EIO}
		}
		return os.Open(name)
	}
	fx.run()
	// MinBackoff 10 ms doubles to MaxBackoff 20 ms after the second try.
	waitFor(t, "maximum backoff", func() bool { return attempts.Load() >= 4 })
	failing.Store(true)
	waitFor(t, "store", func() bool { return fx.condition("B").Condition == state.ConditionStore })
	waitFor(t, "an alert", func() bool { return len(logs.alerts("store")) > 0 })
	time.Sleep(100 * time.Millisecond) // several store retries
	if n := len(logs.alerts("store")); n != 1 {
		t.Fatalf("%d store alerts, want 1", n)
	}
}

// EP-3, REPL-1, SEC-7 (#61): while one friend refuses everything, a
// client re-uploading the same bytes heals the refusal there but sends
// nothing again to a friend that already acknowledged the hash.
func TestHealingReuploadIsNotResentToAnHonestFriend(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.places["B"].fail = func(string, string) error { return fmt.Errorf("%w: BadDigest", ErrRejected) }
	fx.write("laptop", "data/1", "one")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	for range 3 {
		fx.write("laptop", "data/1", "one")
		fx.drained("B")
		fx.drained("C")
	}
	if log, _, _, read := fx.places["C"].snapshot(); len(log) != 1 || read != 3 {
		t.Fatalf("C was sent %v, %d bytes", log, read)
	}
	if _, _, _, read := fx.places["B"].snapshot(); read != 4*3 {
		t.Fatalf("B read %d bytes; each re-upload heals there", read)
	}
	st, _ := fx.db.PlaceStats(t.Context(), time.Now())
	if st["B"].Refused != 1 || st["C"].Refused != 0 || st["C"].Queued != 0 {
		t.Fatalf("places %+v", st)
	}
}

// hungFile is a store file whose reads block until release is closed.
type hungFile struct {
	fs.File
	release chan struct{}
}

func (f *hungFile) Read(p []byte) (int, error) {
	<-f.release
	return 0, errors.New("released")
}

// REPL-3, OPS-1 (#61): a store read that never returns while an object
// is sent ends the send as the store's failure: the place shows store,
// not retrying, and the object is sent once the store answers.
func TestHungStoreReadIsTheStoresFault(t *testing.T) {
	fx := newFixture(t, "B")
	fx.w.StoreStall = 30 * time.Millisecond
	logs := fx.logTo()
	fx.write("laptop", "data/1", "one")
	release := make(chan struct{})
	var hung atomic.Bool
	hung.Store(true)
	fx.w.open = func(name string) (fs.File, error) {
		f, err := os.Open(name)
		if err != nil || !hung.Load() {
			return f, err
		}
		return &hungFile{File: f, release: release}, nil
	}
	fx.run()
	waitFor(t, "store", func() bool { return fx.condition("B").Condition == state.ConditionStore })
	if c := fx.condition("B"); !strings.Contains(c.Detail, "did not return") {
		t.Fatalf("condition %+v", c)
	}
	if len(logs.alerts("store")) == 0 {
		t.Fatal("not alerted")
	}
	// The sender is not stuck behind the hung read: the object is sent
	// while that read still blocks.
	hung.Store(false)
	fx.drained("B")
	close(release)
	// The condition is reset after the FIFO drains, not with it.
	waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
}

// REPL-3, OPS-1 (#61): when a place rejects the bytes, the store's copy
// is re-hashed; a re-hash read that never returns is the store's
// failure too, and the sender is not stuck behind it.
func TestHungReHashIsTheStoresFault(t *testing.T) {
	fx := newFixture(t, "B")
	fx.w.StoreStall = 30 * time.Millisecond
	var reject atomic.Bool
	reject.Store(true)
	fx.places["B"].fail = func(string, string) error {
		if reject.Load() {
			return fmt.Errorf("%w: BadDigest", ErrRejected)
		}
		return nil
	}
	fx.write("laptop", "data/1", "one")
	release := make(chan struct{})
	var opens, hung atomic.Int32
	hung.Store(1)
	fx.w.open = func(name string) (fs.File, error) {
		f, err := os.Open(name)
		// The send's open reads; the re-hash's open hangs.
		if err != nil || opens.Add(1)%2 == 1 || hung.Load() == 0 {
			return f, err
		}
		return &hungFile{File: f, release: release}, nil
	}
	fx.run()
	waitFor(t, "store", func() bool { return fx.condition("B").Condition == state.ConditionStore })
	if c := fx.condition("B"); !strings.Contains(c.Detail, "did not return") {
		t.Fatalf("condition %+v", c)
	}
	hung.Store(0)
	reject.Store(false)
	fx.drained("B")
	close(release)
	// The condition is reset after the FIFO drains, not with it.
	waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
}

// REPL-3, OPS-1 (#61): through the real friend client, whose HTTP
// transport does not return while the request body's Read is blocked, a
// store read that never returns still ends the send as the store's
// failure: the sender is not stuck behind the disk.
func TestHungStoreReadDoesNotHangTheRealClient(t *testing.T) {
	fx := newFixture(t)
	self := newID(t)
	friend, peer := tunnelLender(t, self, &recorder{})
	place := peer.String()
	if err := fx.db.AddFriendship(t.Context(), state.Friendship{Peer: place, Address: "tc", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	fx.w.Connect = func(state.Friendship) (Replica, error) { return friend, nil }
	fx.w.StoreStall = 100 * time.Millisecond
	fx.write("laptop", "data/1", "one")
	release := make(chan struct{})
	var once sync.Once
	unhang := func() { once.Do(func() { close(release) }) }
	var hung atomic.Bool
	hung.Store(true)
	fx.w.open = func(name string) (fs.File, error) {
		f, err := os.Open(name)
		if err != nil || !hung.Load() {
			return f, err
		}
		return &hungFile{File: f, release: release}, nil
	}
	fx.run()
	t.Cleanup(unhang) // runs before the worker stops
	waitFor(t, "store", func() bool { return fx.condition(place).Condition == state.ConditionStore })
	if c := fx.condition(place); !strings.Contains(c.Detail, "did not return") {
		t.Fatalf("condition %+v", c)
	}
	// The sender is free while the hung read still blocks: once the
	// disk answers new reads, the object is sent.
	hung.Store(false)
	fx.drained(place)
	// The condition is reset after the FIFO drains, not with it.
	waitFor(t, "ok", func() bool { return fx.condition(place).Condition == state.ConditionOK })
	unhang()
}
