package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

// upload is an open multipart upload at an in-memory place.
type upload struct {
	key, sha256, replaces string
	parts                 map[int][]byte
}

func (p *place) Create(ctx context.Context, o Object) (string, error) {
	defer p.enter(o.Key)()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		if err := p.fail("create", o.Key); err != nil {
			return "", err
		}
	}
	if p.uploads == nil {
		p.uploads = map[string]*upload{}
	}
	p.nextID++
	id := fmt.Sprintf("%s-up%d", p.name, p.nextID)
	p.uploads[id] = &upload{key: o.Bucket + "/" + o.Key, sha256: o.SHA256, replaces: o.Replaces, parts: map[int][]byte{}}
	l := "create " + o.Bucket + "/" + o.Key
	if o.Replaces != "" {
		l += " replaces " + o.Replaces[:8]
	}
	p.log = append(p.log, l)
	return id, nil
}

func (p *place) Parts(ctx context.Context, bucket, key, id string) (map[int]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	u := p.uploads[id]
	if u == nil {
		return nil, ErrNoSuchUpload
	}
	out := map[int]string{}
	for n, b := range u.parts {
		out[n] = etagOf(b)
	}
	return out, nil
}

func etagOf(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:8]) }

func (p *place) PutPart(ctx context.Context, bucket, key, id string, n int, body io.Reader, size int64) (Part, error) {
	defer p.enter(key)()
	b, err := io.ReadAll(body)
	if err != nil {
		return Part{}, err
	}
	p.mu.Lock()
	p.bytesRead += int64(len(b))
	if p.fail != nil {
		if err := p.fail(fmt.Sprintf("part %d", n), key); err != nil {
			p.mu.Unlock()
			return Part{}, err
		}
	}
	u := p.uploads[id]
	if u == nil {
		p.mu.Unlock()
		return Part{}, ErrNoSuchUpload
	}
	if int64(len(b)) != size {
		p.mu.Unlock()
		return Part{}, fmt.Errorf("part %d: %d bytes for %d", n, len(b), size)
	}
	u.parts[n] = b
	p.log = append(p.log, fmt.Sprintf("part %s/%s %d", bucket, key, n))
	on := p.onPart
	p.mu.Unlock()
	if on != nil {
		on(key, n)
	}
	sum := sha256.Sum256(b)
	return Part{Number: n, ETag: etagOf(b), SHA256: hex.EncodeToString(sum[:]), Size: size}, nil
}

func (p *place) Complete(ctx context.Context, bucket, key, id string, parts []Part) error {
	defer p.enter(key)()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		if err := p.fail("complete", key); err != nil {
			return err
		}
	}
	u := p.uploads[id]
	if u == nil {
		return ErrNoSuchUpload
	}
	var whole []byte
	for i, part := range parts {
		b, ok := u.parts[part.Number]
		if !ok || part.Number != i+1 || etagOf(b) != part.ETag {
			return fmt.Errorf("InvalidPart %d", part.Number)
		}
		whole = append(whole, b...)
	}
	delete(p.uploads, id)
	if sum := sha256.Sum256(whole); hex.EncodeToString(sum[:]) != u.sha256 {
		return ErrRejected
	}
	p.objs[u.key] = whole
	p.log = append(p.log, "complete "+u.key)
	return nil
}

func (p *place) Abort(ctx context.Context, bucket, key, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.uploads[id]; ok {
		delete(p.uploads, id)
		p.log = append(p.log, "abort "+bucket+"/"+key)
	}
	return nil
}

// pacedFixture gives the fixture's worker a pacer on a fake clock that
// its sleeps advance, with the part size and limits of s.
func (fx *fixture) paced(s pace.Settings) (*pace.Pacer, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	p := pace.New(c.now)
	p.Sleep = c.sleep
	p.Set(s)
	fx.w.Pace = p
	return p, c
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) set(t time.Time) { c.mu.Lock(); c.t = t; c.mu.Unlock() }
func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return ctx.Err()
}

func (fx *fixture) placeLog(place string) []string {
	log, _, _, _ := fx.places[place].snapshot()
	return log
}

func (fx *fixture) transfer(place string, seq int64) (state.Transfer, bool) {
	fx.t.Helper()
	tr, ok, err := fx.db.Transfer(fx.t.Context(), place, seq)
	if err != nil {
		fx.t.Fatal(err)
	}
	return tr, ok
}

const twentyThree = "abcdefghijklmnopqrstuvw" // five parts of 5 bytes, the last 3

// REPL-6 (#23): an object larger than the part size is sent in parts;
// the upload ID and each completed part are kept in lendisk.db as the
// parts complete, so a failed part is sent again without the parts
// before it, and the record goes once the place has the object.
func TestALargeObjectIsSentInPartsAndRetriedFromTheFailedPart(t *testing.T) {
	fx := newFixture(t, "B")
	fx.paced(pace.Settings{PartSize: 5})
	var failed atomic.Bool
	seq := fx.write("laptop", "data/big", twentyThree)
	fx.places["B"].fail = func(op, key string) error {
		if op == "part 3" && !failed.Swap(true) {
			tr, ok, err := fx.db.Transfer(context.Background(), "B", seq)
			if err != nil || !ok || len(tr.Parts) != 2 || tr.Parts[1].Number != 2 || tr.Parts[1].SHA256 == "" {
				t.Errorf("at part 3 the row's transfer is %+v %v %v, want parts 1 and 2", tr, ok, err)
			}
			return errors.New("connection reset")
		}
		return nil
	}
	fx.run()
	fx.drained("B")
	want := []string{"create laptop/data/big", "part laptop/data/big 1", "part laptop/data/big 2",
		"part laptop/data/big 3", "part laptop/data/big 4", "part laptop/data/big 5", "complete laptop/data/big"}
	if got := fx.placeLog("B"); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("sent %v, want %v", got, want)
	}
	if string(fx.places["B"].objs["laptop/data/big"]) != twentyThree {
		t.Fatal("assembled wrong")
	}
	if _, ok := fx.transfer("B", seq); ok {
		t.Fatal("the transfer is kept after the acknowledgement")
	}
	if _, _, _, read := fx.places["B"].snapshot(); read != 23+5 {
		t.Fatalf("read %d bytes, want 23 and part 3 again", read)
	}
}

// REPL-6 (#23): a worker stopped mid-object (the daemon restarted)
// continues at the next part from the record in lendisk.db: the place
// lists the parts it holds, only those the record has and the place
// lists with the same ETag count, and the rest are sent. A part the
// place holds but the record missed is sent again.
func TestARestartContinuesAtTheNextPart(t *testing.T) {
	fx := newFixture(t, "B")
	fx.paced(pace.Settings{PartSize: 5})
	seq := fx.write("laptop", "data/big", twentyThree)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	fx.places["B"].onPart = func(key string, n int) {
		if n == 3 {
			// Part 3 is stored at the place, but the worker stops
			// before it records it.
			cancel()
		}
	}
	go func() { defer close(stopped); fx.w.Run(ctx) }()
	<-stopped
	tr, ok := fx.transfer("B", seq)
	if !ok || len(tr.Parts) != 2 {
		t.Fatalf("after the stop: %+v %v, want parts 1 and 2 recorded", tr, ok)
	}
	fx.places["B"].onPart = nil
	before := len(fx.placeLog("B"))
	fx.w = &Worker{DB: fx.db, StoreDir: fx.store, Log: quiet, Pace: fx.w.Pace,
		Connect: func(f state.Friendship) (Replica, error) { return fx.places[f.Peer], nil },
		Poll:    10 * time.Millisecond, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
	fx.run()
	fx.drained("B")
	got := fx.placeLog("B")[before:]
	want := []string{"part laptop/data/big 3", "part laptop/data/big 4", "part laptop/data/big 5", "complete laptop/data/big"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("after the restart sent %v, want %v", got, want)
	}
	if string(fx.places["B"].objs["laptop/data/big"]) != twentyThree {
		t.Fatal("assembled wrong")
	}
}

// REPL-6 (#23): an upload the place no longer has (aborted, expired, or
// the place's store lost it) starts the object over.
func TestAnUploadGoneAtThePlaceStartsOver(t *testing.T) {
	fx := newFixture(t, "B")
	fx.paced(pace.Settings{PartSize: 5})
	b := fx.places["B"]
	var lost atomic.Bool
	b.fail = func(op, key string) error {
		if op == "part 4" && !lost.Swap(true) {
			b.uploads = map[string]*upload{} // under b.mu
			return ErrNoSuchUpload
		}
		return nil
	}
	fx.write("laptop", "data/big", twentyThree)
	fx.run()
	fx.drained("B")
	if string(b.objs["laptop/data/big"]) != twentyThree {
		t.Fatal("assembled wrong")
	}
	creates := 0
	for _, l := range fx.placeLog("B") {
		if strings.HasPrefix(l, "create") {
			creates++
		}
	}
	if creates != 2 {
		t.Fatalf("%d uploads created, want the object started over once: %v", creates, fx.placeLog("B"))
	}
}

// REPL-6 (#23): an object no larger than the part size is one PUT and,
// interrupted, is sent again whole; nothing is recorded for it.
func TestASmallObjectRestartsWhole(t *testing.T) {
	fx := newFixture(t, "B")
	fx.paced(pace.Settings{PartSize: 5})
	var failed atomic.Bool
	fx.places["B"].fail = func(op, key string) error {
		if op == "put" && !failed.Swap(true) {
			return errors.New("connection reset")
		}
		return nil
	}
	seq := fx.write("laptop", "data/small", "abcde")
	fx.run()
	fx.drained("B")
	if got := fx.placeLog("B"); len(got) != 1 || got[0] != "put laptop/data/small" {
		t.Fatalf("sent %v", got)
	}
	if _, _, _, read := fx.places["B"].snapshot(); read != 10 {
		t.Fatalf("read %d, want the object twice", read)
	}
	if _, ok := fx.transfer("B", seq); ok {
		t.Fatal("a single PUT left a transfer")
	}
}

// REPL-7 (#23): outside the schedule nothing is sent; a window closing
// mid-object stops it at the end of the part in flight, and the
// reopened window sends only the rest.
func TestAClosingWindowStopsAtTheEndOfThePart(t *testing.T) {
	fx := newFixture(t, "B")
	s, err := pace.ParseSchedule("01:00-07:00")
	if err != nil {
		t.Fatal(err)
	}
	_, clock := fx.paced(pace.Settings{PartSize: 5, Schedule: s}) // noon: closed
	fx.write("laptop", "data/big", twentyThree)
	fx.run()
	time.Sleep(100 * time.Millisecond)
	if got := fx.placeLog("B"); len(got) != 0 {
		t.Fatalf("sent outside the window: %v", got)
	}
	day := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	fx.places["B"].onPart = func(key string, n int) {
		if n == 2 {
			clock.set(day.Add(7 * time.Hour)) // the window closes during part 2
		}
	}
	clock.set(day.Add(6 * time.Hour))
	waitFor(t, "part 2", func() bool { return len(fx.placeLog("B")) >= 3 })
	time.Sleep(100 * time.Millisecond)
	if got := fx.placeLog("B"); len(got) != 3 || got[2] != "part laptop/data/big 2" {
		t.Fatalf("after the window closed: %v", got)
	}
	fx.places["B"].onPart = nil
	clock.set(day.Add(25 * time.Hour)) // 01:00 the next day
	fx.drained("B")
	got := fx.placeLog("B")
	want := []string{"create laptop/data/big", "part laptop/data/big 1", "part laptop/data/big 2",
		"part laptop/data/big 3", "part laptop/data/big 4", "part laptop/data/big 5", "complete laptop/data/big"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("sent %v, want %v", got, want)
	}
}

// REPL-7 (#23): the limits wrap every body sent, single PUTs and parts:
// 1 MiB in parts and 256 KiB in one PUT at 128 KiB/s take the clock
// what they should, less the first burst.
func TestLimitsWrapEveryReplicaSend(t *testing.T) {
	fx := newFixture(t, "B")
	_, clock := fx.paced(pace.Settings{PartSize: 256 << 10, Peers: map[string]pace.Rates{"B": {Up: 128 << 10}}})
	big := strings.Repeat("x", 1<<20)
	fx.write("laptop", "data/big", big)
	fx.write("laptop", "data/small", big[:200<<10])
	start := clock.now()
	fx.run()
	fx.drained("B")
	el := clock.now().Sub(start)
	want := time.Duration(float64(1<<20+200<<10-pace.Burst) / float64(128<<10) * float64(time.Second))
	if el < want-time.Second || el > want+time.Second {
		t.Fatalf("1.2 MiB at 128 KiB/s took the clock %v, want about %v", el, want)
	}
}

// REPL-7, #76 (#23): a global upload limit admits only as many bodies
// at once as it keeps at pace.MinRate each, whatever the friends: here
// one, so two friends' senders take turns, and neither body trickles.
func TestAGlobalLimitPacesByConcurrency(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.paced(pace.Settings{Up: pace.MinRate + pace.MinRate/2})
	var in, most atomic.Int32
	for _, p := range []string{"B", "C"} {
		fx.places[p].delay = 20 * time.Millisecond
		fx.places[p].fail = func(op, key string) error {
			n := in.Add(1)
			defer in.Add(-1)
			for {
				m := most.Load()
				if n <= m || most.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			return nil
		}
	}
	for i := range 4 {
		fx.write("laptop", fmt.Sprintf("data/%d", i), "x")
	}
	fx.run()
	fx.drained("B")
	fx.drained("C")
	if most.Load() != 1 {
		t.Fatalf("%d bodies at once under a limit with room for one", most.Load())
	}
}

// REPL-6, #61 (#23): rows refused only as too large for one PUT are put
// back in their places' FIFOs when the worker starts, since it can now
// send them in parts; a store refusal is not.
func TestOversizeRowsAreRequeuedAtStart(t *testing.T) {
	fx := newFixture(t, "B")
	big := fx.write("laptop", "data/big", "big enough")
	gone := fx.write("laptop", "data/gone", "gone")
	for _, r := range []struct {
		seq   int64
		cause string
	}{{big, state.CauseOversize}, {gone, state.CauseStore}} {
		if ok, err := fx.db.RefuseObject(t.Context(), r.seq, r.cause, "refused"); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	fx.run()
	waitFor(t, "data/big", func() bool { return fx.holds("B", "laptop", "data/big") })
	if fx.holds("B", "laptop", "data/gone") || fx.refused("laptop") != 1 {
		t.Fatalf("the store refusal was sent, or the bucket counts %d refused", fx.refused("laptop"))
	}
}

// REPL-6 (#23): an object above one PUT (5 GiB) is no longer refused;
// it goes in parts. Above what the parts can carry it still is.
func TestAnObjectAboveOnePutGoesInParts(t *testing.T) {
	fx := newFixture(t, "B")
	bucket := filepath.Join(fx.store, "laptop", "data")
	os.MkdirAll(bucket, 0o700)
	if _, err := fx.db.ExecContext(t.Context(), `UPDATE friendship SET own_number = ?, peer_number = ?`, int64(1<<50), int64(1<<50)); err != nil {
		t.Fatal(err) // room to place them
	}
	// Refused before its file is opened: none is made.
	fx.capture("laptop", "data/too-huge", MaxObjectSize+1, "too-huge")
	f, err := os.Create(filepath.Join(bucket, "huge"))
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(MaxPutSize + 1) // sparse
	f.Close()
	fx.capture("laptop", "data/huge", MaxPutSize+1, "huge")
	// The place refuses the upload at its create, before a byte is read.
	fx.places["B"].fail = func(op, key string) error {
		if op == "create" {
			return ErrQuota
		}
		return nil
	}
	fx.run()
	waitFor(t, "the create", func() bool { return fx.condition("B").Condition == state.ConditionQuota })
	if got := fx.placeLog("B"); len(got) != 0 {
		t.Fatalf("log %v", got)
	}
	waitFor(t, "the refusal", func() bool { return fx.refused("laptop") >= 1 })
	if fx.refused("laptop") != 1 {
		t.Fatalf("refused %d, want only the object above %d bytes", fx.refused("laptop"), int64(MaxObjectSize))
	}
}

// REPL-6, #74 (#23): a rewrite large enough for parts is created marked
// with the version it replaces (x-amz-meta-lendisk-replaces at
// CreateMultipartUpload).
func TestALargeRewriteIsMarkedAtItsCreate(t *testing.T) {
	fx := newFixture(t, "B")
	fx.paced(pace.Settings{PartSize: 5})
	fx.writeAs("laptop", "kopia.repository", "v1", "metadata", true)
	fx.run()
	fx.drained("B")
	fx.writeAs("laptop", "kopia.repository", twentyThree, "metadata", true)
	fx.drained("B")
	v1 := sha256.Sum256([]byte("v1"))
	b := fx.places["B"]
	b.mu.Lock()
	defer b.mu.Unlock()
	if string(b.objs["laptop/kopia.repository"]) != twentyThree {
		t.Fatalf("B holds %q", b.objs["laptop/kopia.repository"])
	}
	want := "create laptop/kopia.repository replaces " + hex.EncodeToString(v1[:])[:8]
	if !strings.Contains(strings.Join(b.log, "|"), want) || b.nextID != 1 {
		t.Fatalf("log %v, want %q", b.log, want)
	}
}
