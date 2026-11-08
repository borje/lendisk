package quota

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// friend is the far side of the quota call: its number, what it answers
// and whether it answers at all.
type friend struct {
	mu     sync.Mutex
	number int64
	lent   int64
	down   bool
	calls  []control.QuotaRequest
}

func (f *friend) call(_ context.Context, _ state.Friendship, req control.QuotaRequest) (control.QuotaResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.down {
		return control.QuotaResponse{}, errors.New("dial: no route to the friend")
	}
	return control.QuotaResponse{Number: f.number, Quota: min(f.number, req.Number), Lent: f.lent,
		RepositorySize: 7 << 30, Protocol: "0.1"}, nil
}

func (f *friend) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *friend) last() control.QuotaRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

type change struct {
	peer          string
	before, after int64
}

type setup struct {
	t       *testing.T
	db      *state.DB
	c       *clock
	b       *friend
	x       *Exchanger
	mu      sync.Mutex
	changes []change
}

func newSetup(t *testing.T) *setup {
	t.Helper()
	db, err := state.Open(context.Background(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	s := &setup{t: t, db: db, c: &clock{t: time.Unix(1_800_000_000, 0)}, b: &friend{number: 50 << 30}}
	db.Clock = s.c.now
	if err := db.AddFriendship(context.Background(), state.Friendship{Peer: "B", Address: "tcB", OwnNumber: 50 << 30,
		PeerNumber: 50 << 30, CreatedAt: s.c.now()}); err != nil {
		t.Fatal(err)
	}
	s.x = &Exchanger{DB: db, Call: s.b.call, Now: s.c.now, Log: slog.New(slog.DiscardHandler),
		Path: func(context.Context, string) string { return "relayed" },
		Changed: func(peer string, before, after int64) {
			s.mu.Lock()
			s.changes = append(s.changes, change{peer, before, after})
			s.mu.Unlock()
		}}
	return s
}

// pass makes one pass and waits for the calls it started.
func (s *setup) pass() {
	s.x.Pass(context.Background())
	s.x.Wait()
}

func (s *setup) friendship() state.Friendship {
	s.t.Helper()
	f, err := s.db.GetFriendship(context.Background(), "B")
	if err != nil {
		s.t.Fatal(err)
	}
	return f
}

func (s *setup) takeChanges() []change {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.changes
	s.changes = nil
	return c
}

// LEND-8, PEER-6 (#24): the quota call is made at once after this
// node's number changes, carries the number, this node's use of the
// friend's space and its repository size rounded up to a GiB, and is
// repeated with a backoff until answered; the answer is applied and the
// number recorded as told. A lowering is passed to Changed at once.
func TestAChangedNumberIsToldAtOnceAndUntilAnswered(t *testing.T) {
	s := newSetup(t)
	ctx := context.Background()
	s.pass() // the first call after start
	if s.b.count() != 1 || s.friendship().ToldNumber != 50<<30 || s.friendship().Path != "relayed" {
		t.Fatalf("first pass: %d calls, %+v", s.b.count(), s.friendship())
	}
	if c := s.takeChanges(); len(c) != 1 || c[0] != (change{"B", -1, 50 << 30}) {
		t.Fatalf("changes at start: %+v", c)
	}
	s.pass()
	if s.b.count() != 1 {
		t.Fatal("called again with nothing to tell")
	}

	// A bucket's object counts toward use and size.
	if err := s.db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 1, AccessKey: "AK", SecretKey: "s", CreatedAt: s.c.now()}); err != nil {
		t.Fatal(err)
	}
	seqs, err := s.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/1", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.FinishPut(ctx, seqs[0], 1000, "h"); err != nil {
		t.Fatal(err)
	}

	s.b.down = true
	if _, _, err := s.db.SetOwnNumber(ctx, "B", 10<<30, 0); err != nil {
		t.Fatal(err)
	}
	s.pass()
	if s.b.count() != 2 {
		t.Fatalf("the change was not told at once: %d calls", s.b.count())
	}
	if r := s.b.last(); r.Number != 10<<30 || r.Use != 1000+state.ObjectOverhead || r.RepositorySize != 1<<30 {
		t.Fatalf("the call carried %+v", r)
	}
	if c := s.takeChanges(); len(c) != 1 || c[0] != (change{"B", 50 << 30, 10 << 30}) {
		t.Fatalf("the lowering reached Changed as %+v", c)
	}
	if f := s.friendship(); f.ExchangeError == "" || f.ToldNumber != 50<<30 {
		t.Fatalf("after a failed call: %+v", f)
	}
	// Retried after the backoff, not before; the backoff doubles.
	s.pass()
	if s.b.count() != 2 {
		t.Fatal("retried before the backoff")
	}
	s.c.add(defaultMinBackoff)
	s.pass()
	s.c.add(defaultMinBackoff)
	s.pass()
	if s.b.count() != 3 {
		t.Fatalf("%d calls: the backoff did not double", s.b.count())
	}
	s.b.down = false
	s.c.add(defaultMinBackoff)
	s.pass()
	if s.b.count() != 4 {
		t.Fatalf("%d calls after the doubled backoff", s.b.count())
	}
	if f := s.friendship(); f.ToldNumber != 10<<30 || f.ExchangeError != "" || f.PeerRepositorySize != 7<<30 {
		t.Fatalf("after the answer: %+v", f)
	}
	s.pass()
	if s.b.count() != 4 {
		t.Fatal("called again once told")
	}
}

// LEND-7, #68 (#24): the call is also a periodic exchange, every
// Refresh, which reconciles this node's count of its use there from the
// friend's own; a Due (a 507 from the friend) asks at once.
func TestTheQuotaCallIsRepeatedPeriodicallyAndOnDemand(t *testing.T) {
	s := newSetup(t)
	s.pass()
	s.c.add(defaultRefresh - time.Second)
	s.pass()
	if s.b.count() != 1 {
		t.Fatal("called before the refresh")
	}
	s.c.add(time.Second)
	s.b.lent = 12345
	s.pass()
	if s.b.count() != 2 {
		t.Fatal("not called at the refresh")
	}
	use, err := s.db.PlaceUse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if use["B"] != 12345 {
		t.Fatalf("this node's count of its use at B: %d, want B's own 12345", use["B"])
	}
	s.x.Due("B")
	s.pass()
	if s.b.count() != 3 {
		t.Fatal("Due did not ask at once")
	}
}

// LEND-8 (#24): a quota the friend raised or lowered with its own call
// reaches Changed at the next pass; raising is in force only as far as
// this node's own number allows.
func TestTheFriendsNumberReachesChanged(t *testing.T) {
	s := newSetup(t)
	ctx := context.Background()
	s.pass()
	if _, err := s.db.QuotaReported(ctx, "B", state.QuotaReport{Number: 20 << 30}, s.c.now()); err != nil {
		t.Fatal(err)
	}
	s.x.Kick()
	s.pass()
	if _, _, err := s.db.SetOwnNumber(ctx, "B", 80<<30, 0); err != nil {
		t.Fatal(err)
	}
	s.b.number = 80 << 30
	s.pass() // tells 80G; the answer raises the friend's number to 80G
	s.pass() // sees the raised quota
	want := []change{{"B", -1, 50 << 30}, {"B", 50 << 30, 20 << 30}, {"B", 20 << 30, 80 << 30}}
	got := s.takeChanges()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("changes %+v, want %+v", got, want)
	}
}
