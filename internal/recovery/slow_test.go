package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

// endless serves empty pages with a rising next, slowly, without end.
func endless(delay time.Duration, pages *atomic.Int64) func(context.Context, state.Friendship, string, string) (control.ListPage, error) {
	return func(ctx context.Context, _ state.Friendship, _, _ string) (control.ListPage, error) {
		n := pages.Add(1)
		select {
		case <-ctx.Done():
			return control.ListPage{}, ctx.Err()
		case <-time.After(delay):
		}
		return control.ListPage{Objects: []control.ListObject{}, Next: fmt.Sprintf("p%012d", n)}, nil
	}
}

// #29 re-review: a friend serving empty pages without end cannot hold
// recovery up. Places are listed side by side, so an honest friend is
// listed whatever the slow one does; recover's round ends within its
// budget, leaving the slow friend's list pending for the daemon, not
// failed; and a friend can be skipped.
func TestSlowFriendDoesNotHoldUpRecovery(t *testing.T) {
	db, l := recovering(t, "restic", "A0", "B", "Z")
	var pages atomic.Int64
	slow := endless(5*time.Millisecond, &pages)
	pack := sum("pack")
	honest := served(map[string][]control.ListObject{"B": {{Key: "data/" + pack[:2] + "/" + pack, Size: 9, SHA256: pack}}})
	var zAsked atomic.Bool
	l.Page = func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
		switch f.Peer {
		case "A0":
			return slow(ctx, f, sealed, after)
		case "Z":
			zAsked.Store(true)
		}
		return honest(ctx, f, sealed, after)
	}
	l.Skip = map[string]bool{"Z": true}
	start := time.Now()
	rs, err := l.RoundWithin(t.Context(), 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("the round took %v", d)
	}
	for _, r := range rs {
		switch r.Place {
		case "A0":
			if !errors.Is(r.Err, ErrOutOfTime) {
				t.Errorf("A0: %+v", r)
			}
		case "B":
			if r.Err != nil || r.Listed != 1 {
				t.Errorf("B: %+v", r)
			}
		case "Z":
			if !errors.Is(r.Err, ErrSkipped) {
				t.Errorf("Z: %+v", r)
			}
		}
	}
	if zAsked.Load() {
		t.Error("a skipped friend was asked")
	}
	if got := heldKeys(t, db, "B"); len(got) != 1 {
		t.Fatalf("B holds %v", got)
	}
	rels, _ := db.Relists(t.Context())
	if len(rels) != 2 {
		t.Fatalf("still to read %+v", rels)
	}
	for _, r := range rels {
		if r.Place != "A0" && r.Place != "Z" || r.Failures != 0 {
			t.Fatalf("still to read %+v", rels)
		}
	}
}

// #29 re-review: a database too busy to record a failed list does not
// end the round (or recover): the list stays as it was, tried again.
func TestUnrecordableFailureIsNotFatal(t *testing.T) {
	db, l := recovering(t, "generic", "B", "C")
	if _, err := db.Exec(`CREATE TRIGGER busy BEFORE UPDATE ON relist BEGIN SELECT RAISE(FAIL, 'database is locked'); END`); err != nil {
		t.Fatal(err)
	}
	ok := sum("ok")
	l.Page = func(_ context.Context, f state.Friendship, _, _ string) (control.ListPage, error) {
		if f.Peer == "B" {
			return control.ListPage{}, errors.New("offline")
		}
		return control.ListPage{Objects: []control.ListObject{{Key: "k", Size: 1, SHA256: ok}}}, nil
	}
	rs, err := l.Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if len(rs) != 2 {
		t.Fatalf("results %+v", rs)
	}
}

// #29 re-review: rounds start at a different place each time, so places
// that use every round's budget cannot keep the ones after them from
// ever being listed.
func TestRoundsRotateTheStart(t *testing.T) {
	_, l := recovering(t, "generic", "A", "B", "C")
	l.Parallel = 1
	var first atomic.Value
	l.Page = func(ctx context.Context, f state.Friendship, _, _ string) (control.ListPage, error) {
		first.CompareAndSwap(nil, f.Peer)
		<-ctx.Done() // slow: never answers within the budget
		return control.ListPage{}, ctx.Err()
	}
	seen := map[any]bool{}
	for range 3 {
		first = atomic.Value{}
		if _, err := l.RoundWithin(t.Context(), 30*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		seen[first.Load()] = true
	}
	if len(seen) != 3 {
		t.Fatalf("three rounds started at %v", seen)
	}
}

// #29 re-review: pages are capped apart from entries, far below the
// entry cap: honest pages are full, so a list of empty pages ends.
func TestEndlessEmptyPagesCapped(t *testing.T) {
	db, l := recovering(t, "generic", "B")
	l.MaxPages = 20
	var pages atomic.Int64
	l.Page = endless(0, &pages)
	rs, err := l.Round(t.Context())
	if err != nil || len(rs) != 1 || rs[0].Err == nil || !strings.Contains(rs[0].Err.Error(), "pages") {
		t.Fatalf("round %+v %v", rs, err)
	}
	if n := pages.Load(); n > 25 {
		t.Fatalf("read %d pages", n)
	}
	if rels, _ := db.Relists(t.Context()); len(rels) != 0 {
		t.Fatalf("the abandoned list is still to read: %+v", rels)
	}
	if DefaultMaxPages >= DefaultMaxKeys/100 {
		t.Fatalf("the default page cap %d is not far below the key cap", DefaultMaxPages)
	}
}
