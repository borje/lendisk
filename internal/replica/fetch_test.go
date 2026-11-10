package replica

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

// REPL-7 (#23): a read-through fetch from a friend is paced by the
// friend's download limit and the global one: 640 KiB at 64 KiB/s takes
// the pacer's clock 9 s (the first burst is free).
func TestFetchesArePacedByTheDownloadLimits(t *testing.T) {
	self := newID(t)
	body := strings.Repeat("x", 640<<10)
	f, lender := tunnelLender(t, self, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"0123"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		io.WriteString(w, body)
	}))
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	place := lender.String()
	if err := db.AddFriendship(t.Context(), state.Friendship{Peer: place, Address: "tcB", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	c := &fakeClock{t: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	p := pace.New(c.now)
	p.Sleep = c.sleep
	p.Set(pace.Settings{Down: 1 << 20, Peers: map[string]pace.Rates{place: {Down: 64 << 10}}})
	x := &Fetcher{DB: db, Self: self, Pace: p, clients: map[string]fetchClient{place: {addr: "tcB", f: f}}}
	start := c.now()
	rc, err := x.Fetch(t.Context(), place, "laptop", "data/ab/abcd")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || len(got) != len(body) {
		t.Fatalf("fetched %d bytes, %v", len(got), err)
	}
	if el := c.now().Sub(start); el < 8900*time.Millisecond || el > 10*time.Second {
		t.Fatalf("640 KiB at 64 KiB/s took the pacer's clock %v", el)
	}
}

// REPL-7 (#87): a fetch holds a fetch slot until its body is closed, so
// a download limit carries only as many fetches as it keeps at
// pace.MinRate each: at a friend's limit of MinRate, a second fetch
// from it waits for the first to close, and is given up with its
// context.
func TestAFetchWaitsForASlotUnderTheDownloadLimit(t *testing.T) {
	self := newID(t)
	f, lender := tunnelLender(t, self, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"0123"`)
		io.WriteString(w, "abc")
	}))
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	place := lender.String()
	if err := db.AddFriendship(t.Context(), state.Friendship{Peer: place, Address: "tcB", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	p := pace.New(nil)
	p.Set(pace.Settings{Peers: map[string]pace.Rates{place: {Down: pace.MinRate}}})
	x := &Fetcher{DB: db, Self: self, Pace: p, clients: map[string]fetchClient{place: {addr: "tcB", f: f}}}
	first, err := x.Fetch(t.Context(), place, "laptop", "data/ab/abcd")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if rc, err := x.Fetch(ctx, place, "laptop", "data/ab/abcd"); err == nil {
		rc.Close()
		t.Fatal("a second fetch went beside the first at a limit of MinRate")
	}
	first.Close()
	second, err := x.Fetch(t.Context(), place, "laptop", "data/ab/abcd")
	if err != nil {
		t.Fatalf("after the first closed: %v", err)
	}
	if b, err := io.ReadAll(second); err != nil || string(b) != "abc" {
		t.Fatalf("%q %v", b, err)
	}
	second.Close()
}
