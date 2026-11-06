package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

var quiet = slog.New(slog.DiscardHandler)

func openDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func addFriend(t *testing.T, db *state.DB, peer string) {
	t.Helper()
	if err := db.AddFriendship(t.Context(), state.Friendship{Peer: peer, Address: "tc" + peer, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func active(t *testing.T, db *state.DB) map[string]state.Alert {
	t.Helper()
	as, err := db.ActiveAlerts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]state.Alert{}
	for _, a := range as {
		out[a.Key] = a
	}
	return out
}

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// run starts s until the test ends.
func run(t *testing.T, s *Service) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
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

// syncBuffer is a log sink tests read while the service writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// OPS-2 (#13): a friend that has not answered for the threshold raises
// one alert, which clears when it answers again; the threshold is the
// configured one.
func TestFriendUnreachablePastThresholdRaisesAlert(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial: no route", t0); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionOK, "", t0); err != nil {
		t.Fatal(err)
	}
	c := &clock{t: t0.Add(24*time.Hour - time.Second)}
	s := &Service{DB: db, Log: quiet, Now: c.now, UnreachableAfter: 24 * time.Hour}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerted before the threshold: %v", as)
	}
	c.set(t0.Add(24 * time.Hour))
	for range 2 { // the second pass deduplicates
		if err := s.evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	as := active(t, db)
	a, ok := as["friend_unreachable:B"]
	if len(as) != 1 || !ok || a.Kind != KindFriendUnreachable || a.Subject != "B" || !strings.Contains(a.Message, "dial: no route") {
		t.Fatalf("alerts %+v", as)
	}

	// B answers: the alert clears.
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionOK, "", t0.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("not cleared: %v", as)
	}

	// A shorter threshold is honoured.
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionRetrying, "timeout", t0.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	c.set(t0.Add(26 * time.Hour))
	s.UnreachableAfter = time.Hour
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := active(t, db)["friend_unreachable:C"]; !ok {
		t.Fatal("a 1h threshold did not alert after 1h")
	}
}

// OPS-2, VER-5, EP-11 (#13): what the replication worker records (a
// store that cannot be read, an object refused for the store, a place's
// own refusals) raises alerts, and they clear when it ends.
func TestReplicationConditionsRaiseAlerts(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	var seqs []int64
	for _, k := range []string{"data/rotted", "data/immutable"} {
		s, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: k, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, s[0], 1, "h"); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s[0])
	}
	if _, err := db.RefuseObject(ctx, seqs[0], state.CauseStore, "no longer matches its capture hash"); err != nil {
		t.Fatal(err)
	}
	if err := db.RefusePlacement(ctx, "B", seqs[1], "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := db.SetPlaceCondition(ctx, "C", state.ConditionStore, "bucket laptop: no such file or directory", now); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	for key, want := range map[string]string{
		"objects_not_replicated:laptop": "1 object",
		"place_refused:B":               "1 object",
		"store_unreadable":              "no such file or directory",
	} {
		if a, ok := as[key]; !ok || !strings.Contains(a.Message, want) {
			t.Errorf("%s: %+v, want a message with %q", key, a, want)
		}
	}
	if len(as) != 3 {
		t.Fatalf("alerts %v", as)
	}

	// The store is readable again: its alert clears, the others stay.
	if err := db.ResetPlaceCondition(ctx, "C", now); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 2 || as["store_unreadable"].Key != "" {
		t.Fatalf("alerts after the store recovered %v", as)
	}
}

// OPS-3 (#13): the hook is a real program that receives the alert as
// the documented JSON object on stdin.
func TestHookReceivesJSONOnStdin(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "alert.json")
	hook := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > '"+out+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	db := openDB(t)
	addFriend(t, db, "B")
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial: no route", t0); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet, Node: "self", Command: hook, Now: func() time.Time { return t0.Add(48 * time.Hour) },
		Interval: 10 * time.Millisecond}
	run(t, s)
	var m Message
	waitFor(t, "the hook's output", func() bool {
		b, err := os.ReadFile(out)
		return err == nil && json.Unmarshal(b, &m) == nil
	})
	want := Message{Schema: 1, Event: "raised", Kind: KindFriendUnreachable, Subject: "B", Node: "self",
		RaisedAt: t0.Add(48 * time.Hour).UTC().Format(time.RFC3339)}
	m.Message = ""
	if m != want {
		t.Fatalf("hook got %+v, want %+v", m, want)
	}
	waitFor(t, "the delivery recorded", func() bool { return !active(t, db)["friend_unreachable:B"].HookAt.IsZero() })
}

// OPS-3 (#13): the webhook receives the same body as the hook; failures
// of either are retried with backoff and logged, never with the
// webhook's secret; a delivered alert is not delivered again, even by
// the next daemon.
func TestDeliveryRetriedAndLoggedWithoutTheSecret(t *testing.T) {
	var hookBodies, webBodies [][]byte
	var mu sync.Mutex
	var webCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || r.URL.Path != "/ping/s3cr3t-t0ken" {
			t.Errorf("webhook request %s %s %q", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		if webCalls.Add(1) <= 2 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		webBodies = append(webBodies, b)
		mu.Unlock()
	}))
	defer srv.Close()
	var hookCalls atomic.Int32
	runner := func(ctx context.Context, command string, stdin []byte) error {
		if command != "/etc/lendisk/hooks/ntfy.sh" {
			t.Errorf("command %q", command)
		}
		if hookCalls.Add(1) <= 2 {
			return errors.New("exit status 1: ntfy unreachable")
		}
		mu.Lock()
		hookBodies = append(hookBodies, stdin)
		mu.Unlock()
		return nil
	}
	db := openDB(t)
	addFriend(t, db, "B")
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionStore, "disk gone", time.Now()); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	webhook := srv.URL + "/ping/s3cr3t-t0ken?key=0th3r-s3cr3t"
	newService := func() *Service {
		return &Service{DB: db, Log: slog.New(slog.NewTextHandler(logs, nil)), Node: "self",
			Command: "/etc/lendisk/hooks/ntfy.sh", Webhook: webhook, RunCommand: runner, HTTP: srv.Client(),
			Interval: 10 * time.Millisecond, MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); newService().Run(ctx) }()
	waitFor(t, "both deliveries", func() bool {
		a := active(t, db)["store_unreadable"]
		return !a.HookAt.IsZero() && !a.WebhookAt.IsZero()
	})
	time.Sleep(100 * time.Millisecond) // time for any repeat
	cancel()
	<-done
	mu.Lock()
	if len(hookBodies) != 1 || len(webBodies) != 1 || !bytes.Equal(hookBodies[0], webBodies[0]) {
		t.Fatalf("hook got %q, webhook %q", hookBodies, webBodies)
	}
	mu.Unlock()
	if n := strings.Count(logs.String(), "alert delivery failed"); n < 4 {
		t.Fatalf("%d failures logged, want 4:\n%s", n, logs)
	}
	if strings.Contains(logs.String(), "s3cr3t") {
		t.Fatalf("the webhook's secret is in the log:\n%s", logs)
	}

	// A restarted daemon does not deliver it again.
	run(t, newService())
	time.Sleep(100 * time.Millisecond)
	if hookCalls.Load() != 3 || webCalls.Load() != 3 {
		t.Fatalf("redelivered: hook %d, webhook %d calls", hookCalls.Load(), webCalls.Load())
	}
}

// OPS-3 (#13): a hook that hangs is stopped at its timeout and retried;
// meanwhile alerts are still raised and Check never waits.
func TestHungHookNeverBlocks(t *testing.T) {
	var calls atomic.Int32
	runner := func(ctx context.Context, _ string, _ []byte) error {
		calls.Add(1)
		<-ctx.Done() // hangs until the timeout
		return ctx.Err()
	}
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	t0 := time.Now()
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionStore, "disk gone", t0); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet, Command: "hook", RunCommand: runner, Interval: time.Hour,
		Timeout: 50 * time.Millisecond, MinBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond}
	run(t, s)
	s.Check()
	waitFor(t, "the hook to hang", func() bool { return calls.Load() >= 1 })
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionRetrying, "timeout", t0.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s.Check()
	if d := time.Since(start); d > 10*time.Millisecond {
		t.Fatalf("Check took %v", d)
	}
	waitFor(t, "the second alert", func() bool { return len(active(t, db)) == 2 })
	waitFor(t, "a retry after the timeout", func() bool { return calls.Load() >= 3 })
}
