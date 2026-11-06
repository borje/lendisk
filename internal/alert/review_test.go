package alert

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// OPS-3 (#13): a hook that hangs is killed with everything it started,
// its process group, so no child outlives the timeout.
func TestHungHookProcessGroupKilled(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	hook := filepath.Join(dir, "hook.sh")
	script := "#!/bin/sh\nsleep 300 &\necho $! > '" + pidFile + "'\nsleep 300\n"
	if err := os.WriteFile(hook, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := RunCommand(ctx, hook, []byte("{}")); err == nil {
		t.Fatal("a hung hook succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("RunCommand returned after %v", d)
	}
	b, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Kill(pid, syscall.SIGKILL) })
	waitFor(t, "the hook's child to be gone", func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	})
}

// OPS-3 (#13): alerts.command is one program with no arguments: an
// absolute path to an executable regular file.
func TestCommandMustBeOneExecutableProgram(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "hook.sh")
	plain := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	spaced := filepath.Join(dir, "my hooks", "ntfy hook.sh")
	if err := os.MkdirAll(filepath.Dir(spaced), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(spaced, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, good := range []string{exe, spaced} {
		if err := CheckCommand(good); err != nil {
			t.Errorf("refused %q: %v", good, err)
		}
	}
	for bad, want := range map[string]string{
		exe + " --topic backups":              "put arguments in a small script",
		"hook.sh":                             "absolute path",
		plain:                                 "not executable",
		dir:                                   "not a regular file",
		filepath.Join(dir, "missing"):         "create it, or clear alerts.command",
		filepath.Join(dir, "missing") + " -v": "create it, or clear alerts.command",
	} {
		err := CheckCommand(bad)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("CheckCommand(%q) = %v, want %q", bad, err, want)
		}
	}
}

// OPS-2 (#13): an unreachable friend is counted from its last answer,
// not from its first failure: B went off at t0 and A's next backup
// failed at t0+20h, so B is alerted at t0+24h, not t0+44h.
func TestUnreachableCountedFromLastAnswer(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionOK, "", t0); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial: no route", t0.Add(20*time.Hour)); err != nil {
		t.Fatal(err)
	}
	c := &clock{t: t0.Add(24*time.Hour - time.Second)}
	s := &Service{DB: db, Log: quiet, Now: c.now, UnreachableAfter: 24 * time.Hour}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerted before 24h without an answer: %v", as)
	}
	c.set(t0.Add(24 * time.Hour))
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := active(t, db)["friend_unreachable:B"]; !ok {
		t.Fatal("not alerted 24h after B's last answer")
	}
}

// OPS-2 (#13): reachability is learned only from replication, so a
// nightly backup that meets a one-minute blip, 24h after the last
// answer, is not alerted: the place must also have been retrying for
// an hour (MinRetrying) before it counts as unreachable.
func TestNightlyBlipNotAlerted(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	t0 := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionOK, "", t0); err != nil {
		t.Fatal(err)
	}
	// The next night's backup fails at t0+24h.
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial: timeout", t0.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	c := &clock{t: t0.Add(24*time.Hour + time.Minute)}
	s := &Service{DB: db, Log: quiet, Now: c.now, UnreachableAfter: 24 * time.Hour}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("a one-minute blip was alerted: %v", as)
	}
	// Still failing an hour later: now it is.
	c.set(t0.Add(25 * time.Hour))
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := active(t, db)["friend_unreachable:B"]; !ok {
		t.Fatal("not alerted after an hour of retrying, 25h after the last answer")
	}

	// The minimum is injectable: B gone at t0, next backup at t0+20h,
	// with a 2h minimum, is alerted at t0+24h, not before t0+22h.
	db2 := openDB(t)
	addFriend(t, db2, "B")
	db2.SetPlaceCondition(t.Context(), "B", state.ConditionOK, "", t0)
	db2.SetPlaceCondition(t.Context(), "B", state.ConditionRetrying, "dial: timeout", t0.Add(20*time.Hour))
	s2 := &Service{DB: db2, Log: quiet, Now: c.now, UnreachableAfter: 24 * time.Hour, MinRetrying: 2 * time.Hour}
	c.set(t0.Add(24*time.Hour - time.Second))
	s2.evaluate(t.Context())
	if as := active(t, db2); len(as) != 0 {
		t.Fatalf("alerted before 24h: %v", as)
	}
	c.set(t0.Add(24 * time.Hour))
	s2.evaluate(t.Context())
	if _, ok := active(t, db2)["friend_unreachable:B"]; !ok {
		t.Fatal("not alerted at t0+24h")
	}
}

// OPS-3 (#13): a hook that exits 0 but leaves a child holding its
// output is delivered (with a warning), not re-sent for ever, and the
// child is killed.
func TestHookExitingWithChildIsDelivered(t *testing.T) {
	defer func(d time.Duration) { waitDelay = d }(waitDelay)
	waitDelay = 200 * time.Millisecond
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	hook := filepath.Join(dir, "hook.sh")
	script := "#!/bin/sh\ncat > /dev/null\nsleep 300 &\necho $! > '" + pidFile + "'\nexit 0\n"
	if err := os.WriteFile(hook, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	err := RunCommand(context.Background(), hook, []byte("{}"))
	if !errors.Is(err, ErrLeftChildren) {
		t.Fatalf("RunCommand = %v, want ErrLeftChildren", err)
	}
	b, rerr := os.ReadFile(pidFile)
	if rerr != nil {
		t.Fatal(rerr)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	t.Cleanup(func() { syscall.Kill(pid, syscall.SIGKILL) })
	waitFor(t, "the hook's child to be gone", func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	})

	// The service counts it as delivered and warns.
	db := openDB(t)
	addFriend(t, db, "B")
	if err := db.SetPlaceCondition(t.Context(), "B", state.ConditionStore, "disk gone", time.Now()); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	s := &Service{DB: db, Log: slog.New(slog.NewTextHandler(logs, nil)), Command: hook}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !s.deliverPending(t.Context(), time.Second) {
		t.Fatalf("not delivered:\n%s", logs)
	}
	if active(t, db)["store_unreadable"].HookAt.IsZero() || !strings.Contains(logs.String(), "left") {
		t.Fatalf("delivery not recorded or not warned:\n%s", logs)
	}
	if b, err := os.ReadFile(pidFile); err == nil {
		if p, _ := strconv.Atoi(strings.TrimSpace(string(b))); p > 0 {
			t.Cleanup(func() { syscall.Kill(p, syscall.SIGKILL) })
		}
	}
}

// OPS-3 (#13): the webhook client follows no redirect (Go would put the
// Location in its errors); a 3xx is a failure, and a redirect or a
// connection error names only scheme and host.
func TestWebhookRedirectAndConnectionErrorsRedacted(t *testing.T) {
	var followed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/elsewhere") {
			followed.Store(true)
			return
		}
		http.Redirect(w, r, "/elsewhere/l0cati0n-s3cr3t", http.StatusFound)
	}))
	defer srv.Close()
	s := &Service{Log: quiet, Webhook: srv.URL + "/ping/s3cr3t?k=s3cr3t"}
	err := s.postWebhook(context.Background(), []byte("{}"))
	if err == nil || followed.Load() {
		t.Fatalf("redirect followed: err %v", err)
	}
	if strings.Contains(err.Error(), "s3cr3t") || !strings.Contains(err.Error(), srv.URL) {
		t.Fatalf("redirect error %q", err)
	}
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	s.Webhook = gone.URL + "/ping/s3cr3t"
	err = s.postWebhook(context.Background(), []byte("{}"))
	if err == nil || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("connection error %v", err)
	}
}
