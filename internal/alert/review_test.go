package alert

import (
	"context"
	"errors"
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
	if err := CheckCommand(exe); err != nil {
		t.Fatalf("refused %s: %v", exe, err)
	}
	for _, bad := range []string{exe + " --topic backups", "hook.sh", plain, dir, filepath.Join(dir, "missing")} {
		err := CheckCommand(bad)
		if err == nil || !strings.Contains(err.Error(), "put arguments in a small script") {
			t.Errorf("CheckCommand(%q) = %v", bad, err)
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
