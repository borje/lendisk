package cli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/state"
)

// OPS-2, OPS-3 (#13): a friend unreachable past alerts.unreachable_after
// raises an alert that the daemon hands to the hook, a real program
// receiving JSON on stdin, and POSTs to the webhook with the same body,
// never logging the webhook's secret; `status` and `status --json` show
// it, also once the daemon has stopped.
func TestUnreachableFriendAlertedThroughHookWebhookAndStatus(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "B", Address: "tcB", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "B", state.ConditionRetrying, "dial tcB: no route", time.Now().Add(-7*time.Hour)); err != nil {
		t.Fatal(err)
	}
	db.Close()

	dir := t.TempDir()
	got := filepath.Join(dir, "alert.json")
	hook := filepath.Join(dir, "hook.sh")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > '"+got+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var posted []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		posted = b
		mu.Unlock()
	}))
	defer srv.Close()

	n.tuneAlerts = func(s *alert.Service) { s.Interval = 20 * time.Millisecond }
	stop := n.serveLogged("--alerts-command", hook, "--alerts-webhook", srv.URL+"/ping/s3cr3t",
		"--alerts-unreachable-after", "6h")
	var hooked []byte
	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		p := posted
		mu.Unlock()
		hooked, _ = os.ReadFile(got)
		if p != nil && json.Valid(hooked) {
			if string(p) != string(hooked) {
				t.Fatalf("webhook got %s, hook %s", p, hooked)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not delivered: hook %q, webhook %q", hooked, p)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var m alert.Message
	if err := json.Unmarshal(hooked, &m); err != nil {
		t.Fatal(err)
	}
	if m.Schema != 1 || m.Event != "raised" || m.Kind != alert.KindFriendUnreachable || m.Subject != "B" || m.Node != n.status().Node.ID {
		t.Fatalf("message %+v", m)
	}
	if logs := stop(); strings.Contains(logs, "s3cr3t") {
		t.Fatalf("the webhook's secret is in the log:\n%s", logs)
	}

	st := n.status()
	if len(st.Alerts) != 1 || st.Alerts[0].Kind != alert.KindFriendUnreachable || st.Alerts[0].Subject != "B" ||
		!strings.Contains(st.Alerts[0].Message, "dial tcB: no route") || st.Alerts[0].RaisedAt != m.RaisedAt {
		t.Fatalf("status alerts %+v", st.Alerts)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "Alerts:  1") || !strings.Contains(text, "friend B has not answered") {
		t.Fatalf("status text:\n%s", text)
	}
}

// OPS-1 (#13): with nothing wrong, status says so.
func TestStatusShowsNoAlerts(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	if st := n.status(); st.Alerts == nil || len(st.Alerts) != 0 {
		t.Fatalf("alerts %#v, want []", st.Alerts)
	}
	if _, text, _ := n.run("", "", "status"); !strings.Contains(text, "Alerts:  none") {
		t.Fatalf("status text:\n%s", text)
	}
}
