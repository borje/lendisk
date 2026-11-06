package gateway

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/versity/versitygw/auth"

	"github.com/borje/lendisk/internal/state"
)

// SEC-8, D15: what the gate refuses before versitygw routes anything.
func TestGateRefuses(t *testing.T) {
	cases := []struct {
		method, path, lock string
		refused            bool
		multiDelete        bool
	}{
		{"PATCH", "/list-users", "", true, false},
		{"PATCH", "/laptop/key", "", true, false},
		{"PUT", "/newbucket", "true", true, false},
		{"PUT", "/newbucket", " TRUE ", true, false},
		{"GET", "/_lendisk/version", "", true, false},
		// The control path however spelled, once cleaned (#53, #54).
		{"GET", "//_lendisk/x", "", true, false},
		{"GET", "/b/../_lendisk/x", "", true, false},
		{"GET", "/./_lendisk/x", "", true, false},
		{"GET", "/_lendisk", "", true, false},
		{"PUT", "/newbucket", "", false, false},
		{"PUT", "/newbucket", "false", false, false},
		{"PUT", "/laptop/data/ab", "", false, false},
		{"GET", "/laptop/_lendisk/x", "", false, false}, // a key, not the control path
		{"DELETE", "/laptop/locks/1", "", false, false},
		// Q-H: a browser-form POST upload; a multi-delete and the
		// multipart calls are POSTs too and pass.
		{"POST", "/laptop", "", true, false},
		{"POST", "/laptop/", "", true, false},
		{"POST", "/laptop", "", false, true},
		{"POST", "/laptop/data/ab", "", false, false},
	}
	for _, c := range cases {
		if got := refuse(request{method: c.method, path: c.path, objectLock: c.lock, multiDelete: c.multiDelete}) != ""; got != c.refused {
			t.Errorf("%s %s lock=%q: refused=%v, want %v", c.method, c.path, c.lock, got, c.refused)
		}
	}
}

func TestIAMNeverAdmin(t *testing.T) {
	db, err := state.Open(context.Background(), t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.AddBucket(context.Background(), state.Bucket{Name: "b", Profile: "restic", Copies: 2,
		AccessKey: "LDKA", SecretKey: "s", CreatedAt: time.Now()})
	iam := &IAM{DB: db, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	a, err := iam.GetUserAccount("LDKA")
	if err != nil || a.Role != auth.RoleUser || a.Secret != "s" {
		t.Fatalf("account %+v %v", a, err)
	}
	if _, err := iam.GetUserAccount("nobody"); err != auth.ErrNoSuchUser {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := iam.ListUserAccounts(); err == nil {
		t.Fatal("ListUserAccounts must be refused (it would list secrets)")
	}
	if err := iam.CreateAccount(auth.Account{Access: "x", Role: auth.RoleAdmin}); err == nil {
		t.Fatal("CreateAccount accepted")
	}
	missing, _ := iam.ResolveAccounts([]string{"LDKA", "nobody"})
	if strings.Join(missing, ",") != "nobody" {
		t.Fatalf("missing %v", missing)
	}
	// A closed database refuses every key.
	db.Close()
	if _, err := iam.GetUserAccount("LDKA"); err == nil {
		t.Fatal("closed database admitted a key")
	}
}

// A data_dir too long for a unix socket path still serves (§3.7 gw.sock).
func TestLongSocketPathFallsBack(t *testing.T) {
	dir := t.TempDir()
	long := filepath.Join(dir, strings.Repeat("d", 100))
	store := filepath.Join(dir, "store")
	os.MkdirAll(store, 0o700)
	db, err := state.Open(context.Background(), dir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	g := &Gateway{StoreDir: store, Socket: SocketPath(long), DB: db, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errc := make(chan error, 1)
	go func() { errc <- g.Run(ctx, ready) }()
	select {
	case <-ready:
	case err := <-errc:
		t.Fatalf("gateway failed: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("not ready")
	}
	if len(g.Socket) > maxSocketPath {
		t.Fatalf("socket path still %d bytes", len(g.Socket))
	}
	fi, err := os.Stat(filepath.Dir(g.Socket))
	if err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("socket directory: %v %v", fi.Mode(), err)
	}
	cancel()
	if err := within(t, "the gateway stopping", errc); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(g.Socket)); !os.IsNotExist(err) {
		t.Fatal("temporary socket directory left behind")
	}
}

// Stopping during startup must not leave versitygw running: only one may
// run per process, so a leak makes the next start fail.
func TestStopDuringStartDoesNotLeak(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for i := 0; i < 5; i++ {
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "store"), 0o700)
		db, err := state.Open(context.Background(), dir, quiet)
		if err != nil {
			t.Fatal(err)
		}
		g := &Gateway{StoreDir: filepath.Join(dir, "store"), Socket: SocketPath(dir), DB: db, Log: quiet}
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- g.Run(ctx, nil) }()
		time.Sleep(time.Duration(i) * 2 * time.Millisecond)
		cancel()
		if err := within(t, "the gateway stopping", errc); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		db.Close()
	}
}
