package cli

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/pace"
)

// REPL-7 (#87): a config file that no longer loads keeps the rates in
// force, with an error logged, and the next file that loads applies.
func TestABadConfigFileOnReloadKeepsTheRatesInForce(t *testing.T) {
	parallel(t, 0)
	path := filepath.Join(t.TempDir(), "config.toml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[replication]\nupload_limit = \"1M\"\n")
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	flags := config.RegisterFlags(fs)
	if err := fs.Parse([]string{"--config", path}); err != nil {
		t.Fatal(err)
	}
	cfg, err := flags.Load()
	if err != nil {
		t.Fatal(err)
	}
	p := pace.New(nil)
	p.Set(paceSettings(&cfg.Config))
	var logged lockedBuf
	hup := make(chan os.Signal, 1)
	rl := &reloader{Load: flags.Load, Path: path, Pace: p, Log: slog.New(slog.NewTextHandler(&logged, nil)),
		Every: time.Hour, Hup: hup, started: cfg.Config}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); rl.Run(ctx) }()
	defer func() { cancel(); <-done }()
	reloaded := func(what string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !strings.Contains(logged.String(), what); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("no %q in the log:\n%s", what, logged.String())
			}
		}
	}

	write("[replication]\nupload_limit = \"2M\"\n[[[ not toml\n")
	hup <- syscall.SIGHUP
	reloaded("not reloaded; the settings in force stay")
	if up := p.Settings().Up; up != 1<<20 {
		t.Fatalf("after a bad file the upload limit is %d, want 1M kept", up)
	}

	write("[replication]\nupload_limit = \"2M\"\n")
	hup <- syscall.SIGHUP
	reloaded("configuration reloaded")
	if up := p.Settings().Up; up != 2<<20 {
		t.Fatalf("after a good file the upload limit is %d, want 2M", up)
	}
}

// REPL-7 (#87): the SIGHUP handler is in place before anything that can
// take long at start-up (opening lendisk.db, its migrations): a SIGHUP
// then (a `systemctl reload` racing the start) does not kill the
// daemon, whose default action for the signal is to end, and the
// reload it asks for happens once the daemon runs.
func TestASIGHUPDuringStartUpReloadsTheRatesAndDoesNotKill(t *testing.T) {
	parallel(t, 1)
	an := friendNode(t)
	a := newProcNode(t, an, envStartHold+"=2s")
	a.p = a.startProc(t, a.addr, a.logw, 0, a.env...)
	log := func() string { b, _ := os.ReadFile(a.logf); return string(b) }
	for deadline := time.Now().Add(time.Minute); !strings.Contains(log(), startHeld); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) || a.p.exited() {
			t.Fatalf("the daemon never reached its start-up hold:\n%s", log())
		}
	}
	if err := a.p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	a.p.waitListening(t, a.addr, a.logf)
	for deadline := time.Now().Add(time.Minute); !strings.Contains(log(), "why=SIGHUP"); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a SIGHUP during start-up was not reloaded")
		}
	}
	if a.p.exited() {
		t.Fatal("the daemon exited")
	}
}

// lockedBuf is a log sink the reloader's goroutine writes while the
// test reads it.
type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
