package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// daemonProc is `lendisk serve` as a real process: this test binary
// re-executed (see TestMain), so SIGKILL is a real crash.
type daemonProc struct {
	cmd  *exec.Cmd
	done chan error
}

// startProc starts the daemon; with crashAfterStore > 0 it kills itself
// with that probability in capture's crash window (see TestMain).
func (n *node) startProc(t *testing.T, addr string, logw io.Writer, crashAfterStore float64) *daemonProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "serve", "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", addr)
	cmd.Env = append(os.Environ(), envExec+"=1", fmt.Sprintf("%s=%g", envCrashAfterStore, crashAfterStore))
	cmd.Stdout, cmd.Stderr = logw, logw
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &daemonProc{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() { p.kill() })
	return p
}

func (p *daemonProc) kill() {
	p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.done
	p.done <- nil // later kills return at once
}

// exited reports whether the process has ended.
func (p *daemonProc) exited() bool {
	select {
	case err := <-p.done:
		p.done <- err
		return true
	default:
		return false
	}
}

// waitListening waits until addr accepts connections.
func (p *daemonProc) waitListening(t *testing.T, addr string, logf string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.Close()
			return
		}
		select {
		case err := <-p.done:
			p.done <- err
			b, _ := os.ReadFile(logf)
			t.Fatalf("daemon exited before listening: %v\n%s", err, tail(b))
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("daemon not listening after 60s")
}

func tail(b []byte) string {
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}

// EP-3 AC: SIGKILL the daemon at random points during restic backups,
// restart it each time; afterwards every object in the store has exactly
// one queue entry, with the SHA-256 of its bytes, no entry is left
// pending, every queued put is in the store, and no local-only key is
// queued. LENDISK_CRASH_ROUNDS and LENDISK_CRASH_MB (per round) make it
// larger.
func TestCrashDuringResticBackup(t *testing.T) {
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	rounds, mb := envInt("LENDISK_CRASH_ROUNDS", 12), envInt("LENDISK_CRASH_MB", 16)
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("LENDISK_CRASH_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	t.Logf("seed %d (LENDISK_CRASH_SEED), %d rounds of %d MiB", seed, rounds, mb)
	rng := rand.New(rand.NewPCG(seed, 0))

	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "laptop", "restic")
	addr := freeAddr(t, "127.0.0.1")
	logf := filepath.Join(t.TempDir(), "daemon.log")
	logw, err := os.Create(logf)
	if err != nil {
		t.Fatal(err)
	}
	defer logw.Close()
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(logf)
			t.Logf("daemon log:\n%s", tail(b))
		}
	})

	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
		"RESTIC_REPOSITORY=s3:http://"+addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	restics := func(args ...string) *exec.Cmd {
		cmd := exec.Command(restic, args...)
		cmd.Env = env
		return cmd
	}
	d := n.startProc(t, addr, logw, 0)
	d.waitListening(t, addr, logf)
	if out, err := restics("init").CombinedOutput(); err != nil {
		t.Fatalf("restic init: %v\n%s", err, out)
	}

	src := t.TempDir()
	buf := make([]byte, 1<<20)
	addData := func(round int) {
		for i := 0; i < mb; i++ {
			for j := range buf {
				buf[j] = byte(rng.Uint32())
			}
			os.WriteFile(filepath.Join(src, fmt.Sprintf("r%02d-f%03d", round, i)), buf, 0o600)
		}
	}
	var backup *exec.Cmd
	var backupDone chan error
	running := func() bool {
		if backup == nil {
			return false
		}
		select {
		case err := <-backupDone:
			backupDone <- err
			return false
		default:
			return true
		}
	}
	startBackup := func() {
		backup = restics("backup", "--pack-size", strconv.Itoa(envInt("LENDISK_CRASH_PACK_MB", 4)), "--limit-upload", "8192", "--no-scan", src)
		out := &strings.Builder{}
		backup.Stdout, backup.Stderr = out, out
		if err := backup.Start(); err != nil {
			t.Fatal(err)
		}
		backupDone = make(chan error, 1)
		b := backup
		go func() { backupDone <- b.Wait() }()
	}
	t.Cleanup(func() {
		if backup != nil && backup.ProcessState == nil {
			backup.Process.Kill()
		}
	})

	// During the rounds the daemon also kills itself, with this
	// probability per request, in the window between the store accepting
	// a write and its row being completed, which random kills rarely hit.
	const crashAfterStore = 0.1
	kills, selfKills, midBackup, resticKills := 0, 0, 0, 0
	// killBackup kills restic too, as a machine crash would: what it had
	// in flight is then never retried, so only startup can settle it.
	killBackup := func() {
		if running() {
			backup.Process.Kill()
			<-backupDone
			backupDone <- fmt.Errorf("killed")
			resticKills++
		}
	}
	restart := func(p float64) {
		d = n.startProc(t, addr, logw, p)
		d.waitListening(t, addr, logf)
	}
	// wait sleeps for dur or until cond is false, restarting the daemon
	// whenever it killed itself.
	wait := func(dur time.Duration, cond func() bool) {
		for end := time.Now().Add(dur); time.Now().Before(end) && cond(); {
			if d.exited() {
				selfKills++
				killBackup()
				restart(crashAfterStore)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	d.kill()
	restart(crashAfterStore)
	for round := 0; round < rounds; round++ {
		if running() {
			midBackup++
		} else {
			addData(round)
			startBackup()
		}
		// Let the backup make progress since the last crash (restic backs
		// off after one), then kill anywhere in the next second: mostly
		// mid-upload, sometimes between requests.
		packs := filepath.Join(n.data, "store", "laptop", "data")
		before := countFiles(packs)
		wait(60*time.Second, func() bool { return countFiles(packs) <= before && running() })
		wait(time.Duration(rng.IntN(1000))*time.Millisecond, func() bool { return true })
		d.kill()
		kills++
		if rng.IntN(3) == 0 {
			killBackup()
		}
		restart(crashAfterStore)
	}
	d.kill()
	restart(0)
	// Let the last backup finish against the running daemon; if restic
	// gave up during the crashes, back up once more.
	select {
	case err := <-backupDone:
		backupDone <- err
	case <-time.After(5 * time.Minute):
		t.Fatal("backup did not finish")
	}
	if err := <-backupDone; err != nil {
		t.Logf("last backup failed after the crashes (%v); running another", err)
		if out, err := restics("backup", "--no-scan", src).CombinedOutput(); err != nil {
			t.Fatalf("backup after crashes: %v\n%s", err, out)
		}
	}
	// A killed restic leaves its lock, which check, being exclusive,
	// does not override.
	if out, err := restics("unlock").CombinedOutput(); err != nil {
		t.Fatalf("restic unlock: %v\n%s", err, out)
	}
	if out, err := restics("check").CombinedOutput(); err != nil {
		t.Fatalf("restic check: %v\n%s", err, out)
	}
	// One more crash with nothing in flight, then a clean restart.
	d.kill()
	restart(0)
	d.cmd.Process.Signal(syscall.SIGTERM)
	if err := <-d.done; err != nil {
		t.Fatalf("daemon exit: %v", err)
	}
	d.done <- nil

	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if p, _ := db.PendingCaptures(ctx); len(p) != 0 {
		t.Fatalf("%d rows left pending: %+v", len(p), p)
	}
	entries, err := db.QueueEntries(ctx, "laptop")
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string][]state.Entry{}
	multipart := 0
	for _, e := range entries {
		byKey[e.Key] = append(byKey[e.Key], e)
		if e.Upload != "" {
			multipart++
		}
	}
	onDisk := storeObjects(t, filepath.Join(n.data, "store", "laptop"))
	locks := 0
	for key, sum := range onDisk {
		if strings.HasPrefix(key, "locks/") {
			locks++
			if len(byKey[key]) != 0 {
				t.Errorf("local-only %s queued", key)
			}
			continue
		}
		es := byKey[key]
		var puts []state.Entry
		for _, e := range es {
			if e.Op == state.OpPut {
				puts = append(puts, e)
			}
		}
		switch {
		case len(es) == 0:
			t.Errorf("%s is in the store but not queued", key)
		case len(puts) != 1 || es[len(es)-1].Op != state.OpPut:
			t.Errorf("%s has %d entries (%d puts), want exactly one put: %+v", key, len(es), len(puts), es)
		case puts[0].Upload == "" && puts[0].SHA256 != sum:
			t.Errorf("%s queued with hash %s, store has %s", key, puts[0].SHA256, sum)
		}
	}
	for key, es := range byKey {
		if strings.HasPrefix(key, "locks/") {
			t.Errorf("local-only %s queued", key)
		}
		if _, ok := onDisk[key]; !ok && es[len(es)-1].Op == state.OpPut {
			t.Errorf("%s queued as a put but not in the store", key)
		}
	}
	logb, _ := os.ReadFile(logf)
	t.Logf("%d kills and %d self-kills after a store (%d with restic killed too), %d with a backup already running; "+
		"%d starts settled rows a kill left pending; "+
		"%d objects in the store (%d locks); %d queue entries, %d multipart",
		kills, selfKills, resticKills, midBackup, strings.Count(string(logb), "settled queue rows"), len(onDisk), locks, len(entries), multipart)
}

// storeObjects maps each object key under a posix bucket directory to
// its SHA-256, skipping versitygw's dot directories.
func storeObjects(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(de.Name(), ".") && p != dir {
			if de.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if de.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		s := sha256.Sum256(b)
		out[filepath.ToSlash(rel)] = hex.EncodeToString(s[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func countFiles(dir string) int {
	n := 0
	filepath.WalkDir(dir, func(_ string, de fs.DirEntry, err error) error {
		if err == nil && !de.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}
