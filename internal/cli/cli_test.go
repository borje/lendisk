package cli

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/store"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

const testPhrase = "abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon art"

// node is one simulated machine: its own config and data directories,
// and a local DERP relay standing in for the public ones.
type node struct {
	t       *testing.T
	config  string
	data    string
	regions transport.RegionSource
	// clock and dialTimeout, when set, are given to every command.
	clock       func() time.Time
	dialTimeout time.Duration
	// redeemTransport and beforeFriendship, when set, are given to
	// every command (app's fields of those names).
	redeemTransport  func(http.RoundTripper) http.RoundTripper
	beforeFriendship func() error
	// afterFetch, when set, is given to every command (recover's hook).
	afterFetch func()
	// listBudget, when set, is recover's listing budget.
	listBudget time.Duration
	// stdin, when set, is every command's stdin in place of run's.
	stdin io.Reader
	// tuneBridge, when set, is given to serveLogged's daemon.
	tuneBridge func(*bridge.Bridge)
	// tuneAlerts, when set, is given to serveLogged's daemon.
	tuneAlerts func(*alert.Service)
	// tuneReconcile, when set, is given to serve's daemon.
	tuneReconcile func(*gateway.ReconcileSettings)
	// sleep, when set, is every command's pause between polls.
	sleep func(context.Context, time.Duration) error
	// tuneEnding, when set, is given to serveLogged's daemon.
	tuneEnding func(*farewell)
	// beforeRead, when set, is given to serveLogged's daemon: it runs
	// before each read of a borrower's body (gateway.BeforeRead).
	beforeRead func(key string, read int64)
	// lendSpace is the store's size every command reads for the default
	// lending cap (#24); zero is 1 PiB, so tests that lend 80G run on a
	// small disk.
	lendSpace uint64
	// env, when set, is the environment every command reads overflow
	// credentials from (app.env); unset variables are absent.
	env map[string]string
	// flags, when set, are added to every command run gives.
	flags []string
	// betweenStatusReads, when set, is given to every command (app's
	// field of that name).
	betweenStatusReads func()
}

func newNode(t *testing.T) *node {
	root := t.TempDir()
	return &node{t: t, config: filepath.Join(root, "etc", "lendisk.toml"), data: filepath.Join(root, "var"),
		regions: &transporttest.Source{D: transporttest.Start(t)}}
}

// run executes the CLI with stdin and returns exit code, stdout, stderr.
func (n *node) run(stdin string, phrase string, args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	var in io.Reader = strings.NewReader(stdin)
	if n.stdin != nil {
		in = n.stdin
	}
	a := &app{
		in: bufio.NewReader(in), out: &out, errOut: &errOut,
		pick:      func(int, int) []int { return []int{0, 11, 23} },
		newPhrase: func() (string, error) { return phrase, nil },
		regions:   n.regions,
		clock:     n.clock, dialTimeout: n.dialTimeout,
		redeemTransport: n.redeemTransport, beforeFriendship: n.beforeFriendship,
		afterFetch: n.afterFetch, listBudget: n.listBudget, sleep: n.sleep,
		lendStatfs: func(string) (store.Space, error) { return store.Space{Total: cmp.Or(n.lendSpace, 1<<50)}, nil },
		env:        func(k string) (string, bool) { v, ok := n.env[k]; return v, ok },

		betweenStatusReads: n.betweenStatusReads,
	}
	args = append(append(args, n.flags...), "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", "127.0.0.1:0")
	code := a.run(args)
	return code, out.String(), errOut.String()
}

func (n *node) status() Status {
	n.t.Helper()
	code, out, errOut := n.run("", "", "status", "--json")
	if code != 0 {
		n.t.Fatalf("status failed: %s", errOut)
	}
	var st Status
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		n.t.Fatalf("status --json: %v\n%s", err, out)
	}
	return st
}

func TestInitConfirmsAndStatusShowsID(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	// Words 1, 12 and 24, the second with sloppy case and spacing.
	code, out, errOut := n.run("abandon\n  ABANDON \nart\n", testPhrase, "init")
	if code != 0 {
		t.Fatalf("init exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, " 1. abandon") || !strings.Contains(out, "24. art") {
		t.Fatalf("phrase not shown:\n%s", out)
	}
	want, _ := identity.FromPhrase(testPhrase)
	st := n.status()
	if st.Node.ID != want.NodeID().String() || st.Schema != StatusSchema || st.Node.Role != "full" {
		t.Fatalf("status = %+v", st)
	}
	if !strings.Contains(out, st.Node.ID) {
		t.Fatal("init did not print the node ID")
	}
	code, text, _ := n.run("", "", "status")
	if code != 0 || !strings.Contains(text, "Node ID: "+st.Node.ID) {
		t.Fatalf("status text: %s", text)
	}
	if code, out, _ := n.run("", "", "status", "--quiet"); code != 0 || out != "" {
		t.Fatalf("status --quiet: %d %q", code, out)
	}

	// Modes: config dir 0700, key and database 0600, config file 0600.
	modes := map[string]os.FileMode{
		filepath.Dir(n.config):                                0o700,
		filepath.Join(filepath.Dir(n.config), "identity.key"): 0o600,
		filepath.Join(n.data, "lendisk.db"):                   0o600,
		n.config:                                              0o600,
	}
	for p, m := range modes {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != m {
			t.Errorf("%s mode %04o, want %04o", p, fi.Mode().Perm(), m)
		}
	}

	// The phrase is never written anywhere (ID-2, ID-3).
	assertNoPhrase(t, filepath.Dir(filepath.Dir(n.config)), testPhrase)
	if strings.Contains(errOut, "art") {
		t.Fatalf("phrase words in logs: %s", errOut)
	}

	// A second init refuses to replace the identity.
	if code, _, errOut := n.run("", testPhrase, "init"); code == 0 || !strings.Contains(errOut, "already has an identity") {
		t.Fatalf("second init: %d %s", code, errOut)
	}
}

func assertNoPhrase(t *testing.T, root, phrase string) {
	t.Helper()
	words := strings.Fields(phrase)
	// Any run of four consecutive phrase words counts as a leak.
	needle := strings.Join(words[20:], " ")
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, _ := os.ReadFile(p)
		if bytes.Contains(b, []byte(needle)) {
			t.Errorf("phrase found in %s", p)
		}
		return nil
	})
}

func TestInitWrongWordsWritesNothing(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	code, _, errOut := n.run("abandon\nwrong\nwrong\nwrong\n", testPhrase, "init")
	if code == 0 || !strings.Contains(errOut, "confirmation failed") {
		t.Fatalf("init with wrong words: %d %s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(n.config), "identity.key")); !os.IsNotExist(err) {
		t.Fatal("key written despite failed confirmation")
	}
	// Input ending early also aborts.
	if code, _, _ := n.run("abandon\n", testPhrase, "init"); code == 0 {
		t.Fatal("init accepted truncated confirmation")
	}
	if code, _, _ := n.run("", "", "status"); code == 0 {
		t.Fatal("status succeeded with no identity")
	}
}

// #53: init never chmods a directory the user keeps other things in,
// such as $HOME when the config is ./lendisk.toml; it refuses instead.
func TestInitRefusesSharedDirectory(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	home := filepath.Dir(n.config)
	os.MkdirAll(home, 0o755)
	os.Chmod(home, 0o755)
	os.WriteFile(filepath.Join(home, ".bashrc"), []byte("x"), 0o644)
	code, _, errOut := n.run("abandon\nabandon\nart\n", testPhrase, "init")
	if code != 1 || !strings.Contains(errOut, "dedicated directory") || !strings.Contains(errOut, home) {
		t.Fatalf("init in a shared directory: %d %s", code, errOut)
	}
	if fi, _ := os.Stat(home); fi.Mode().Perm() != 0o755 {
		t.Fatalf("mode changed to %04o", fi.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(home, "identity.key")); !os.IsNotExist(err) {
		t.Fatal("key written into a shared directory")
	}
}

// #53: an existing directory holding nothing but lendisk's own files,
// such as a fresh compose bind mount or the example config, is
// tightened to 0700.
func TestInitTightensItsOwnDirectories(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	for _, d := range []string{filepath.Dir(n.config), n.data} {
		os.MkdirAll(d, 0o755)
		os.Chmod(d, 0o755)
	}
	os.WriteFile(n.config, []byte("[log]\nlevel = \"info\"\n"), 0o600)
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
	for _, d := range []string{filepath.Dir(n.config), n.data} {
		if fi, _ := os.Stat(d); fi.Mode().Perm() != 0o700 {
			t.Errorf("%s mode %04o, want 0700", d, fi.Mode().Perm())
		}
	}
}

func TestRestoreOnSecondMachineGivesSameID(t *testing.T) {
	parallel(t, 0)
	// ID-1 AC through the CLI: a second machine restored from the phrase
	// has the same node ID.
	a, b := newNode(t), newNode(t)
	if code, _, e := a.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
	if code, _, e := b.run(testPhrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	if a.status().Node.ID != b.status().Node.ID {
		t.Fatal("restored node has a different ID")
	}
	c := newNode(t)
	if code, _, _ := c.run("abandon abandon art\n", "", "init", "--restore"); code == 0 {
		t.Fatal("restore accepted a short phrase")
	}
}

func TestInitRefusesForeignDatabase(t *testing.T) {
	parallel(t, 0)
	a := newNode(t)
	if code, _, e := a.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
	// Same data directory, different config dir and identity.
	b := newNode(t)
	b.data = a.data
	other, _ := identity.NewPhrase()
	code, _, errOut := b.run(other+"\n", "", "init", "--restore")
	if code == 0 || !strings.Contains(errOut, "belongs to node") {
		t.Fatalf("adopted another node's database: %d %s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(b.config), "identity.key")); !os.IsNotExist(err) {
		t.Fatal("key written for a foreign database")
	}
}

func TestServeLogsStructuredAndStops(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx, regions: n.regions}
	done := make(chan int)
	go func() {
		done <- a.run([]string{"serve", "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", "127.0.0.1:0", "--log-format", "json", "--log-level", "debug"})
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("serve exit %d: %s", code, errOut.String())
	}
	var sawStart bool
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["msg"] == "lendisk started" {
			sawStart = rec["node_id"] != "" && rec["level"] == "INFO"
		}
	}
	if !sawStart {
		t.Fatalf("no start record:\n%s", out.String())
	}
}

// #53: when the loopback endpoint cannot bind, serve stops the gateway
// before it returns, so the database is never closed under it.
func TestServeBindFailureStopsGateway(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	n.mustInit(t)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	var out, errOut bytes.Buffer
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: t.Context(), regions: n.regions}
	code := a.run([]string{"serve", "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", taken.Addr().String()})
	if code != 1 || !strings.Contains(errOut.String(), "S3 endpoint") {
		t.Fatalf("serve exit %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "gateway serving") {
		t.Fatalf("the gateway never started:\n%s", out.String())
	}
	if c, err := net.Dial("unix", gateway.SocketPath(n.data)); err == nil {
		c.Close()
		t.Fatal("the gateway still answers after serve returned")
	}
	// And a second daemon in this process starts cleanly.
	n.serve(t).stop()
}

func TestUsageErrorExitCode(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	if code, _, _ := n.run("", "", "status", "--no-such-flag"); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if code, _, _ := n.run("", "", "status", "--log-level", "loud"); code != 1 {
		t.Fatalf("bad setting exit %d, want 1", code)
	}
}

// AGENTS.md: cobra's own usage errors (a missing or extra argument, an
// unknown subcommand) exit 2, like a bad flag.
func TestCobraUsageErrorsExitTwo(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	for _, args := range [][]string{
		{"bucket", "add"},          // missing argument
		{"status", "extra"},        // argument where none is taken
		{"no-such-command"},        // unknown command
		{"bucket", "no-such-verb"}, // unknown subcommand
		{"bucket", "help", "nope"}, // help for an unknown subcommand
	} {
		if code, _, e := n.run("", "", args...); code != 2 {
			t.Errorf("%v: exit %d, want 2 (%s)", args, code, e)
		}
	}
	// A command group on its own still prints its help.
	for _, args := range [][]string{{}, {"bucket"}, {"bucket", "help"}, {"peer", "help"}, {"help", "bucket"}} {
		if code, out, e := n.run("", "", args...); code != 0 || !strings.Contains(out, "Available Commands") {
			t.Errorf("%v: exit %d, want 0 and help (%s%s)", args, code, out, e)
		}
	}
	if code, out, e := n.run("", "", "bucket", "help", "add"); code != 0 || !strings.Contains(out, "bucket add") {
		t.Errorf("bucket help add: exit %d (%s%s)", code, out, e)
	}
}

// EP-12: status reports the linked versitygw and tailcat versions, as
// go.sum pins them.
func TestStatusReportsVersitygwAndTailcatVersions(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	st := n.status()
	if !strings.HasPrefix(st.Versions.Versitygw, "v1.") {
		t.Errorf("versitygw version %q", st.Versions.Versitygw)
	}
	if !strings.HasPrefix(st.Versions.Tailcat, "v0.") {
		t.Errorf("tailcat version %q", st.Versions.Tailcat)
	}
	_, out, _ := n.run("", "", "status")
	if !strings.Contains(out, "tailcat "+st.Versions.Tailcat) {
		t.Errorf("status does not name the tailcat version:\n%s", out)
	}
}

// SEC-16's search (filesHolding) finds a needle in a file's contents,
// in a file's name and in a directory's name.
func TestFileSearchFindsNamesAndContents(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	if err := os.MkdirAll(filepath.Join(n.data, "store", "lent-NEEDLEDIR"), 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(n.data, "x-NEEDLEFILE"), nil, 0o600)
	os.WriteFile(filepath.Join(n.data, "plain"), []byte("..NEEDLEBODY.."), 0o600)
	for _, needle := range []string{"NEEDLEDIR", "NEEDLEFILE", "NEEDLEBODY"} {
		if got := filesHolding(n, needle); len(got) != 1 {
			t.Errorf("search for %s found %v, want one path", needle, got)
		}
	}
}
