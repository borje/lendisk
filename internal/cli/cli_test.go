package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

const testPhrase = "abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon art"

// node is one simulated machine: its own config and data directories.
type node struct {
	t      *testing.T
	config string
	data   string
}

func newNode(t *testing.T) *node {
	root := t.TempDir()
	return &node{t: t, config: filepath.Join(root, "etc", "lendisk.toml"), data: filepath.Join(root, "var")}
}

// run executes the CLI with stdin and returns exit code, stdout, stderr.
func (n *node) run(stdin string, phrase string, args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	a := &app{
		in: bufio.NewReader(strings.NewReader(stdin)), out: &out, errOut: &errOut,
		pick:      func(int, int) []int { return []int{0, 11, 23} },
		newPhrase: func() (string, error) { return phrase, nil },
	}
	args = append(args, "--config", n.config, "--data-dir", n.data)
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

func TestRestoreOnSecondMachineGivesSameID(t *testing.T) {
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
	n := newNode(t)
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx}
	done := make(chan int)
	go func() {
		done <- a.run([]string{"serve", "--config", n.config, "--data-dir", n.data, "--log-format", "json", "--log-level", "debug"})
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

func TestUsageErrorExitCode(t *testing.T) {
	n := newNode(t)
	if code, _, _ := n.run("", "", "status", "--no-such-flag"); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if code, _, _ := n.run("", "", "status", "--log-level", "loud"); code != 1 {
		t.Fatalf("bad setting exit %d, want 1", code)
	}
}
