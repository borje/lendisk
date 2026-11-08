package cli

import (
	"bytes"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/profile"
)

// kopiaTool runs Kopia with a configuration, cache and logs of its own,
// so tests never touch the user's.
type kopiaTool struct {
	t    *testing.T
	path string
	env  []string
}

func newKopia(t *testing.T) *kopiaTool {
	t.Helper()
	path, err := exec.LookPath("kopia")
	if err != nil {
		t.Skip("kopia not installed")
	}
	dir := t.TempDir()
	return &kopiaTool{t: t, path: path, env: append(os.Environ(),
		"HOME="+dir, "KOPIA_CONFIG_PATH="+filepath.Join(dir, "repository.config"),
		"KOPIA_CACHE_DIRECTORY="+filepath.Join(dir, "cache"), "KOPIA_LOG_DIR="+filepath.Join(dir, "logs"),
		"KOPIA_PASSWORD=test-password", "KOPIA_CHECK_FOR_UPDATES=false")}
}

// run runs kopia with args and fails the test unless it succeeds.
func (k *kopiaTool) run(args ...string) string {
	k.t.Helper()
	cmd := exec.Command(k.path, args...)
	cmd.Env = k.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		k.t.Fatalf("kopia %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

var reKopiaCreate = regexp.MustCompile(`(?m)^  kopia repository create s3 (.*) \\\n +(.*)$`)

// kopiaLines is the `kopia repository create s3` arguments bucket add or
// show printed, the endpoint replaced by addr (the daemon's port is
// chosen at serve in tests).
func kopiaLines(t *testing.T, out, addr string) []string {
	t.Helper()
	m := reKopiaCreate.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no Kopia lines in:\n%s", out)
	}
	args := strings.Fields(m[1] + " " + m[2])
	for i, a := range args {
		if strings.HasPrefix(a, "--endpoint=") {
			args[i] = "--endpoint=" + addr
		}
	}
	return append([]string{"repository", "create", "s3"}, args...)
}

// kopiaReplicable is what of a kopia bucket in n's store is replicated:
// all but the local-only logs.
func kopiaReplicable(t *testing.T, n *node, bucket string) map[string]string {
	t.Helper()
	out := storeObjects(t, filepath.Join(n.data, "store", bucket))
	maps.DeleteFunc(out, func(k, _ string) bool { return strings.HasPrefix(k, "_log") })
	return out
}

// sameFiles fails unless every file of src is in dst/src with the same
// bytes (kopia restores a snapshot's root into the target).
func sameFiles(t *testing.T, src, dst string) {
	t.Helper()
	ents, _ := os.ReadDir(src)
	if len(ents) == 0 {
		t.Fatal("nothing to compare")
	}
	for _, e := range ents {
		w, _ := os.ReadFile(filepath.Join(src, e.Name()))
		g, err := os.ReadFile(filepath.Join(dst, e.Name()))
		if err != nil || !bytes.Equal(g, w) {
			t.Fatalf("restored %s differs: %v", e.Name(), err)
		}
	}
}

// EP-1 AC, EP-6, EP-7 (#7): Kopia against a real `lendisk serve`. The
// lines `bucket add` prints create the repository; `kopia repository
// validate-provider` passes; two snapshots, a full verify and a restore
// succeed. Every key is captured in the class the kopia profile gives
// it, and the logs (_log*) are stored but never queued.
func TestKopiaEndToEnd(t *testing.T) {
	k := newKopia(t)
	n := newNode(t)
	n.mustInit(t)
	code, out, errOut := n.run("", "", "bucket", "add", "vault", "--profile", "kopia")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	if !strings.Contains(out, "--disable-tls --bucket=vault") {
		t.Fatalf("Kopia lines:\n%s", out)
	}
	d := n.serve(t)

	k.run(kopiaLines(t, out, d.addr)...)
	k.run("repository", "validate-provider")
	src := t.TempDir()
	randomFiles(t, src, "f", 8, 256<<10)
	k.run("snapshot", "create", src)
	os.WriteFile(filepath.Join(src, "f-00"), []byte("changed"), 0o600)
	randomFiles(t, src, "g", 2, 64<<10)
	k.run("snapshot", "create", src)
	if list := k.run("snapshot", "list", src); strings.Count(list, "files:") < 2 && strings.Count(list, "\n") < 2 {
		t.Fatalf("snapshots:\n%s", list)
	}
	k.run("snapshot", "verify", "--verify-files-percent=100")
	dst := t.TempDir()
	k.run("snapshot", "restore", src, dst)
	sameFiles(t, src, dst)
	d.stop()

	set, err := profile.Load("")
	if err != nil {
		t.Fatal(err)
	}
	prof := set.Get("kopia")
	caps := latestCaptures(t, n, "vault")
	seen := map[profile.Class]int{}
	for key := range storeObjects(t, filepath.Join(n.data, "store", "vault")) {
		class := prof.Classify(key)
		seen[class]++
		if class == profile.Data && key[0] != 'p' && key[0] != 'q' {
			t.Errorf("%s is data by no rule: Kopia wrote a key the profile does not know", key)
		}
		e, ok := caps[key]
		switch {
		case class == profile.LocalOnly && ok:
			t.Errorf("local-only %s was queued", key)
		case class == profile.LocalOnly:
		case !ok:
			t.Errorf("%s is in the store but not captured", key)
		case e.Class != string(class):
			t.Errorf("%s captured as %s, want %s", key, e.Class, class)
		}
	}
	t.Logf("classes in the store: %v", seen)
	for _, c := range []profile.Class{profile.Snapshot, profile.Metadata, profile.Data, profile.LocalOnly} {
		if seen[c] == 0 {
			t.Errorf("no %s key in the store: %v", c, seen)
		}
	}
	for _, key := range []string{"kopia.repository", "kopia.blobcfg"} {
		if caps[key].Class != string(profile.Metadata) {
			t.Errorf("%s: %+v", key, caps[key])
		}
	}
}

// The Kopia counterpart of TestReplicateResticBackupToFriend and
// TestResticRestoreThroughFriendsAfterRecovery (EP-1, EP-7, REPL-12,
// ID-5, EP-10; #7):
//
//   - A backs up with Kopia; every key but the logs reaches friend B,
//     byte for byte; a second snapshot follows it.
//   - `kopia maintenance run --full` after the second snapshot rewrites
//     kopia.maintenance in place. That is what #74 is about: B refuses
//     the new version (EP-11, LEND-4), A raises the immutability alert,
//     and B keeps the old one. Everything else maintenance writes or
//     deletes replicates, and the queue drains.
//   - A's machine is lost; a fresh machine recovers from B, and Kopia
//     connects with the new credential and restores the latest snapshot
//     through the read-through, from B.
func TestReplicateKopiaToFriend(t *testing.T) {
	k := newKopia(t)
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an, bn := newNode(t), newNode(t)
	bn.regions = an.regions // one relay for both
	for n, p := range map[*node]string{an: phraseA, bn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	code, out, errOut := an.run("", "", "bucket", "add", "vault", "--profile", "kopia")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	aID := an.identity()
	bKey := bn.identity().NodeID().String()

	k.run(kopiaLines(t, out, a.addr)...)
	src := t.TempDir()
	randomFiles(t, src, "one", 6, 1<<20)
	k.run("snapshot", "create", src)
	an.drained(bKey, 90*time.Second)
	sameObjects(t, kopiaReplicable(t, an, "vault"), lentObjects(t, bn, aID, "vault"))
	if logs := len(storeObjects(t, filepath.Join(an.data, "store", "vault"))) - len(kopiaReplicable(t, an, "vault")); logs == 0 {
		t.Error("Kopia wrote no logs; the local-only class went untested")
	}

	randomFiles(t, src, "two", 4, 1<<20)
	k.run("snapshot", "create", src)
	an.drained(bKey, 90*time.Second)
	sameObjects(t, kopiaReplicable(t, an, "vault"), lentObjects(t, bn, aID, "vault"))
	refusedAlert := func() bool {
		for _, al := range an.status().Alerts {
			if al.Kind == alert.KindPlaceRefused {
				return true
			}
		}
		return false
	}
	if p := an.place(bKey); p.Refused != 0 || refusedAlert() {
		t.Fatalf("before maintenance: %+v, alerts %+v", p, an.status().Alerts)
	}

	// Full maintenance, as a user would run it (safety=none so that it
	// rewrites and deletes now rather than after its usual delays).
	maintBefore := kopiaReplicable(t, an, "vault")["kopia.maintenance"]
	t.Logf("kopia maintenance run:\n%s", k.run("maintenance", "run", "--full", "--safety=none"))
	maintAfter := kopiaReplicable(t, an, "vault")["kopia.maintenance"]
	p := an.drained(bKey, 90*time.Second)
	st := an.status()
	t.Logf("after maintenance: place %+v", p)
	for _, al := range st.Alerts {
		t.Logf("alert %s %s: %s", al.Kind, al.Subject, al.Message)
	}
	_, text, _ := an.run("", "", "status")
	t.Logf("status:\n%s", text)
	local, lent := kopiaReplicable(t, an, "vault"), lentObjects(t, bn, aID, "vault")
	var differ []string
	for key, h := range local {
		if lent[key] != h {
			differ = append(differ, key)
		}
	}
	for key := range lent {
		if _, ok := local[key]; !ok {
			differ = append(differ, key+" (deleted at A)")
		}
	}
	t.Logf("kopia.maintenance before %q, after %q; keys B holds differently from A: %v", maintBefore, maintAfter, differ)
	// #74: the rewritten kopia.maintenance is the one difference, and it
	// is refused and alerted, not stuck in the queue. When #74 lets a
	// profile declare rewritable keys, this part changes.
	if maintBefore == "" || maintBefore == maintAfter {
		t.Fatalf("maintenance did not rewrite kopia.maintenance (%q, %q); the #74 finding no longer holds", maintBefore, maintAfter)
	}
	if len(differ) != 1 || differ[0] != "kopia.maintenance" || lent["kopia.maintenance"] != maintBefore {
		t.Errorf("after maintenance B differs from A in %v", differ)
	}
	if p.Refused != 1 || !refusedAlert() {
		t.Errorf("the refused rewrite: place %+v, alerts %+v", p, st.Alerts)
	}
	// Every later maintenance run rewrites it again: the count stays at
	// one (only the key's latest row counts) and the alert never clears.
	k.run("maintenance", "run")
	if again := kopiaReplicable(t, an, "vault")["kopia.maintenance"]; again == maintAfter {
		t.Fatal("a quick maintenance run did not rewrite kopia.maintenance")
	}
	if p = an.drained(bKey, 90*time.Second); p.Refused != 1 || !refusedAlert() {
		t.Errorf("after a second maintenance run: place %+v, alerts %+v", p, an.status().Alerts)
	}

	// A's machine is gone; a fresh one recovers from B.
	bAddr := bn.status().Node.Address
	a.p.kill()
	os.RemoveAll(filepath.Dir(filepath.Dir(an.config)))
	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 15 * time.Second
	if code, out, errOut := fresh.run("y\n"+phraseA+"\n", "", "recover", bAddr); code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	fa := newProcNode(t, fresh, fast)
	fa.start()
	_, shown, _ := fresh.run("", "", "bucket", "show", "vault")
	connect := kopiaLines(t, shown, fa.addr)
	connect[1] = "connect"
	k2 := newKopia(t)
	k2.run(connect...)
	dst := t.TempDir()
	k2.run("snapshot", "restore", src, dst)
	sameFiles(t, src, dst)
	logs, _ := os.ReadFile(fa.logf)
	if !strings.Contains(string(logs), "read-through: fetched") {
		t.Fatalf("no fetch logged:\n%s", tail(logs))
	}
}
