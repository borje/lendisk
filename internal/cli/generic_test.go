package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// EP-8: a generic bucket is refused without --i-understand-no-encryption,
// and nothing is created; with it, the bucket is made.
func TestGenericBucketRefusedWithoutNoEncryptionFlag(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	code, _, errOut := n.run("", "", "bucket", "add", "tools", "--profile", "generic")
	if code != 2 || !strings.Contains(errOut, "--i-understand-no-encryption") {
		t.Fatalf("generic bucket without the flag: exit %d\n%s", code, errOut)
	}
	if _, out, _ := n.run("", "", "bucket", "list"); strings.Contains(out, "tools") {
		t.Fatalf("a refused bucket was created:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(n.data, "store", "tools")); err == nil {
		t.Fatal("a refused bucket is in the store")
	}
	code, out, errOut := n.run("", "", "bucket", "add", "tools", "--profile", "generic", "--i-understand-no-encryption")
	if code != 0 {
		t.Fatalf("generic bucket with the flag: exit %d\n%s", code, errOut)
	}
	if !strings.Contains(out, "not encrypted") {
		t.Fatalf("bucket add does not say the bucket is not encrypted:\n%s", out)
	}
}

// DESIGN §3.3 EP-2 (#7): server-side copies are refused for every
// profile, so the generic lines tell rclone users to copy by download
// and re-upload and not to update modtimes in place, say why, and say
// that --backup-dir and --suffix, which rclone allows only with a
// server-side copy, cannot be used; at add and at show.
func TestGenericLinesGiveRcloneFlags(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	_, added, _ := n.run("", "", "bucket", "add", "tools", "--profile", "generic", "--i-understand-no-encryption")
	_, shown, _ := n.run("", "", "bucket", "show", "tools")
	for name, out := range map[string]string{"add": added, "show": shown} {
		for _, want := range []string{"--disable Copy,Move", "--no-update-modtime", "server-side cop", "--backup-dir"} {
			if !strings.Contains(out, want) {
				t.Errorf("bucket %s lacks %q:\n%s", name, want, out)
			}
		}
	}
	// restic and Kopia do not copy server-side; their lines say nothing of it.
	_, out, _ := n.run("", "", "bucket", "add", "laptop", "--profile", "restic")
	if strings.Contains(out, "--disable") {
		t.Errorf("restic lines carry rclone advice:\n%s", out)
	}
}

// EP-8: status marks a generic bucket as outside the trust guarantee,
// in --json and in text; restic and Kopia buckets are inside it.
func TestStatusMarksGenericBucketOutsideTrust(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "tools", "generic")
	n.addBucket(t, "laptop", "restic")
	n.addBucket(t, "desktop", "kopia")
	for _, b := range n.status().Buckets {
		if want := b.Name == "tools"; b.OutsideTrust != want {
			t.Errorf("bucket %s (%s): outside_trust %v", b.Name, b.Profile, b.OutsideTrust)
		}
	}
	_, text, _ := n.run("", "", "status")
	// Each bucket's line and the one under it.
	var tools, laptop string
	lines := strings.Split(text, "\n")
	for i, l := range lines[:len(lines)-1] {
		switch {
		case strings.Contains(l, "tools (generic"):
			tools = l + lines[i+1]
		case strings.Contains(l, "laptop (restic"):
			laptop = l + lines[i+1]
		}
	}
	if !strings.Contains(tools, "outside the trust guarantee") || strings.Contains(laptop, "outside") {
		t.Fatalf("status text:\n%s", text)
	}
}

// rcloneRemote is an on-the-fly rclone remote for bucket c at addr, with
// the settings the generic lines give.
func rcloneRemote(addr string, c creds) string {
	return fmt.Sprintf(":s3,provider=Other,access_key_id=%s,secret_access_key=%s,endpoint='http://%s',force_path_style=true,no_check_bucket=true:",
		c.access, c.secret, addr)
}

// rcloneRun runs rclone with args, failing the test unless the exit
// status is want (0: success).
func rcloneRun(t *testing.T, rclone string, want int, args ...string) string {
	t.Helper()
	cmd := exec.Command(rclone, append(args, "--config", "/dev/null", "--retries", "1")...)
	out, err := cmd.CombinedOutput()
	if got := cmd.ProcessState.ExitCode(); got != want {
		t.Fatalf("rclone %s: exit %d (%v), want %d\n%s", args[0], got, err, want, out)
	}
	return string(out)
}

// COMPAT-1, DESIGN §3.3 EP-2 (#7): rclone against a generic bucket with
// the flags the lines give. Syncs of new, changed and deleted files, a
// sync after a file's modtime alone changed (by size and modtime, and
// with --checksum), and an `rclone move` all succeed; the bucket ends up
// as the source; and every object in the store is captured.
func TestRcloneSyncWithAdvisedFlags(t *testing.T) {
	rclone, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not installed")
	}
	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "tools", "generic")
	d := n.serve(t)
	remote := rcloneRemote(d.addr, c)
	advised := []string{"--disable", "Copy,Move", "--no-update-modtime"}
	src := t.TempDir()
	randomFiles(t, src, "f", 4, 64<<10)
	sync := func(extra ...string) {
		t.Helper()
		rcloneRun(t, rclone, 0, append(append([]string{"sync", src, remote + "tools/current"}, advised...), extra...)...)
	}
	sync()
	b := make([]byte, 64<<10)
	rand.Read(b)
	os.WriteFile(filepath.Join(src, "f-01"), b, 0o600)
	os.Remove(filepath.Join(src, "f-02"))
	sync()
	// Only a modtime changes: without --no-update-modtime rclone sets it
	// on the object in place with a CopyObject, which is refused.
	future := time.Now().Add(48 * time.Hour)
	os.Chtimes(filepath.Join(src, "f-03"), future, future)
	sync()
	os.Chtimes(filepath.Join(src, "f-00"), future, future)
	sync("--checksum")
	rcloneRun(t, rclone, 0, "check", src, remote+"tools/current")
	// A move between prefixes is a download and a re-upload, then a delete.
	rcloneRun(t, rclone, 0, append([]string{"move", remote + "tools/current/f-03", remote + "tools/moved"}, advised...)...)

	store := filepath.Join(n.data, "store", "tools")
	if got, err := os.ReadFile(filepath.Join(store, "current", "f-01")); err != nil || !bytes.Equal(got, b) {
		t.Errorf("current/f-01 in the store: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store, "current", "f-02")); err == nil {
		t.Error("the deleted file is still current")
	}
	want, _ := os.ReadFile(filepath.Join(src, "f-03"))
	if got, err := os.ReadFile(filepath.Join(store, "moved", "f-03")); err != nil || !bytes.Equal(got, want) {
		t.Errorf("moved/f-03 in the store: %v", err)
	}
	d.stop()
	objects := storeObjects(t, store)
	captured := capturedKeys(t, n, "tools")
	for k := range objects {
		if !captured[k] {
			t.Errorf("%s is in the store but was not captured", k)
		}
	}
	for k, put := range captured {
		if _, ok := objects[k]; ok != put {
			t.Errorf("%s: captured as present %v, in the store %v", k, put, ok)
		}
	}
}

// DESIGN §3.3 EP-2 (#7): rclone's --backup-dir (and --suffix) need a
// server-side copy or move, which lendisk refuses for every profile.
// With the advised flags rclone refuses the sync before it touches the
// bucket; without them each server-side copy gets 501 and rclone
// neither overwrites nor deletes anything. Either way the bucket is
// unchanged, never half-synced.
func TestRcloneBackupDirRefusedWithoutHarm(t *testing.T) {
	rclone, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not installed")
	}
	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "tools", "generic")
	d := n.serve(t)
	remote := rcloneRemote(d.addr, c)
	src := t.TempDir()
	randomFiles(t, src, "f", 3, 64<<10)
	rcloneRun(t, rclone, 0, "sync", src, remote+"tools/current")
	store := filepath.Join(n.data, "store", "tools")
	before := storeObjects(t, store)
	randomFiles(t, src, "f", 2, 32<<10) // f-00 and f-01 change
	os.Remove(filepath.Join(src, "f-02"))

	backup := []string{"sync", src, remote + "tools/current", "--backup-dir", remote + "tools/old"}
	out := rcloneRun(t, rclone, 7, append(backup, "--disable", "Copy,Move", "--no-update-modtime")...)
	if !strings.Contains(out, "server-side move or copy") {
		t.Errorf("rclone's refusal:\n%s", out)
	}
	out = rcloneRun(t, rclone, 1, backup...)
	if !strings.Contains(out, "501") {
		t.Errorf("rclone without the flags:\n%s", out)
	}
	if after := storeObjects(t, store); !maps.Equal(after, before) {
		t.Fatalf("the bucket changed: %v, was %v", after, before)
	}
}

// capturedKeys is the keys of bucket whose latest done queue row is a put.
func capturedKeys(t *testing.T, n *node, bucket string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for k, e := range latestCaptures(t, n, bucket) {
		out[k] = e.Op == state.OpPut
	}
	return out
}

// latestCaptures is each key's latest done queue row in bucket.
func latestCaptures(t *testing.T, n *node, bucket string) map[string]state.Entry {
	t.Helper()
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	es, err := db.QueueEntries(t.Context(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]state.Entry{}
	for _, e := range es {
		if e.State == "done" {
			out[e.Key] = e
		}
	}
	return out
}

// COMPAT-1: Duplicacy's S3 storage against a generic bucket: init,
// backup, a second backup after a change, check and restore.
func TestDuplicacySmoke(t *testing.T) {
	duplicacy, err := exec.LookPath("duplicacy")
	if err != nil {
		t.Skip("duplicacy not installed")
	}
	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "dup", "generic")
	d := n.serve(t)
	// Duplicacy's minio:// is plain HTTP S3 with path-style addressing;
	// the storage is named "default", whose keys come from the env.
	url := "minio://us-east-1@" + d.addr + "/dup/repo"
	env := append(os.Environ(), "DUPLICACY_S3_ID="+c.access, "DUPLICACY_S3_SECRET="+c.secret,
		"DUPLICACY_PASSWORD=test-password", "HOME="+t.TempDir())
	run := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command(duplicacy, args...)
		cmd.Dir, cmd.Env = dir, env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("duplicacy %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	src := t.TempDir()
	randomFiles(t, src, "f", 6, 256<<10)
	run(src, "init", "-e", "laptop", url)
	run(src, "backup", "-stats")
	os.WriteFile(filepath.Join(src, "f-00"), []byte("changed"), 0o600)
	run(src, "backup", "-stats")
	run(src, "check", "-files")

	dst := t.TempDir()
	run(dst, "init", "-e", "laptop", url)
	run(dst, "restore", "-r", "2")
	for i := range 6 {
		name := fmt.Sprintf("f-%02d", i)
		w, _ := os.ReadFile(filepath.Join(src, name))
		g, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil || !bytes.Equal(g, w) {
			t.Fatalf("restored %s differs: %v", name, err)
		}
	}
}
