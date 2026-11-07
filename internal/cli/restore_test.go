package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// The recovery demo, end to end (ID-5's AC, ID-6, EP-10, scenario 3;
// #30): "my NAS died and I got my data back from my friends".
//
//   - A backs up with restic to a bucket at two copies over three
//     friends B, C and D: a first snapshot, then files removed and others
//     added, a second. Replication drains to all three.
//   - A's machine and every file of it are gone. On a fresh machine,
//     `lendisk recover` with the phrase and B's address; then D goes
//     offline, and `lendisk serve` starts.
//   - `restic restore latest`, with the credential `lendisk bucket show`
//     prints and the restic password, restores the second snapshot's
//     files exactly, fetching each object it reads from B or C.
//   - The local store then holds only what restic read, each object the
//     version A wrote: the first snapshot's packs were never fetched, so
//     less than the whole repository is downloaded.
func TestResticRestoreThroughFriendsAfterRecovery(t *testing.T) {
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an, bn, cn, dn := newNode(t), newNode(t), newNode(t), newNode(t)
	for _, n := range []*node{bn, cn, dn} {
		n.regions = an.regions // one relay for all
	}
	for n, p := range map[*node]string{an: phraseA, bn: "", cn: "", dn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	c0 := an.addBucket(t, "laptop", "restic")
	an.addBucket(t, "photos", "restic") // a second bucket, never read
	fast := envReplFast + "=1"
	a, b, c, d := newProcNode(t, an, fast), newProcNode(t, bn, fast), newProcNode(t, cn, fast), newProcNode(t, dn, fast)
	b.start()
	c.start()
	d.start()
	a.start()
	for _, n := range []*node{bn, cn, dn} {
		befriend(t, n, an)
	}
	aID := an.identity()
	bKey, cKey, dKey := bn.identity().NodeID().String(), cn.identity().NodeID().String(), dn.identity().NodeID().String()
	bAddr := bn.status().Node.Address

	src := t.TempDir()
	resticEnv := func(addr string, cr creds) []string {
		return append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+cr.access, "AWS_SECRET_ACCESS_KEY="+cr.secret,
			"RESTIC_REPOSITORY=s3:http://"+addr+"/laptop", "RESTIC_PASSWORD=test-password",
			"RESTIC_CACHE_DIR="+t.TempDir())
	}
	runRestic := func(env []string, args ...string) string {
		t.Helper()
		cmd := exec.Command(restic, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	env := resticEnv(a.addr, c0)
	runRestic(env, "init")
	randomFiles(t, src, "old", 4, 1<<20)
	runRestic(env, "backup", "--pack-size", "4", "--no-scan", src)
	for i := range 4 {
		os.Remove(filepath.Join(src, "old-0"+string(rune('0'+i))))
	}
	randomFiles(t, src, "new", 4, 1<<20)
	runRestic(env, "backup", "--pack-size", "4", "--no-scan", src)
	for _, k := range []string{bKey, cKey, dKey} {
		an.drained(k, 90*time.Second)
	}
	want := replicable(t, an, "laptop")
	// Every friend holds A's manifest naming all three and the buckets.
	deadline := time.Now().Add(60 * time.Second)
	for {
		ok := true
		for _, n := range []*node{bn, cn, dn} {
			m, err := heldManifest(t, n, aID)
			if err != nil || len(m.Friends) != 3 || len(m.Buckets) != 2 {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			m, err := heldManifest(t, bn, aID)
			t.Fatalf("the manifest at B: %+v %v", m, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// A's machine is gone with every file on it.
	a.p.kill()
	os.RemoveAll(filepath.Dir(filepath.Dir(an.config)))

	// A fresh machine: the phrase and B's address. Then D goes offline.
	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 15 * time.Second
	code, out, errOut := fresh.run("y\n"+phraseA+"\n", "", "recover", bAddr)
	if code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	d.p.kill()
	fa := newProcNode(t, fresh, fast)
	fa.start()
	cr := bucketCreds(t, fresh, "laptop")
	if cr == c0 {
		t.Fatal("the recovered bucket kept its old credential")
	}

	dst := t.TempDir()
	runRestic(resticEnv(fa.addr, cr), "restore", "latest", "--target", dst)
	for i := range 4 {
		name := "new-0" + string(rune('0'+i))
		w, _ := os.ReadFile(filepath.Join(src, name))
		g, err := os.ReadFile(filepath.Join(dst, src, name))
		if err != nil || !bytes.Equal(g, w) {
			t.Fatalf("restored %s differs: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, src, "old-00")); err == nil {
		t.Fatal("a file of the older snapshot was restored")
	}

	// Scenario 3: only what restic read was fetched, each the version A
	// wrote; the older snapshot's packs stayed with the friends.
	got := replicable(t, fresh, "laptop")
	for k, h := range got {
		if want[k] != h {
			t.Errorf("fetched %s: %s, A wrote %q", k, h, want[k])
		}
	}
	var unread int
	for k := range want {
		if _, ok := got[k]; !ok && strings.HasPrefix(k, "data/") {
			unread++
		}
	}
	t.Logf("the repository has %d objects; the restore fetched %d, leaving %d packs unread", len(want), len(got), unread)
	if unread == 0 || len(got) >= len(want) {
		t.Fatalf("the restore downloaded the whole repository: %d of %d objects", len(got), len(want))
	}
	if n := len(replicable(t, fresh, "photos")); n != 0 {
		t.Fatalf("a bucket nobody read holds %d objects", n)
	}
	logs, _ := os.ReadFile(fa.logf)
	if !strings.Contains(string(logs), "read-through: fetched") {
		t.Fatalf("no fetch logged:\n%s", tail(logs))
	}
}

// bucketCreds runs `bucket show` and reads the credential it prints.
func bucketCreds(t *testing.T, n *node, bucket string) creds {
	t.Helper()
	code, out, e := n.run("", "", "bucket", "show", bucket)
	if code != 0 {
		t.Fatalf("bucket show %s: %s", bucket, e)
	}
	a, s := reAccess.FindStringSubmatch(out), reSecret.FindStringSubmatch(out)
	if a == nil || s == nil {
		t.Fatalf("no credential in output:\n%s", out)
	}
	return creds{a[1], s[1]}
}
