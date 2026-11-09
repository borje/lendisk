package cli

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// threeFriends is A, with a bucket, and its friends B and C, each a
// daemon process on one relay: A's bucket at two copies is at both.
type threeFriends struct {
	t          *testing.T
	phrase     string
	an, bn, cn *node
	a, b, c    *procNode
	cr         creds
	shown      string // bucket add's output
}

func newThreeFriends(t *testing.T, bucket, profile string) *threeFriends {
	t.Helper()
	f := &threeFriends{t: t}
	f.phrase, _ = identity.NewPhrase()
	f.an, f.bn, f.cn = newNode(t), newNode(t), newNode(t)
	for _, n := range []*node{f.bn, f.cn} {
		n.regions = f.an.regions // one relay for all
	}
	for n, p := range map[*node]string{f.an: f.phrase, f.bn: "", f.cn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	code, out, errOut := f.an.run("", "", "bucket", "add", bucket, "--profile", profile)
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	f.shown = out
	f.cr = bucketCreds(t, f.an, bucket)
	fast := envReplFast + "=1"
	f.a, f.b, f.c = newProcNode(t, f.an, fast), newProcNode(t, f.bn, fast), newProcNode(t, f.cn, fast)
	f.b.start()
	f.c.start()
	f.a.start()
	befriend(t, f.bn, f.an)
	befriend(t, f.cn, f.an)
	return f
}

func (f *threeFriends) drain() {
	f.t.Helper()
	for _, n := range []*node{f.bn, f.cn} {
		if p := f.an.drained(n.identity().NodeID().String(), 90*time.Second); p.Refused != 0 {
			f.t.Fatalf("place %+v", p)
		}
	}
}

// manifests waits until B and C hold A's manifest naming them both and
// its bucket, so a recovery from B restores both friendships.
func (f *threeFriends) manifests() {
	f.t.Helper()
	aID := f.an.identity()
	deadline := time.Now().Add(60 * time.Second)
	for {
		ok := true
		for _, n := range []*node{f.bn, f.cn} {
			m, err := heldManifest(f.t, n, aID)
			if err != nil || len(m.Friends) != 2 || len(m.Buckets) != 1 {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			m, err := heldManifest(f.t, f.bn, aID)
			f.t.Fatalf("the manifest at B: %+v %v", m, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// wipeAndRecover kills A, removes every file of it, and recovers it on a
// fresh machine from B's address, whose daemon it starts.
func (f *threeFriends) wipeAndRecover() (*node, *procNode) {
	f.t.Helper()
	bAddr := f.bn.status().Node.Address
	f.a.p.kill()
	os.RemoveAll(filepath.Dir(filepath.Dir(f.an.config)))
	fresh := newNode(f.t)
	fresh.regions = f.an.regions
	fresh.dialTimeout = 15 * time.Second
	if code, out, errOut := fresh.run("y\n"+f.phrase+"\n", "", "recover", bAddr); code != 0 {
		f.t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	fa := newProcNode(f.t, fresh, envReplFast+"=1")
	fa.start()
	return fresh, fa
}

// tombstonedAt counts the keys of A's bucket lender holds tombstoned.
func tombstonedAt(t *testing.T, lender *node, borrower *identity.Identity, bucket string) int {
	t.Helper()
	var n int
	if err := openDB(t, lender).QueryRowContext(t.Context(), `SELECT count(DISTINCT key) FROM tombstone WHERE peer_key = ? AND sealed = ?`,
		borrower.NodeID().String(), borrower.SealedName(bucket)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Scenario 4, ransomware (RET-1, RET-3, ID-5, ID-6, EP-10; #32): an
// attacker on A runs `restic forget --prune` of every snapshot, which
// A replicates as deletes, and wipes A's disk. B and C keep every
// deleted object as a tombstone. A recovers from B and starts its
// daemon; restic sees no snapshot. `lendisk undelete laptop` shows from
// the friends' lists what would come back, the two snapshots among it,
// and asks: declined, nothing changes; accepted, B and C bring every
// object back. `restic snapshots` lists both snapshots again, `restic
// check --read-data` passes reading every pack through the read-through,
// the latest snapshot restores exactly, and the bucket is offsite at
// two copies again without a byte sent.
func TestScenarioRansomwareIsUndone(t *testing.T) {
	parallel(t, 3)
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	f := newThreeFriends(t, "laptop", "restic")
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
	snapshots := func(env []string) []string {
		t.Helper()
		var ss []struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(runRestic(env, "snapshots", "--json", "--no-lock")), &ss); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, s := range ss {
			ids = append(ids, s.ID)
		}
		return ids
	}
	env := resticEnv(f.a.addr, f.cr)
	runRestic(env, "init")
	randomFiles(t, src, "one", 3, 256<<10)
	runRestic(env, "backup", "--no-scan", src)
	randomFiles(t, src, "two", 3, 256<<10)
	runRestic(env, "backup", "--no-scan", src)
	before := snapshots(env)
	if len(before) != 2 {
		t.Fatalf("snapshots %v", before)
	}
	f.drain()
	f.manifests()

	// The attacker forgets every snapshot and prunes; A replicates the
	// deletes, which B and C keep as tombstones.
	runRestic(env, append([]string{"forget", "--prune"}, before...)...)
	if ids := snapshots(env); len(ids) != 0 {
		t.Fatalf("after the prune: %v", ids)
	}
	f.drain()
	aID := f.an.identity()
	for _, n := range []*node{f.bn, f.cn} {
		if got := tombstonedAt(t, n, aID, "laptop"); got < 4 {
			t.Fatalf("%d keys tombstoned at a friend", got)
		}
	}
	fresh, fa := f.wipeAndRecover()
	cr := bucketCreds(t, fresh, "laptop")
	env = resticEnv(fa.addr, cr)
	if ids := snapshots(env); len(ids) != 0 {
		t.Fatalf("recovered before undelete: %v", ids)
	}

	// Declined: nothing comes back.
	code, out, errOut := fresh.run("n\n", "", "undelete", "laptop")
	if code != 1 || !strings.Contains(out, "2 snapshots") || !strings.Contains(errOut, "not confirmed") {
		t.Fatalf("declined undelete: %d\n%s\n%s", code, out, errOut)
	}
	if ids := snapshots(env); len(ids) != 0 {
		t.Fatalf("after a declined undelete: %v", ids)
	}
	for _, n := range []*node{f.bn, f.cn} {
		if got := tombstonedAt(t, n, aID, "laptop"); got < 4 {
			t.Fatalf("a declined undelete restored at a friend: %d tombstoned", got)
		}
	}
	// Bad input: a usage error, nothing asked.
	if code, _, errOut := fresh.run("", "", "undelete", "laptop", "--since", "last tuesday"); code != 2 {
		t.Fatalf("a bad --since: %d %s", code, errOut)
	}
	if code, _, errOut := fresh.run("", "", "undelete", "nope"); code != 1 || !strings.Contains(errOut, "no bucket") {
		t.Fatalf("an unknown bucket: %d %s", code, errOut)
	}

	code, out, errOut = fresh.run("y\n", "", "undelete", "laptop", "--since", "1h")
	t.Logf("undelete:\n%s%s", out, errOut)
	if code != 0 || !strings.Contains(out, "Done.") {
		t.Fatalf("undelete: %d\n%s\n%s", code, out, errOut)
	}
	for _, n := range []*node{f.bn, f.cn} {
		if got := tombstonedAt(t, n, aID, "laptop"); got != 0 {
			t.Fatalf("%d keys still tombstoned at a friend", got)
		}
	}
	if ids := snapshots(env); len(ids) != 2 || !(ids[0] == before[0] && ids[1] == before[1]) {
		t.Fatalf("after undelete: %v, before the attack %v", ids, before)
	}
	runRestic(env, "check", "--read-data")
	dst := t.TempDir()
	runRestic(env, "restore", "latest", "--target", dst)
	sameFiles(t, src, filepath.Join(dst, src))
	logs, _ := os.ReadFile(fa.logf)
	if !strings.Contains(string(logs), "read-through: fetched") {
		t.Fatalf("no fetch logged:\n%s", tail(logs))
	}
	// REPL-11: the restored objects count as offsite at both friends.
	if code, out, errOut := fresh.run("", "", "status", "--wait", "laptop", "--timeout", "2m"); code != 0 ||
		!strings.Contains(out, "offsite at 2 copies") {
		t.Fatalf("status --wait: %d\n%s%s", code, out, errOut)
	}
}

// Scenario 9, Kopia (EP-7, REPL-2, RET-1, RET-3, ID-6; #32): a Kopia
// bucket offsite at two copies, at B and C. Kopia's full maintenance,
// after the first snapshot is deleted, deletes packs and indexes; each
// friend holding one keeps it as a tombstone. A's machine is lost; a
// fresh one recovers from B, and a file of the latest snapshot restores
// through the read-through. Undelete then brings back every blob
// maintenance deleted at both friends, and the repository still
// verifies and restores.
func TestScenarioKopiaRestoresAfterMaintenanceAndRecovery(t *testing.T) {
	parallel(t, 3)
	k := newKopia(t)
	if testing.Short() {
		t.Skip("long")
	}
	f := newThreeFriends(t, "photos", "kopia")
	k.run(kopiaLines(t, f.shown, f.a.addr)...)
	src := t.TempDir()
	randomFiles(t, src, "one", 4, 256<<10)
	k.run("snapshot", "create", src)
	for i := 1; i < 4; i++ {
		os.Remove(filepath.Join(src, "one-0"+string(rune('0'+i))))
	}
	randomFiles(t, src, "two", 2, 256<<10)
	k.run("snapshot", "create", src)
	var snaps []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(k.run("snapshot", "list", src, "--json")), &snaps); err != nil || len(snaps) != 2 {
		t.Fatalf("snapshots %+v %v", snaps, err)
	}
	f.drain()
	if code, out, errOut := f.an.run("", "", "status", "--wait", "photos", "--timeout", "2m"); code != 0 ||
		!strings.Contains(out, "offsite at 2 copies") {
		t.Fatalf("status --wait: %d\n%s%s", code, out, errOut)
	}
	aID := f.an.identity()
	held := lentObjects(t, f.bn, aID, "photos")
	k.run("snapshot", "delete", snaps[0].ID, "--delete")
	k.run("maintenance", "run", "--full", "--safety=none")
	f.drain()
	local := kopiaReplicable(t, f.an, "photos")
	var deleted, packs int
	for key := range held {
		if _, ok := local[key]; ok {
			continue
		}
		deleted++
		if strings.HasPrefix(key, "p") {
			packs++
		}
	}
	if packs == 0 {
		t.Fatal("maintenance deleted no pack the friends held")
	}
	for _, n := range []*node{f.bn, f.cn} {
		if got := tombstonedAt(t, n, aID, "photos"); got < deleted {
			t.Fatalf("a friend keeps %d of the %d blobs maintenance deleted", got, deleted)
		}
	}
	f.manifests()
	fresh, fa := f.wipeAndRecover()
	_, shown, _ := fresh.run("", "", "bucket", "show", "photos")
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

	// Undelete brings back what maintenance deleted at both friends (the
	// versions kopia.maintenance's rewrites replaced stay tombs: not
	// asked for); Kopia still verifies and restores.
	code, out, errOut := fresh.run("", "", "undelete", "photos", "--yes")
	t.Logf("undelete:\n%s%s", out, errOut)
	if code != 0 {
		t.Fatalf("undelete: %d", code)
	}
	for _, n := range []*node{f.bn, f.cn} {
		if got := deletedAt(t, n, aID, "photos"); got != 0 {
			t.Fatalf("%d deleted keys still tombstoned at a friend", got)
		}
	}
	k2.run("snapshot", "verify")
	dst2 := t.TempDir()
	k2.run("snapshot", "restore", src, dst2)
	sameFiles(t, src, dst2)
}

// deletedAt counts the keys of borrower's bucket lender holds as a
// delete's tombstone, not a rewrite's.
func deletedAt(t *testing.T, lender *node, borrower *identity.Identity, bucket string) int {
	t.Helper()
	var n int
	if err := openDB(t, lender).QueryRowContext(t.Context(), `SELECT count(DISTINCT key) FROM tombstone
		WHERE peer_key = ? AND sealed = ? AND coalesce(rewrite, 0) = 0`,
		borrower.NodeID().String(), borrower.SealedName(bucket)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
