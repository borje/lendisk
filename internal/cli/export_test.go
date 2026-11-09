package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/sheet"
)

// engineAlone runs the storage engine alone on a store directory, as
// docs/restore-without-lendisk.md has the user start it:
//
//	versitygw --access restore --secret … --port 127.0.0.1:7070 --readonly \
//	  posix --exclude-dirs .lendisk <store>
//
// It is versitygw 1.10.0's own gateway and posix backend, embedded
// (the caller holds serving: one versitygw per process) with the
// binary's defaults, plus AbsolutePaths, so posix does not change the
// test process's directory. No lendisk code is in the path. stop ends it.
func engineAlone(t *testing.T, store string) (addr string, c creds, stop func()) {
	t.Helper()
	be, err := posix.New(store, meta.XattrMeta{}, posix.PosixOpts{AbsolutePaths: true, ExcludeDirs: []string{".lendisk"}})
	if err != nil {
		t.Fatal(err)
	}
	c = creds{"restore", "restore-secret-0123456789"}
	addr = freeAddr(t, "127.0.0.1")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- embedgw.RunVersityGW(ctx, be, &embedgw.Config{RootUserAccess: c.access, RootUserSecret: c.secret,
			Ports: []string{addr}, MaxConnections: 250000, MaxRequests: 100000, MultipartMaxParts: 10000,
			Readonly: true, Quiet: true})
	}()
	stopped := false
	stop = func() {
		if !stopped {
			stopped = true
			cancel()
			if err := <-errc; err != nil {
				t.Errorf("versitygw: %v", err)
			}
		}
	}
	t.Cleanup(stop)
	for deadline := time.Now().Add(30 * time.Second); ; {
		if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			conn.Close()
			return addr, c, stop
		}
		select {
		case err := <-errc:
			stopped = true
			t.Fatalf("versitygw ended: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("versitygw not listening after 30s")
		}
	}
}

// printedSheet is what a node's recovery sheet says a lendisk-free
// restore of one bucket takes, read from the printed page: the bucket's
// name at every friend, the export command and the commands that
// combine the replicas and open the result.
type printedSheet struct {
	stored  string
	export  string // with <stored name> and <directory>
	combine string // sheet.CombineCp as printed
	text    string
}

// readSheet prints n's recovery sheet and reads bucket's steps from it.
func readSheet(t *testing.T, n *node, bucket string) printedSheet {
	t.Helper()
	code, text, errOut := n.run("", "", "recovery-sheet")
	if code != 0 {
		t.Fatalf("recovery-sheet: %s", errOut)
	}
	ps := printedSheet{text: text}
	lines := strings.Split(text, "\n")
	inBucket, inCombine := false, false
	var combine []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(l, "   "+bucket+" ("):
			inBucket = true
		case inBucket && strings.HasPrefix(trimmed, "at every friend: "):
			ps.stored, inBucket = strings.TrimPrefix(trimmed, "at every friend: "), false
		case strings.HasPrefix(trimmed, "lendisk export "):
			ps.export = trimmed
		case ps.export != "" && combine == nil && l == "     mkdir -p repo":
			inCombine = true
		case inCombine && !strings.HasPrefix(l, "     "):
			inCombine = false
		}
		if inCombine {
			combine = append(combine, strings.TrimPrefix(l, "     "))
		}
	}
	ps.combine = strings.Join(combine, "\n")
	if ps.stored == "" || ps.export == "" || ps.combine == "" {
		t.Fatalf("the sheet lacks a step for %s (stored %q, export %q, combine %q):\n%s", bucket, ps.stored, ps.export, ps.combine, text)
	}
	return ps
}

// exportAs runs, on lender, the sheet's export command for the stored
// name into dir, with --kopia-layout for a Kopia bucket as the sheet
// says.
func (ps printedSheet) exportAs(t *testing.T, lender *node, dir string, kopia bool) {
	t.Helper()
	if ps.export != sheet.ExportCommand {
		t.Fatalf("the sheet prints %q", ps.export)
	}
	line := strings.NewReplacer("<stored name>", ps.stored, "<directory>", dir).Replace(ps.export)
	args := strings.Fields(line)[1:] // without "lendisk"
	if kopia {
		if !strings.Contains(ps.text, "adding --kopia-layout for a Kopia") {
			t.Fatal("the sheet does not say to add --kopia-layout")
		}
		args = append(args, "--kopia-layout")
	}
	code, out, errOut := lender.run("", "", args...)
	if code != 0 || !strings.HasPrefix(out, "Exported ") {
		t.Fatalf("%s: exit %d\n%s%s", line, code, out, errOut)
	}
}

// combineIn runs the sheet's combine command in dir, which holds
// replica-1, replica-2…; the result is dir/repo.
func (ps printedSheet) combineIn(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("sh", "-ec", ps.combine)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("combine: %v\n%s", err, out)
	}
	return filepath.Join(dir, "repo")
}

// resticOn runs restic with password and a cache of the test's, failing
// the test unless it succeeds.
func resticOn(t *testing.T, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command("restic", args...)
	cmd.Env = append(append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir()), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// LEND-6 (#35): export refuses what is not a lent bucket's name and an
// unknown one, says what it needs, and warns when a Kopia bucket is
// exported in restic's layout.
func TestExportRefusesWhatIsNoLentBucket(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "laptop", "restic")
	dir := filepath.Join(t.TempDir(), "out")
	if code, _, e := n.run("", "", "export", dir); code != 2 || !strings.Contains(e, "--lent") {
		t.Fatalf("no --lent: exit %d %s", code, e)
	}
	if code, _, e := n.run("", "", "export", "--lent", "laptop", dir); code != 2 || !strings.Contains(e, "lent-<id16>-<sealed>") {
		t.Fatalf("own bucket: exit %d %s", code, e)
	}
	p, _ := identity.NewPhrase()
	friend, _ := identity.FromPhrase(p)
	stored := gateway.LentName(friend.NodeID(), friend.SealedName("vault"))
	if code, _, e := n.run("", "", "export", "--lent", stored, dir); code != 1 || !strings.Contains(e, "holds no lent bucket") {
		t.Fatalf("unknown bucket: exit %d %s", code, e)
	}
	// A Kopia bucket, as a lender's store holds one.
	if err := os.MkdirAll(filepath.Join(n.data, "store", stored), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(n.data, "store", stored, "kopia.repository"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, e := n.run("", "", "export", "--lent", stored, dir)
	if code != 0 || !strings.Contains(out, "Exported 1 objects") || !strings.Contains(e, "--kopia-layout") {
		t.Fatalf("Kopia bucket in restic's layout: exit %d\n%s%s", code, out, e)
	}
}

// Scenario 7, zero lock-in (P-5, LEND-6, EP-15, COMPAT-4; #35), on
// scenario 1's four friends: A's restic bucket at two copies, offsite.
// Then lendisk is stopped everywhere and D's store is removed, so D is
// never consulted.
//
//   - On A, the storage engine alone (versitygw, as the user guide starts
//     it) serves A's store, and `restic -r s3:…` lists A's snapshot from
//     A's own copy.
//   - The same on B and on C: the keys of A's replica, under the name A's
//     recovery sheet gives at every friend, are copied with rclone into
//     replica-1 and replica-2, combined with the sheet's command, and
//     `restic -r repo check --read-data` passes.
//   - Then the recovery sheet's printed steps, end to end, with nothing
//     running: B and C each run the sheet's `lendisk export` command,
//     the sheet's combine command unites the exports, `restic -r repo
//     check --read-data` passes and the sheet's restore line restores
//     the backup byte for byte.
func TestScenarioZeroLockIn(t *testing.T) {
	serving(t, 4)
	rclone, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not installed")
	}
	f := newFourFriends(t)
	id := f.backup("one", 8)
	f.waitOffsite(id)
	f.drain()
	want := replicable(t, f.an, "laptop")
	ps := readSheet(t, f.an, "laptop")
	if ps.stored != gateway.LentName(f.aID.NodeID(), f.aID.SealedName("laptop")) {
		t.Fatalf("the sheet names the bucket %s at every friend", ps.stored)
	}
	if !strings.Contains(strings.Join(strings.Fields(ps.text), " "), "the replicas of any 2 of the 3 places, combined") {
		t.Fatalf("the sheet does not say N - c + 1 = 2:\n%s", ps.text)
	}

	// lendisk stopped everywhere; D gone.
	f.a.p.kill()
	var friends []*node
	for _, pn := range f.friends {
		pn.p.kill()
		friends = append(friends, pn.node)
	}
	b, c, d := friends[0], friends[1], friends[2]
	if err := os.RemoveAll(filepath.Join(d.data, "store")); err != nil {
		t.Fatal(err)
	}
	if len(lentObjects(t, b, f.aID, "laptop")) == len(want) && len(lentObjects(t, c, f.aID, "laptop")) == len(want) {
		t.Log("B and C each hold every object: the union is not tested beyond one replica")
	}

	// A's own copy through the engine alone.
	addr, root, stop := engineAlone(t, filepath.Join(f.an.data, "store"))
	snaps := resticOn(t, []string{"AWS_ACCESS_KEY_ID=" + root.access, "AWS_SECRET_ACCESS_KEY=" + root.secret},
		"-r", "s3:http://"+addr+"/laptop", "--no-lock", "snapshots")
	if !strings.Contains(snaps, id) {
		t.Fatalf("A's snapshot %s not listed:\n%s", id, snaps)
	}
	stop()

	// B's and C's replicas through the engine alone, copied with rclone.
	viaS3 := t.TempDir()
	for i, lender := range []*node{b, c} {
		addr, root, stop := engineAlone(t, filepath.Join(lender.data, "store"))
		rcloneRun(t, rclone, 0, "copy", rcloneRemote(addr, root)+ps.stored, filepath.Join(viaS3, fmt.Sprintf("replica-%d", i+1)))
		stop()
	}
	repo := ps.combineIn(t, viaS3)
	sameObjects(t, want, storeObjects(t, repo))
	resticOn(t, nil, "-r", repo, "check", "--read-data")

	// The recovery sheet's steps: export, combine, check, restore.
	exported := t.TempDir()
	for i, lender := range []*node{b, c} {
		ps.exportAs(t, lender, filepath.Join(exported, fmt.Sprintf("replica-%d", i+1)), false)
	}
	repo = ps.combineIn(t, exported)
	sameObjects(t, want, storeObjects(t, repo))
	resticOn(t, nil, "-r", repo, "check", "--read-data")
	target := t.TempDir()
	if !strings.Contains(ps.text, "restic -r repo restore latest --target <directory>") {
		t.Fatal("the sheet has no restore line")
	}
	cmd := exec.Command("sh", "-ec", "restic -r repo restore latest --target "+target)
	cmd.Dir = exported
	cmd.Env = append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restore: %v\n%s", err, out)
	}
	sameFiles(t, f.src, filepath.Join(target, f.src))
}

// LEND-6, P-5 (#35): exports from several lenders, copied into one
// directory, form a repository Kopia opens. A's Kopia bucket at one copy
// over B and C, so each holds only part of the data and both replicas
// are needed (N - c + 1 = 2). With lendisk stopped, the storage engine
// alone serves A's own copy to Kopia connected read-only (scenario 7 for
// Kopia); B and C run the
// recovery sheet's export command with --kopia-layout, the sheet's
// combine command unites them, and Kopia connects to the result as a
// filesystem repository, verifies every file and restores byte for byte.
func TestKopiaExportsFromTwoLendersFormARepository(t *testing.T) {
	serving(t, 3)
	k := newKopia(t)
	if testing.Short() {
		t.Skip("long")
	}
	an, bn, cn := newNode(t), newNode(t), newNode(t)
	bn.regions, cn.regions = an.regions, an.regions
	for _, n := range []*node{an, bn, cn} {
		p, _ := identity.NewPhrase()
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	code, out, errOut := an.run("", "", "bucket", "add", "vault", "--profile", "kopia", "--copies", "1")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	fast := envReplFast + "=1"
	a, b, c := newProcNode(t, an, fast), newProcNode(t, bn, fast), newProcNode(t, cn, fast)
	b.start()
	c.start()
	a.start()
	befriend(t, bn, an)
	befriend(t, cn, an)
	k.run(kopiaLines(t, out, a.addr)...)
	src := t.TempDir()
	for i := range 4 { // a pack each, placed one at B, the next at C
		randomFiles(t, src, fmt.Sprintf("s%d", i), 2, 1<<20)
		k.run("snapshot", "create", src)
	}
	aID := an.identity()
	for _, n := range []*node{bn, cn} {
		an.drained(n.identity().NodeID().String(), 90*time.Second)
	}
	want := kopiaReplicable(t, an, "vault")
	bHas, cHas := lentObjects(t, bn, aID, "vault"), lentObjects(t, cn, aID, "vault")
	if len(bHas) == len(want) || len(cHas) == len(want) {
		t.Fatalf("one friend holds every object (%d, %d of %d): the test shows no union", len(bHas), len(cHas), len(want))
	}
	ps := readSheet(t, an, "vault")
	for _, pn := range []*procNode{a, b, c} {
		pn.p.kill()
	}

	// A's own copy through the engine alone, connected read-only as the
	// user guide says.
	addr, root, stop := engineAlone(t, filepath.Join(an.data, "store"))
	k1 := newKopia(t)
	k1.run("repository", "connect", "s3", "--endpoint="+addr, "--disable-tls", "--bucket=vault",
		"--access-key="+root.access, "--secret-access-key="+root.secret, "--readonly")
	if list := k1.run("snapshot", "list", src); strings.Count(strings.TrimSpace(list), "\n") < 4 {
		t.Fatalf("A's four snapshots not listed:\n%s", list)
	}
	stop()

	dir := t.TempDir()
	for i, lender := range []*node{bn, cn} {
		ps.exportAs(t, lender, filepath.Join(dir, fmt.Sprintf("replica-%d", i+1)), true)
	}
	repo := ps.combineIn(t, dir)
	if !strings.Contains(ps.text, "kopia repository connect filesystem --path repo") {
		t.Fatal("the sheet has no Kopia line")
	}
	k2 := newKopia(t)
	k2.run("repository", "connect", "filesystem", "--path", repo)
	k2.run("snapshot", "verify", "--verify-files-percent=100")
	dst := t.TempDir()
	k2.run("snapshot", "restore", src, dst)
	sameFiles(t, src, dst)
}
