package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
)

// overflowEnv is the credential of srv in target b2's default variables.
func overflowEnv(srv *overflowtest.Server) map[string]string {
	return map[string]string{"LENDISK_OVERFLOW_B2_ACCESS_KEY": srv.Access, "LENDISK_OVERFLOW_B2_SECRET_KEY": srv.Secret}
}

func envList(m map[string]string) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

// filesHoldingAny lists the files under root whose name or contents
// hold needle.
func filesHoldingAny(t *testing.T, root, needle string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if strings.Contains(p, needle) {
			out = append(out, p)
		}
		if d.Type().IsRegular() {
			if b, _ := os.ReadFile(p); strings.Contains(string(b), needle) {
				out = append(out, p)
			}
		}
		return nil
	})
	return out
}

// S3-1, S3-4, S3-5, SEC-3: `overflow add` reads the credential from the
// environment only and tests it before the target is accepted, reports
// versioning and version-removal permission, and offers the lifecycle
// rule; the credential is written nowhere and shown nowhere, and the
// daemon refuses to start while the target's variables are unset.
func TestOverflowAddTestsTheCredentialAndServeRefusesWithoutIt(t *testing.T) {
	serving(t, 0) // the provider is versitygw, in this process
	srv := overflowtest.Start(t, "offsite", true)
	n := newNode(t)
	n.mustInit(t)
	add := func(extra ...string) (int, string, string) {
		return n.run("", "", append([]string{"overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", "offsite",
			"--max-bytes", "450G", "--price-per-gb", "0.006"}, extra...)...)
	}
	// No credential in the environment: refused, naming the variables.
	code, out, errOut := add()
	if code != 1 || !strings.Contains(errOut, "LENDISK_OVERFLOW_B2_ACCESS_KEY and LENDISK_OVERFLOW_B2_SECRET_KEY not set") {
		t.Fatalf("without a credential: %d %s%s", code, out, errOut)
	}
	// A wrong one: tested and refused, nothing added.
	n.env = map[string]string{"LENDISK_OVERFLOW_B2_ACCESS_KEY": srv.Access, "LENDISK_OVERFLOW_B2_SECRET_KEY": "wrong-secret-value"}
	code, out, errOut = add()
	if code != 1 || !strings.Contains(errOut, "not added") || strings.Contains(errOut, "wrong-secret-value") {
		t.Fatalf("with a wrong credential: %d %s%s", code, out, errOut)
	}
	if len(n.status().Capacity.Overflow) != 0 {
		t.Fatal("a target with a wrong credential was added")
	}
	// Usage errors exit 2.
	for _, args := range [][]string{
		{"overflow", "add", "s3", "B2", "--endpoint", srv.Endpoint, "--bucket", "offsite", "--max-bytes", "1G"},
		{"overflow", "add", "s3", "b2", "--endpoint", "s3.example.com", "--bucket", "offsite", "--max-bytes", "1G"},
		{"overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", "offsite"},
		{"overflow", "add", "gcs", "b2"},
	} {
		if code, _, e := n.run("", "", args...); code != 2 {
			t.Errorf("%v: exit %d (%s)", args, code, e)
		}
	}

	n.env = overflowEnv(srv)
	code, out, errOut = add()
	if code != 0 {
		t.Fatalf("overflow add: %d %s%s", code, out, errOut)
	}
	for _, want := range []string{"Overflow target b2 added", "versioning enabled", "version removal allowed",
		"can remove object versions", "LENDISK_OVERFLOW_B2_SECRET_KEY"} {
		if !strings.Contains(out, want) {
			t.Errorf("overflow add does not say %q:\n%s", want, out)
		}
	}
	if code, _, e := add(); code != 1 || !strings.Contains(e, "exists already") {
		t.Fatalf("second add: %d %s", code, e)
	}
	st := n.status()
	if len(st.Capacity.Overflow) != 1 {
		t.Fatalf("capacity %+v", st.Capacity)
	}
	o := st.Capacity.Overflow[0]
	if o.Name != "b2" || o.MaxBytes != 450<<30 || o.Bucket != "offsite" || o.Prefix != "lendisk" || o.Versioning != "enabled" ||
		o.VersionRemoval != "allowed" || o.PricePerGB != 0.006 || o.EnvPrefix != "LENDISK_OVERFLOW_B2_" {
		t.Fatalf("overflow in status %+v", o)
	}
	if !placeListed(st, "b2", "overflow") {
		t.Fatalf("places %+v", st.Places)
	}
	_, text, _ := n.run("", "", "status")
	_, js, _ := n.run("", "", "status", "--json")
	if !strings.Contains(text, "b2: holds") {
		t.Errorf("status text does not show the target:\n%s", text)
	}
	for _, s := range []string{text, js} {
		if strings.Contains(s, srv.Secret) || strings.Contains(s, srv.Access) {
			t.Fatalf("status shows the credential:\n%s", s)
		}
	}
	// Never written: not in the config, the database or any file.
	root := filepath.Dir(filepath.Dir(n.config))
	for _, k := range []string{srv.Secret, srv.Access} {
		if got := filesHoldingAny(t, root, k); len(got) != 0 {
			t.Fatalf("the credential is in %v", got)
		}
	}

	// The daemon refuses to start without the variables.
	n.env = nil
	code, _, errOut = n.run("", "", "serve")
	if code != 1 || !strings.Contains(errOut, "overflow target b2") || !strings.Contains(errOut, "LENDISK_OVERFLOW_B2_ACCESS_KEY") {
		t.Fatalf("serve without the credential: %d %s", code, errOut)
	}
}

func placeListed(st Status, place, kind string) bool {
	for _, p := range st.Places {
		if p.Place == place && p.Kind == kind {
			return true
		}
	}
	return false
}

// copyTree copies the regular files under from into to, leaving out
// lendisk's housekeeping directory.
func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == gateway.HousekeepingDir {
			return filepath.SkipDir
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, _ := filepath.Rel(from, p)
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		os.MkdirAll(filepath.Join(to, filepath.Dir(rel)), 0o700)
		return os.WriteFile(filepath.Join(to, rel), b, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Scenario 12's first half (REPL-12, S3-1, S3-2, S3-3, S3-7, #38),
// scaled down: A backs up about 16 MB with restic at two copies to
// friends B at 40 MB and C at 6 MB, so once C is full only one friend
// has room and objects stay below copies. A adds an S3 bucket as an
// overflow target: re-placement puts the missing copies there, the
// bucket's metadata first; an alert says it is receiving copies, and
// the snapshot becomes offsite-complete with every data object at two
// places, one of them a friend. Copying the keys under the target's
// prefix and the friends' replicas into one directory gives a
// repository restic opens (S3-2 AC). The credential is in no file and
// no log.
//
// Not covered: scenario 12's friends of equal quotas fill together, so
// D6's choice leaves the objects written after they are full with one
// copy, at the target (DESIGN §5 Q-AD).
func TestScenarioOverflow(t *testing.T) {
	serving(t, 3) // the provider is versitygw, in this process; three daemons
	scenarioOverflow(t, overflowtest.Start(t, "offsite", true), "backups/lendisk")
}

// Scenario 12's first half against a provider running elsewhere (DESIGN
// §3.11's tiers): CI runs a Garage container and sets
// LENDISK_TEST_OVERFLOW_ENDPOINT, _REGION, _BUCKET, _ACCESS_KEY and
// _SECRET_KEY (.github/garage.sh); skipped without them.
func TestScenarioOverflowAtAnExternalProvider(t *testing.T) {
	ep := os.Getenv("LENDISK_TEST_OVERFLOW_ENDPOINT")
	if ep == "" {
		t.Skip("LENDISK_TEST_OVERFLOW_ENDPOINT not set (CI runs Garage: .github/garage.sh)")
	}
	parallel(t, 3)
	srv := overflowtest.Connect(t, ep, os.Getenv("LENDISK_TEST_OVERFLOW_REGION"), os.Getenv("LENDISK_TEST_OVERFLOW_BUCKET"),
		os.Getenv("LENDISK_TEST_OVERFLOW_ACCESS_KEY"), os.Getenv("LENDISK_TEST_OVERFLOW_SECRET_KEY"))
	scenarioOverflow(t, srv, fmt.Sprintf("ci/%d", time.Now().UnixNano()))
}

func scenarioOverflow(t *testing.T, srv *overflowtest.Server, root string) {
	env := overflowEnv(srv)
	f := newCircleAt(t, 3, "40M", envList(env)...)
	f.an.env = env
	// A and C agree 6 MB: C fills while B has room, and from then fewer
	// than two friends have room for a copy (D6 step 3).
	var small string
	for pid, pn := range f.friends {
		if small != "" {
			break
		}
		small = pid
		if code, out, e := f.an.run("", "", "peer", "quota", pid, "6M"); code != 0 {
			t.Fatalf("A's number: %s%s", out, e)
		}
		if code, out, e := pn.node.run("", "", "peer", "quota", f.aID.NodeID().String(), "6M"); code != 0 {
			t.Fatalf("C's number: %s%s", out, e)
		}
	}
	within(t, 60*time.Second, "6 MB between A and C", func() bool { return f.an.peerOf(small).Quota == 6<<20 })
	id := f.backup("one", 16)
	within(t, 90*time.Second, "objects below copies, for want of room", func() bool {
		return bucketOf(t, f.an.status(), "laptop").BelowCopies.Objects > 0
	})

	if code, out, e := f.an.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", srv.Bucket,
		"--prefix", root, "--region", srv.Region, "--max-bytes", "64M", "--price-per-gb", "0.006"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	f.waitOffsite(id)
	f.drain()
	f.an.drained("b2", 90*time.Second)
	st := f.an.status()
	b := bucketOf(t, st, "laptop")
	if !b.Offsite.Complete || b.BelowCopies.Objects != 0 {
		t.Fatalf("after overflow add: %+v", b)
	}
	receiving := false
	for _, a := range st.Alerts {
		receiving = receiving || a.Kind == alert.KindOverflowReceiving && a.Subject == "b2"
	}
	if !receiving {
		t.Fatalf("no overflow_receiving alert: %+v", st.Alerts)
	}
	o := st.Capacity.Overflow[0]
	if o.DataObjects == 0 || o.Use == 0 || o.MonthlyCost < 0 || o.Percent > 100 {
		t.Fatalf("overflow in status %+v", o)
	}

	// Every data object at two places, at least one a friend; the
	// metadata at every place.
	want := replicable(t, f.an, "laptop")
	prefix := root + "/" + f.aID.SealedName("laptop") + "/"
	keys, err := srv.Keys(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	at := map[string]int{}
	friendHolds := map[string]bool{}
	for _, pn := range f.friends {
		for k, h := range lentObjects(t, pn.node, f.aID, "laptop") {
			if want[k] == h {
				at[k]++
				friendHolds[k] = true
			}
		}
	}
	for k := range keys {
		at[strings.TrimPrefix(k, prefix)]++
	}
	for k := range want {
		meta := !strings.HasPrefix(k, "data/")
		switch {
		case meta && at[k] != 3:
			t.Errorf("metadata %s at %d places, want 3", k, at[k])
		case !meta && (at[k] != 2 || !friendHolds[k]):
			t.Errorf("data %s at %d places (a friend: %t), want 2 with a friend", k, at[k], friendHolds[k])
		}
	}
	// The target's copies carry their SHA-256 (REPL-10, S3-3).
	for k := range keys {
		if strings.HasPrefix(k, prefix+"data/") {
			_, sha, err := srv.Get(t.Context(), k)
			if err != nil || sha != want[strings.TrimPrefix(k, prefix)] {
				t.Fatalf("%s: sha %q, want %q (%v)", k, sha, want[strings.TrimPrefix(k, prefix)], err)
			}
			break
		}
	}

	// S3-2 AC: the union of the replicas is a repository restic opens.
	union := t.TempDir()
	for _, pn := range f.friends {
		copyTree(t, filepath.Join(pn.node.data, "store", gateway.LentName(f.aID.NodeID(), f.aID.SealedName("laptop"))), union)
	}
	if err := srv.Download(t.Context(), prefix, func(rel string, b []byte) error {
		os.MkdirAll(filepath.Join(union, filepath.Dir(rel)), 0o700)
		return os.WriteFile(filepath.Join(union, rel), b, 0o600)
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "restic", "-r", union, "--no-cache", "check", "--read-data")
	cmd.Env = append(os.Environ(), "RESTIC_PASSWORD=test-password")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restic check of the union: %v\n%s", err, out)
	}
	cmd = exec.CommandContext(ctx, "restic", "-r", union, "--no-cache", "snapshots")
	cmd.Env = append(os.Environ(), "RESTIC_PASSWORD=test-password")
	if out, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(out), id) {
		t.Fatalf("restic snapshots of the union: %v\n%s", err, out)
	}

	// SEC-3: the credential is in no file of any node and in no log.
	for _, k := range []string{srv.Secret, srv.Access} {
		for _, n := range []*node{f.an} {
			if got := filesHoldingAny(t, filepath.Dir(filepath.Dir(n.config)), k); len(got) != 0 {
				t.Fatalf("the credential is in %v", got)
			}
		}
		if b, _ := os.ReadFile(f.a.logf); strings.Contains(string(b), k) {
			t.Fatal("the credential is in the daemon's log")
		}
	}
}
