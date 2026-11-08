package cli

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
)

// EP-7, EP-11, LEND-4, RET-1 (#74): restic rewrites its config in
// place when `restic migrate upgrade_repo_v2` upgrades a version 1
// repository. The restic profile names config rewritable, so the new
// config replicates to friend B like a new write: B holds exactly A's
// objects afterwards, keeps the old config as a tombstone, and A shows
// no refusal and no alert. The upgraded repository on B's disk is a
// version 2 repository restic checks.
func TestResticMigrateReplicatesTheNewConfig(t *testing.T) {
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c := an.addBucket(t, "laptop", "restic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	aID := an.identity()
	bKey := bn.identity().NodeID().String()

	src := t.TempDir()
	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
		"RESTIC_REPOSITORY=s3:http://"+a.addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(restic, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	run("init", "--repository-version", "1")
	randomFiles(t, src, "one", 3, 256<<10)
	run("backup", "--no-scan", src)
	an.drained(bKey, 60*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
	before := replicable(t, an, "laptop")["config"]

	t.Logf("restic migrate:\n%s", run("migrate", "upgrade_repo_v2"))
	after := replicable(t, an, "laptop")["config"]
	if before == "" || before == after {
		t.Fatalf("migrate did not rewrite config (%q, %q)", before, after)
	}
	p := an.drained(bKey, 60*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
	st := an.status()
	for _, al := range st.Alerts {
		if al.Kind == alert.KindPlaceRefused || al.Kind == alert.KindKeyChanged {
			t.Errorf("alert %s %s: %s", al.Kind, al.Subject, al.Message)
		}
	}
	// EP-11 exempts the rewritable config (#22): no changed key.
	if b := bucketOf(t, st, "laptop"); len(b.Changed) != 0 || b.Offsite.Stuck.Immutable != 0 {
		t.Errorf("changed keys %+v, stuck %+v", b.Changed, b.Offsite.Stuck)
	}
	if p.Refused != 0 {
		t.Errorf("place after the migration: %+v", p)
	}
	ts, err := openDB(t, bn).TombstonesOf(t.Context(), aID.NodeID().String(), aID.SealedName("laptop"), "config")
	if err != nil || len(ts) != 1 || ts[0].SHA256 != before || !ts[0].Rewrite {
		t.Fatalf("B's tombstones of config: %+v %v", ts, err)
	}
	// One more backup on the upgraded repository replicates too.
	randomFiles(t, src, "two", 2, 256<<10)
	run("backup", "--no-scan", src)
	an.drained(bKey, 60*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
	if v := run("cat", "config"); !strings.Contains(v, `"version": 2`) {
		t.Fatalf("config after the migration: %s", v)
	}
}
