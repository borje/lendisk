package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
)

// #96: `overflow set` changes a target's max_bytes, and its endpoint
// only after the credential is tested there; a failure changes
// nothing. Usage errors exit 2, an unknown target 1.
func TestOverflowSetChangesMaxBytesAndRetestsAnEndpoint(t *testing.T) {
	serving(t, 0) // the provider is versitygw, in this process
	srv := overflowtest.Start(t, "offsite", true)
	n := newNode(t)
	n.mustInit(t)
	n.env = overflowEnv(srv)
	if code, out, e := n.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", "offsite",
		"--max-bytes", "450G"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	for _, args := range [][]string{
		{"overflow", "set", "b2"},
		{"overflow", "set", "b2", "--max-bytes", "lots"},
		{"overflow", "set", "b2", "--endpoint", "s3.example.com"},
		{"overflow", "set", "b2", "--region", "eu-1"},
	} {
		if code, _, e := n.run("", "", args...); code != 2 {
			t.Errorf("%v: exit %d (%s)", args, code, e)
		}
	}
	if code, _, e := n.run("", "", "overflow", "set", "nope", "--max-bytes", "1G"); code != 1 || !strings.Contains(e, "no overflow target nope") {
		t.Fatalf("an unknown target: %d %s", code, e)
	}

	if code, out, e := n.run("", "", "overflow", "set", "b2", "--max-bytes", "100G"); code != 0 || !strings.Contains(out, "up to 100G") {
		t.Fatalf("set --max-bytes: %d %s%s", code, out, e)
	}
	if o := n.status().Capacity.Overflow[0]; o.MaxBytes != 100<<30 || o.Endpoint != srv.Endpoint {
		t.Fatalf("after set: %+v", o)
	}

	// An endpoint where the credential does not work: nothing changes.
	n.env = map[string]string{"LENDISK_OVERFLOW_B2_ACCESS_KEY": srv.Access, "LENDISK_OVERFLOW_B2_SECRET_KEY": "wrong-secret-value"}
	code, out, e := n.run("", "", "overflow", "set", "b2", "--endpoint", srv.Endpoint+"/", "--max-bytes", "1G")
	if code != 1 || !strings.Contains(e, "not changed") || strings.Contains(e, "wrong-secret-value") {
		t.Fatalf("set with a wrong credential: %d %s%s", code, out, e)
	}
	if o := n.status().Capacity.Overflow[0]; o.MaxBytes != 100<<30 {
		t.Fatalf("a failed set changed the target: %+v", o)
	}
	// The same bucket at another URL for it (localhost for 127.0.0.1):
	// tested, then written.
	n.env = overflowEnv(srv)
	moved := strings.Replace(srv.Endpoint, "127.0.0.1", "localhost", 1)
	code, out, e = n.run("", "", "overflow", "set", "b2", "--endpoint", moved, "--region", "us-east-1")
	if code != 0 || !strings.Contains(out, "credential: tested there; versioning enabled") {
		t.Fatalf("set --endpoint: %d %s%s", code, out, e)
	}
	if o := n.status().Capacity.Overflow[0]; o.Endpoint != moved || o.MaxBytes != 100<<30 || o.Versioning != "enabled" {
		t.Fatalf("after set --endpoint: %+v", o)
	}
}

// #96: `overflow remove` marks the target removing, which status shows,
// and says what happens next; --keep-data given again turns deleting
// off. An unknown target exits 1.
func TestOverflowRemoveSaysWhatHappensAndStatusShowsIt(t *testing.T) {
	serving(t, 0)
	srv := overflowtest.Start(t, "offsite", false)
	n := newNode(t)
	n.mustInit(t)
	n.env = overflowEnv(srv)
	if code, out, e := n.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", "offsite",
		"--max-bytes", "1G", "--retention", "7d"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	if code, _, e := n.run("", "", "overflow", "remove", "nope"); code != 1 || !strings.Contains(e, "no overflow target nope") {
		t.Fatalf("an unknown target: %d %s", code, e)
	}
	code, out, e := n.run("", "", "overflow", "remove", "b2")
	if code != 0 {
		t.Fatalf("overflow remove: %d %s%s", code, out, e)
	}
	for _, want := range []string{"is being removed: no new copy goes there", "copied to a friend or another target first",
		"held for its retention of 7d", "dropped once nothing is left there"} {
		if !strings.Contains(out, want) {
			t.Errorf("overflow remove does not say %q:\n%s", want, out)
		}
	}
	o := n.status().Capacity.Overflow[0]
	if o.Removing == "" || o.KeepData {
		t.Fatalf("status after remove: %+v", o)
	}
	if code, _, e := n.run("", "", "overflow", "set", "b2", "--max-bytes", "2G"); code != 1 || !strings.Contains(e, "being removed") {
		t.Fatalf("set while removing: %d %s", code, e)
	}
	code, out, _ = n.run("", "", "overflow", "remove", "b2", "--keep-data")
	if code != 0 || !strings.Contains(out, "being removed since") || !strings.Contains(out, "left there (--keep-data)") {
		t.Fatalf("remove --keep-data again: %d %s", code, out)
	}
	if o2 := n.status().Capacity.Overflow[0]; o2.Removing != o.Removing || !o2.KeepData {
		t.Fatalf("status after --keep-data: %+v", o2)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "being removed since") {
		t.Fatalf("status text does not say the target is being removed:\n%s", text)
	}
}

// #96, scenario 12 scaled down as TestScenarioOverflowDrainsBackToANewFriend:
// A backs up at two copies with one friend, B, too small for two copies,
// so every data object is at B and the target (with versioning). The
// user removes the target: nothing has room for its copies, so they
// stay, and the snapshot stays offsite. Then F joins: A's node copies
// the target's objects to F, deletes them at the target (delete
// markers), and drops the target, which status no longer lists; the
// snapshot is offsite at B and F, and a new backup goes there only.
func TestScenarioOverflowRemoveMovesItsCopiesThenDropsTheTarget(t *testing.T) {
	serving(t, 3) // the provider is versitygw, in this process; A, B and F
	srv := overflowtest.Start(t, "offsite", true)
	env := overflowEnv(srv)
	f := newCircleWith(t, 2, "16M", nil, append(envList(env), envReplaceEvery+"=2s")...)
	f.an.env = env
	if code, out, e := f.an.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", srv.Bucket,
		"--max-bytes", "60M"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	id := f.backup("one", 10)
	f.waitOffsite(id)
	f.drain()
	f.an.drained("b2", 90*time.Second)
	prefix := "lendisk/" + f.aID.SealedName("laptop") + "/"
	keys, err := srv.Keys(t.Context(), prefix)
	if err != nil || len(keys) == 0 {
		t.Fatalf("nothing at the target before the removal: %v", err)
	}

	if code, out, e := f.an.run("", "", "overflow", "remove", "b2"); code != 0 || !strings.Contains(out, "stay at the target") {
		t.Fatalf("overflow remove: %d %s%s", code, out, e)
	}
	// No room elsewhere: the copies stay, the backup offsite.
	time.Sleep(5 * time.Second) // two re-placement passes
	if k, err := srv.Keys(t.Context(), prefix); err != nil || len(k) != len(keys) {
		t.Fatalf("the target holds %d keys, held %d before (%v)", len(k), len(keys), err)
	}
	if st := f.an.status(); len(st.Capacity.Overflow) != 1 || st.Capacity.Overflow[0].Removing == "" {
		t.Fatalf("status while removing: %+v", st.Capacity.Overflow)
	}

	// F joins.
	fn := newNode(t)
	fn.regions = f.an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := fn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	fp := newProcNode(t, fn, envReplFast+"=1")
	fp.start()
	if code, _, e := fn.run("\n", "", "peer", "add", f.an.invite("--quota", "64M")); code != 0 {
		t.Fatalf("peer add: %s", e)
	}
	f.friends[fn.identity().NodeID().String()] = fp
	within(t, 3*time.Minute, "the target dropped", func() bool {
		return len(f.an.status().Capacity.Overflow) == 0
	})
	if keys, err := srv.Keys(t.Context(), prefix); err != nil || len(keys) != 0 {
		t.Fatalf("the target still holds %v (%v)", overflowtest.SortedKeys(keys), err)
	}
	f.waitOffsite(id)
	id2 := f.backup("two", 2)
	f.waitOffsite(id2)
	if keys, err := srv.Keys(t.Context(), prefix); err != nil || len(keys) != 0 {
		t.Fatalf("a backup after the removal went to the target: %v (%v)", overflowtest.SortedKeys(keys), err)
	}
	if b := bucketOf(t, f.an.status(), "laptop"); !b.Offsite.Complete || b.BelowCopies.Objects != 0 {
		t.Fatalf("after the removal: %+v", b)
	}
}
