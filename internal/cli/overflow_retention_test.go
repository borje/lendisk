package cli

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/overflow/overflowtest"
)

// S3-4, S3-6, RET-3, VER-2 (#39), with versitygw in this process as the
// provider: versioning on (deletes are delete markers, undelete copies
// the version before each back) and off (deletes are held as
// client-side tombstones for the target's window).
func TestScenarioOverflowRetentionVersioned(t *testing.T) {
	serving(t, 2)
	scenarioOverflowRetention(t, overflowtest.Start(t, "offsite", true), "backups/lendisk", true, 8*time.Second)
}

func TestScenarioOverflowRetentionUnversioned(t *testing.T) {
	serving(t, 2)
	scenarioOverflowRetention(t, overflowtest.Start(t, "offsite", false), "backups/lendisk", false, 8*time.Second)
}

// The same against providers running elsewhere (DESIGN §3.11's tiers):
// CI runs RustFS with a versioned bucket (.github/rustfs.sh, the
// LENDISK_TEST_OVERFLOW_VERSIONED_* variables) and Garage without
// versioning (.github/garage.sh, LENDISK_TEST_OVERFLOW_*); each test
// skips without its variables. Their window is longer: a prune and the
// drains after it take several seconds on a CI runner, and the held
// deletes must not come due before the test has looked (#40).
func TestScenarioOverflowRetentionAtAVersionedProvider(t *testing.T) {
	srv := providerFromEnv(t, "LENDISK_TEST_OVERFLOW_VERSIONED_", ".github/rustfs.sh")
	scenarioOverflowRetention(t, srv, fmt.Sprintf("ci/%d", time.Now().UnixNano()), true, providerWindow)
}

func TestScenarioOverflowRetentionAtAnUnversionedProvider(t *testing.T) {
	srv := providerFromEnv(t, "LENDISK_TEST_OVERFLOW_", ".github/garage.sh")
	scenarioOverflowRetention(t, srv, fmt.Sprintf("ci/%d", time.Now().UnixNano()), false, providerWindow)
}

// providerWindow is the retention window of the tests against providers
// running elsewhere.
const providerWindow = 30 * time.Second

// providerFromEnv connects to the provider the variables with prefix
// name, or skips.
func providerFromEnv(t *testing.T, prefix, script string) *overflowtest.Server {
	t.Helper()
	ep := os.Getenv(prefix + "ENDPOINT")
	if ep == "" {
		t.Skipf("%sENDPOINT not set (CI runs %s)", prefix, script)
	}
	parallel(t, 2)
	return overflowtest.Connect(t, ep, os.Getenv(prefix+"REGION"), os.Getenv(prefix+"BUCKET"),
		os.Getenv(prefix+"ACCESS_KEY"), os.Getenv(prefix+"SECRET_KEY"))
}

// scenarioOverflowRetention: A backs up with restic at two copies with
// one friend, B, and an overflow target, so every data object is at B
// and the target (D6 step 3). The target is checked by its stored
// checksums and in full by download. An attacker's `restic forget
// --prune` deletes everything: at a versioned target the keys are gone
// (delete markers); at an unversioned one every key stays, the deletes
// held, and status says "retention: client-side". `lendisk undelete`
// brings the snapshot back at B and the target, and restic opens it.
// At the unversioned target the deletes it cancelled are never sent,
// past the window; a second prune's are sent once the window has
// passed.
func scenarioOverflowRetention(t *testing.T, srv *overflowtest.Server, root string, versioned bool, window time.Duration) {
	env := overflowEnv(srv)
	f := newCircleWith(t, 2, "200M", nil, append(envList(env), envReplaceEvery+"=2s")...)
	f.an.env = env
	code, out, errOut := f.an.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", srv.Bucket,
		"--prefix", root, "--region", srv.Region, "--max-bytes", "200M", "--retention", window.String())
	if code != 0 {
		t.Fatalf("overflow add: %s%s", out, errOut)
	}
	if !versioned && !strings.Contains(out, "retention: client-side") {
		t.Fatalf("overflow add does not say retention is client-side:\n%s", out)
	}
	id := f.backup("one", 4)
	f.waitOffsite(id)
	f.an.drained("b2", 90*time.Second)
	prefix := root + "/" + f.aID.SealedName("laptop") + "/"
	before, err := srv.Keys(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	dataKeys := func(m map[string]int64) []string {
		var out []string
		for _, k := range overflowtest.SortedKeys(m) {
			if strings.HasPrefix(k, prefix+"data/") {
				out = append(out, k)
			}
		}
		return out
	}
	data := dataKeys(before)
	if len(data) == 0 {
		t.Fatalf("no data at the target: %v", overflowtest.SortedKeys(before))
	}

	// S3-6, VER-2, VER-3: checked by its stored checksums, and whole.
	for _, args := range [][]string{{"verify", "laptop", "--peer", "overflow:b2"}, {"verify", "laptop", "--peer", "overflow:b2", "--full"}} {
		code, out, errOut := f.an.run("", "", args...)
		if code != 0 || !strings.Contains(out, "verified") {
			t.Fatalf("%v: %d %s%s", args, code, out, errOut)
		}
	}

	// The attacker prunes everything.
	f.restic("forget", "--prune", id)
	f.drain()
	f.an.drained("b2", 90*time.Second)
	after, err := srv.Keys(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	st := f.an.status()
	o := st.Capacity.Overflow[0]
	if versioned {
		if len(dataKeys(after)) != 0 || o.Retention != "provider" || o.Tombstones != 0 {
			t.Fatalf("after the prune at a versioned target: %v live, %+v", overflowtest.SortedKeys(after), o)
		}
		if _, markers, err := srv.Versions(t.Context(), data[0]); err != nil || markers != 1 {
			t.Fatalf("a delete marker: %d, %v", markers, err)
		}
	} else {
		if len(after) != len(before) {
			t.Fatalf("the unversioned target lost keys at the prune: %d of %d left", len(after), len(before))
		}
		if o.Retention != "client-side" || o.Tombstones < int64(len(data)) {
			t.Fatalf("status %+v, want every pruned key's delete held", o)
		}
		_, text, _ := f.an.run("", "", "status")
		if !strings.Contains(text, "retention: client-side") {
			t.Fatalf("status does not label the target:\n%s", text)
		}
	}

	// RET-3: undelete brings everything back, at B and the target.
	code, out, errOut = f.an.run("y\n", "", "undelete", "laptop")
	t.Logf("undelete:\n%s%s", out, errOut)
	if code != 0 || !strings.Contains(out, "overflow:b2: ") || strings.Contains(out, "overflow:b2: 0 objects back") {
		t.Fatalf("undelete: %d\n%s%s", code, out, errOut)
	}
	if !strings.Contains(f.restic("snapshots"), id) {
		t.Fatal("restic does not see the snapshot after undelete")
	}
	f.restic("check")
	back, err := srv.Keys(t.Context(), prefix)
	if err != nil || len(back) != len(before) {
		t.Fatalf("at the target after undelete: %d keys of %d (%v)", len(back), len(before), err)
	}
	if o := f.an.status().Capacity.Overflow[0]; o.Tombstones != 0 {
		t.Fatalf("held deletes left after undelete: %+v", o)
	}
	if versioned {
		// VER-2, VER-3: a copy the provider rots, same size and hash
		// metadata, fails the check by download and marks the replica
		// unverified.
		b, sha, err := srv.Get(t.Context(), data[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.Put(t.Context(), data[0], make([]byte, len(b)), map[string]string{"lendisk-sha256": sha}); err != nil {
			t.Fatal(err)
		}
		code, out, errOut := f.an.run("", "", "verify", "laptop", "--peer", "overflow:b2", "--full")
		if code != 4 || !strings.Contains(errOut, "hashing to") {
			t.Fatalf("verify --full of a rotted copy: %d %s%s", code, out, errOut)
		}
		return
	}
	{
		// The cancelled deletes are never sent, past their window.
		time.Sleep(window + 2*time.Second)
		if back, err := srv.Keys(t.Context(), prefix); err != nil || len(back) != len(before) {
			t.Fatalf("after the window: %d keys of %d (%v)", len(back), len(before), err)
		}
		// A second prune's deletes are sent once their window ends.
		f.restic("forget", "--prune", id)
		f.drain()
		f.an.drained("b2", 90*time.Second)
		if now, _ := srv.Keys(t.Context(), prefix); len(now) != len(before) {
			t.Fatalf("deletes sent before the window ended: %d of %d keys left", len(now), len(before))
		}
		within(t, window+90*time.Second, "the held deletes sent after the window", func() bool {
			now, err := srv.Keys(t.Context(), prefix)
			return err == nil && len(dataKeys(now)) == 0 && f.an.status().Capacity.Overflow[0].Tombstones == 0
		})
	}
}
