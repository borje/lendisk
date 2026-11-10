package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
	"github.com/borje/lendisk/internal/replica"
)

// #96: `overflow set` changes a target's max_bytes, and its endpoint
// only after the credential is tested there and the objects the target
// holds are found there; a failure changes nothing. Usage errors exit 2, an unknown target 1.
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
	if o := n.status().Capacity.Overflow[0]; o.MaxBytes != 100<<30 || o.Endpoint != srv.Endpoint || o.FullCycleSeconds != 0 {
		t.Fatalf("after set: %+v", o)
	}

	// VER-1a (#105): a rolling full check at a target is opt-in, since
	// each object it checks is downloaded; 0 or off turns it off.
	for _, bad := range []string{"monthly", "400", "12h", "-1"} {
		if code, _, e := n.run("", "", "overflow", "set", "b2", "--full-cycle", bad); code != 2 || !strings.Contains(e, "--full-cycle") {
			t.Errorf("--full-cycle %s: exit %d (%s)", bad, code, e)
		}
	}
	for _, c := range []struct {
		arg  string
		days int64
		want string
	}{{"90", 90, "once in 90 days"}, {"off", 0, "full cycle: off"}, {"7d", 7, "once in 7 days"}, {"0", 0, "full cycle: off"}} {
		code, out, e := n.run("", "", "overflow", "set", "b2", "--full-cycle", c.arg)
		if code != 0 || !strings.Contains(out, c.want) || !strings.Contains(out, "up to 100G") {
			t.Fatalf("set --full-cycle %s: %d %s%s", c.arg, code, out, e)
		}
		if o := n.status().Capacity.Overflow[0]; o.FullCycleSeconds != c.days*86400 || o.MaxBytes != 100<<30 {
			t.Fatalf("after set --full-cycle %s: %+v", c.arg, o)
		}
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
	// The target holds a pack, as the index says. An endpoint that
	// reaches a bucket without it, or with other bytes there, is not the
	// same bucket: refused, nothing changes (#96 review).
	n.env = overflowEnv(srv)
	moved := strings.Replace(srv.Endpoint, "127.0.0.1", "localhost", 1)
	const pack, sha = "data/ab/abcdef", "ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23"
	db := openDB(t, n)
	r, err := db.Exec(`INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
		VALUES ('laptop', ?, 'put', 'data', 'done', 4, ?, 1)`, pack, sha)
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := r.LastInsertId()
	if _, err := db.Exec(`INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
		VALUES (?, 'overflow:b2', 'laptop', ?, 'put', 'acked', ?, 1, 4100)`, seq, pack, sha); err != nil {
		t.Fatal(err)
	}
	db.Close()
	key := replica.OverflowKey("lendisk", n.identity().SealedName("laptop"), pack)
	for _, c := range []struct {
		what string
		put  func() error
		want string
	}{
		{"a bucket without the pack", func() error { return nil }, "does not reach the same bucket"},
		{"other bytes at the key", func() error {
			return srv.Put(t.Context(), key, []byte("othr"), map[string]string{replica.MetaSHA256: strings.Repeat("0", 64)})
		}, "another version"},
	} {
		if err := c.put(); err != nil {
			t.Fatal(err)
		}
		code, out, e := n.run("", "", "overflow", "set", "b2", "--endpoint", moved, "--region", "us-east-1")
		if code != 1 || !strings.Contains(e, "not changed") || !strings.Contains(e, c.want) {
			t.Fatalf("set --endpoint at %s: %d %s%s", c.what, code, out, e)
		}
		if o := n.status().Capacity.Overflow[0]; o.Endpoint != srv.Endpoint {
			t.Fatalf("set --endpoint at %s changed the target: %+v", c.what, o)
		}
	}
	// The same bucket at another URL for it (localhost for 127.0.0.1),
	// holding the pack: tested, then written.
	if err := srv.Put(t.Context(), key, []byte("pack"), map[string]string{replica.MetaSHA256: sha}); err != nil {
		t.Fatal(err)
	}
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

// #102: a removal whose credential is gone cannot finish, and serve
// refuses to start without it, naming --force. `overflow remove
// --force` lists the copies objects still need that the target holds
// and asks first: refused, nothing changes; confirmed (or --yes), the
// target is dropped and what is left there is listed.
func TestOverflowRemoveForceAsksBeforeLosingCopiesObjectsNeed(t *testing.T) {
	serving(t, 0) // the provider is versitygw, in this process
	srv := overflowtest.Start(t, "offsite", true)
	n := newNode(t)
	n.mustInit(t)
	n.env = overflowEnv(srv)
	if code, out, e := n.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", "offsite",
		"--max-bytes", "1G"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	// The target holds the only offsite copy of a pack.
	const pack, sha = "data/ab/abcdef", "ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23ab01cd23"
	db := openDB(t, n)
	r, err := db.Exec(`INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
		VALUES ('laptop', ?, 'put', 'data', 'done', 4, ?, 1)`, pack, sha)
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := r.LastInsertId()
	if _, err := db.Exec(`INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
		VALUES (?, 'overflow:b2', 'laptop', ?, 'put', 'acked', ?, 1, 4100)`, seq, pack, sha); err != nil {
		t.Fatal(err)
	}
	if code, out, e := n.run("", "", "overflow", "remove", "b2"); code != 0 {
		t.Fatalf("overflow remove: %d %s%s", code, out, e)
	}
	// The credential is revoked and gone from the environment.
	n.env = nil
	err = checkOverflowCredentials(t.Context(), db, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "lendisk overflow remove --force b2") {
		t.Fatalf("serve's credential check: %v", err)
	}

	if code, _, e := n.run("", "", "overflow", "remove", "b2", "--yes"); code != 2 || !strings.Contains(e, "--force") {
		t.Fatalf("--yes without --force: %d %s", code, e)
	}
	if code, _, e := n.run("", "", "overflow", "remove", "nope", "--force"); code != 1 || !strings.Contains(e, "no overflow target nope") {
		t.Fatalf("forcing an unknown target: %d %s", code, e)
	}
	code, out, e := n.run("n\n", "", "overflow", "remove", "b2", "--force")
	if code != 1 || !strings.Contains(e, "not confirmed") {
		t.Fatalf("force refused: %d %s%s", code, out, e)
	}
	for _, want := range []string{"holds copies objects still need: 1 data object", "1 of them would have no copy offsite",
		"laptop/" + pack, "Drop it anyway"} {
		if !strings.Contains(out, want) {
			t.Errorf("force does not say %q:\n%s", want, out)
		}
	}
	if st := n.status(); len(st.Capacity.Overflow) != 1 {
		t.Fatalf("a refused force dropped the target: %+v", st.Capacity.Overflow)
	}

	code, out, e = n.run("", "", "overflow", "remove", "b2", "--force", "--yes")
	if code != 0 {
		t.Fatalf("force --yes: %d %s%s", code, out, e)
	}
	for _, want := range []string{"dropped (--force)", "fewer copies than their bucket's copies: 1",
		"1 objects lendisk put there and never deleted", "offsite/lendisk/"} {
		if !strings.Contains(out, want) {
			t.Errorf("force --yes does not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Drop it anyway") {
		t.Errorf("force --yes asked:\n%s", out)
	}
	if st := n.status(); len(st.Capacity.Overflow) != 0 {
		t.Fatalf("the target is still listed: %+v", st.Capacity.Overflow)
	}
	if err := checkOverflowCredentials(t.Context(), db, func(string) (string, bool) { return "", false }); err != nil {
		t.Fatalf("serve still refuses: %v", err)
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
