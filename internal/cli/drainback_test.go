package cli

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
	"github.com/borje/lendisk/internal/state"
)

// Scenario 12's second half, REPL-13, S3-4 (#40), scaled down and with
// versitygw in this process as a provider with versioning: A backs up
// at two copies with one friend, B, at 16 MB, which cannot hold two
// copies of anything, so every data object is at B and the overflow
// target. Then F, with room for the repository, becomes A's friend at
// 64 MB (400 GB beside 100 GB friends). A's node moves the copies at
// the target to F and deletes them there (delete markers, which the
// provider keeps for its window), and its copies of the metadata with
// them: the target holds no key, and `status` shows it empty, while
// the snapshot stays offsite at two copies, every data object at B and
// F.
func TestScenarioOverflowDrainsBackToANewFriend(t *testing.T) {
	serving(t, 3) // the provider is versitygw, in this process; A, B and F
	srv := overflowtest.Start(t, "offsite", true)
	env := overflowEnv(srv)
	f := newCircleWith(t, 2, "16M", nil, append(envList(env), envReplaceEvery+"=2s")...)
	f.an.env = env
	if code, out, e := f.an.run("", "", "overflow", "add", "s3", "b2", "--endpoint", srv.Endpoint, "--bucket", srv.Bucket,
		"--prefix", "backups/lendisk", "--max-bytes", "60M"); code != 0 {
		t.Fatalf("overflow add: %s%s", out, e)
	}
	id := f.backup("one", 10)
	f.waitOffsite(id)
	f.drain()
	f.an.drained("b2", 90*time.Second)
	prefix := "backups/lendisk/" + f.aID.SealedName("laptop") + "/"
	keys, err := srv.Keys(t.Context(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	held := 0
	for k := range keys {
		if strings.HasPrefix(k, prefix+"data/") {
			held++
		}
	}
	if held == 0 {
		t.Fatalf("no data at the target before F joins: %v", overflowtest.SortedKeys(keys))
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
	fID := fn.identity().NodeID().String()
	f.friends[fID] = fp
	within(t, 3*time.Minute, "the target empty in status", func() bool {
		st := f.an.status()
		if len(st.Capacity.Overflow) != 1 {
			return false
		}
		o := st.Capacity.Overflow[0]
		for _, p := range st.Places {
			if p.Place == "b2" && (p.Dropping != 0 || p.Queue.Objects != 0) {
				return false // deletes still to send
			}
		}
		return o.DataObjects == 0 && o.Use == 0 && o.DeleteDenied == 0
	})
	f.drain()
	f.an.drained("b2", 90*time.Second)
	if keys, err := srv.Keys(t.Context(), prefix); err != nil || len(keys) != 0 {
		t.Fatalf("the target still holds %v (%v)", overflowtest.SortedKeys(keys), err)
	}
	_, text, _ := f.an.run("", "", "status")
	if !strings.Contains(text, "b2: holds 0 B") {
		t.Fatalf("status does not show the target empty:\n%s", text)
	}
	f.waitOffsite(id)
	want := replicable(t, f.an, "laptop")
	at := map[string]int{}
	for _, pn := range f.friends {
		for k, h := range lentObjects(t, pn.node, f.aID, "laptop") {
			if want[k] == h {
				at[k]++
			}
		}
	}
	for k := range want {
		if strings.HasPrefix(k, "data/") && at[k] != 2 {
			t.Errorf("%s at %d friends, want B and F", k, at[k])
		}
	}
	if b := bucketOf(t, f.an.status(), "laptop"); !b.Offsite.Complete || b.BelowCopies.Objects != 0 {
		t.Fatalf("after drain-back: %+v", b)
	}
}

// S3-5 (#40): status counts the copies a target keeps because its
// credential may not delete them, in its use and on their own line.
func TestStatusCountsCopiesTheTargetMayNotDelete(t *testing.T) {
	parallel(t, 0)
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "b2", Endpoint: "https://s3.example", Bucket: "offsite",
		MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, Versioning: state.VersioningEnabled, AddedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for i, size := range []int64{3 << 20, 1 << 20} {
		if _, err := db.Exec(`INSERT INTO overflow_tomb (place, seq, bucket, key, size, drained, deleted_at, due_at, denied_at)
			VALUES ('overflow:b2', ?, 'laptop', 'data/x', ?, 1, 1, 2, 1)`, i+1, size); err != nil {
			t.Fatal(err)
		}
	}
	got, err := overflowStatus(t.Context(), db)
	if err != nil || len(got) != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	o := got[0]
	if o.DeleteDenied != 2 || o.DeleteDeniedBytes != 4<<20 || o.Use != 4<<20 || o.Tombstones != 0 {
		t.Fatalf("status %+v", o)
	}
	if l := overflowLine(o); !strings.Contains(l, "2 copies kept, 4.0 MiB: its credential may not delete them") {
		t.Fatalf("status line: %s", l)
	}
}

// S3-5, RET-3 (#98): of the copies kept, the user's deletes made longer
// ago than the retention window are counted apart, in status --json and
// its text line; drained copies and deletes inside the window are not.
func TestStatusCountsDeletesHeldPastTheWindowForWantOfPermission(t *testing.T) {
	parallel(t, 0)
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now()
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "g", Endpoint: "https://s3.example", Bucket: "offsite",
		MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, Versioning: state.VersioningOff, AddedAt: now}); err != nil {
		t.Fatal(err)
	}
	for i, r := range []struct {
		size    int64
		drained any
		deleted time.Time
	}{
		{3 << 20, nil, now.Add(-2 * time.Hour)},    // a user delete past the window
		{1 << 20, nil, now.Add(-30 * time.Minute)}, // inside it
		{5 << 20, 1, now.Add(-2 * time.Hour)},      // a drained copy
	} {
		if _, err := db.Exec(`INSERT INTO overflow_tomb (place, seq, bucket, key, size, drained, deleted_at, due_at, denied_at)
			VALUES ('overflow:g', ?, 'laptop', ?, ?, ?, ?, ?, ?)`, i+1, fmt.Sprintf("data/%d", i), r.size, r.drained,
			r.deleted.Unix(), now.Add(time.Hour).Unix(), now.Unix()); err != nil {
			t.Fatal(err)
		}
	}
	got, err := overflowStatus(t.Context(), db)
	if err != nil || len(got) != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	o := got[0]
	if o.DeleteDenied != 3 || o.DeleteDeniedPastWindow != 1 || o.DeleteDeniedPastWindowBytes != 3<<20 {
		t.Fatalf("status %+v", o)
	}
	b, _ := json.Marshal(o)
	if !strings.Contains(string(b), `"delete_denied_past_window":1,"delete_denied_past_window_bytes":3145728`) {
		t.Fatalf("json %s", b)
	}
	if l := overflowLine(o); !strings.Contains(l, "1 of them deleted objects, 3.0 MiB, held past the retention window because the credential may not delete them") {
		t.Fatalf("status line: %s", l)
	}
}

// REPL-13 (#98): status says whether drain-back runs, in
// capacity.overflow_drain and a text line: absent without a target,
// active while the friends hold the repository's window, off while
// placement is split, waiting with the earliest start otherwise.
func TestStatusSaysWhetherDrainBackRuns(t *testing.T) {
	parallel(t, 0)
	if drainLine(nil) != "" {
		t.Fatal("a drain-back line without a target")
	}
	from := "2026-12-10T00:00:00Z"
	for _, c := range []struct {
		d    StatusOverflowDrain
		json string
		line string
	}{
		{StatusOverflowDrain{State: state.DrainActive}, `{"state":"active"}`, "drain-back: active"},
		{StatusOverflowDrain{State: state.DrainSplit}, `{"state":"split"}`, "drain-back: off while placement is split"},
		{StatusOverflowDrain{State: state.DrainWaiting, From: from}, `{"state":"waiting","from":"` + from + `"}`,
			"drain-back: waiting, the friends cannot hold the largest the repository was in the last 30 days; it starts at " + from},
	} {
		if b, _ := json.Marshal(c.d); string(b) != c.json {
			t.Errorf("json %s, want %s", b, c.json)
		}
		if l := drainLine(&c.d); !strings.Contains(l, c.line) {
			t.Errorf("line %q, want %q", l, c.line)
		}
	}
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := &app{}
	c, err := a.capacityStatus(t.Context(), db, 2, time.Now())
	if err != nil || c.OverflowDrain != nil {
		t.Fatalf("without a target: %+v, %v", c, err)
	}
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "g", Endpoint: "https://s3.example", Bucket: "offsite",
		MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, AddedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	c, err = a.capacityStatus(t.Context(), db, 2, time.Now())
	if err != nil || c.OverflowDrain == nil || c.OverflowDrain.State != state.DrainActive {
		t.Fatalf("an empty repository with a target: %+v, %v", c.OverflowDrain, err)
	}
	if !slices.ContainsFunc(capacityLines(c, "owner"), func(l string) bool { return strings.Contains(l, "drain-back: active") }) {
		t.Fatalf("lines %q", capacityLines(c, "owner"))
	}
}
