package cli

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// captureDirect records done puts of size bytes in n's bucket, straight
// into its lendisk.db, as the endpoint would.
func captureDirect(t *testing.T, n *node, bucket string, objects map[string]int64) {
	t.Helper()
	db := openDB(t, n)
	for key, size := range objects {
		class := "data"
		if !strings.HasPrefix(key, "data/") {
			class = "metadata"
		}
		seqs, err := db.BeginCaptures(t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: class}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(t.Context(), seqs[0], size, sha256Hex([]byte(key))); err != nil {
			t.Fatal(err)
		}
	}
}

func capacityOf(t *testing.T, st Status) StatusCapacity {
	t.Helper()
	if st.Capacity == nil {
		t.Fatal("status has no capacity section")
	}
	return *st.Capacity
}

// LEND-7, LEND-8, OPS-1 (#37): status shows, in one place, the local
// copy, the use at each friend and at each overflow target (none yet),
// what each friend is lent and uses, the protectable size and the most
// fair quotas could protect. A repository grown past what its two
// friends at 50 MB protect at two copies is said to be, with per
// friendship the number that would cover it (the LEND-8 AC's figures in
// MiB).
func TestStatusShowsCapacityAndTheNumbersThatCoverADeficit(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	n.addBucket(t, "laptop", "restic")
	// Captured before the friendships, so nothing is queued to them: the
	// use there is the friends' count alone.
	captureDirect(t, n, "laptop", map[string]int64{"index/1": 300 << 10, "data/1": 30 << 20, "data/2": 30 << 20})
	ids := befriendDirect(t, n, 50<<20, 2)
	slices.Sort(ids) // made in one second: listed by node ID
	db := openDB(t, n)
	now := time.Now()
	for i, id := range ids {
		if _, err := db.QuotaAnswered(t.Context(), id, 50<<20, now, state.QuotaAnswer{Number: 50 << 20, Lent: 45 << 20,
			RepositorySize: int64(i+1) << 30}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetLentUsage(t.Context(), ids[0], 5<<20, now); err != nil {
		t.Fatal(err)
	}

	c := capacityOf(t, n.status())
	size := int64(60<<20+300<<10) + 3*state.ObjectOverhead
	if c.RepositorySize != size || c.LocalCopy != 60<<20+300<<10 || c.Copies != 2 {
		t.Errorf("repository %d (want %d), local copy %d, copies %d", c.RepositorySize, size, c.LocalCopy, c.Copies)
	}
	if c.Protectable < 49<<20 || c.Protectable > 50<<20 || c.Deficit != size-c.Protectable || !c.Alerting || !c.Coverable {
		t.Errorf("protectable %d, deficit %d, alerting %v, coverable %v", c.Protectable, c.Deficit, c.Alerting, c.Coverable)
	}
	// Fair quotas: each friend's repository size, 1 and 2 GiB: one copy
	// each, at most 1 GiB of D.
	if c.FairProtectable < 1000<<20 || c.FairProtectable > 1<<30 {
		t.Errorf("fair protectable %d", c.FairProtectable)
	}
	if c.Lent != 100<<20 || c.LentUsed != 5<<20 || c.Received != 100<<20 || c.ReceivedUnused != 10<<20 {
		t.Errorf("lent %d used %d, received %d unused %d", c.Lent, c.LentUsed, c.Received, c.ReceivedUnused)
	}
	if c.Overflow == nil || len(c.Overflow) != 0 {
		t.Errorf("overflow %v, want empty", c.Overflow)
	}
	if len(c.Friends) != 2 {
		t.Fatalf("friends %+v", c.Friends)
	}
	for i, f := range c.Friends {
		if f.Peer != ids[i] || f.Quota != 50<<20 || f.UseThere != 45<<20 || f.RepositorySize == nil || *f.RepositorySize != int64(i+1)<<30 {
			t.Errorf("friend %d: %+v", i, f)
		}
		if f.Cover < 60<<20 || f.Cover > 80<<20 || f.Cover%(1<<20) != 0 {
			t.Errorf("friend %d: cover %d, want a whole MiB in 60..80", i, f.Cover)
		}
	}
	if c.Friends[0].UseHere != 5<<20 || c.Friends[1].UseHere != 0 {
		t.Errorf("use here %d, %d", c.Friends[0].UseHere, c.Friends[1].UseHere)
	}

	_, text, _ := n.run("", "", "status")
	for _, want := range []string{"protectable", "paused for quota", "with fair quotas", "lendisk peer quota", "Overflow targets: none",
		ids[0] + ": " + formatSize(c.Friends[0].Cover) + " (now 50M)", ids[1] + ": " + formatSize(c.Friends[1].Cover) + " (now 50M)"} {
		if !strings.Contains(text, want) {
			t.Errorf("status lacks %q:\n%s", want, text)
		}
	}
}

// LEND-7 (#37): a friendship where one side has used less than half of
// the quota for 30 days is flagged, on either side, so the two can lower
// the number; not before 30 days, and no more once it uses half.
func TestFriendshipsUnderHalfUsedFor30DaysAreFlagged(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	t0 := time.Unix(1_900_000_000, 0)
	b := befriendDirect(t, n, 100<<20, 1)[0]
	db := openDB(t, n)
	if err := db.SetLentUsage(t.Context(), b, 10<<20, t0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QuotaAnswered(t.Context(), b, 100<<20, t0, state.QuotaAnswer{Number: 100 << 20, Lent: 70 << 20}, t0); err != nil {
		t.Fatal(err)
	}
	at := func(d time.Duration) (StatusCapacityFriend, string) {
		t.Helper()
		n.clock = func() time.Time { return t0.Add(d) }
		f := capacityOf(t, n.status()).Friends[0]
		_, text, _ := n.run("", "", "status")
		return f, text
	}
	if f, text := at(29 * 24 * time.Hour); f.LowHereSince != "" || f.LowThereSince != "" || strings.Contains(text, "under half") {
		t.Fatalf("after 29 days: %+v\n%s", f, text)
	}
	f, text := at(31 * 24 * time.Hour)
	if f.LowHereSince != t0.UTC().Format(time.RFC3339) || f.LowThereSince != "" {
		t.Fatalf("after 31 days: %+v", f)
	}
	if !strings.Contains(text, "has used under half of the quota here since "+f.LowHereSince) || !strings.Contains(text, "lower the number") {
		t.Fatalf("status:\n%s", text)
	}
	// It stores 60 MiB from then: no longer flagged.
	if err := db.SetLentUsage(t.Context(), b, 60<<20, t0); err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(31 * 24 * time.Hour)
	if _, err := db.QuotaReported(t.Context(), b, state.QuotaReport{Number: 100 << 20}, t1); err != nil {
		t.Fatal(err)
	}
	if f, _ := at(32 * 24 * time.Hour); f.LowHereSince != "" {
		t.Fatalf("once it uses more than half: %+v", f)
	}
}

// PEER-9, LEND-7 (#37): a lender-only node's status shows the space it
// lends, what its friends use of it, and the quota its friends lend it
// back, unused while it has no bucket; no protectable size is alerted.
func TestALenderOnlyNodesStatusShowsLentSpaceAndUnusedQuota(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	p := newPhrase(t)
	if code, _, e := n.run(p+"\n", "", "init", "--restore", "--role", "lender"); code != 0 {
		t.Fatal(e)
	}
	ids := befriendDirect(t, n, 40<<20, 2)
	if err := openDB(t, n).SetLentUsage(t.Context(), ids[1], 10<<20, time.Now()); err != nil {
		t.Fatal(err)
	}
	st := n.status()
	c := capacityOf(t, st)
	if st.Node.Role != "lender" || c.Lent != 80<<20 || c.LentUsed != 10<<20 || c.Received != 80<<20 || c.ReceivedUnused != 80<<20 ||
		c.Alerting || c.RepositorySize != 0 {
		t.Fatalf("role %s, capacity %+v", st.Node.Role, c)
	}
	_, text, _ := n.run("", "", "status")
	for _, want := range []string{"Lending only: lends 80.0 MiB to 2 friends, who use 10.0 MiB of it",
		"received 80.0 MiB from them, unused until this node has a bucket"} {
		if !strings.Contains(text, want) {
			t.Errorf("status lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "protectable") {
		t.Errorf("a lender-only node's status speaks of protecting:\n%s", text)
	}
}

// LEND-7 (#37): init asks for the expected repository size and copies,
// and suggests lending.max_total = c × S plus 20% for tombstones,
// written into the config it creates. Flags answer without asking; with
// nothing on stdin it asks nothing, for scripts.
func TestInitSuggestsWhatToLend(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	code, out, e := n.run(newPhrase(t)+"\n100G\n3\n", "", "init", "--restore")
	if code != 0 {
		t.Fatal(e)
	}
	if !strings.Contains(out, "Expected repository size") || !strings.Contains(out, "lend about 360G in total") ||
		!strings.Contains(out, "about 460G") {
		t.Fatalf("init:\n%s", out)
	}
	conf, err := os.ReadFile(n.config)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^max_total = "360G"$`).Match(conf) || !regexp.MustCompile(`(?m)^copies = 3$`).Match(conf) {
		t.Fatalf("config:\n%s", conf)
	}
	if code, out, e := n.run("", "", "status"); code != 0 || !strings.Contains(out, "at most 360.0 GiB (lending.max_total)") {
		t.Fatalf("status with the suggestion: %s%s", out, e)
	}

	flags := newNode(t)
	if code, out, e := flags.run(newPhrase(t)+"\n", "", "init", "--restore", "--repository-size", "10G", "--copies", "2"); code != 0 ||
		strings.Contains(out, "Expected repository size") || !strings.Contains(out, "lend about 24G in total") {
		t.Fatalf("init with flags: %s%s", out, e)
	}

	quiet := newNode(t)
	if code, out, e := quiet.run(newPhrase(t)+"\n", "", "init", "--restore"); code != 0 {
		t.Fatalf("init with nothing more on stdin: %s%s", out, e)
	}
	conf, _ = os.ReadFile(quiet.config)
	if strings.Contains(string(conf), "max_total") {
		t.Fatalf("a cap nobody asked for:\n%s", conf)
	}
	if code, _, e := newNode(t).run(newPhrase(t)+"\n", "", "init", "--restore", "--repository-size", "lots"); code != 2 ||
		!strings.Contains(e, "lots") {
		t.Fatalf("a malformed size: %d %s", code, e)
	}
}

func newPhrase(t *testing.T) string {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	return p
}
