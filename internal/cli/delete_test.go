package cli

import (
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// lentDir is lender's directory of borrower's bucket.
func lentDir(lender *node, borrower *identity.Identity, bucket string) string {
	return filepath.Join(lender.data, "store", gateway.LentName(borrower.NodeID(), borrower.SealedName(bucket)))
}

// lentUse is what the borrower's count would hold for the live objects
// lender keeps of its bucket: each object's size plus the per-object
// overhead (§3.4).
func lentUse(t *testing.T, lender *node, borrower *identity.Identity, bucket string) int64 {
	t.Helper()
	dir := lentDir(lender, borrower, bucket)
	var n int64
	for k := range lentObjects(t, lender, borrower, bucket) {
		fi, err := os.Stat(filepath.Join(dir, filepath.FromSlash(k)))
		if err != nil {
			t.Fatal(err)
		}
		n += fi.Size() + state.ObjectOverhead
	}
	return n
}

// mergeReplicas copies the live objects of dirs into one new directory:
// the pool's copy of a bucket, as a recovery would read it.
func mergeReplicas(t *testing.T, dirs ...string) string {
	t.Helper()
	out := t.TempDir()
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.HasPrefix(de.Name(), ".") && p != dir {
				if de.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if de.IsDir() {
				return nil
			}
			rel, _ := filepath.Rel(dir, p)
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			dst := filepath.Join(out, rel)
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return err
			}
			return os.WriteFile(dst, b, 0o600)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// REPL-2, RET-1, REPL-1, ID-5 (#21), end to end: A backs up with restic
// twice at one copy to friends B and C, then runs `restic forget
// --keep-last 1 --prune`, which repacks, writes a new index and deletes
// the first snapshot, the old index and the packs nothing references.
// Once A's queues drain:
//
//   - each friend that held a deleted object keeps it as a tombstone,
//     and a friend that never held it has none;
//   - every live object at each friend is the one it held before, byte
//     for byte, and the friends together hold exactly A's objects;
//   - the pool's copy is a restic repository `restic check --read-data`
//     passes;
//   - A's count of each friend's use is what the friend holds live, and
//     nothing is below copies or queued;
//   - a fresh machine that recovers A from B lists both friends and
//     recovers none of the deleted keys (REPL-2 with ID-5: a tomb is not
//     resurrected).
func TestResticPruneTombstonesAtEveryFriend(t *testing.T) {
	parallel(t, 4)
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an, bn, cn := newNode(t), newNode(t), newNode(t)
	bn.regions, cn.regions = an.regions, an.regions // one relay for all
	for n, p := range map[*node]string{an: phraseA, bn: "", cn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	code, out, errOut := an.run("", "", "bucket", "add", "laptop", "--profile", "restic", "--copies", "1")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	ak, sk := reAccess.FindStringSubmatch(out), reSecret.FindStringSubmatch(out)
	if ak == nil || sk == nil {
		t.Fatalf("no credential in output:\n%s", out)
	}
	fast := envReplFast + "=1"
	a, b, c := newProcNode(t, an, fast), newProcNode(t, bn, fast), newProcNode(t, cn, fast)
	b.start()
	c.start()
	a.start()
	befriend(t, bn, an)
	befriend(t, cn, an)
	aID := an.identity()
	friends := map[string]*node{bn.identity().NodeID().String(): bn, cn.identity().NodeID().String(): cn}

	src := t.TempDir()
	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+ak[1], "AWS_SECRET_ACCESS_KEY="+sk[1],
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
	drain := func() {
		t.Helper()
		for f := range friends {
			if p := an.drained(f, 90*time.Second); p.Refused != 0 {
				t.Fatalf("place %+v", p)
			}
		}
	}
	held := func() map[string]map[string]string { // friend → key → hash
		t.Helper()
		out := map[string]map[string]string{}
		pool := map[string]string{}
		for f, n := range friends {
			out[f] = lentObjects(t, n, aID, "laptop")
			maps.Copy(pool, out[f])
		}
		sameObjects(t, replicable(t, an, "laptop"), pool)
		return out
	}

	run("init")
	randomFiles(t, src, "one", 6, 1<<20)
	run("backup", "--pack-size", "4", "--no-scan", src)
	// The second backup keeps a little of the first's data, so the
	// prune both deletes packs and repacks the ones partly in use.
	for i := 1; i < 6; i++ {
		os.Remove(filepath.Join(src, "one-0"+string(rune('0'+i))))
	}
	randomFiles(t, src, "two", 4, 1<<20)
	run("backup", "--pack-size", "4", "--no-scan", src)
	drain()
	before := held()
	for f, objs := range before {
		t.Logf("%s holds %d objects before the prune", f, len(objs))
	}

	t.Logf("restic forget --prune:\n%s", run("forget", "--keep-last", "1", "--prune", "--max-unused", "0"))
	drain()
	after := held()
	live := replicable(t, an, "laptop")

	deleted := map[string]bool{}
	for _, objs := range before {
		for k := range objs {
			if _, ok := live[k]; !ok {
				deleted[k] = true
			}
		}
	}
	var packs, snapshots int
	for k := range deleted {
		switch {
		case strings.HasPrefix(k, "data/"):
			packs++
		case strings.HasPrefix(k, "snapshots/"):
			snapshots++
		}
	}
	if packs == 0 || snapshots != 1 {
		t.Fatalf("the prune deleted %d packs and %d snapshots: %v", packs, snapshots, deleted)
	}
	sealed := aID.SealedName("laptop")
	for f, n := range friends {
		db := openDB(t, n)
		for k := range deleted {
			ts, err := db.TombstonesOf(t.Context(), aID.NodeID().String(), sealed, k)
			if err != nil {
				t.Fatal(err)
			}
			h, had := before[f][k]
			switch {
			case had && (len(ts) != 1 || ts[0].SHA256 != h || ts[0].Rewrite || ts[0].Pending):
				t.Errorf("%s held %s: tombstones %+v, want one of %s", f, k, ts, h)
			case !had && len(ts) != 0:
				t.Errorf("%s never held %s, yet has tombstones %+v", f, k, ts)
			case had:
				b, err := os.ReadFile(filepath.Join(lentDir(n, aID, "laptop"), filepath.FromSlash(ts[0].Tomb)))
				if err != nil || sha256Hex(b) != h {
					t.Errorf("%s's tomb of %s does not hold its bytes: %v", f, k, err)
				}
			}
			if _, ok := after[f][k]; ok {
				t.Errorf("%s still holds deleted %s live", f, k)
			}
		}
		for k, h := range after[f] {
			if old, ok := before[f][k]; ok && old != h {
				t.Errorf("%s's live %s changed from %s to %s", f, k, old, h)
			}
		}
	}
	t.Logf("the prune deleted %d objects (%d packs); the friends keep them as tombstones", len(deleted), packs)

	// The pool's copy is a repository restic checks, every byte read.
	pool := mergeReplicas(t, lentDir(bn, aID, "laptop"), lentDir(cn, aID, "laptop"))
	check := exec.Command(restic, "check", "--read-data")
	check.Env = append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir(),
		"RESTIC_REPOSITORY="+pool)
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("restic check of the pool's copy: %v\n%s", err, out)
	}

	// A's bookkeeping: each friend counted at what it holds live (its
	// tombs are the lender's to count until purged, #68), nothing below
	// copies, nothing queued.
	use, err := openDB(t, an).PlaceUse(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for f, n := range friends {
		if want := lentUse(t, n, aID, "laptop"); use[f] != want {
			t.Errorf("A counts %d bytes at %s, which holds %d live", use[f], f, want)
		}
	}
	st := an.status()
	if len(st.Buckets) != 1 || st.Buckets[0].BelowCopies.Objects != 0 {
		t.Errorf("buckets after the prune: %+v", st.Buckets)
	}
	for _, p := range st.Places {
		if p.Queue.Objects != 0 || p.Refused != 0 {
			t.Errorf("place after the prune: %+v", p)
		}
	}

	// ID-5: a fresh machine recovers A from B; the deleted keys stay
	// deleted.
	deadline := time.Now().Add(60 * time.Second)
	for {
		m, err := heldManifest(t, bn, aID)
		if err == nil && len(m.Friends) == 2 && len(m.Buckets) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B's copy of A's manifest: %+v %v", m, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	bAddr := bn.status().Node.Address
	a.p.kill()
	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 15 * time.Second
	if code, out, errOut := fresh.run("y\n"+phraseA+"\n", "", "recover", bAddr); code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	fa := newProcNode(t, fresh, fast)
	fa.start()
	deadline = time.Now().Add(90 * time.Second)
	for len(fresh.status().Recovery.Pending) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("lists still pending: %+v", fresh.status().Recovery.Pending)
		}
		time.Sleep(200 * time.Millisecond)
	}
	recovered := map[string]string{}
	for f := range friends {
		got := ackedAt(t, fresh, f, "laptop")
		if !maps.Equal(got, after[f]) {
			t.Errorf("recovered placement at %s: %d keys, want the %d it holds live", f, len(got), len(after[f]))
		}
		maps.Copy(recovered, got)
	}
	for k := range deleted {
		if _, ok := recovered[k]; ok {
			t.Errorf("recovery resurrected deleted %s", k)
		}
	}
	if !maps.Equal(recovered, live) {
		t.Errorf("recovered %d keys, A held %d", len(recovered), len(live))
	}
}
