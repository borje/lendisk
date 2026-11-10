package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// ID-5, REPL-12, EP-7 (#78): Kopia recovered from two friends at
// different progress, with real Kopia.
//
//   - A backs up with Kopia to friends B and C, one copy of each pack
//     (with two, an offline C would hold back B's metadata, REPL-1), and
//     runs full maintenance. C goes offline; A takes a second snapshot,
//     deletes the first, runs full maintenance again, which writes index
//     blobs and a deletion watermark superseding the first, deletes the
//     superseded watermark as Kopia's cleanup does, and takes a third
//     snapshot. B follows all of it; C lacks the blobs it never received
//     and still holds the watermark B saw deleted.
//   - A's machine is lost; a fresh one recovers with both friends
//     online. Each lists metadata blobs the other lacks, yet no key is
//     in conflict: Kopia writes each of its transient blobs (x*, s*, n*)
//     once under its name, and reads the union of what it lists (the
//     newest watermark and epoch marker count, a compacted epoch's index
//     supersedes its uncompacted blobs), so a key one friend lists is a
//     single source.
//   - Kopia connects through the recovered node, lists the latest
//     snapshot and restores it byte for byte, and verifies it.
func TestRecoverKopiaFromTwoFriendsAtDifferentProgress(t *testing.T) {
	serving(t, 3)
	k := newKopia(t)
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
	code, out, errOut := an.run("", "", "bucket", "add", "vault", "--profile", "kopia", "--copies", "1")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	fast := envReplFast + "=1"
	clock := filepath.Join(t.TempDir(), "clock")
	a := newProcNode(t, an, fast, envClock+"="+clock, envRerouteEvery+"=200ms")
	b, c := newProcNode(t, bn, fast), newProcNode(t, cn, fast)
	b.start()
	c.start()
	a.start()
	befriend(t, bn, an)
	befriend(t, cn, an)
	aID := an.identity()
	bKey, cKey := bn.identity().NodeID().String(), cn.identity().NodeID().String()
	bAddr := bn.status().Node.Address

	k.run(kopiaLines(t, out, a.addr)...)
	src := t.TempDir()
	randomFiles(t, src, "one", 6, 256<<10)
	k.run("snapshot", "create", src)
	// Full maintenance writes a deletion watermark (xw<time>), which a
	// later run supersedes.
	k.run("maintenance", "run", "--full", "--safety=none")
	watermarks := func() []string {
		var ws []string
		for key := range kopiaReplicable(t, an, "vault") {
			if strings.HasPrefix(key, "xw") {
				ws = append(ws, key)
			}
		}
		slices.Sort(ws)
		return ws
	}
	oldMark := watermarks()
	if len(oldMark) != 1 {
		t.Fatalf("watermarks after the first maintenance: %v", oldMark)
	}
	an.drained(bKey, 90*time.Second)
	an.drained(cKey, 90*time.Second)

	// C goes offline; A moves on without it. Once A has seen C fail for
	// reroute_after (A's clock goes two hours on), C's unsent packs move
	// to B and hold back no metadata (REPL-1).
	c.p.kill()
	for i := 1; i < 6; i++ {
		os.Remove(filepath.Join(src, fmt.Sprintf("one-%02d", i)))
	}
	randomFiles(t, src, "two", 3, 256<<10)
	k.run("snapshot", "create", src)
	deadline := time.Now().Add(60 * time.Second)
	for an.place(cKey).State != "retrying" {
		if time.Now().After(deadline) {
			t.Fatalf("A does not see C fail: %+v", an.place(cKey))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := os.WriteFile(clock, []byte((2 * time.Hour).String()), 0o600); err != nil {
		t.Fatal(err)
	}
	var snaps []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(k.run("snapshot", "list", src, "--json")), &snaps); err != nil || len(snaps) != 2 {
		t.Fatalf("snapshots %+v %v", snaps, err)
	}
	k.run("snapshot", "delete", snaps[0].ID, "--delete")
	time.Sleep(time.Second) // the next watermark is named by a later second
	k.run("maintenance", "run", "--full", "--safety=none")
	if ws := watermarks(); len(ws) != 2 {
		t.Fatalf("watermarks after the second maintenance: %v", ws)
	}
	// Kopia deletes a superseded watermark once its cleanup safety margin
	// has passed (at least 10 minutes, internal/epoch cleanupWatermarks);
	// the test deletes it now as Kopia would then. B receives the delete.
	k.run("blob", "delete", "--dangerous-commands=enabled", oldMark[0])
	randomFiles(t, src, "three", 2, 64<<10)
	k.run("snapshot", "create", src)
	an.drained(bKey, 90*time.Second)
	atB, atC := lentObjects(t, bn, aID, "vault"), lentObjects(t, cn, aID, "vault")
	isMeta := func(key string) bool { return !strings.HasPrefix(key, "p") && !strings.HasPrefix(key, "q") }
	localMeta, metaB := kopiaReplicable(t, an, "vault"), maps.Clone(atB)
	maps.DeleteFunc(localMeta, func(k, _ string) bool { return !isMeta(k) })
	maps.DeleteFunc(metaB, func(k, _ string) bool { return !isMeta(k) })
	sameObjects(t, localMeta, metaB)

	// The friends differ in transient metadata both ways.
	var onlyB, onlyC []string
	for key := range atB {
		if _, ok := atC[key]; !ok && isMeta(key) {
			onlyB = append(onlyB, key)
		}
	}
	for key := range atC {
		if _, ok := atB[key]; !ok && isMeta(key) {
			onlyC = append(onlyC, key)
		}
	}
	slices.Sort(onlyB)
	slices.Sort(onlyC)
	t.Logf("metadata only B holds: %v", onlyB)
	t.Logf("metadata only C holds: %v", onlyC)
	if len(onlyB) == 0 || !slices.Contains(onlyC, oldMark[0]) {
		t.Fatalf("the friends do not differ in metadata both ways: only B %v, only C %v", onlyB, onlyC)
	}

	// A's machine is gone; a fresh one recovers with B and C online.
	a.p.kill()
	os.RemoveAll(filepath.Dir(filepath.Dir(an.config)))
	c.start()
	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 15 * time.Second
	code, out, errOut = fresh.run("y\n"+phraseA+"\n", "", "recover", bAddr)
	if code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	t.Logf("recover printed:\n%s", out)
	st := fresh.status()
	if len(st.Recovery.Pending) != 0 {
		t.Fatalf("lists still pending after recover: %+v", st.Recovery.Pending)
	}
	if len(st.Recovery.Conflicts) != 0 || strings.Contains(out, "in conflict") {
		t.Fatalf("conflicts on Kopia's transient metadata: %+v", st.Recovery.Conflicts)
	}
	db := openDB(t, fresh)
	keys, err := db.RecoveredKeys(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	verdicts := map[string]string{}
	for _, rk := range keys {
		verdicts[rk.Key] = rk.Verdict
	}
	for _, key := range append(slices.Clone(onlyB), onlyC...) {
		if verdicts[key] != state.VerdictSingle {
			t.Errorf("%s, listed by one friend: verdict %q, want %q", key, verdicts[key], state.VerdictSingle)
		}
	}

	// Kopia reads the union through the recovered node and restores the
	// latest snapshot.
	fa := newProcNode(t, fresh, fast)
	fa.start()
	_, shown, _ := fresh.run("", "", "bucket", "show", "vault")
	connect := kopiaLines(t, shown, fa.addr)
	connect[1] = "connect"
	k2 := newKopia(t)
	k2.run(connect...)
	t.Logf("snapshots after recovery:\n%s", k2.run("snapshot", "list", src))
	dst := t.TempDir()
	k2.run("snapshot", "restore", src, dst)
	sameFiles(t, src, dst)
	k2.run("snapshot", "verify", "--verify-files-percent=100")
}
