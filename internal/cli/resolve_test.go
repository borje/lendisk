package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/state"
)

// #29 review: when a friend did not serve its manifest, a newer version
// may be there; recover says so, naming it.
func TestRecoverWarnsOfLendersNotHeard(t *testing.T) {
	parallel(t, 0)
	b, c := identity.NodeID{1}, identity.NodeID{2}
	var w bytes.Buffer
	printFetched(&w, []recovery.Fetched{{Peer: b, Seq: 4}, {Peer: c, Err: errors.New("offline")}}, &manifest.Manifest{Seq: 4})
	out := w.String()
	if !strings.Contains(out, b.String()+": version 4 (newest)") || !strings.Contains(out, c.String()+": offline") ||
		!strings.Contains(out, "newer version") {
		t.Fatalf("printed:\n%s", out)
	}
	w.Reset()
	printFetched(&w, []recovery.Fetched{{Peer: b, Seq: 4}}, &manifest.Manifest{Seq: 4})
	if strings.Contains(w.String(), "newer version") {
		t.Fatalf("warned with every friend heard:\n%s", w.String())
	}
}

// #29 review (user decision 2026-11-07): status shows a recovered key
// in conflict with what each place listed, and `bucket resolve` picks a
// place's version; a key only one place lists is marked unverified in a
// generic bucket only, since restic's own MAC catches a forged one.
func TestStatusShowsConflictsAndBucketResolvePicks(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "laptop", "restic")
	n.addBucket(t, "files", "generic")
	db := openDB(t, n)
	ctx := t.Context()
	for _, f := range []string{"B", "C"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"laptop", "files"}); err != nil {
		t.Fatal(err)
	}
	rec := func(place, bucket string, objs ...state.Listed) {
		if _, err := db.RecordListed(ctx, place, bucket, objs, len(objs), "", true); err != nil {
			t.Fatal(err)
		}
		if err := db.SettleFinished(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rec("B", "laptop", state.Listed{Key: "config", Size: 1, SHA256: "hb", Class: "metadata"},
		state.Listed{Key: "data/x", Size: 1, SHA256: "hx", Class: "data"})
	rec("C", "laptop", state.Listed{Key: "config", Size: 1, SHA256: "hc", Class: "metadata"})
	rec("B", "files", state.Listed{Key: "doc", Size: 1, SHA256: "hd", Class: "data"})
	rec("C", "files")

	st := n.status()
	if cs := st.Recovery.Conflicts; len(cs) != 1 || cs[0].Bucket != "laptop" || cs[0].Key != "config" ||
		cs[0].Places["B"] != "hb" || cs[0].Places["C"] != "hc" || cs[0].Reason == "" {
		t.Fatalf("conflicts %+v", cs)
	}
	if u := st.Recovery.Unverified; len(u) != 1 || u[0].Bucket != "files" || u[0].Objects != 1 {
		t.Fatalf("unverified %+v", u)
	}
	if _, out, _ := n.run("", "", "status"); !strings.Contains(out, "lendisk bucket resolve laptop config") {
		t.Fatalf("status text:\n%s", out)
	}
	if code, _, e := n.run("", "", "bucket", "resolve", "laptop", "config", "D"); code != 1 || !strings.Contains(e, "does not list") {
		t.Fatalf("resolve with a place not listing it: %d %s", code, e)
	}
	if code, _, e := n.run("", "", "bucket", "resolve", "laptop", "config"); code != 2 {
		t.Fatalf("resolve without a place: %d %s", code, e)
	}
	code, out, e := n.run("", "", "bucket", "resolve", "laptop", "config", "C")
	if code != 0 || !strings.Contains(out, "config") {
		t.Fatalf("resolve: %d %s %s", code, out, e)
	}
	if cs := n.status().Recovery.Conflicts; len(cs) != 0 {
		t.Fatalf("still in conflict: %+v", cs)
	}
	if h, ok, _ := db.AckedHash(ctx, "C", "laptop", "config"); !ok || h != "hc" {
		t.Fatalf("C's config after resolving: %q %v", h, ok)
	}
	// #29 re-review: what is no conflict is refused, never reported as
	// resolved: the key just resolved, and one the node wrote since.
	if code, out, e := n.run("", "", "bucket", "resolve", "laptop", "config", "B"); code != 1 ||
		!strings.Contains(e, "not a conflict") || strings.Contains(out, "using") {
		t.Fatalf("resolve again: %d %q %s", code, out, e)
	}
	rec("C", "files", state.Listed{Key: "doc", Size: 1, SHA256: "hother", Class: "data"})
	if cs := n.status().Recovery.Conflicts; len(cs) != 1 || cs[0].Key != "doc" {
		t.Fatalf("conflicts %+v", cs)
	}
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "doc", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], 4, "hmine"); err != nil {
		t.Fatal(err)
	}
	if code, out, e := n.run("", "", "bucket", "resolve", "files", "doc", "B"); code != 1 ||
		!strings.Contains(e, "not a conflict") || strings.Contains(out, "using") {
		t.Fatalf("resolve of a key written here: %d %q %s", code, out, e)
	}
	if cs := n.status().Recovery.Conflicts; len(cs) != 0 {
		t.Fatalf("conflicts after the node wrote the key: %+v", cs)
	}
}

// #29 re-review: recover takes friends to skip by node ID.
func TestRecoverSkipNeedsNodeIDs(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	other := newNode(t)
	other.mustInit(t)
	if code, _, e := n.run("", "", "recover", "--skip", "not-a-node", other.status().Node.Address); code != 2 ||
		!strings.Contains(e, "--skip") || !strings.Contains(e, "node ID") {
		t.Fatalf("a bad --skip: %d %s", code, e)
	}
}

// #29 re-review: recover says which friends' lists it left for the
// daemon, out of time or skipped.
func TestRecoverSaysWhatItLeftPending(t *testing.T) {
	parallel(t, 0)
	var w bytes.Buffer
	a := &app{}
	m := &manifest.Manifest{Content: manifest.Content{Buckets: []manifest.Bucket{{Name: "laptop", Profile: "restic", Copies: 2}}}}
	a.printRecovered(&w, m, recovery.Restored{}, []recovery.Result{
		{Place: "SLOW", Bucket: "laptop", Err: recovery.ErrOutOfTime},
		{Place: "SKIP", Bucket: "laptop", Err: recovery.ErrSkipped},
		{Place: "B", Bucket: "laptop", ListedCount: state.ListedCount{Listed: 1}},
	}, "", map[string]map[string]int64{"laptop": {"B": 1}}, nil, nil, 1)
	out := w.String()
	for _, s := range []string{"at SLOW: not finished in time", "at SKIP: skipped", "at B: 1 objects", "lendisk serve", "is not finished yet, so the counts above may change"} {
		if !strings.Contains(out, s) {
			t.Errorf("lacks %q:\n%s", s, out)
		}
	}
}
