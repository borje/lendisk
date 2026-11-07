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
}
