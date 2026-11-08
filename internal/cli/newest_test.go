package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/state"
)

// ID-5 (#74 review, M1): a recovered rewritable key whose friends
// disagreed, decided as the newest version because one friend's
// rewrites replaced the others' version, is shown to the user: in
// `status` (text and JSON) and in recover's output, naming the friend
// whose replaced versions decided it, and how to pick another with
// `bucket resolve`.
func TestStatusAndRecoverShowNewestVerdicts(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "vault", "kopia")
	db := openDB(t, n)
	ctx := t.Context()
	for _, f := range []string{"B", "C"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"vault"}); err != nil {
		t.Fatal(err)
	}
	rec := func(place string, objs ...state.Listed) {
		if _, err := db.RecordListed(ctx, place, "vault", objs, len(objs), "", true); err != nil {
			t.Fatal(err)
		}
		if err := db.SettleFinished(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rec("B", state.Listed{Key: "kopia.maintenance", Size: 1, SHA256: "h2", Class: "metadata", Rewritable: true, Replaced: []string{"h1"}})
	rec("C", state.Listed{Key: "kopia.maintenance", Size: 1, SHA256: "h1", Class: "metadata", Rewritable: true})

	st := n.status()
	if len(st.Recovery.Conflicts) != 0 {
		t.Fatalf("conflicts %+v", st.Recovery.Conflicts)
	}
	ns := st.Recovery.Newest
	if len(ns) != 1 || ns[0].Bucket != "vault" || ns[0].Key != "kopia.maintenance" || ns[0].SHA256 != "h2" ||
		ns[0].DecidedBy != "B" || ns[0].Places["C"] != "h1" || ns[0].Reason == "" {
		t.Fatalf("newest %+v", ns)
	}
	_, out, _ := n.run("", "", "status")
	for _, s := range []string{"kopia.maintenance", "B replaced", "lendisk bucket resolve vault kopia.maintenance C"} {
		if !strings.Contains(out, s) {
			t.Errorf("status text lacks %q:\n%s", s, out)
		}
	}

	newest, err := db.NewestKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var w bytes.Buffer
	a := &app{}
	m := &manifest.Manifest{Content: manifest.Content{Buckets: []manifest.Bucket{{Name: "vault", Profile: "kopia", Copies: 2}}}}
	a.printRecovered(&w, m, recovery.Restored{}, nil, "", map[string]map[string]int64{}, nil, newest, 0)
	for _, s := range []string{"1 object", "newest", "kopia.maintenance", "B", "bucket resolve"} {
		if !strings.Contains(w.String(), s) {
			t.Errorf("recover's output lacks %q:\n%s", s, w.String())
		}
	}
}
