package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// #29 re-review: a friend whose list is abandoned (past the cap) leaves
// nothing behind: the keys only it listed are no conflicts, which
// nobody could resolve, but gone, so it cannot flood status.
func TestAbandonedListLeavesNoConflicts(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "X")
	list(t, db, "B", "laptop", Listed{Key: "a", Size: 1, SHA256: "ha", Class: "data"})
	var flood []Listed
	for i := range 200 {
		flood = append(flood, Listed{Key: fmt.Sprintf("k%04d", i), Size: 1, SHA256: "hx", Class: "data"})
	}
	if _, err := db.RecordListed(ctx, "X", "laptop", flood, len(flood), "k0199", false); err != nil {
		t.Fatal(err)
	}
	if err := db.AbandonRelist(ctx, "X", "laptop"); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	ks, err := db.RecoveredKeys(ctx, "laptop")
	if err != nil || len(ks) != 1 || ks[0].Key != "a" {
		t.Fatalf("recovered keys after abandoning X: %d %v", len(ks), err)
	}
	if cs, _ := db.Conflicts(ctx); len(cs) != 0 {
		t.Fatalf("%d conflicts", len(cs))
	}
	if es, _ := db.QueueEntries(ctx, "laptop"); len(es) != 1 {
		t.Fatalf("%d queue rows", len(es))
	}
	if rs := mustRelists(t, db); len(rs) != 0 {
		t.Fatalf("an abandoned list is still to read: %+v", rs)
	}
}

// #29 re-review: for the lone-metadata rule only places that listed the
// bucket to its end count; an abandoned or pending friend's silence is
// no evidence.
func TestLoneMetadataCountsOnlyFinishedLists(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "X", "P")
	list(t, db, "B", "laptop", Listed{Key: "index/9", Size: 1, SHA256: "h9", Class: "metadata"})
	if err := db.AbandonRelist(ctx, "X", "laptop"); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	// P is still pending.
	if v := verdict(t, db, "laptop", "index/9"); v.Verdict != VerdictSingle {
		t.Fatalf("with no other finished list: %+v", v)
	}
	list(t, db, "P", "laptop")
	v := verdict(t, db, "laptop", "index/9")
	if v.Verdict != VerdictConflict || !strings.Contains(v.Reason, "P") || strings.Contains(v.Reason, "X") ||
		!strings.Contains(v.Reason, "to its end") {
		t.Fatalf("after P finished: %+v", v)
	}
}

// #29 re-review: resolve settles a conflict only; a key that is not
// one (held, or written on this node since recovery) is refused.
func TestResolveRefusesWhatIsNoConflict(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop", Listed{Key: "x", Size: 1, SHA256: "h1", Class: "data"},
		Listed{Key: "doc", Size: 3, SHA256: "h-other", Class: "data"})
	list(t, db, "C", "laptop", Listed{Key: "x", Size: 1, SHA256: "h1", Class: "data"},
		Listed{Key: "doc", Size: 3, SHA256: "h-third", Class: "data"})
	if err := db.ResolveKey(ctx, "laptop", "x", "B"); !errors.Is(err, ErrNotConflict) {
		t.Fatalf("a held key: %v", err)
	}
	if cs, _ := db.Conflicts(ctx); len(cs) != 1 || cs[0].Key != "doc" {
		t.Fatalf("conflicts %+v", cs)
	}
	capture(t, db, "laptop", "doc", OpPut) // written here since: "h-doc"
	if err := db.ResolveKey(ctx, "laptop", "doc", "B"); !errors.Is(err, ErrNotConflict) {
		t.Fatalf("a key written here: %v", err)
	}
	if cs, _ := db.Conflicts(ctx); len(cs) != 0 {
		t.Fatalf("the node's own doc is still shown in conflict: %+v", cs)
	}
	if _, ok, _ := db.KeyConflict(ctx, "laptop", "doc"); ok {
		t.Fatal("the read-through would refuse the node's own doc")
	}
}

// #29 re-review: pages count apart from entries, so a list of empty
// pages is bounded too.
func TestRelistCountsPages(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B")
	for i, next := range []string{"a", "b", "c"} {
		if _, err := db.RecordListed(ctx, "B", "laptop", nil, 0, next, false); err != nil {
			t.Fatal(err)
		}
		if rs := mustRelists(t, db); rs[0].Pages != int64(i+1) || rs[0].Seen != 0 {
			t.Fatalf("after %d pages: %+v", i+1, rs[0])
		}
	}
}
