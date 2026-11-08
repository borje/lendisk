package alert

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// EP-11 (#22): a key written with other bytes than a friend
// acknowledged raises key_changed for its bucket, naming the key and
// both hashes; a lender's 412 to a key that is not rewritable raises the
// same kind, not place_refused. The alert is a condition: it clears
// when the key is written back to the acknowledged bytes or deleted.
func TestAChangedKeyRaisesKeyChangedUntilWrittenBackOrDeleted(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	put := func(bucket, key, sha string) int64 {
		t.Helper()
		s, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, s[0], 1, sha); err != nil {
			t.Fatal(err)
		}
		return s[0]
	}
	ackAll := func(seq int64, sha string) {
		t.Helper()
		for _, p := range []string{"B", "C"} {
			if err := db.AckPlacement(ctx, p, seq, sha, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := &Service{DB: db, Log: quiet}
	eval := func() map[string]state.Alert {
		t.Helper()
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
		return active(t, db)
	}
	ackAll(put("files", "doc", "aaa"), "aaa")
	put("files", "doc", "bbb")
	as := eval()
	a, ok := as["key_changed:files"]
	if !ok || len(as) != 1 || a.Kind != KindKeyChanged || a.Subject != "files" {
		t.Fatalf("alerts %+v", as)
	}
	for _, want := range []string{"doc", "aaa", "bbb", "not replicated"} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("message %q lacks %q", a.Message, want)
		}
	}

	// Written back: cleared.
	put("files", "doc", "aaa")
	if as := eval(); len(as) != 0 {
		t.Fatalf("after the write back %+v", as)
	}
	// Changed again, then deleted: cleared.
	put("files", "doc", "bbb")
	if as := eval(); as["key_changed:files"].Key == "" {
		t.Fatalf("changed again %+v", as)
	}
	d, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "files", Key: "doc", Op: state.OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, d...); err != nil {
		t.Fatal(err)
	}
	if as := eval(); len(as) != 0 {
		t.Fatalf("after the delete %+v", as)
	}

	// A lender's 412: the same kind, with the hash it said it holds.
	seq := put("laptop", "data/1", "ppp")
	if err := db.RefuseChanged(ctx, "B", seq, "qqq", "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	as = eval()
	a, ok = as["key_changed:laptop"]
	if !ok || len(as) != 1 || !strings.Contains(a.Message, "qqq") || !strings.Contains(a.Message, "ppp") {
		t.Fatalf("after a 412 %+v", as)
	}
}
