package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// VER-5, OPS-2 (#31): rotten own objects raise bitrot for their bucket,
// naming what became of them, while one is not repaired and for a week
// after the last repair; a friend's rotten objects raise lent_bitrot
// for that friend, saying this node cannot repair them, while it holds
// them.
func TestBitrotIsAlertedOnBothSides(t *testing.T) {
	db := openDB(t)
	ctx := t.Context()
	now := time.Unix(1_800_000_000, 0)
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	kept := state.Rot{Bucket: "laptop", Key: "data/aa", Seq: 1, SHA256: "h1", Found: "x1", Size: 9, FoundAt: now,
		Detail: "no friend holds the version captured"}
	fixed := state.Rot{Bucket: "photos", Key: "data/bb", Seq: 2, SHA256: "h2", Found: "x2", Size: 9, FoundAt: now,
		RemovedAt: now, RepairedAt: now, Detail: "fetched again from a friend"}
	lent := state.Rot{Bucket: "lent-0123456789abcdef-s1", Key: "data/cc", Borrower: "FRIEND", Sealed: "s1", SHA256: "h3",
		Found: "x3", Size: 9, FoundAt: now}
	for _, r := range []state.Rot{kept, fixed, lent} {
		if err := db.RecordRot(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	if len(as) != 3 {
		t.Fatalf("alerts %v", as)
	}
	for key, wants := range map[string][]string{
		KindBitrot + ":laptop":     {"data/aa", "kept", "disk"},
		KindBitrot + ":photos":     {"data/bb", "fetched again", "disk"},
		KindLentBitrot + ":FRIEND": {"data/cc", "cannot repair", "disk"},
	} {
		a, ok := as[key]
		if !ok {
			t.Fatalf("no %s in %v", key, as)
		}
		for _, w := range wants {
			if !strings.Contains(a.Message, w) {
				t.Errorf("%s lacks %q: %s", key, w, a.Message)
			}
		}
	}

	// A week after its repair the repaired one clears; the kept one holds
	// until repaired; the friend's ends when the object is gone.
	now = now.Add(state.RotShown + time.Minute)
	if err := db.DropRot(ctx, lent.Bucket, lent.Key); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 1 || as[KindBitrot+":laptop"].Kind == "" {
		t.Fatalf("alerts a week on %v", as)
	}
}
