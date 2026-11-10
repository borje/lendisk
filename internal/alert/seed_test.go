package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// REPL-8, OPS-2 (#95): a pending seed whose friend's list has confirmed
// nothing for state.SeedStaleAfter (7 days) after the export raises an
// alert naming the friend and bucket, saying when the hold ends and how
// to import or cancel it; it clears at the first confirmation, and a
// seed that ended is not alerted.
func TestAStaleSeedRaisesAlert(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	addFriend(t, db, "B")
	exported := time.Unix(1_800_000_000, 0)
	now := exported
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	for _, b := range []string{"laptop", "ended"} {
		if err := db.StartSeed(ctx, "B", b, 1, exported); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.EndSeed(ctx, "B", "ended", state.SeedCancelled, exported); err != nil {
		t.Fatal(err)
	}
	now = exported.Add(state.SeedStaleAfter - time.Minute)
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerted before a week: %v", as)
	}
	now = exported.Add(state.SeedStaleAfter)
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	a, ok := as[KindSeedStale+":B/laptop"]
	if len(as) != 1 || !ok || a.Subject != "B/laptop" ||
		!strings.Contains(a.Message, "lendisk import") || !strings.Contains(a.Message, "lendisk export --cancel laptop --for B") ||
		!strings.Contains(a.Message, exported.Add(state.SeedExpiry).UTC().Format(time.RFC3339)) {
		t.Fatalf("alerts %+v", as)
	}
	if err := db.SeedChecked(ctx, "B", "laptop", 1, "", now); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("not cleared by a confirmation: %v", as)
	}
}
