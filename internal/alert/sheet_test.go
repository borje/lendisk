package alert

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ID-7 (#33): a printed recovery sheet that no longer matches the
// node's state (a friend's address changed, a friend added or removed)
// raises an alert naming what changed and saying to print it again; it
// clears once the sheet matches, and a node that never printed one, or
// a Service with no Sheet, is not alerted.
func TestStaleRecoverySheetRaisesAlert(t *testing.T) {
	db := openDB(t)
	printed := time.Unix(1_800_000_000, 0)
	var changes []string
	at := time.Time{}
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return printed.Add(time.Hour) },
		Sheet: func(context.Context) (time.Time, []string, error) { return at, changes, nil }}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerted with nothing printed: %v", as)
	}
	at, changes = printed, []string{"friend B's address changed", "friend C added"}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	a, ok := as[KindRecoverySheetStale]
	if len(as) != 1 || !ok || a.Subject != "" ||
		!strings.Contains(a.Message, "friend B's address changed, friend C added") ||
		!strings.Contains(a.Message, "lendisk recovery-sheet") || !strings.Contains(a.Message, "2027-01-15T08:00:00Z") {
		t.Fatalf("alerts %+v", as)
	}
	changes = nil
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("not cleared: %v", as)
	}
	s.Sheet = nil
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
}
