package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// COMPAT-1, OPS-2 (#79): a bucket marked as holding a Kopia format-1
// repository raises kopia_format1 once, naming the upgrade command; it
// stays raised over later passes, clears once the upgrade's marker is
// captured, and clears when the bucket is deleted.
func TestKopiaFormat1BucketIsAlertedUntilUpgraded(t *testing.T) {
	db := openDB(t)
	for _, name := range []string{"vault", "old"} {
		if err := db.AddBucket(t.Context(), state.Bucket{Name: name, Profile: "kopia", Copies: 1,
			AccessKey: "AK" + name, SecretKey: "s", CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	addFriend(t, db, "B")
	now := time.Unix(1_800_000_000, 0)
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts before any format-1 key: %v", as)
	}
	for _, name := range []string{"vault", "old"} {
		if _, err := db.MarkFormat1(t.Context(), name, "n0123", now); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := s.evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Hour)
	}
	as := active(t, db)
	a, ok := as[KindKopiaFormat1+":vault"]
	if !ok || len(as) != 2 || a.Raise != 1 {
		t.Fatalf("alerts %v", as)
	}
	for _, want := range []string{"vault", "format version 1", "not support", "kopia repository set-parameters --upgrade", "n0123"} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("message lacks %q: %s", want, a.Message)
		}
	}

	if err := db.MarkUpgraded(t.Context(), "vault", now); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteBucket(t.Context(), "old"); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts after the upgrade and the delete: %v", as)
	}
}
