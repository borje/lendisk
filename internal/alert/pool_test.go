package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// OPS-2, EP-6 (#19): a bucket whose copies exceed the places in the pool
// raises pool_too_small, one per bucket; it clears when a friend joins.
// A friend that refuses this node (removed) is not a place.
func TestPoolWithFewerPlacesThanCopiesIsAlerted(t *testing.T) {
	db := openDB(t)
	for _, b := range []struct {
		name   string
		copies int
	}{{"laptop", 2}, {"photos", 1}} {
		if err := db.AddBucket(t.Context(), state.Bucket{Name: b.name, Profile: "restic", Copies: b.copies,
			AccessKey: "AK" + b.name, SecretKey: "s", CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionRemoved, "AccessDenied", time.Now()); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	a, ok := as[KindPoolTooSmall+":laptop"]
	if !ok || len(as) != 1 {
		t.Fatalf("alerts %v", as)
	}
	if !strings.Contains(a.Message, "laptop") || !strings.Contains(a.Message, "1 place") {
		t.Fatalf("message %q", a.Message)
	}
	addFriend(t, db, "D")
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts after D joined %v", as)
	}
}
