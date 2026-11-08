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

// OPS-2 (#19 review): a friend gone past replace_after is not a place
// in the pool, so a copies-2 bucket whose only other friend is gone
// raises pool_too_small.
func TestAGoneFriendIsNotAPlaceInThePool(t *testing.T) {
	db := openDB(t)
	if err := db.AddBucket(t.Context(), state.Bucket{Name: "laptop", Profile: "restic", Copies: 2,
		AccessKey: "AK", SecretKey: "s", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	now := time.Now()
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionOK, "", now.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(t.Context(), "C", state.ConditionRetrying, "dial: no route", now.Add(-8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a, ok := active(t, db)[KindPoolTooSmall+":laptop"]; !ok || !strings.Contains(a.Message, "1 place") {
		t.Fatalf("alerts %v", active(t, db))
	}
}

// OPS-2, REPL-4 (#19 review): copies dropped at a friend that is full
// or gone, with no other friend with room, raise copies_dropped for the
// bucket: its data is held at fewer places than copies.
func TestDroppedCopiesAreAlerted(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2,
		AccessKey: "AK", SecretKey: "s", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	// data/1, then the index that waits for it.
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/1", Op: state.OpPut, Class: "data"},
		{Bucket: "laptop", Key: "index/1", Op: state.OpPut, Class: "metadata"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range seqs {
		if _, err := db.FinishPut(ctx, s, 100, "h"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AckPlacement(ctx, "B", seqs[0], "h", time.Now()); err != nil {
		t.Fatal(err)
	}
	// C has answered 507 for two hours (reroute_after is 1 hour).
	if err := db.SetPlaceCondition(ctx, "C", state.ConditionQuota, "507", time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, dropped, err := db.Reroute(ctx, nil, nil); err != nil || dropped != 1 {
		t.Fatalf("dropped %d %v", dropped, err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	a, ok := as[KindCopiesDropped+":laptop"]
	if !ok || !strings.Contains(a.Message, "1 object") || len(as) != 1 {
		t.Fatalf("alerts %v", as)
	}
}

// OPS-2, REPL-1 (#20 second review): a completed upload whose row
// cannot be matched to the object in the store holds the bucket's
// metadata, and so every friend's queue, until it settles: it raises
// upload_unmatchable for its bucket once, naming what to do, and the
// alert clears when the row settles.
func TestAnUnmatchableUploadIsAlertedUntilItSettles(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	addFriend(t, db, "B")
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/big", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkUnmatchable(ctx, seqs[0], "no upload ID"); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	for range 2 {
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	as := active(t, db)
	a, ok := as[KindUploadUnmatchable+":laptop"]
	if !ok || len(as) != 1 || a.Raise != 1 || !strings.Contains(a.Message, "restore the store's xattrs or rewrite the key") ||
		!strings.Contains(a.Message, "data/big") {
		t.Fatalf("alerts %+v", as)
	}
	if _, err := db.FinishPut(ctx, seqs[0], 10, "h"); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts after the row settled %+v", as)
	}
}
