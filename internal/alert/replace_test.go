package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// OPS-2, REPL-13 (#26): objects below copies for longer than the
// re-placement threshold (replication.replace_after) raise below_copies
// for the bucket, which re-placement cannot fix: the pool needs a
// friend or more quota. Not before the threshold, and it clears once a
// pass finds none below.
func TestObjectsBelowCopiesPastTheThresholdAreAlerted(t *testing.T) {
	db := openDB(t)
	now := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return now }
	if err := db.AddBucket(t.Context(), state.Bucket{Name: "laptop", Profile: "restic", Copies: 3,
		AccessKey: "AK", SecretKey: "s", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	seqs, err := db.BeginCaptures(t.Context(), []state.Capture{{Bucket: "laptop", Key: "data/1", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(t.Context(), seqs[0], 1000, "h"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Replace(t.Context(), nil, nil); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet, BelowAfter: 7 * 24 * time.Hour, Now: func() time.Time { return now }}
	evaluate := func() map[string]state.Alert {
		t.Helper()
		if err := s.evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
		return active(t, db)
	}
	if _, ok := evaluate()[KindBelowCopies+":laptop"]; ok {
		t.Fatal("below_copies before the threshold")
	}
	now = now.Add(7*24*time.Hour + time.Minute)
	if _, err := db.Replace(t.Context(), nil, nil); err != nil {
		t.Fatal(err)
	}
	a, ok := evaluate()[KindBelowCopies+":laptop"]
	if !ok || !strings.Contains(a.Message, "1 object") || !strings.Contains(a.Message, "3") {
		t.Fatalf("alerts %v", active(t, db))
	}
	addFriend(t, db, "D")
	if _, err := db.Replace(t.Context(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := evaluate()[KindBelowCopies+":laptop"]; ok {
		t.Fatal("below_copies after a third friend took the copy")
	}
}
