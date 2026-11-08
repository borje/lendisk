package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// OPS-2, VER-1 (#27): a failed verification raises replica_unverified
// for that friend's replica of that bucket, naming both, what failed and
// how to clear it. It holds while the replica is unverified, clears when
// a full check passes, and is not raised for a former friend.
func TestAFailedVerificationIsAlerted(t *testing.T) {
	db := openDB(t)
	now := time.Unix(1_800_000_000, 0)
	ctx := t.Context()
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2,
		AccessKey: "AK", SecretKey: "s", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	evaluate := func() map[string]state.Alert {
		t.Helper()
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
		return active(t, db)
	}
	key := KindReplicaUnverified + ":B/laptop"
	if err := db.RecordCheckError(ctx, "B", "laptop", "connection reset", now); err != nil {
		t.Fatal(err)
	}
	if _, ok := evaluate()[key]; ok {
		t.Fatal("alerted on a challenge that got no answer")
	}
	if err := db.RecordUnverified(ctx, "B", "laptop", "friend B does not hold data/ab/x, which it acknowledged", now); err != nil {
		t.Fatal(err)
	}
	a, ok := evaluate()[key]
	if !ok || !strings.Contains(a.Message, "data/ab/x") || !strings.Contains(a.Message, "laptop") ||
		!strings.Contains(a.Message, "lendisk verify laptop --peer B --full") {
		t.Fatalf("alerts %v", active(t, db))
	}
	if err := db.RecordVerified(ctx, "B", "laptop", true, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := evaluate()[key]; ok {
		t.Fatal("still alerted after a full check passed")
	}
	if err := db.RecordUnverified(ctx, "B", "laptop", "lost", now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RemoveFriendship(ctx, "B", false, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := evaluate()[key]; ok {
		t.Fatal("alerted for a former friend")
	}
}
