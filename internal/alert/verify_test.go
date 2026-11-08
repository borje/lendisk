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

// SEC-7, VER-1, OPS-2 (#27 review): a replica holding acknowledged data
// that has not passed a challenge for UncheckedAfter (3 days) raises
// replica_unchecked, without draining it: a friend that answers every
// challenge with an error (a 5xx) is never failed, and one whose FIFO
// never empties (at quota, a slow uplink) was never challenged, but
// neither stays silent. Counted from the last pass, or from the first
// acknowledgement when there was none; cleared by a pass.
func TestAReplicaNeverVerifiedIsAlerted(t *testing.T) {
	db := openDB(t)
	t0 := time.Unix(1_800_000_000, 0)
	now := t0
	ctx := t.Context()
	db.Clock = func() time.Time { return now }
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 1,
		AccessKey: "AK", SecretKey: "s", CreatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/1", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], 100, "h"); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, "B", seqs[0], "h", t0); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	key := KindReplicaUnchecked + ":B/laptop"
	raised := func() (state.Alert, bool) {
		t.Helper()
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
		a, ok := active(t, db)[key]
		return a, ok
	}
	// A friend that answers every challenge with a 5xx.
	for _, d := range []time.Duration{time.Hour, 24 * time.Hour, 48 * time.Hour} {
		now = t0.Add(d)
		if err := db.RecordCheckError(ctx, "B", "laptop", "the peer refused (500): reading failed", now); err != nil {
			t.Fatal(err)
		}
		if _, ok := raised(); ok {
			t.Fatalf("raised %v after the first acknowledgement", d)
		}
	}
	now = t0.Add(3*24*time.Hour + time.Minute)
	if a, ok := raised(); !ok || !strings.Contains(a.Message, "reading failed") || !strings.Contains(a.Message, "laptop") {
		t.Fatalf("alerts %v", active(t, db))
	}
	if cs, _ := db.ReplicaChecks(ctx); cs[0].Unverified() {
		t.Fatal("an unchecked replica was marked unverified")
	}
	if err := db.RecordVerified(ctx, "B", "laptop", false, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := raised(); ok {
		t.Fatal("still raised after a pass")
	}
	// A friend never challenged since (its FIFO never empties).
	now = now.Add(3*24*time.Hour + time.Minute)
	if _, ok := raised(); !ok {
		t.Fatalf("no alert 3 days after the last pass: %v", active(t, db))
	}
	s.UncheckedAfter = 7 * 24 * time.Hour
	if _, ok := raised(); ok {
		t.Fatal("raised before UncheckedAfter")
	}
}
