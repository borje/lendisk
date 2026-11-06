package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func keys(as []Alert) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Key)
	}
	return out
}

// OPS-2 (#13): an alert is persisted once while its condition holds,
// its message kept current; it is cleared when the condition ends, and
// raised anew, undelivered, when it comes back.
func TestAlertsDeduplicatedWhileHeldAndClearedWhenEnded(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	t0 := time.Unix(1_800_000_000, 0)
	kinds := []string{"friend_unreachable", "store_unreadable"}
	b := Alert{Key: "friend_unreachable:B", Kind: "friend_unreachable", Subject: "B", Message: "B is gone"}
	s := Alert{Key: "store_unreadable", Kind: "store_unreadable", Message: "disk"}

	raised, cleared, err := db.SyncAlerts(ctx, kinds, []Alert{b, s}, t0)
	if err != nil || len(raised) != 2 || len(cleared) != 0 {
		t.Fatalf("first sync: raised %v cleared %v err %v", keys(raised), keys(cleared), err)
	}
	if err := db.MarkAlertDelivered(ctx, raised[0], ChannelHook, t0); err != nil {
		t.Fatal(err)
	}

	// The same condition again: nothing new, the message updated.
	b2 := b
	b2.Message = "B is still gone"
	raised, cleared, err = db.SyncAlerts(ctx, kinds, []Alert{b2, s}, t0.Add(time.Minute))
	if err != nil || len(raised) != 0 || len(cleared) != 0 {
		t.Fatalf("second sync: raised %v cleared %v err %v", keys(raised), keys(cleared), err)
	}
	active, err := db.ActiveAlerts(ctx)
	if err != nil || len(active) != 2 {
		t.Fatalf("active %v %v", keys(active), err)
	}
	for _, a := range active {
		if !a.RaisedAt.Equal(t0) {
			t.Errorf("%s raised at %v, want %v", a.Key, a.RaisedAt, t0)
		}
		if a.Key == b.Key && (a.Message != "B is still gone" || a.HookAt.IsZero()) {
			t.Errorf("B's alert %+v", a)
		}
	}

	// An alert of a kind this sync does not own is left alone.
	other := Alert{Key: "quota:B", Kind: "quota", Subject: "B", Message: "90%"}
	if _, _, err := db.SyncAlerts(ctx, []string{"quota"}, []Alert{other}, t0); err != nil {
		t.Fatal(err)
	}

	// The store recovers: its alert clears, B's stays.
	raised, cleared, err = db.SyncAlerts(ctx, kinds, []Alert{b2}, t0.Add(2*time.Minute))
	if err != nil || len(raised) != 0 || len(cleared) != 1 || cleared[0].Key != s.Key {
		t.Fatalf("third sync: raised %v cleared %v err %v", keys(raised), keys(cleared), err)
	}
	if active, _ := db.ActiveAlerts(ctx); len(active) != 2 || active[0].Key != b.Key || active[1].Key != other.Key {
		t.Fatalf("active after clearing %v", keys(active))
	}

	// It fails again: raised anew, not yet delivered.
	t3 := t0.Add(3 * time.Minute)
	raised, _, err = db.SyncAlerts(ctx, kinds, []Alert{b2, s}, t3)
	if err != nil || len(raised) != 1 || raised[0].Key != s.Key || !raised[0].RaisedAt.Equal(t3) ||
		!raised[0].HookAt.IsZero() || !raised[0].WebhookAt.IsZero() {
		t.Fatalf("re-raise %+v %v", raised, err)
	}

	// A delivery of an earlier raise does not mark a later one.
	old := raised[0]
	old.RaisedAt = t0
	if err := db.MarkAlertDelivered(ctx, old, ChannelWebhook, t3); err != nil {
		t.Fatal(err)
	}
	for _, a := range mustActive(t, db) {
		if a.Key == s.Key && !a.WebhookAt.IsZero() {
			t.Fatal("an earlier raise's delivery marked the later one")
		}
	}

	// Persisted: a reopened database has the same alerts.
	db.Close()
	db2, err := Open(ctx, filepath.Dir(db.Path), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if active := mustActive(t, db2); len(active) != 3 {
		t.Fatalf("after reopening %v", keys(active))
	}
}

func mustActive(t *testing.T, db *DB) []Alert {
	t.Helper()
	as, err := db.ActiveAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return as
}

// OPS-2 (#13): the alert evaluator counts each place's own refusals.
func TestRefusedAtPlaces(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	if err := db.AddFriendship(ctx, Friendship{Peer: "B", Address: "tcB", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, k := range []string{"a", "b", "c"} {
		s, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: k, Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, s[0], 1, "h"); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s[0])
	}
	if err := db.RefusePlacement(ctx, "B", seqs[0], "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RefuseObject(ctx, seqs[1], CauseStore, "gone"); err != nil {
		t.Fatal(err)
	}
	r, err := db.RefusedAtPlaces(ctx)
	if err != nil || len(r) != 1 || r["B"] != 1 {
		t.Fatalf("refused at places %v %v", r, err)
	}
}
