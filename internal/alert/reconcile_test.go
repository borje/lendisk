package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// EP-14, OPS-2 (#8): deletes the store reconciler held back raise
// reconcile_held for the bucket, naming the count and the command that
// allows them, until a pass ends the hold; a missing store marker
// raises store_marker_missing until it is back.
func TestReconcileHoldAndMissingMarkerAreAlerted(t *testing.T) {
	db := openDB(t)
	ctx := t.Context()
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 1, AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	now := time.Unix(1_800_000_000, 0)
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	if err := db.FinishReconcile(ctx, "laptop", now, 40, 50, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.SetNode(ctx, state.NodeMarkerMissing, "/srv/store/.lendisk/store-marker"); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	h, ok := as[KindReconcileHeld+":laptop"]
	if !ok {
		t.Fatalf("alerts %v", as)
	}
	for _, want := range []string{"laptop", "40", "50", "lendisk bucket reconcile laptop --allow-deletes 40"} {
		if !strings.Contains(h.Message, want) {
			t.Errorf("held message lacks %q: %s", want, h.Message)
		}
	}
	m, ok := as[KindStoreMarkerMissing]
	if !ok || !strings.Contains(m.Message, "/srv/store/.lendisk/store-marker") || !strings.Contains(m.Message, "touch") {
		t.Fatalf("marker alert %+v in %v", m, as)
	}
	if err := db.FinishReconcile(ctx, "laptop", now, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.SetNode(ctx, state.NodeMarkerMissing, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts after the hold ended and the marker came back: %v", as)
	}
}
