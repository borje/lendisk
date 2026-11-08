package alert

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// #24 review: a friend toggling its number every second, so the quota
// alerts' conditions flip at every pass, raises each alert once: a
// quota alert stays raised until its condition has been false for
// QuotaHold, so the hook and the webhook get one delivery per window.
func TestToggledNumbersRaiseAQuotaAlertOnce(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	addFriend(t, db, "B")
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	if _, _, err := db.SetOwnNumber(ctx, "B", 100<<30, 0); err != nil {
		t.Fatal(err)
	}
	if err := db.AddLentUsage(ctx, "B", 50<<30); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QuotaAnswered(ctx, "B", 100<<30, c.now(), state.QuotaAnswer{Number: 1 << 40, Lent: 50 << 30}, c.now()); err != nil {
		t.Fatal(err)
	}
	var delivered int
	s := &Service{DB: db, Log: quiet, Now: c.now, Command: "/bin/true",
		RunCommand: func(context.Context, string, []byte) error { delivered++; return nil }}
	for i := range 30 {
		number := int64(100 << 30)
		if i%2 == 0 {
			number = 10 << 30 // below both sides' use
		}
		if _, err := db.QuotaReported(ctx, "B", state.QuotaReport{Number: number}, c.now()); err != nil {
			t.Fatal(err)
		}
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
		s.deliverPending(ctx, time.Millisecond)
		c.set(c.now().Add(time.Second))
	}
	if delivered != 2 { // quota_exhausted and lent_over_quota, once each
		t.Fatalf("%d deliveries while the number toggled, want 2", delivered)
	}
	// Steady below use for QuotaHold: cleared.
	if _, err := db.QuotaReported(ctx, "B", state.QuotaReport{Number: 100 << 30}, c.now()); err != nil {
		t.Fatal(err)
	}
	c.set(c.now().Add(DefaultQuotaHold))
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("after %v steady: %v", DefaultQuotaHold, as)
	}
}

// OPS-2, PEER-6 (#24): this node's use of a friend's space above 90% of
// the quota raises quota_high; at or above the quota, or with the
// friend's last answer a 507, quota_exhausted instead. A quota lowered
// below what the friend stores here raises lent_over_quota: both sides
// of a lowering below use are told. Each clears when the use falls or
// the quota rises.
func TestQuotaUseRaisesAlerts(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	now := c.now()
	// later lets a cleared condition outlast QuotaHold.
	later := func() { c.set(c.now().Add(DefaultQuotaHold)) }
	set := func(peer string, own, use int64) {
		t.Helper()
		if _, _, err := db.SetOwnNumber(ctx, peer, own, 0); err != nil {
			t.Fatal(err)
		}
		// The friend's answer sets this node's count of its use there.
		if _, err := db.QuotaAnswered(ctx, peer, own, now, state.QuotaAnswer{Number: 1 << 40, Lent: use}, now); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{DB: db, Log: quiet, Now: c.now}
	eval := func() map[string]state.Alert {
		t.Helper()
		if err := s.evaluate(ctx); err != nil {
			t.Fatal(err)
		}
		return active(t, db)
	}
	set("B", 100<<30, 90<<30)
	set("C", 100<<30, 91<<30)
	as := eval()
	if _, ok := as[KindQuotaHigh+":B"]; ok || len(as) != 1 {
		t.Fatalf("at 90%% and 91%%: %v", as)
	}
	if a := as[KindQuotaHigh+":C"]; !strings.Contains(a.Message, "C") || !strings.Contains(a.Message, "91%") {
		t.Fatalf("quota_high: %q", a.Message)
	}
	set("C", 80<<30, 91<<30) // lowered below use
	as = eval()
	if a, ok := as[KindQuotaExhausted+":C"]; !ok || len(as) != 1 || !strings.Contains(a.Message, "nothing is deleted") {
		t.Fatalf("lowered below use: %v", as)
	}
	set("C", 200<<30, 91<<30)
	later()
	if as := eval(); len(as) != 0 {
		t.Fatalf("after a raise: %v", as)
	}
	// A 507 from the friend is exhaustion whatever this node counts.
	if err := db.SetPlaceCondition(ctx, "B", state.ConditionQuota, "QuotaExceeded", now); err != nil {
		t.Fatal(err)
	}
	if as := eval(); len(as) != 1 || as[KindQuotaExhausted+":B"].Kind == "" {
		t.Fatalf("a friend answering 507: %v", as)
	}
	if err := db.ResumePlace(ctx, "B", now); err != nil {
		t.Fatal(err)
	}
	later()

	// The lender's side: B stores 50G here; this node lowers to 40G.
	if err := db.AddLentUsage(ctx, "B", 50<<30); err != nil {
		t.Fatal(err)
	}
	set("B", 40<<30, 0)
	as = eval()
	a, ok := as[KindLentOverQuota+":B"]
	if !ok || len(as) != 1 || !strings.Contains(a.Message, "no new objects") || !strings.Contains(a.Message, "nothing is deleted") {
		t.Fatalf("lowered below what the friend stores: %v", as)
	}
	set("B", 60<<30, 0)
	later()
	if as := eval(); len(as) != 0 {
		t.Fatalf("after the lender raised its number: %v", as)
	}
}
