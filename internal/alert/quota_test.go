package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

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
	now := time.Now()
	set := func(peer string, own, use int64) {
		t.Helper()
		if _, _, err := db.SetOwnNumber(ctx, peer, own, 0); err != nil {
			t.Fatal(err)
		}
		// The friend's answer sets this node's count of its use there.
		if _, err := db.QuotaAnswered(ctx, peer, own, state.QuotaAnswer{Number: 1 << 40, Lent: use}, now); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{DB: db, Log: quiet}
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
	if as := eval(); len(as) != 0 {
		t.Fatalf("after the lender raised its number: %v", as)
	}
}
