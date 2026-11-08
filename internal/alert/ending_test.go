package alert

import (
	"strings"
	"testing"
	"time"
)

// PEER-5 (#25): a friendship the friend ended raises friend_removed for
// it at once, saying the friend's copies no longer count and when the
// replicas are purged; one this node ended raises nothing then. Seven
// days before the purge, either raises replica_purge_soon, which names
// the date and how to keep them. Both clear once the former friend is
// forgotten (the purge).
func TestEndingAFriendshipIsAlertedAndSoIsItsPurge(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	addFriend(t, db, "C")
	now := time.Unix(1_800_000_000, 0)
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	if _, err := db.RemoveFriendship(t.Context(), "B", true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RemoveFriendship(t.Context(), "C", false, now); err != nil {
		t.Fatal(err)
	}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	a, ok := as[KindFriendRemoved+":B"]
	if !ok || !strings.Contains(a.Message, "B ended the friendship") || !strings.Contains(a.Message, "2027-02-14") {
		t.Fatalf("alerts after B ended it: %v", as)
	}
	if _, ok := as[KindFriendRemoved+":C"]; ok {
		t.Fatalf("this node's own removal of C raised friend_removed: %v", as)
	}
	if _, ok := as[KindPurgeSoon+":B"]; ok {
		t.Fatalf("the purge alert raised 30 days early: %v", as)
	}
	now = now.Add(23*24*time.Hour - time.Second)
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := active(t, db)[KindPurgeSoon+":C"]; ok {
		t.Fatal("the purge alert raised more than 7 days before the purge")
	}
	now = now.Add(time.Second)
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as = active(t, db)
	for _, p := range []string{"B", "C"} {
		a, ok := as[KindPurgeSoon+":"+p]
		if !ok || !strings.Contains(a.Message, "2027-02-14") || !strings.Contains(a.Message, "lendisk invite") {
			t.Fatalf("7 days before the purge of %s: %v", p, as)
		}
	}
	for _, p := range []string{"B", "C"} {
		if err := db.ForgetFormerFriend(t.Context(), p); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts after the purge: %v", as)
	}
}
