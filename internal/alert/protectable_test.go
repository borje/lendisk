package alert

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

const mib = int64(1) << 20

// capture records a done put of size bytes in bucket laptop.
func capture(t *testing.T, db *state.DB, key, class string, size int64) {
	t.Helper()
	s, err := db.BeginCaptures(t.Context(), []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpPut, Class: class}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(t.Context(), s[0], size, "h-"+key); err != nil {
		t.Fatal(err)
	}
}

func addLaptop(t *testing.T, db *state.DB) {
	t.Helper()
	if err := db.AddBucket(t.Context(), state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

// OPS-2, LEND-7, LEND-8 (#37): the repository above 90% of the
// protectable size raises protectable_size, naming per friendship the
// number that would cover it; it clears once the numbers are agreed,
// after QuotaHold, as the other quota alerts do. The LEND-8 AC's
// figures, in MiB: A, B and C friends at 50; A grows to 60.
func TestRepositoryNearTheProtectableSizeNamesANumberPerFriendship(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	for _, p := range []string{"B", "C"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: p, Address: "tc" + p, OwnNumber: 50 * mib, PeerNumber: 50 * mib,
			CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	addLaptop(t, db)
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	s := &Service{DB: db, Log: quiet, Now: c.now}
	capture(t, db, "index/1", "metadata", 300<<10)
	capture(t, db, "data/1", "data", 40*mib)
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("40 MiB of about 50 protectable: %v", as)
	}
	capture(t, db, "data/2", "data", 20*mib) // past what B and C protect
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	a, ok := active(t, db)[KindProtectable]
	if !ok {
		t.Fatalf("no %s: %v", KindProtectable, active(t, db))
	}
	for _, p := range []string{"B", "C"} {
		m := regexp.MustCompile(`friend ` + p + `: (\d+)M\b`).FindStringSubmatch(a.Message)
		if m == nil {
			t.Fatalf("no number for %s: %s", p, a.Message)
		}
		if n, _ := strconv.Atoi(m[1]); n < 60 || n > 80 {
			t.Errorf("%s: %dM, want 60..80", p, n)
		}
	}
	for _, want := range []string{"protectable size", "lendisk peer quota"} {
		if !strings.Contains(a.Message, want) {
			t.Errorf("message lacks %q: %s", want, a.Message)
		}
	}
	for _, p := range []string{"B", "C"} {
		if _, _, err := db.SetOwnNumber(ctx, p, 80*mib, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := db.QuotaReported(ctx, p, state.QuotaReport{Number: 80 * mib}, c.now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := active(t, db)[KindProtectable]; !ok {
		t.Fatal("cleared before QuotaHold")
	}
	c.set(c.now().Add(DefaultQuotaHold))
	if err := s.evaluate(ctx); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("with the numbers agreed: %v", as)
	}
}

// OPS-2 (#37): with fewer friends than copies no quota covers it; the
// alert says a friend or an overflow target is needed.
func TestTooFewFriendsToProtectSaysSo(t *testing.T) {
	db := openDB(t)
	addFriend(t, db, "B")
	addLaptop(t, db)
	capture(t, db, "data/1", "data", mib)
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(context.Background()); err != nil {
		t.Fatal(err)
	}
	a, ok := active(t, db)[KindProtectable]
	if !ok || !strings.Contains(a.Message, "another friend") {
		t.Fatalf("%+v", a)
	}
}
