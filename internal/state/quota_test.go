package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// LEND-1, LEND-8 (#24): a node's own number for a friendship changes at
// once, and the quota with it; a value that takes the sum of the node's
// numbers (its friendships, its open invites and its redeems in
// progress) past lending.max_total is refused and changes nothing.
func TestOwnNumberRefusedPastTheMaxTotal(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 50)
	p.friend("C", 50)
	if err := p.db.AddInvite(ctx, Invite{Key: "ik", Number: 20 * unit, Retention: time.Hour, ExpiresAt: p.now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := p.db.StartRedeem(ctx, PendingRedeem{InviteKey: "ik2", Peer: "D", Number: 10 * unit, StartedAt: p.now}); err != nil {
		t.Fatal(err)
	}
	total, err := p.db.LentNumbers(ctx, p.now, "")
	if err != nil || total != 130*unit {
		t.Fatalf("numbers total %d (%v), want %d", total, err, 130*unit)
	}
	if others, _ := p.db.LentNumbers(ctx, p.now, "B"); others != 80*unit {
		t.Fatalf("numbers without B %d", others)
	}
	// An expired invite counts no more.
	if later, _ := p.db.LentNumbers(ctx, p.now.Add(2*time.Hour), ""); later != 110*unit {
		t.Fatalf("numbers after the invite expired %d", later)
	}

	max := 150 * unit
	_, _, err = p.db.SetOwnNumber(ctx, "B", 71*unit, max)
	var over *MaxTotalError
	if !errors.Is(err, ErrOverMaxTotal) || !errors.As(err, &over) || over.Others != 80*unit || over.Max != max {
		t.Fatalf("a number past the max total: %v", err)
	}
	if f, _ := p.db.GetFriendship(ctx, "B"); f.OwnNumber != 50*unit {
		t.Fatalf("a refused number was stored: %+v", f)
	}
	before, after, err := p.db.SetOwnNumber(ctx, "B", 70*unit, max)
	if err != nil {
		t.Fatal(err)
	}
	if before.OwnNumber != 50*unit || after.OwnNumber != 70*unit || after.Quota() != 50*unit {
		t.Fatalf("raised alone: before %+v after %+v", before, after)
	}
	// Lowering takes effect at once, below the friend's number too.
	if _, after, err = p.db.SetOwnNumber(ctx, "B", 5*unit, 0); err != nil || after.Quota() != 5*unit {
		t.Fatalf("lowered: %+v %v", after, err)
	}
	if _, _, err := p.db.SetOwnNumber(ctx, "Z", unit, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a stranger's number: %v", err)
	}
}

// LEND-7, LEND-8 (#24): the quota call's two sides. The callee applies
// the caller's number, use and repository size as reported; the caller
// applies the answer, and records the number it sent as told, unless it
// changed meanwhile.
func TestQuotaReportsApplyTheOthersNumber(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 50)
	f, err := p.db.QuotaReported(ctx, "B", QuotaReport{Number: 30 * unit, Use: 7, RepositorySize: 3 << 30, Protocol: "0.1"}, p.now)
	if err != nil {
		t.Fatal(err)
	}
	if f.PeerNumber != 30*unit || f.Quota() != 30*unit || f.PeerUse != 7 || f.PeerRepositorySize != 3<<30 ||
		f.PeerProtocol != "0.1" || !f.SeenAt.Equal(p.now) {
		t.Fatalf("after the friend's call: %+v", f)
	}
	if f.ToldNumber != 0 {
		t.Fatalf("answering told the friend nothing yet: %+v", f)
	}
	if _, err := p.db.QuotaReported(ctx, "Z", QuotaReport{Number: 1}, p.now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a stranger's report: %v", err)
	}

	p.now = p.now.Add(time.Minute)
	if err := p.db.QuotaFailed(ctx, "B", "dial: no route", p.now); err != nil {
		t.Fatal(err)
	}
	if f, _ = p.db.GetFriendship(ctx, "B"); f.ExchangeError != "dial: no route" {
		t.Fatalf("after a failed call: %+v", f)
	}
	a := QuotaAnswer{Number: 60 * unit, Use: 9, Lent: 2 * unit, RepositorySize: 4 << 30, Protocol: "0.1", Path: "direct"}
	// An answer to a number that is no longer this node's tells nothing.
	if f, _ = p.db.QuotaAnswered(ctx, "B", 45*unit, a, p.now); f.ToldNumber != 0 {
		t.Fatalf("an answer to another number told it: %+v", f)
	}
	f, err = p.db.QuotaAnswered(ctx, "B", 50*unit, a, p.now)
	if err != nil {
		t.Fatal(err)
	}
	if f.PeerNumber != 60*unit || f.Quota() != 50*unit || f.ToldNumber != 50*unit || f.LentThere != 2*unit ||
		f.PeerUse != 9 || f.PeerRepositorySize != 4<<30 || f.Path != "direct" || f.ExchangeError != "" {
		t.Fatalf("after the answer: %+v", f)
	}
	// The number changed while the call was out: it is not told.
	if _, _, err := p.db.SetOwnNumber(ctx, "B", 40*unit, 0); err != nil {
		t.Fatal(err)
	}
	if f, _ = p.db.QuotaAnswered(ctx, "B", 50*unit, a, p.now); f.ToldNumber != 50*unit || f.OwnNumber != 40*unit {
		t.Fatalf("an answer to an older number: %+v", f)
	}
}

// #68: the borrower's count of a friend's use is replaced by the
// lender's own count from the quota answer, plus what is queued and not
// yet acknowledged there; deletes it counted as freed stay counted (the
// lender keeps tombs until the purge), and so do bytes it never knew of
// (an orphan copy). Placement then uses the corrected room.
func TestQuotaAnswerReconcilesThePlacesUse(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 1)
	use := func(place string) int64 {
		t.Helper()
		u, err := p.db.PlaceUse(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return u[place]
	}
	d1 := p.put("laptop", "data/1", "data", 2) // at B: B, C tie on fraction, B first by ID
	p.ack("B", d1)
	d2 := p.put("laptop", "data/2", "data", 3) // at C, larger fraction
	p.wantPlaces("data/2", d2, "C")
	if use("B") != 2*unit {
		t.Fatalf("B's count %d", use("B"))
	}
	// B, as lender, holds 2 units of ours plus an orphan of 5 units the
	// count never knew of, and keeps them: 7 units.
	if _, err := p.db.QuotaAnswered(ctx, "B", 10*unit, QuotaAnswer{Number: 10 * unit, Lent: 7 * unit}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("B") != 7*unit {
		t.Fatalf("B's count after the answer %d, want the lender's 7 units", use("B"))
	}
	// Queued, unsent rows count on top of the lender's word.
	d3 := p.put("laptop", "data/3", "data", 1) // C: 3/10 used, B: 7/10, so C
	p.wantPlaces("data/3", d3, "C")
	if _, err := p.db.QuotaAnswered(ctx, "C", 10*unit, QuotaAnswer{Number: 10 * unit, Lent: 0}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("C") != 4*unit {
		t.Fatalf("C's count %d, want its 4 queued units", use("C"))
	}
	// Placement uses the corrected room: B (3 left of 10) is passed over
	// for C (6 left of 10) although B's own count said 8 left.
	d4 := p.put("laptop", "data/4", "data", 1)
	p.wantPlaces("data/4", d4, "C")
	// A delete placed at B frees nothing until the next answer: the
	// lender keeps the tomb counted for its window.
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("the delete", del, "B")
	if _, err := p.db.QuotaAnswered(ctx, "B", 10*unit, QuotaAnswer{Number: 10 * unit, Lent: 7 * unit}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("B") != 7*unit {
		t.Fatalf("B's count with a delete queued %d, want the lender's 7 units", use("B"))
	}
}

// LEND-8 AC (#24): a raised quota resumes a friend paused for quota
// (its last answer a 507) at once, without waiting for it to
// acknowledge a row; another condition is left as it is.
func TestResumePlaceEndsAQuotaPause(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.condition("B", ConditionQuota, p.now.Add(-time.Hour), p.now.Add(-2*time.Hour))
	p.condition("C", ConditionRetrying, p.now.Add(-time.Hour), time.Time{})
	for _, place := range []string{"B", "C", "D"} {
		if err := p.db.ResumePlace(ctx, place, p.now); err != nil {
			t.Fatal(err)
		}
	}
	cs, err := p.db.PlaceConditions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c := cs["B"]; c.Condition != ConditionOK || !c.Since.Equal(p.now) || !c.LastOK.Equal(p.now.Add(-2*time.Hour)) {
		t.Fatalf("B after a raise: %+v", c)
	}
	if cs["C"].Condition != ConditionRetrying {
		t.Fatalf("C after a raise: %+v", cs["C"])
	}
	if _, ok := cs["D"]; ok {
		t.Fatal("a condition was made up for a place with none")
	}
}

// LEND-7 (#24): the repository size the quota call reports is the live
// bytes of the node's own buckets: each key's latest done write, if a
// put.
func TestRepositorySizeIsTheLiveBytes(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.bucket("laptop", 1)
	p.bucket("nas", 1)
	p.put("laptop", "data/1", "data", 2)
	p.put("laptop", "data/2", "data", 3)
	p.rewrite("laptop", "data/2", "data", 1)
	p.del("laptop", "data/1", "data")
	p.put("nas", "index/1", "metadata", 4)
	n, err := p.db.RepositorySize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := 5*unit - 2*ObjectOverhead; n != want {
		t.Fatalf("repository size %d, want %d", n, want)
	}
}
