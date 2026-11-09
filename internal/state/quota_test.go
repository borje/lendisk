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
	if f, _ = p.db.QuotaAnswered(ctx, "B", 45*unit, p.now, a, p.now); f.ToldNumber != 0 {
		t.Fatalf("an answer to another number told it: %+v", f)
	}
	f, err = p.db.QuotaAnswered(ctx, "B", 50*unit, p.now, a, p.now)
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
	if f, _ = p.db.QuotaAnswered(ctx, "B", 50*unit, p.now, a, p.now); f.ToldNumber != 50*unit || f.OwnNumber != 40*unit {
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
	if _, err := p.db.QuotaAnswered(ctx, "B", 10*unit, p.now, QuotaAnswer{Number: 10 * unit, Lent: 7 * unit}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("B") != 7*unit {
		t.Fatalf("B's count after the answer %d, want the lender's 7 units", use("B"))
	}
	// Queued, unsent rows count on top of the lender's word.
	d3 := p.put("laptop", "data/3", "data", 1) // C: 3/10 used, B: 7/10, so C
	p.wantPlaces("data/3", d3, "C")
	if _, err := p.db.QuotaAnswered(ctx, "C", 10*unit, p.now, QuotaAnswer{Number: 10 * unit, Lent: 0}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("C") != 4*unit {
		t.Fatalf("C's count %d, want its 4 queued units", use("C"))
	}
	// Placement uses the corrected room: B (3 left of 10) is passed over
	// for C (6 left of 10) although B's own count said 8 left.
	d4 := p.put("laptop", "data/4", "data", 1)
	p.wantPlaces("data/4", d4, "C")
	// A delete placed at B frees nothing, queued or acknowledged, until
	// the lender's answer stops counting it: the lender keeps the tomb
	// counted for its window (#68). Placement sees the same count.
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("the delete", del, "B")
	if use("B") != 7*unit {
		t.Fatalf("B's count with a delete queued %d, want the lender's 7 units still", use("B"))
	}
	p.ack("B", del)
	if use("B") != 7*unit {
		t.Fatalf("B's count with the delete acknowledged %d, want 7 units still", use("B"))
	}
	d5 := p.put("laptop", "data/5", "data", 1) // B: 7/10 used, C: 5/10, so C
	p.wantPlaces("data/5", d5, "C")
	// The lender purged the tomb: its answer frees it.
	if _, err := p.db.QuotaAnswered(ctx, "B", 10*unit, p.now, QuotaAnswer{Number: 10 * unit, Lent: 5 * unit}, p.now); err != nil {
		t.Fatal(err)
	}
	if use("B") != 5*unit {
		t.Fatalf("B's count after the purge %d, want the lender's 5 units", use("B"))
	}
}

// #24 review: an answer to a call sent before the friend's own call set
// its number carries the friend's older number and does not overwrite
// the newer one.
func TestAnAnswerOlderThanTheFriendsReportKeepsItsNumber(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 50)
	sent := p.now
	p.now = p.now.Add(time.Second)
	if _, err := p.db.QuotaReported(ctx, "B", QuotaReport{Number: 20 * unit}, p.now); err != nil {
		t.Fatal(err)
	}
	p.now = p.now.Add(time.Second)
	f, err := p.db.QuotaAnswered(ctx, "B", 50*unit, sent, QuotaAnswer{Number: 40 * unit, Lent: unit}, p.now)
	if err != nil {
		t.Fatal(err)
	}
	if f.PeerNumber != 20*unit || f.ToldNumber != 50*unit || f.LentThere != unit {
		t.Fatalf("after a stale answer: %+v", f)
	}
	// An answer to a call sent after the report applies.
	if f, _ = p.db.QuotaAnswered(ctx, "B", 50*unit, p.now, QuotaAnswer{Number: 30 * unit}, p.now); f.PeerNumber != 30*unit {
		t.Fatalf("after a fresh answer: %+v", f)
	}
}

// LEND-1 (#24 review): an invite and a redeem in progress are checked
// against lending.max_total in the transaction that stores them; a
// redeem settles any other redeem with the same inviter, so that one is
// not counted twice; and the check does not overflow.
func TestInvitesAndRedeemsCheckedAgainstTheMaxTotal(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 50)
	inv := Invite{Key: "ik", Number: 30 * unit, Retention: time.Hour, ExpiresAt: p.now.Add(time.Hour)}
	if err := p.db.AddInviteWithin(ctx, inv, 79*unit, p.now); !errors.Is(err, ErrOverMaxTotal) {
		t.Fatalf("an invite past the max total: %v", err)
	}
	if err := p.db.AddInviteWithin(ctx, inv, 80*unit, p.now); err != nil {
		t.Fatal(err)
	}
	r := PendingRedeem{InviteKey: "ik2", Peer: "D", Number: 10 * unit, StartedAt: p.now}
	if err := p.db.StartRedeemWithin(ctx, r, 89*unit, p.now); !errors.Is(err, ErrOverMaxTotal) {
		t.Fatalf("a redeem past the max total: %v", err)
	}
	if err := p.db.StartRedeemWithin(ctx, r, 90*unit, p.now); err != nil {
		t.Fatal(err)
	}
	// Another redeem with D settles the first: 50 + 30 + 15 fits 95.
	r2 := PendingRedeem{InviteKey: "ik3", Peer: "D", Number: 15 * unit, StartedAt: p.now}
	if err := p.db.StartRedeemWithin(ctx, r2, 95*unit, p.now); err != nil {
		t.Fatalf("a second redeem with the same inviter: %v", err)
	}
	if _, _, err := p.db.SetOwnNumber(ctx, "B", 1<<62, 1<<62); !errors.Is(err, ErrOverMaxTotal) {
		t.Fatalf("a number that overflows the sum: %v", err)
	}
	if err := p.db.AddInviteWithin(ctx, Invite{Key: "big", Number: 1 << 62, ExpiresAt: p.now.Add(time.Hour)}, 1<<62, p.now); !errors.Is(err, ErrOverMaxTotal) {
		t.Fatalf("an invite that overflows the sum: %v", err)
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

// LEND-7 (#37): the repository as capacity planning counts it, as
// places do: its live data and its live metadata, each object with its
// overhead, and the largest copies of a bucket holding data.
func TestRepositoryUseSplitsDataFromMetadata(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	if u, err := p.db.RepositoryUse(ctx); err != nil || u != (RepositoryUse{}) {
		t.Fatalf("no buckets: %+v %v", u, err)
	}
	p.bucket("laptop", 2)
	p.bucket("nas", 3)
	p.bucket("empty", 4)
	p.put("laptop", "data/1", "data", 2)
	p.put("laptop", "data/2", "data", 3)
	p.del("laptop", "data/1", "data")
	p.put("laptop", "index/1", "metadata", 1)
	p.put("nas", "snapshots/1", "snapshot", 0.5)
	p.put("nas", "data/3", "data", 4)
	u, err := p.db.RepositoryUse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u.Data != 7*unit || u.Metadata != unit+unit/2 || u.Copies != 3 {
		t.Fatalf("%+v, want data %d, metadata %d, copies 3", u, 7*unit, unit+unit/2)
	}
}

// LEND-7 (#37): each quota call marks since when each side of a
// friendship has used less than half of the quota (this node at the
// friend, by its count reconciled from the friend's; the friend here,
// by this node's count as lender), and clears the mark once it uses
// half. A quota of zero marks nothing.
func TestQuotaCallsMarkSinceWhenUseIsUnderHalf(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 100)
	t0 := p.now
	if _, err := p.db.QuotaReported(ctx, "B", QuotaReport{Number: 100 * unit}, t0); err != nil {
		t.Fatal(err)
	}
	f, _ := p.db.GetFriendship(ctx, "B")
	if !f.LowThereSince.Equal(t0) || !f.LowHereSince.Equal(t0) {
		t.Fatalf("nothing used: %+v", f)
	}
	// The friend stores 60 here; this node 10 there: only this side is low.
	if err := p.db.SetLentUsage(ctx, "B", 60*unit, t0); err != nil {
		t.Fatal(err)
	}
	t1 := t0.Add(24 * time.Hour)
	if _, err := p.db.QuotaAnswered(ctx, "B", 100*unit, t1, QuotaAnswer{Number: 100 * unit, Lent: 10 * unit}, t1); err != nil {
		t.Fatal(err)
	}
	f, _ = p.db.GetFriendship(ctx, "B")
	if !f.LowThereSince.Equal(t0) || !f.LowHereSince.IsZero() {
		t.Fatalf("this node low since t0, the friend not: %+v", f)
	}
	// This node now stores 60 there: no side is low.
	t2 := t1.Add(24 * time.Hour)
	if _, err := p.db.QuotaAnswered(ctx, "B", 100*unit, t2, QuotaAnswer{Number: 100 * unit, Lent: 60 * unit}, t2); err != nil {
		t.Fatal(err)
	}
	if f, _ = p.db.GetFriendship(ctx, "B"); !f.LowThereSince.IsZero() || !f.LowHereSince.IsZero() {
		t.Fatalf("both use more than half: %+v", f)
	}
	// Both numbers go to 200: 60 of 200 is under half again.
	t3 := t2.Add(time.Hour)
	if _, _, err := p.db.SetOwnNumber(ctx, "B", 200*unit, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.QuotaReported(ctx, "B", QuotaReport{Number: 200 * unit}, t3); err != nil {
		t.Fatal(err)
	}
	if f, _ = p.db.GetFriendship(ctx, "B"); !f.LowThereSince.Equal(t3) || !f.LowHereSince.Equal(t3) {
		t.Fatalf("both under half of 200: %+v", f)
	}
	if _, err := p.db.QuotaReported(ctx, "B", QuotaReport{Number: 0}, t3.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if f, _ = p.db.GetFriendship(ctx, "B"); !f.LowThereSince.IsZero() || !f.LowHereSince.IsZero() {
		t.Fatalf("a quota of zero: %+v", f)
	}
}
