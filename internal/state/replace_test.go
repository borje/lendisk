package state

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"
)

// REPL-13, ID-5 (#26): after recover, a bucket whose friends' lists are
// not all read and settled is left alone: a copy that looks missing may
// be one a friend not yet listed holds, which its list records.
func TestReplacementWaitsForRecoveryLists(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	if _, err := p.db.Exec(`DELETE FROM placement WHERE place = 'C'`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Exec(`INSERT INTO relist (place, bucket) VALUES ('C', 'laptop')`); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Placed != 0 {
		t.Fatalf("placed %d copies while C's list is unread", s.Placed)
	}
	if _, err := p.db.Exec(`UPDATE relist SET done = 1, settled = 1`); err != nil {
		t.Fatal(err)
	}
	p.replace()
	p.wantPlaces("data/1", d1, "B", "C")
}

// REPL-13 (#26 review): metadata the store could not supply is sent
// nowhere, so a friend added later does not wait for it: it gets data
// once the rest of the bucket's metadata is queued there.
func TestStoreRefusedMetadataDoesNotKeepANewFriendFromData(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	m1 := p.put("laptop", "index/1", "metadata", 0.1)
	if _, err := p.db.RefuseObject(ctx, m1, CauseStore, "gone"); err != nil {
		t.Fatal(err)
	}
	p.friend("D", 100)
	p.replace()
	d1 := p.put("laptop", "data/1", "data", 1)
	if got := p.places(d1); !slices.Contains(got, "D") {
		t.Fatalf("data/1 placed at %v: D lacks only metadata no friend gets", got)
	}
}

// EP-6, REPL-13 (#26 review): a surplus copy at an unreachable friend
// goes first, however much room it has: the others answer.
func TestASurplusCopyAtAnUnreachableFriendGoesFirst(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 100) // the least full
	p.friend("D", 10)
	p.bucket("laptop", 3)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d1)
	p.condition("C", ConditionRetrying, p.now.Add(-2*time.Hour), p.now.Add(-2*time.Hour))
	if err := p.db.SetCopies(context.Background(), "laptop", 2); err != nil {
		t.Fatal(err)
	}
	p.replace()
	p.wantState("C", d1, PlacementDropping)
	p.wantPlaces("data/1", d1, "B", "D")
}

// EP-6, REPL-13 (#26, CI failure on 64c5649): a surplus copy its
// sender has taken may land, so the pass leaves it alone, and says so
// (Skipped) even when it has nothing else to do for the object: no
// change to the pool follows the copy's acknowledgement, and without
// it the worker would not look again for an hour.
func TestAPassLeavingATakenSurplusCopyAsksToLookAgain(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	p.ackRow("B", d1)
	if !p.start("C", d1) {
		t.Fatal("C's row not taken")
	}
	if err := p.db.SetCopies(context.Background(), "laptop", 1); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Skipped != 1 || s.Cancelled != 0 || s.Dropping != 0 {
		t.Fatalf("pass with C's copy taken: %+v, want one skipped and nothing done", s)
	}
	p.wantState("C", d1, PlacementQueued)
	p.wantState("B", d1, PlacementAcked)
	p.ackRow("C", d1)
	if s := p.replace(); s.Skipped != 0 || s.Dropping != 1 {
		t.Fatalf("pass with both acknowledged: %+v, want one dropping", s)
	}
}

// REPL-13, VER-1 (#26, #27): a copy at an unverified friend that its
// sender has taken is left alone, and the pass says so: once it is
// acknowledged it is drained, which no change to the pool announces.
func TestAPassLeavingATakenCopyAtAnUnverifiedFriendAsksToLookAgain(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackRow("B", d1)
	if !p.start("C", d1) {
		t.Fatal("C's row not taken")
	}
	if err := p.db.RecordUnverified(context.Background(), "C", "laptop", "injected", p.now); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Skipped != 1 {
		t.Fatalf("pass with C's copy taken: %+v, want one skipped", s)
	}
	p.wantState("C", d1, PlacementQueued)
}

// OPS-2 (#26 review): a batch that fails does not keep the pass from
// recording what is below copies.
func TestBelowCopiesIsRecordedWhenABatchFails(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 3)
	p.put("laptop", "data/1", "data", 1)
	p.friend("C", 10)
	if _, err := p.db.Exec(`CREATE TRIGGER fail BEFORE INSERT ON placement WHEN NEW.place = 'C'
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Replace(ctx, nil, nil); err == nil {
		t.Fatal("no error from a failing batch")
	}
	if below, err := p.db.BelowCopiesSince(ctx); err != nil || below["laptop"].Objects != 1 {
		t.Fatalf("below copies %+v %v", below, err)
	}
}

// replace runs one re-placement pass and fails the test on an error.
func (p *pool) replace() ReplaceStats {
	p.t.Helper()
	s, err := p.db.Replace(context.Background(), nil, nil)
	if err != nil {
		p.t.Fatal(err)
	}
	return s
}

// rowOf is place's row of seq: its state, and the place it replaces;
// "" when there is no row.
func (p *pool) rowOf(place string, seq int64) (state, replaces string) {
	p.t.Helper()
	var r sql.NullString
	err := p.db.QueryRow(`SELECT state, replaces FROM placement WHERE place = ? AND seq = ?`, place, seq).Scan(&state, &r)
	if err == sql.ErrNoRows {
		return "", ""
	} else if err != nil {
		p.t.Fatal(err)
	}
	return state, r.String
}

func (p *pool) wantState(place string, seq int64, want string) {
	p.t.Helper()
	if got, _ := p.rowOf(place, seq); got != want {
		p.t.Fatalf("%s's row of %d is %q, want %q", place, seq, got, want)
	}
}

// gone makes place unreachable past replace_after (7 days by default).
func (p *pool) gone(place string) {
	p.condition(place, ConditionRetrying, p.now.Add(-8*24*time.Hour), p.now.Add(-8*24*time.Hour))
}

// REPL-13, PEER-5 (#26, from the #20 and #25 reviews): removing a friend
// re-places its copies. The missing copy goes where D6 would place it at
// capture, by largest free fraction among the friends that do not hold
// the object; the old row stays until the new copy is acknowledged, and
// is then marked dropping, so the watermark is complete again.
func TestARemovedFriendsCopiesArePlacedByTheChoiceExcludingHolders(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.friend("E", 20) // the largest free fraction
	p.bucket("laptop", 2)
	idx := p.put("laptop", "index/1", "metadata", 0.1)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "E")
	p.ackAll(idx)
	p.ackAll(d1)
	if !p.offsite("laptop", 0).Complete() {
		t.Fatal("not offsite before the removal")
	}
	if _, err := p.db.RemoveFriendship(ctx, "E", false, p.now); err != nil {
		t.Fatal(err)
	}
	if p.offsite("laptop", 0).Complete() {
		t.Fatal("offsite while a removed friend holds a copy")
	}
	p.replace()
	// C and D have the same free fraction: C by ID; never B, which
	// holds it. E's row stays until then.
	p.wantPlaces("data/1", d1, "B", "C", "E")
	if s, r := p.rowOf("C", d1); s != PlacementQueued || r != "E" {
		t.Fatalf("C's row is %s replacing %q, want queued replacing E", s, r)
	}
	p.wantState("E", d1, PlacementAcked)
	if again := p.replace(); again.Placed != 0 {
		t.Fatalf("a second pass placed %d more", again.Placed)
	}
	p.ackRow("C", d1)
	p.wantState("E", d1, PlacementDropping)
	if o := p.offsite("laptop", 0); !o.Complete() {
		t.Fatalf("not offsite after re-placement: %+v", o)
	}
}

// REPL-13 (#26): a copy at a friend gone past replace_after is replaced
// and then deleted there, never before its replacement is acknowledged:
// the friend's FIFO gets the delete only then, and its acknowledgement
// leaves the row drained (a tomb at the lender, RET-1). The bytes the
// place still counts for it stay counted until its next quota answer.
func TestADrainedCopyIsDeletedOnlyAfterItsReplacementIsAcknowledged(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 5)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	p.ackAll(d1)
	if _, err := p.db.QuotaAnswered(ctx, "C", 10*unit, p.now, QuotaAnswer{Number: 10 * unit, Lent: unit}, p.now); err != nil {
		t.Fatal(err)
	}
	p.gone("C")
	p.replace()
	p.wantState("D", d1, PlacementQueued)
	p.wantState("C", d1, PlacementAcked)
	if _, ok, _ := p.db.NextPlacement(ctx, "C"); ok {
		t.Fatal("C's FIFO has a row before the replacement is acknowledged")
	}
	before, _ := p.db.PlaceUse(ctx)
	p.ackRow("D", d1)
	p.wantState("C", d1, PlacementDropping)
	after, _ := p.db.PlaceUse(ctx)
	if after["C"] != before["C"] {
		t.Fatalf("C counts %d after the drop, %d before: the lender keeps the tomb", after["C"], before["C"])
	}
	next, ok, err := p.db.NextPlacement(ctx, "C")
	if err != nil || !ok || next.Seq != d1 || next.Op != OpDelete || next.Key != "data/1" {
		t.Fatalf("C's head is %+v %v %v, want the delete of data/1", next, ok, err)
	}
	if ok, err := p.db.StartPlacement(ctx, "C", d1, p.now); err != nil || !ok {
		t.Fatalf("taking the delete: %v %v", ok, err)
	}
	if err := p.db.AckPlacement(ctx, "C", d1, "", p.now); err != nil {
		t.Fatal(err)
	}
	p.wantState("C", d1, PlacementDrained)
	if _, ok, _ := p.db.NextPlacement(ctx, "C"); ok {
		t.Fatal("C's FIFO is not empty after the delete")
	}
	// A key written again over the tomb names the version it holds there.
	if h, ok, _ := p.db.ReplacedHash(ctx, "C", "laptop", "data/1", d1+100); !ok || h != "h-data/1" {
		t.Fatalf("ReplacedHash %q %v, want the drained version", h, ok)
	}
}

// REPL-13, REPL-12, REPL-1 (#26): a friend added later receives the
// bucket's existing metadata, in sequence order, before data counts it
// as a place: until its metadata rows are queued it gets no data, and
// afterwards new data follows the metadata in its FIFO.
func TestANewFriendReceivesTheExistingMetadataFirst(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	m1 := p.put("laptop", "index/1", "metadata", 0.1)
	d1 := p.put("laptop", "data/1", "data", 1)
	m2 := p.put("laptop", "snapshots/1", "snapshot", 0.1)
	for _, s := range []int64{m1, d1, m2} {
		p.ackAll(s)
	}
	p.friend("D", 100) // by far the largest free fraction
	d2 := p.put("laptop", "data/2", "data", 1)
	p.wantPlaces("data/2 before D has the metadata", d2, "B", "C")
	p.replace()
	p.wantPlaces("index/1", m1, "B", "C", "D")
	p.wantPlaces("snapshots/1", m2, "B", "C", "D")
	p.wantPlaces("data/1, at copies already", d1, "B", "C")
	d3 := p.put("laptop", "data/3", "data", 1)
	if got := p.places(d3); !slices.Contains(got, "D") {
		t.Fatalf("data/3 placed at %v: D has its metadata queued now", got)
	}
	var fifo []int64
	for range 3 {
		n, ok, err := p.db.NextPlacement(ctx, "D")
		if err != nil || !ok {
			t.Fatalf("D's FIFO ended early: %v", err)
		}
		fifo = append(fifo, n.Seq)
		p.ackRow("D", n.Seq)
	}
	if !slices.Equal(fifo, []int64{m1, m2, d3}) {
		t.Fatalf("D's FIFO sent %v, want the metadata %d, %d before data %d", fifo, m1, m2, d3)
	}
}

// REPL-13, REPL-11 (#26, from the #20 review): a friend gone past
// replace_after whose copies were moved away does not count them as
// held when it returns, before the deletes trim them: here the other
// holder is removed, so only the replacement holds the object.
func TestAReturningFriendsMovedCopiesDoNotCount(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	p.ackAll(d1)
	p.gone("C")
	p.replace()
	p.ackRow("D", d1)
	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); err != nil {
		t.Fatal(err)
	}
	p.condition("C", ConditionOK, p.now, p.now) // C is back
	o := p.offsite("laptop", 0)
	if o.Complete() {
		t.Fatalf("offsite at 2 copies with C's moved copy counted: %+v", o)
	}
	next, ok, _ := p.db.NextPlacement(ctx, "C")
	if !ok || next.Op != OpDelete || next.Seq != d1 {
		t.Fatalf("C's head is %+v, want the delete of its moved copy", next)
	}
}

// EP-6, REPL-13 (#26): raising copies adds a copy to every object;
// lowering it cancels unsent surplus copies first and then deletes
// acknowledged ones, at the fullest place, never below the new copies.
func TestChangingCopiesAddsAndRemovesCopies(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 20)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d1)
	d2 := p.put("laptop", "data/2", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	p.wantPlaces("data/2", d2, "B", "D")
	if err := p.db.SetCopies(ctx, "laptop", 3); err != nil {
		t.Fatal(err)
	}
	p.replace()
	p.wantPlaces("data/1", d1, "B", "C", "D")
	p.wantPlaces("data/2", d2, "B", "C", "D")
	p.ackRow("D", d1)
	if err := p.db.SetCopies(ctx, "laptop", 1); err != nil {
		t.Fatal(err)
	}
	p.replace()
	// data/1: all three acked; two are deleted, the fullest places
	// first (B and C hold more of their quota than D).
	p.wantPlaces("data/1", d1, "D")
	p.wantState("B", d1, PlacementDropping)
	p.wantState("C", d1, PlacementDropping)
	// data/2: none acked; the surplus unsent copies are cancelled.
	if got := p.places(d2); len(got) != 1 {
		t.Fatalf("data/2 at %v, want one place", got)
	}
	if err := p.db.SetCopies(ctx, "laptop", 0); err == nil {
		t.Fatal("copies 0 accepted")
	}
	if err := p.db.SetCopies(ctx, "nosuch", 2); err != ErrNotFound {
		t.Fatalf("SetCopies of no bucket: %v", err)
	}
}

// PEER-6, REPL-13 (#26): a friendship whose quota is lowered below what
// the friend holds has copies moved away, oldest first, until what it
// still holds fits; the rest stay.
func TestAFriendBelowItsUseDrainsWhatNoLongerFits(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	var ds []int64
	for i := range 4 {
		s := p.put("laptop", "data/"+string(rune('a'+i)), "data", 2)
		p.ackAll(s)
		ds = append(ds, s)
	}
	// B and C hold two each... by fraction the copies spread; find C's.
	var atC []int64
	for _, s := range ds {
		if slices.Contains(p.places(s), "C") {
			atC = append(atC, s)
		}
	}
	if len(atC) < 3 {
		t.Fatalf("C holds %d objects; the test needs three", len(atC))
	}
	if _, err := p.db.Exec(`UPDATE friendship SET own_number = ?, peer_number = ? WHERE peer_key = 'C'`, 5*unit, 5*unit); err != nil {
		t.Fatal(err)
	}
	p.replace()
	// C holds 2 units per object: at 5 units (less the reserve) it keeps
	// two, so the oldest one moves.
	for i, s := range atC {
		_, r := p.rowOf("C", s)
		moving := false
		for _, pl := range p.places(s) {
			if _, rr := p.rowOf(pl, s); rr == "C" {
				moving = true
			}
		}
		if want := i < len(atC)-2; moving != want {
			t.Fatalf("object %d of %d at C: replacement queued %v, want %v (%s)", i, len(atC), moving, want, r)
		}
	}
}

// REPL-13 (#25 review): a removed friend's unsent rows, dropped at the
// removal, are not queued again when it is befriended again, as its
// copies were placed elsewhere meanwhile; the copies it held that were
// moved away are deleted there. Live metadata is placed at every friend
// (REPL-12), so the metadata it lacks is sent.
func TestARemovedFriendsDroppedRowsAreNotRequeuedWhenBefriendedAgain(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	p.ackAll(d1)
	d2 := p.put("laptop", "data/2", "data", 1)
	p.wantPlaces("data/2", d2, "B", "C")
	p.friend("D", 10)
	m1 := p.put("laptop", "index/1", "metadata", 0.1)
	if _, err := p.db.RemoveFriendship(ctx, "C", false, p.now); err != nil {
		t.Fatal(err)
	}
	p.wantState("C", d2, PlacementDropped)
	p.wantState("C", m1, PlacementDropped)
	p.replace()
	for _, s := range []int64{d1, d2} {
		got := slices.DeleteFunc(p.places(s), func(pl string) bool { return pl == "C" })
		if st, _ := p.rowOf("C", s); len(got) != 2 || st == PlacementQueued {
			t.Fatalf("object %d at %v and C's row %s, want two places besides C", s, got, st)
		}
	}
	p.ackAll(d1)
	p.ackAll(d2)
	p.wantState("C", d1, PlacementDropping)
	p.friend("C", 10)
	p.replace()
	p.wantState("C", d2, PlacementDropped)
	p.wantState("C", d1, PlacementDropping)
	p.wantState("C", m1, PlacementQueued)
}

// REPL-13 (#26): the bytes come only from the local copy, so an object
// deleted since, or one the store could not supply, is not re-placed.
func TestObjectsWithoutLocalBytesAreNotReplaced(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d1)
	d2 := p.put("laptop", "data/2", "data", 1)
	if _, err := p.db.RefuseObject(ctx, d2, CauseStore, "gone"); err != nil {
		t.Fatal(err)
	}
	p.del("laptop", "data/1", "data")
	if _, err := p.db.RemoveFriendship(ctx, "C", false, p.now); err != nil {
		t.Fatal(err)
	}
	if s := p.replace(); s.Placed != 0 {
		t.Fatalf("placed %d copies of objects without local bytes", s.Placed)
	}
	if got := p.places(d2); len(got) != 0 {
		t.Fatalf("data/2 at %v", got)
	}
}

// OPS-2 (#26): the pass records since when a bucket has had objects
// below copies, and forgets it once none are.
func TestBelowCopiesIsRecordedWithItsStart(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 3)
	p.put("laptop", "data/1", "data", 1)
	start := p.now
	p.replace()
	p.now = p.now.Add(time.Hour)
	p.replace()
	below, err := p.db.BelowCopiesSince(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b := below["laptop"]; !b.Since.Equal(start) || b.Objects != 1 || b.Bytes != unit-ObjectOverhead {
		t.Fatalf("below copies %+v, want one object since %v", b, start)
	}
	p.friend("D", 10)
	p.replace()
	if below, _ = p.db.BelowCopiesSince(ctx); len(below) != 0 {
		t.Fatalf("below copies %+v after a third friend", below)
	}
}

// REPL-13 (#26): the pool's fingerprint changes with every change the
// pass answers: a friend added, removed, gone or back, a quota, and a
// bucket's copies. Nothing else changes it.
func TestThePoolFingerprintChangesWithThePool(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	fp := func() string {
		t.Helper()
		s, err := p.db.PoolFingerprint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	p.friend("B", 10)
	p.bucket("laptop", 2)
	last := fp()
	step := func(what string, change func()) {
		t.Helper()
		change()
		if now := fp(); now == last {
			t.Fatalf("%s left the fingerprint as it was", what)
		} else {
			last = now
		}
	}
	step("a friend added", func() { p.friend("C", 10) })
	p.put("laptop", "data/1", "data", 1)
	if fp() != last {
		t.Fatal("a capture changed the fingerprint")
	}
	step("a quota changed", func() {
		p.db.Exec(`UPDATE friendship SET own_number = ? WHERE peer_key = 'C'`, 5*unit)
	})
	step("a friend gone", func() { p.gone("C") })
	step("a friend back", func() { p.condition("C", ConditionOK, p.now, p.now) })
	step("a friend paused for quota", func() { p.condition("C", ConditionQuota, p.now, p.now) })
	step("its pause ended", func() { p.db.ResumePlace(ctx, "C", p.now) })
	step("copies changed", func() { p.db.SetCopies(ctx, "laptop", 1) })
	step("a friend removed", func() { p.db.RemoveFriendship(ctx, "C", false, p.now) })
}

// §3.4 (#26): a pass over many objects that all need a copy is batched
// and grows no worse than linearly, so it never holds the write lock
// for long (passLimit without the race detector).
func TestReplaceOfManyObjectsIsFast(t *testing.T) {
	pass := func(rows int) (placed int64, took time.Duration) {
		t.Helper()
		ctx := context.Background()
		p := newPool(t)
		manyWaiting(t, p, rows, false)
		p.friend("E", 1<<20)
		if _, err := p.db.RemoveFriendship(ctx, "C", false, p.now); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		s, err := p.db.Replace(ctx, nil, nil)
		took = time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		return s.Placed, took
	}
	placed, took := pass(manyRows)
	t.Logf("a pass over %d objects took %v, placed %d", manyRows, took, placed)
	if placed < manyRows {
		t.Fatalf("placed %d copies, want at least %d", placed, manyRows)
	}
	if passGrowth == 0 {
		if took > 2*passLimit {
			t.Fatalf("a pass over %d objects took %v", manyRows, took)
		}
		return
	}
	quarter := time.Duration(1 << 62)
	for i := range 2 {
		_, q := pass(manyRows / 4)
		quarter = min(quarter, q)
		if i == 0 {
			_, again := pass(manyRows)
			took = min(took, again)
		}
	}
	if ratio := float64(took) / float64(quarter); ratio > passGrowth {
		t.Fatalf("a pass over %d objects took %v, %.1f times the %v over %d", manyRows, took, ratio, quarter, manyRows/4)
	}
}
