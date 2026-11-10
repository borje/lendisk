package state

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// The follow-ups to removing an overflow target (#102): a forced
// removal for one whose credential is gone, the puts abandoned there,
// keep_data against the worker's held deletes, and (recovery tests)
// the removing state in the manifest.

func (p *pool) force(name string, accept bool) (Loss, LeftBehind, error) {
	p.t.Helper()
	return p.db.ForceRemoveOverflow(context.Background(), name, accept, p.now)
}

func (p *pool) rowsOf(place string) int {
	p.t.Helper()
	var n int
	p.db.QueryRow(`SELECT (SELECT count(*) FROM placement WHERE place = ?1) + (SELECT count(*) FROM place_use WHERE place = ?1)
		+ (SELECT count(*) FROM overflow_tomb WHERE place = ?1)`, place).Scan(&n)
	return n
}

// #102: `overflow remove --force` never drops a target holding a copy
// its object still needs (I1) unless the user accepts that loss: it
// refuses, changing nothing, and says which objects and how many would
// have no copy offsite at all. Accepted, the target and its rows go
// (I3), and what it still holds is returned.
func TestAForcedRemovalRefusesWhileTheTargetHoldsCopiesObjectsNeed(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	p.ackQueued()
	p.wantPlaces("data", d, "A", o)

	loss, _, err := p.force("b2", false)
	if !errors.Is(err, ErrWouldLose) || loss.Objects != 1 || loss.Bytes != unit || loss.Unprotected != 0 ||
		!slices.Equal(loss.Keys, []string{"r/data/1"}) {
		t.Fatalf("forced without consent: %+v, %v", loss, err)
	}
	if got, err := p.db.GetOverflow(ctx, "b2"); err != nil || got.Removing() {
		t.Fatalf("a refused force changed the target: %+v, %v", got, err)
	}
	p.wantPlaces("data after a refused force", d, "A", o)

	// A gone: the target's copy is the only one offsite.
	p.gone("A")
	if loss, _, err = p.force("b2", false); !errors.Is(err, ErrWouldLose) || loss.Unprotected != 1 {
		t.Fatalf("forced with the only offsite copy there: %+v, %v", loss, err)
	}

	loss, left, err := p.force("b2", true)
	if err != nil || loss.Objects != 1 || left.Copies != 1 || left.CopyBytes != unit {
		t.Fatalf("forced with the loss accepted: %+v, left %+v, %v", loss, left, err)
	}
	if _, err := p.db.GetOverflow(ctx, "b2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the target is still there: %v", err)
	}
	if n := p.rowsOf(o); n != 0 {
		t.Fatalf("%d rows of the target left", n)
	}
	if _, _, err := p.force("b2", true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forcing a target that is gone: %v", err)
	}
}

// #102: a removal stuck because the target's credential was revoked:
// its copies are held elsewhere, but their deletes there can never be
// sent. --force drops it without asking, and says what is left there:
// the copies whose deletes never went, and a held delete.
func TestAForcedRemovalOfATargetWhoseCopiesAreHeldElsewhereNeedsNoConsent(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	d1 := p.put("r", "data/1", "data", 1)
	p.put("r", "data/2", "data", 1)
	p.ackQueued()
	p.del("r", "data/2", "data")
	p.holdDrops(o) // the prune's delete, held for the window
	p.removeOverflow("b2", false)
	p.friend("B", 10)
	p.replace()
	p.ack("B", d1)
	p.wantState(o, d1, PlacementDropping) // its delete never goes
	if got := p.finishRemovals(); len(got) != 0 {
		t.Fatalf("removed %v with its deletes unsent", got)
	}

	loss, left, err := p.force("b2", false)
	if err != nil || loss.Objects != 0 {
		t.Fatalf("forced: %+v, %v", loss, err)
	}
	if left.Copies != 1 || left.CopyBytes != unit || left.Deletes != 1 || left.Abandoned != 0 {
		t.Fatalf("left behind %+v, want the dropping copy and the held delete", left)
	}
	if _, err := p.db.GetOverflow(ctx, "b2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the target is still there: %v", err)
	}
	if n := p.rowsOf(o); n != 0 {
		t.Fatalf("%d rows of the target left", n)
	}
	p.wantPlaces("data/1 after the force", d1, "A", "B")
}

// #102: a put the target's sender took and then gave up, superseded by
// a later write of its key, is abandoned (its send may have landed).
// The target being removed is still dropped once nothing depends on it,
// and the put is reported as left there, before (OverflowLeftAt, for
// `overflow remove`) and as the row goes.
func TestAPutAbandonedAtARemovedTargetIsReportedLeftBehind(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	d := p.put("r", "data/1", "data", 1)
	if !p.start(o, d) {
		t.Fatal("the target's put not taken")
	}
	p.rewrite("r", "data/1", "data", 1)
	if err := p.db.CancelPlacement(ctx, o, d); err != nil { // the worker finds it superseded
		t.Fatal(err)
	}
	p.wantState(o, d, PlacementAbandoned)
	p.removeOverflow("b2", false)
	p.friend("B", 10)
	p.settled()
	if left, err := p.db.OverflowLeftAt(ctx, "b2"); err != nil || left.Abandoned != 1 || left.AbandonedBytes != unit {
		t.Fatalf("left at the target before it goes: %+v, %v", left, err)
	}
	done := p.finishRemovalsLeft()
	if len(done) != 1 || done[0].Name != "b2" || done[0].Left.Abandoned != 1 || done[0].Left.AbandonedBytes != unit ||
		done[0].Left.Copies != 0 {
		t.Fatalf("removed %+v, want b2 with the abandoned put left", done)
	}
}

// #102: --keep-data forgets the held deletes the worker has not claimed;
// one it claimed is left to it, and asked about again before it is sent
// (TombToSend): not sent, and forgotten. No tomb of the target is due
// from then on, and the removal finishes once the claim is given back.
func TestKeepDataStopsHeldDeletesTheWorkerClaimed(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("A", 10)
	o := p.overflow("b2", 100)
	p.bucket("r", 2)
	p.put("r", "data/1", "data", 1)
	p.put("r", "data/2", "data", 1)
	p.put("r", "data/3", "data", 1)
	p.ackQueued()
	for _, k := range []string{"data/1", "data/2", "data/3"} {
		p.del("r", k, "data")
	}
	p.holdDrops(o)
	p.now = p.now.AddDate(0, 0, 31)
	claimed := p.due(o) // the worker's issueTombs claims a batch
	if len(claimed) != 3 {
		t.Fatalf("%d deletes due, want 3", len(claimed))
	}
	if ok, err := p.db.TombToSend(ctx, o, claimed[0].Seq); err != nil || !ok {
		t.Fatalf("a claimed delete before --keep-data: send %v, %v", ok, err)
	}
	// Sent: the first. Then the user removes the target keeping its data.
	if err := p.db.DropOverflowTomb(ctx, o, claimed[0].Seq); err != nil {
		t.Fatal(err)
	}
	p.removeOverflow("b2", true)
	if n := p.tombsAt(o); n != 2 {
		t.Fatalf("%d tombs left, want the two the worker holds", n)
	}
	if ok, err := p.db.TombToSend(ctx, o, claimed[1].Seq); err != nil || ok {
		t.Fatalf("a claimed delete after --keep-data: send %v, %v", ok, err)
	}
	if n := p.tombsAt(o); n != 1 {
		t.Fatalf("%d tombs left, want the one still claimed", n)
	}
	// The worker gives the last back (it stopped, or failed); none is
	// due there any more, and the removal finishes.
	if err := p.db.UnclaimOverflowTombs(ctx, o, []int64{claimed[2].Seq}); err != nil {
		t.Fatal(err)
	}
	if ts := p.due(o); len(ts) != 0 {
		t.Fatalf("deletes due at a target kept: %+v", ts)
	}
	if got := p.finishRemovals(); !slices.Equal(got, []string{"b2"}) {
		t.Fatalf("removed %v, want b2", got)
	}
}
