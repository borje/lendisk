package state

import (
	"context"
	"testing"
	"time"
)

// pending begins a put whose row stays pending: a completed multipart
// upload the client was answered for while its hash is still being
// taken (capture, #49), or one stranded until the next start.
func (p *pool) pending(bucket, key, class string) int64 {
	p.t.Helper()
	seqs, err := p.db.BeginCaptures(context.Background(), []Capture{{Bucket: bucket, Key: key, Op: OpPut, Class: class}})
	if err != nil {
		p.t.Fatal(err)
	}
	return seqs[0]
}

// REPL-11 (#20 review): a multipart pack whose row is still pending
// below an acknowledged index and snapshot has no placement yet, so the
// snapshot is not offsite: the watermark stops before it, and status
// counts it as waiting (still being written), not stuck. Once its row is
// done and acknowledged, the snapshot is offsite.
func TestAPendingUploadBelowTheSnapshotHoldsTheWatermark(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	small := p.put("laptop", "data/small", "data", 1)
	p.ackAll(small)
	big := p.pending("laptop", "data/big", "data")
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
	p.ackAll(idx)
	p.ackAll(snap)
	o := p.offsite("laptop", snap)
	if o.Reached() || o.Complete() || o.Watermark != big-1 || o.Waiting.Objects != 1 || o.Waiting.Pending != 1 ||
		o.Stuck.Objects != 0 {
		t.Fatalf("a pending upload below the snapshot: %+v", o)
	}
	if _, err := p.db.FinishPut(ctx, big, 20*unit, "h-big"); err != nil {
		t.Fatal(err)
	}
	if o := p.offsite("laptop", snap); o.Reached() {
		t.Fatalf("the upload done but unsent: %+v", o)
	}
	p.ackAll(big)
	if o := p.offsite("laptop", snap); !o.Reached() || !o.Complete() {
		t.Fatalf("the upload acknowledged: %+v", o)
	}
}

// REPL-11 (#20 second review): a pending upload that cannot be matched
// to the object in the store (its xattrs lost) may never settle. The
// barrier still holds the metadata after it, which is safe for the
// index, but the row is marked: Offsite counts it as stuck with its own
// reason, not as still being written, and UnmatchableRows names its
// bucket for the alert. Settling the row clears the mark.
func TestAnUnmatchablePendingUploadIsStuckNotBeingWritten(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	big := p.pending("laptop", "data/big", "data")
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	if err := p.db.MarkUnmatchable(ctx, big, "no upload ID"); err != nil {
		t.Fatal(err)
	}
	o := p.offsite("laptop", idx)
	if o.Stuck.Objects != 1 || o.Stuck.Unmatchable != 1 || o.Waiting.Pending != 0 || o.Waiting.Objects != 2 { // it and the index
		t.Fatalf("an unmatchable upload: %+v", o)
	}
	if !p.held(idx) {
		t.Fatal("metadata after an unmatchable upload is not held")
	}
	if n, err := p.db.UnmatchableRows(ctx); err != nil || n["laptop"].Objects != 1 || n["laptop"].Key != "data/big" || len(n) != 1 {
		t.Fatalf("unmatchable rows %v %v", n, err)
	}
	if _, err := p.db.FinishPut(ctx, big, unit, "h-big"); err != nil {
		t.Fatal(err)
	}
	if n, err := p.db.UnmatchableRows(ctx); err != nil || len(n) != 0 {
		t.Fatalf("unmatchable rows after settling %v %v", n, err)
	}
	if o := p.offsite("laptop", idx); o.Stuck.Objects != 0 {
		t.Fatalf("settled: %+v", o)
	}
}

// REPL-1 (#20 review): the metadata barrier holds metadata behind a
// data upload still pending before it, which has no placement row yet;
// a pending upload after it, or metadata's own, does not.
func TestAPendingDataUploadHoldsTheMetadataBarrier(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	big := p.pending("laptop", "data/big", "data")
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	p.pending("laptop", "data/later", "data")
	if !p.held(idx) {
		t.Fatal("metadata after a pending upload is not held")
	}
	if _, err := p.db.FinishPut(ctx, big, unit, "h-big"); err != nil {
		t.Fatal(err)
	}
	if !p.held(idx) {
		t.Fatal("metadata after a queued upload is not held")
	}
	p.ackAll(big)
	if p.held(idx) {
		t.Fatal("metadata held by a pending upload after it")
	}
}

// REPL-11, REPL-12 (#20 review): only friends in the pool hold copies.
// A friend that refuses this node (removed) holds none; nor does one
// gone past replace_after, which REPL-12 no longer counts as room and
// REPL-13 re-places: an offsite claim does not rest on a friend silent
// for a week. Such copies are stuck until re-placement (#26). A friend
// merely unreachable still counts.
func TestARemovedOrGoneFriendHoldsNoCopy(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d)
	if o := p.offsite("laptop", 0); !o.Complete() {
		t.Fatalf("at B and C: %+v", o)
	}
	p.condition("C", ConditionRetrying, p.now.Add(-2*time.Hour), p.now.Add(-3*time.Hour))
	if o := p.offsite("laptop", 0); !o.Complete() {
		t.Fatalf("C unreachable for two hours: %+v", o)
	}
	p.condition("C", ConditionRetrying, p.now.Add(-8*24*time.Hour), p.now.Add(-9*24*time.Hour))
	p.replace() // B alone is left: nowhere for the copy
	o := p.offsite("laptop", 0)
	if o.Complete() || o.Stuck.Objects != 1 || o.Stuck.Gone != 1 {
		t.Fatalf("C gone past replace_after: %+v", o)
	}
	p.condition("C", ConditionRemoved, p.now.Add(-time.Minute), p.now.Add(-time.Hour))
	p.replace()
	if o := p.offsite("laptop", 0); o.Complete() || o.Stuck.Gone != 1 {
		t.Fatalf("C refusing this node: %+v", o)
	}
}

// §3.4 REPL-11 (#20 review): status reads each bucket's watermark once
// and recounts what waits for another target with For, which gives what
// a fresh read for that target gives.
func TestForRecountsLikeAFreshRead(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d1 := p.put("laptop", "data/1", "data", 1)
	snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
	p.ackAll(snap)
	p.put("laptop", "data/2", "data", 1)
	p.pending("laptop", "data/3", "data")
	base := p.offsite("laptop", 0)
	for _, target := range []int64{0, d1, snap, snap + 1} {
		got, want := base.For(target), p.offsite("laptop", target)
		if got.Target != want.Target || got.Waiting != want.Waiting || got.Stuck != want.Stuck || got.Watermark != want.Watermark {
			t.Errorf("For(%d) = %+v, a fresh read %+v", target, got, want)
		}
	}
}

// REPL-11 (#20 review), the holds rules. An abandoned row between a
// place's acknowledgement and now means the place may hold other bytes:
// that acknowledgement no longer counts. An older acknowledgement of the
// same bytes counts for a healing re-upload of them.
func TestHoldsRules(t *testing.T) {
	t.Run("abandoned in between", func(t *testing.T) {
		p := newPool(t)
		ctx := context.Background()
		p.bucket("laptop", 2)
		p.friend("B", 100)
		p.friend("C", 100)
		v1 := p.put("laptop", "config", "metadata", 0.01)
		p.ackAll(v1)
		v2 := p.rewrite("laptop", "config", "metadata", 0.01)
		if ok, err := p.db.StartPlacement(ctx, "B", v2, p.now); err != nil || !ok {
			t.Fatalf("taking v2 at B: %v %v", ok, err)
		}
		snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
		p.ackAll(snap)
		// v2 is superseded by a third version; B's taken send of it is
		// abandoned, and may have landed.
		p.put("laptop", "config", "metadata", 0.02)
		for _, place := range []string{"B", "C"} {
			if err := p.db.CancelPlacement(ctx, place, v2); err != nil {
				t.Fatal(err)
			}
		}
		if o := p.offsite("laptop", snap); o.Watermark != v1-1 {
			t.Fatalf("B's v1 acknowledgement counted past an abandoned v2: %+v", o)
		}
	})
	t.Run("older healing acknowledgement", func(t *testing.T) {
		p := newPool(t)
		ctx := context.Background()
		p.bucket("laptop", 2)
		p.friend("B", 100)
		p.friend("C", 100)
		r1 := p.put("laptop", "data/x", "data", 1)
		p.ackRow("B", r1)
		if err := p.db.RefusePlacement(ctx, "C", r1, "BadDigest"); err != nil {
			t.Fatal(err)
		}
		// The same bytes again heal the refusal: queued at C, and (here,
		// to isolate the rule) not acknowledged anew at B, whose
		// acknowledgement of r1 holds them.
		seqs, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/x", Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		r2 := seqs[0]
		if _, err := p.db.FinishPut(ctx, r2, unit-ObjectOverhead, "h-data/x"); err != nil {
			t.Fatal(err)
		}
		if _, err := p.db.Exec(`DELETE FROM placement WHERE seq = ? AND place = 'B'`, r2); err != nil {
			t.Fatal(err)
		}
		p.ackRow("C", r2)
		if o := p.offsite("laptop", 0); !o.Complete() {
			t.Fatalf("r2's bytes at C and, by r1's acknowledgement, at B: %+v", o)
		}
	})
}
