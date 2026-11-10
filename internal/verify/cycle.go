package verify

import (
	"context"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// The rolling full check (VER-1a, #105; DESIGN §3.6). The daily
// challenge's eight random objects catch a large loss within a day but
// a small one only by chance: a friend that lost 50 of 100,000 objects
// is caught with about 0.4% probability a day. So, besides them, every
// replica at a friend is walked in write order (queue seq), a slice a
// day, each object of the slice challenged whole as `verify --full`
// challenges it (paced by the friend's whole-object budget), so that
// every object is challenged once a cycle (FullCycle, verify.full_cycle).
//
// A slice is due a day (Every) after the last one, under the daily
// challenge's rules (state.DB.DueCycles): it waits while the replica's
// FIFO is busy, until more than a day overdue. Its size is what the
// pass has left over the slices left before the cycle ends, rounded up,
// so the pass ends within its cycle whatever was written meanwhile; an
// object written since the pass began joins it where the cursor
// reaches it. The cursor (the last object decided) is kept in
// lendisk.db after every slice, and after one that got no answer, so a
// restart goes on where the pass stopped.
//
// Verdicts are the daily challenge's: a failed proof marks the replica
// unverified, alerts and starts re-placement, and ends the pass; no
// answer decides nothing and is retried with DefaultRetry; an object
// whose local copy is gone or no longer matches its capture hash is
// skipped and counted. A pass that reaches the end with no failure is
// the replica's last full cycle (state.ReplicaCheck.AllCheckedAt); one
// that skipped everything is ended unrecorded.
//
// An overflow target has a pass only once given a cycle of its own
// (`overflow set --full-cycle`): each object of its slice is downloaded
// through the download limits, which costs egress.

// DefaultFullCycle is how long the rolling full check takes to
// challenge every object at a friend (VER-1a).
const DefaultFullCycle = 30 * 24 * time.Hour

// cycles runs every due slice.
func (v *Verifier) cycles(ctx context.Context) {
	every := or(v.Every, DefaultEvery)
	due, err := v.DB.DueCycles(ctx, v.now(), every, or(v.Retry, DefaultRetry))
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Error("verification: reading the full-check slices due failed", "err", err)
		}
		return
	}
	for _, c := range due {
		v.slice(ctx, c, every)
		if ctx.Err() != nil {
			return
		}
	}
}

// slice runs c's next slice and records it.
func (v *Verifier) slice(ctx context.Context, c state.FullCycle, every time.Duration) {
	log := v.Log.With("peer", c.Place, "bucket", c.Bucket)
	now := v.now()
	cycle := c.Cycle
	if cycle <= 0 {
		cycle = or(v.FullCycle, DefaultFullCycle)
	}
	if c.StartedAt.IsZero() {
		c.StartedAt, c.After, c.Checked, c.Skipped = now, 0, 0, 0
	}
	left, err := v.DB.ChallengeableAfter(ctx, c.Place, c.Bucket, c.After)
	if err != nil {
		if ctx.Err() == nil {
			log.Error("verification: reading the full check's objects failed", "err", err)
		}
		return
	}
	// The slices left, this one included: the pass ends by the last.
	slices := int64((c.StartedAt.Add(cycle).Sub(now) + every - 1) / every)
	n := (left + max(slices, 1) - 1) / max(slices, 1)
	var o state.VerifyOutcome
	var werr error
	if n > 0 {
		k, err := v.checker(ctx, c.Place, c.Bucket)
		if err == nil {
			c.After, werr = k.walk(ctx, v.DB, c.Place, c.Bucket, c.After, n)
			k.done()
			o = k.o
		} else {
			werr = err
		}
	}
	c.Checked += o.Checked
	c.Skipped += o.Skipped
	now = v.now()
	var rerr error
	switch {
	case ctx.Err() != nil:
		// Stopped: what it decided is kept for the next start.
		rerr = v.DB.SaveCycle(context.WithoutCancel(ctx), c, time.Time{})
	case werr != nil:
		log.Warn("verification: the full check's slice got no answer; nothing is decided, and it goes on within the hour",
			"err", werr, "checked", o.Checked, "skipped", o.Skipped)
		if rerr = v.DB.SaveCycle(ctx, c, time.Time{}); rerr == nil {
			rerr = v.DB.RecordCheckError(ctx, c.Place, c.Bucket, werr.Error(), now)
		}
	case o.Outcome == state.VerifyFailed:
		log.Error("verification failed: the friend does not hold what it acknowledged; its replica is unverified and its copies move elsewhere",
			"alert", true, "detail", o.Detail, "full_cycle", true)
		rerr = v.DB.RecordUnverified(ctx, c.Place, c.Bucket, o.Detail, now)
		if rerr == nil && v.Alerted != nil {
			v.Alerted()
		}
	default:
		var rest int64
		if rest, rerr = v.DB.ChallengeableAfter(ctx, c.Place, c.Bucket, c.After); rerr != nil {
			break
		}
		switch {
		case rest > 0:
			log.Debug("verification: a full-check slice passed", "checked", o.Checked, "skipped", o.Skipped, "left", rest)
			rerr = v.DB.SaveCycle(ctx, c, now)
		case c.Checked > 0:
			log.Info("verification: a full cycle passed: every object held there was checked whole",
				"checked", c.Checked, "skipped", c.Skipped, "since", c.StartedAt)
			rerr = v.DB.CompleteCycle(ctx, c.Place, c.Bucket, now)
		default:
			log.Info("verification: a full cycle found nothing this node can check there (no local copy to compare with)",
				"skipped", c.Skipped)
			rerr = v.DB.EndCycle(ctx, c.Place, c.Bucket, now)
		}
	}
	if rerr != nil {
		log.Error("verification: recording the full check failed", "err", rerr)
	}
}
