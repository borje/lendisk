package verify

import (
	"context"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// Overflow targets' versioning (S3-4, #97). overflow add reads it once,
// and a user may suspend or enable it at the provider at any time
// after: deletes sent to a bucket whose versioning was suspended are
// final. So each pass reads it again for every target whose last answer
// is Every (a day) old, alongside the daily check of its replicas, and
// records the answer (state.RecordOverflowVersioning). A change switches
// the target's retention from then on: enabled to suspended or off
// holds every later delete as a client-side tomb and alerts
// (overflow_versioning, alert package); off to enabled sends later
// deletes as delete markers, and the tombs already held stay held. A
// read that gets no answer decides nothing and is tried again after
// Retry.

// recheckVersioning reads the versioning of each overflow target due.
func (v *Verifier) recheckVersioning(ctx context.Context) {
	if v.Overflow == nil {
		return
	}
	targets, err := v.DB.Overflows(ctx)
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Error("verification: reading the overflow targets failed", "err", err)
		}
		return
	}
	now := v.now()
	every, retry := or(v.Every, DefaultEvery), or(v.Retry, DefaultRetry)
	for _, o := range targets {
		last := o.VersioningCheckedAt
		if last.IsZero() {
			last = o.AddedAt // overflow add's read
		}
		tried := v.versioningTried(o.Name)
		// A clock set back makes a read due at once.
		if (now.Sub(last) < every && !now.Before(last)) || (now.Sub(tried) < retry && !now.Before(tried)) {
			continue
		}
		v.readVersioning(ctx, o, now)
		if ctx.Err() != nil {
			return
		}
	}
}

// versioningTried is when target name's versioning was last read and
// got no answer; zero if not since the last answer.
func (v *Verifier) versioningTried(name string) time.Time {
	v.triedMu.Lock()
	defer v.triedMu.Unlock()
	return v.tried[name]
}

func (v *Verifier) setVersioningTried(name string, at time.Time) {
	v.triedMu.Lock()
	defer v.triedMu.Unlock()
	if v.tried == nil {
		v.tried = map[string]time.Time{}
	}
	if at.IsZero() {
		delete(v.tried, name)
		return
	}
	v.tried[name] = at
}

// readVersioning reads target o's versioning at now and records it.
func (v *Verifier) readVersioning(ctx context.Context, o state.Overflow, now time.Time) {
	log := v.Log.With("peer", o.Place())
	t, err := v.Overflow(o)
	if err != nil {
		log.Warn("verification: cannot build the overflow target's client; its versioning is read again later", "err", err)
		v.setVersioningTried(o.Name, now)
		return
	}
	got, err := t.Versioning(ctx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		log.Warn("verification: the overflow target's versioning could not be read; nothing changes, and it is read again later",
			"err", err, "retry_in", or(v.Retry, DefaultRetry))
		v.setVersioningTried(o.Name, now)
		return
	}
	v.setVersioningTried(o.Name, time.Time{})
	ch, err := v.DB.RecordOverflowVersioning(ctx, o.Name, got, now)
	if err != nil {
		log.Error("verification: recording the overflow target's versioning failed", "err", err)
		return
	}
	switch {
	case ch == nil:
		log.Debug("verification: the overflow target's versioning is unchanged", "versioning", got)
	case ch.Previous == state.VersioningEnabled:
		log.Error("overflow alert: the target's bucket no longer has versioning; deletes there are held as client-side tombstones from now on",
			"alert", true, "bucket", o.Bucket, "versioning", got, "was", ch.Previous)
		if v.Alerted != nil {
			v.Alerted()
		}
	case got == state.VersioningEnabled:
		log.Info("verification: the overflow target's bucket has versioning now; deletes from now on are delete markers the provider keeps, and the deletes held already stay held for their window",
			"bucket", o.Bucket, "was", ch.Previous)
	default:
		log.Info("verification: the overflow target's versioning changed; its retention stays client-side",
			"bucket", o.Bucket, "versioning", got, "was", ch.Previous)
	}
}
