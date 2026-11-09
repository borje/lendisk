package undelete

import (
	"context"
	"fmt"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Overflow targets (S3-4, RET-3, #39): the same command and preview as
// for friends, by each target's own retention.
//
//   - A target with versioning holds a deleted key as a delete marker
//     over the version it had. Undelete lists the markers made since the
//     request's time (Target.ListDeleted) and copies the version before
//     each onto its key (Target.Restore), which makes it current again;
//     the marker is left for the lifecycle rule, since S3-5's credential
//     cannot remove versions. A copy is made before it is recorded, and
//     a copy whose record was lost is adopted after the run, as a
//     friend's revival is.
//   - A target without versioning still holds the object, since its
//     delete is held in lendisk.db until the window ends
//     (state.HoldOverflowDelete). Undelete claims the held deletes made
//     since the time and inside the window, checks the target still
//     holds each with the bytes deleted, records it, and drops the
//     held deletes that record supersedes; the rest are given back.
//
// A copy re-placement drained or moved away from the target (REPL-13)
// is no delete of the user's and is left as it is: a versioned target's
// marker counts only where the target's last row of the key is a delete
// (state.PlaceDeleted), and only deletes of the key are held, never
// drained copies. That holds whatever order the places are asked in: a
// friend restoring the key first makes the target's record an
// acknowledgement of the same version.

// Target is an overflow target as undelete asks it:
// replica.Client is one.
type Target interface {
	ListDeleted(ctx context.Context, bucket string, since time.Time) ([]replica.Deleted, error)
	Restore(ctx context.Context, bucket string, d replica.Deleted) error
	Stat(ctx context.Context, bucket, key string) (replica.ObjectStat, bool, error)
	ListLive(ctx context.Context, bucket string) ([]string, error)
}

// targets lists the overflow targets with their clients; a target whose
// client cannot be built is returned with the error.
type target struct {
	o   state.Overflow
	t   Target
	err error
}

func (r *Runner) targets(ctx context.Context) ([]target, error) {
	if r.Overflow == nil {
		return nil, nil
	}
	os, err := r.DB.Overflows(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]target, 0, len(os))
	for _, o := range os {
		t, err := r.Overflow(o)
		out = append(out, target{o: o, t: t, err: err})
	}
	return out, nil
}

// candidate is an object a target would bring back.
type candidate struct {
	obj  control.ListObject
	d    replica.Deleted      // a versioned target's
	tomb []state.OverflowTomb // an unversioned target's held deletes of the key
}

// targetCandidates lists what tg would bring back of the request's
// bucket since since: a versioned target's keys deleted since then, an
// unversioned one's held deletes since then inside the window
// (claimed, when claim is set). Drained or moved copies, local-only
// keys and entries that cannot be the node's are left out.
func (r *Runner) targetCandidates(ctx context.Context, tg target, q state.UndeleteRequest, prof *profile.Profile,
	since time.Time, claim bool) ([]candidate, error) {
	var out []candidate
	keep := func(c candidate) (bool, error) {
		if prof.Classify(c.obj.Key) == profile.LocalOnly || reject(prof, c.obj) != "" {
			return false, nil
		}
		if c.tomb != nil {
			return true, nil // held deletes are deletes of the key, never drained copies
		}
		return r.DB.PlaceDeleted(ctx, tg.o.Place(), q.Bucket, c.obj.Key)
	}
	if !tg.o.ClientSide() {
		ds, err := tg.t.ListDeleted(ctx, q.Bucket, since)
		if err != nil {
			return nil, err
		}
		for _, d := range ds {
			c := candidate{obj: control.ListObject{Key: d.Key, Size: d.Size, SHA256: d.SHA256}, d: d}
			if ok, err := keep(c); err != nil {
				return nil, err
			} else if ok {
				out = append(out, c)
			}
		}
		return out, nil
	}
	var ts []state.OverflowTomb
	var err error
	if claim {
		ts, err = r.DB.ClaimOverflowTombs(ctx, tg.o.Place(), q.Bucket, since, r.now())
	} else {
		ts, err = r.DB.UndeletableTombs(ctx, tg.o.Place(), q.Bucket, since, r.now())
	}
	if err != nil {
		return nil, err
	}
	byKey := map[string]int{}
	for _, t := range ts {
		if i, ok := byKey[t.Key]; ok {
			out[i].tomb = append(out[i].tomb, t)
			continue
		}
		byKey[t.Key] = len(out)
		out = append(out, candidate{obj: control.ListObject{Key: t.Key, Size: max(t.Size-state.ObjectOverhead, 0), SHA256: t.SHA256},
			tomb: []state.OverflowTomb{t}})
	}
	var kept []candidate
	var back []int64
	for _, c := range out {
		if c.obj.SHA256 == "" {
			// The version deleted was never acknowledged there: the
			// target's own word, checked again at the undelete.
			st, ok, err := tg.t.Stat(ctx, q.Bucket, c.obj.Key)
			if err != nil {
				return nil, err
			}
			if ok {
				c.obj.SHA256, c.obj.Size = st.SHA256, st.Size
			}
		}
		ok, err := keep(c)
		if err != nil {
			return nil, err
		}
		if ok {
			kept = append(kept, c)
			continue
		}
		for _, t := range c.tomb {
			back = append(back, t.Seq)
		}
	}
	if claim && len(back) > 0 {
		if err := r.DB.UnclaimOverflowTombs(ctx, tg.o.Place(), back); err != nil {
			return nil, err
		}
	}
	return kept, nil
}

// previewTarget is a target's part of the preview.
func (r *Runner) previewTarget(ctx context.Context, tg target, q state.UndeleteRequest, prof *profile.Profile,
	since time.Time, all map[string]int64, snaps map[string]bool) FriendPreview {
	fp := FriendPreview{Peer: tg.o.Place()}
	if tg.err != nil {
		fp.Error = tg.err.Error()
		return fp
	}
	cs, err := r.targetCandidates(ctx, tg, q, prof, since, false)
	if err != nil {
		fp.Error = err.Error()
		return fp
	}
	for _, c := range cs {
		fp.Keys++
		fp.Bytes += c.obj.Size
		all[c.obj.Key] = max(all[c.obj.Key], c.obj.Size)
		if prof.Classify(c.obj.Key) == profile.Snapshot {
			snaps[c.obj.Key] = true
		}
	}
	return fp
}

// undeleteTarget brings back what tg holds deleted of the request's
// bucket since since, and records it.
func (r *Runner) undeleteTarget(ctx context.Context, tg target, q state.UndeleteRequest, prof *profile.Profile,
	since time.Time, fr *FriendResult) error {
	if tg.err != nil {
		return tg.err
	}
	place := tg.o.Place()
	cs, err := r.targetCandidates(ctx, tg, q, prof, since, true)
	if err != nil {
		return err
	}
	var claimed []int64
	for _, c := range cs {
		for _, t := range c.tomb {
			claimed = append(claimed, t.Seq)
		}
	}
	// Whatever happens, the claims not superseded by a record go back,
	// so those deletes are sent when due.
	defer func() {
		bg := context.WithoutCancel(ctx)
		if _, err := r.DB.DropSupersededTombs(bg, place); err != nil {
			r.log().Error("undelete: dropping the held deletes an undelete cancelled failed", "place", place, "err", err)
		}
		if err := r.DB.UnclaimOverflowTombs(bg, place, claimed); err != nil {
			r.log().Error("undelete: giving back held deletes failed; they are sent within the hour after their window", "place", place, "err", err)
		}
	}()
	const page = 100
	for start := 0; start < len(cs); start += page {
		var objs []state.Restored
		for _, c := range cs[start:min(start+page, len(cs))] {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			why := ""
			if tg.o.ClientSide() {
				// Still held there with the bytes deleted?
				st, ok, err := tg.t.Stat(ctx, q.Bucket, c.obj.Key)
				switch {
				case err != nil:
					return err
				case !ok:
					why = "the target no longer holds it"
				case st.SHA256 != c.obj.SHA256:
					why = fmt.Sprintf("the target holds other bytes (%s)", st.SHA256)
				}
				c.obj.Size = st.Size
			} else if err := tg.t.Restore(ctx, q.Bucket, c.d); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				why = err.Error()
			}
			if why != "" {
				fr.Failed++
				if len(fr.FailedKeys) < MaxNamed {
					fr.FailedKeys = append(fr.FailedKeys, control.UndeleteSkip{Key: c.obj.Key, Reason: gateway.SkipFailed, Detail: why})
				}
				r.log().Warn("undelete: an object at an overflow target cannot come back", "place", place, "bucket", q.Bucket,
					"key", c.obj.Key, "why", why)
				continue
			}
			fr.Restored++
			fr.Bytes += c.obj.Size
			objs = append(objs, state.Restored{Key: c.obj.Key, Size: c.obj.Size, SHA256: c.obj.SHA256,
				Class: string(prof.Classify(c.obj.Key)), Rewritable: prof.Rewritable(c.obj.Key)})
		}
		rr, err := r.record(ctx, place, q.Bucket, q.ID, objs)
		if err != nil {
			return fmt.Errorf("recording what the overflow target restored: %w", err)
		}
		r.tally(fr, place, q.Bucket, rr)
	}
	return nil
}

// adoptTarget adopts, at a versioned target, the copies a run made whose
// record was lost: keys live there that the index holds deleted with
// that delete acknowledged there (state.AdoptCandidates). An
// unversioned target holds every deleted key live until its window
// ends, so nothing is adopted there.
func (r *Runner) adoptTarget(ctx context.Context, tg target, q state.UndeleteRequest, prof *profile.Profile) (int, error) {
	if tg.err != nil || tg.o.ClientSide() {
		return 0, nil
	}
	live, err := tg.t.ListLive(ctx, q.Bucket)
	if err != nil {
		return 0, err
	}
	keys, err := r.DB.AdoptCandidates(ctx, tg.o.Place(), q.Bucket, q.ID, live)
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	var objs []state.Restored
	for _, k := range keys {
		st, ok, err := tg.t.Stat(ctx, q.Bucket, k)
		if err != nil {
			return 0, err
		}
		o := control.ListObject{Key: k, Size: st.Size, SHA256: st.SHA256}
		if !ok || reject(prof, o) != "" || prof.Classify(k) == profile.LocalOnly {
			continue
		}
		objs = append(objs, state.Restored{Key: k, Size: st.Size, SHA256: st.SHA256,
			Class: string(prof.Classify(k)), Rewritable: prof.Rewritable(k)})
	}
	return r.DB.AdoptRestored(ctx, tg.o.Place(), q.Bucket, q.ID, objs)
}
