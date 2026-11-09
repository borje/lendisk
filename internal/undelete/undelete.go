// Package undelete is the borrower's side of undelete (RET-3, #32): the
// daemon's runner of the requests `lendisk undelete` makes through
// lendisk.db (state.RequestUndelete). For each it first reads every
// friend's list of the bucket (the NET-7 list call, which carries each
// tombstoned key with when) and writes a preview of what would come
// back; once the user confirms, it asks every friend, through the
// control API undelete call, to move the bucket's tombs deleted since
// that time back to the live set, a page at a time, and records what
// each restored in the index (state.DB.RecordRestored): a done put of
// the version, acknowledged at that friend, not held locally, so the
// read-through serves it and the offsite watermark counts it. Overflow
// targets are asked the same command by their own retention
// (overflow.go, S3-4): a versioned one copies the version before each
// delete marker back, an unversioned one's held deletes are cancelled.
//
// Rules a caller must keep:
//
//   - One Runner per daemon, dialling through the daemon's transport: a
//     one-shot process dialling with the node's keys would stall the
//     daemon's clients at the relay (§3.7, #69).
//   - Nothing a friend answers is trusted beyond what recovery trusts of
//     a list (ID-5): keys a borrower may not name, impossible sizes,
//     malformed hashes and hash-named keys whose hash is not their name
//     are rejected and counted, never recorded; a key this node has no
//     history of is taken only if the preview the user confirmed listed
//     it (state.DB.RecordRestored).
//   - What a friend revived must reach the index: a page that fails to
//     record is recorded again before the next is asked for, and after
//     each run the friends' lists are read again to adopt revivals whose
//     answer was lost (state.DB.AdoptRestored).
//   - The local store is never written: restored objects are fetched on
//     demand (ID-6, EP-10).
package undelete

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// DefaultPoll is how often Run looks for requests.
const DefaultPoll = time.Second

// maxWaits bounds how often one friend's undelete waits out its 503s: a
// friend at the default pace answers a 6,000-key bucket after a few.
const maxWaits = 1000

// MaxNamed is how many keys a preview or result names in each list (the
// snapshots that come back, the conflicts); the counts are whole.
const MaxNamed = 50

// Preview is what an undelete would bring back, from the friends' lists:
// per friend, and in all (each key once, whichever friends hold it).
type Preview struct {
	Bucket    string          `json:"bucket"`
	Since     int64           `json:"since"` // Unix seconds; 0: every tomb in the window
	Rewrites  bool            `json:"rewrites,omitempty"`
	Friends   []FriendPreview `json:"friends"`
	Keys      int             `json:"keys"`
	Bytes     int64           `json:"bytes"`
	Snapshots int             `json:"snapshots"`
	Named     []string        `json:"named,omitempty"` // snapshot keys that come back, at most MaxNamed
	Rewritten int             `json:"rewritten,omitempty"`

	keys []string // every key that would come back: what the user confirms
}

// FriendPreview is one friend's part of a preview: the tombstoned keys it
// lists deleted since then, their bytes, the live keys it lists with
// versions replaced by rewrites (with Rewrites; the list does not say
// when), or why it could not be listed.
type FriendPreview struct {
	Peer      string `json:"peer"`
	Keys      int    `json:"keys"`
	Bytes     int64  `json:"bytes"`
	Rewritten int    `json:"rewritten,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Result is what an undelete did, per friend.
type Result struct {
	Friends []FriendResult `json:"friends"`
}

// FriendResult is one friend's undelete: the keys it restored and their
// bytes; how the index took them (Recorded, Acked, Kept, Busy, Unknown:
// state.RestoreResult); the keys it left as they were (Conflicts: live
// with other bytes; Live: with the same; Failed: it could not revive
// them now); keys it revived whose answer was lost, found live in its
// list after the run (Adopted); entries rejected; and why it could not
// finish.
type FriendResult struct {
	Peer          string                 `json:"peer"`
	Restored      int                    `json:"restored"`
	Bytes         int64                  `json:"bytes"`
	Recorded      int                    `json:"recorded"`
	Acked         int                    `json:"acked"`
	Kept          []state.KeptVersion    `json:"kept,omitempty"` // at most MaxNamed
	KeptCount     int                    `json:"kept_count,omitempty"`
	Busy          int                    `json:"busy,omitempty"`
	BusyKeys      []string               `json:"busy_keys,omitempty"` // at most MaxNamed
	Unknown       int                    `json:"unknown,omitempty"`
	Failed        int                    `json:"failed,omitempty"`
	FailedKeys    []control.UndeleteSkip `json:"failed_keys,omitempty"` // at most MaxNamed
	Adopted       int                    `json:"adopted,omitempty"`
	Conflicts     []control.UndeleteSkip `json:"conflicts,omitempty"` // at most MaxNamed
	ConflictCount int                    `json:"conflict_count,omitempty"`
	Live          int                    `json:"live,omitempty"`
	Rejected      int                    `json:"rejected,omitempty"`
	Error         string                 `json:"error,omitempty"`
}

// Runner runs the undelete requests in lendisk.db.
type Runner struct {
	DB   *state.DB
	Self *identity.Identity
	// Dial reaches a friend's tunnel port: the daemon's transport.
	Dial manifest.DialFunc
	// Profiles classify each restored key by its bucket's profile
	// (EP-7); nil means the compiled-in ones.
	Profiles profile.Set
	// List reads one page of the list of sealed at f after the key
	// after, and Call makes one undelete call at f; nil means the control
	// API over Dial. Tests set them.
	List func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error)
	Call func(ctx context.Context, f state.Friendship, req control.UndeleteRequest) (control.UndeleteResponse, error)
	// Record records a page a friend restored; nil means
	// state.DB.RecordRestored. Tests set it.
	Record func(ctx context.Context, place, bucket string, request int64, objs []state.Restored) (state.RestoreResult, error)
	// Now is the clock; nil means time.Now. Poll is how often Run looks
	// for requests (DefaultPoll); Timeout bounds one call (1 min).
	Now     func() time.Time
	Poll    time.Duration
	Timeout time.Duration
	// Wait waits out a friend's 503 for d; nil waits on a timer.
	Wait func(ctx context.Context, d time.Duration) error
	// Overflow builds an overflow target's client (S3-4, #39): the
	// daemon's reads the credential from the environment, as the
	// worker's does. Nil leaves overflow targets out.
	Overflow func(o state.Overflow) (Target, error)
	Log      *slog.Logger
}

func (r *Runner) now() time.Time {
	if r.Now == nil {
		return time.Now()
	}
	return r.Now()
}

func (r *Runner) log() *slog.Logger {
	if r.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return r.Log
}

// Run serves requests every Poll until ctx ends.
func (r *Runner) Run(ctx context.Context) {
	t := time.NewTicker(cmp.Or(r.Poll, DefaultPoll))
	defer t.Stop()
	for {
		if err := r.Once(ctx); err != nil && ctx.Err() == nil {
			r.log().Error("undelete: reading the requests failed; trying again", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Once serves every pending request: a preview for each asked, the
// undelete for each confirmed.
func (r *Runner) Once(ctx context.Context) error {
	reqs, err := r.DB.PendingUndeletes(ctx)
	if err != nil {
		return err
	}
	for _, q := range reqs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch q.Stage {
		case state.UndeleteAsked:
			p, err := r.Preview(ctx, q)
			if err != nil {
				r.finish(ctx, q.ID, true, map[string]string{"error": err.Error()})
				continue
			}
			b, _ := json.Marshal(p)
			if err := r.DB.PreviewUndelete(ctx, q.ID, string(b), p.keys, r.now()); err != nil && !errors.Is(err, state.ErrNotFound) {
				return err
			}
		case state.UndeleteConfirmed:
			res, err := r.Undelete(ctx, q)
			if err != nil {
				r.finish(ctx, q.ID, true, map[string]string{"error": err.Error()})
				continue
			}
			r.finish(ctx, q.ID, false, res)
		}
	}
	return nil
}

func (r *Runner) finish(ctx context.Context, id int64, failed bool, v any) {
	b, _ := json.Marshal(v)
	if err := r.DB.FinishUndelete(ctx, id, failed, string(b), r.now()); err != nil && !errors.Is(err, state.ErrNotFound) {
		r.log().Error("undelete: recording a request's outcome failed", "id", id, "err", err)
	}
}

// holders are the friends asked: every friend with an address. After an
// attacker's prune, placement no longer says who holds the bucket's
// tombs, so every friend is asked, and one holding none says so
// (NoSuchBucket) or lists nothing deleted since.
func (r *Runner) holders(ctx context.Context) ([]state.Friendship, error) {
	fs, err := r.DB.Friendships(ctx)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(fs, func(f state.Friendship) bool { return f.Address == "" }), nil
}

func (r *Runner) bucketProfile(ctx context.Context, bucket string) (*profile.Profile, error) {
	b, err := r.DB.GetBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	profiles := r.Profiles
	if profiles == nil {
		if profiles, err = profile.Load(""); err != nil {
			return nil, err
		}
	}
	return profiles.Get(b.Profile), nil
}

// Preview reads every friend's list of the request's bucket and counts
// what would come back: each tombstoned key whose newest tomb is from
// since on, and, with rewrites, each live key with replaced versions.
// A friend that cannot be listed is reported, not fatal.
func (r *Runner) Preview(ctx context.Context, q state.UndeleteRequest) (Preview, error) {
	p := Preview{Bucket: q.Bucket, Rewrites: q.Rewrites}
	if !q.Since.IsZero() {
		p.Since = q.Since.Unix()
	}
	prof, err := r.bucketProfile(ctx, q.Bucket)
	if err != nil {
		return p, err
	}
	fs, err := r.holders(ctx)
	if err != nil {
		return p, err
	}
	sealed := r.Self.SealedName(q.Bucket)
	all := map[string]int64{}
	snaps := map[string]bool{}
	for _, f := range fs {
		fp := FriendPreview{Peer: f.Peer}
		list, done := r.lister(f)
		err := func() error {
			defer done()
			after := ""
			for pages := 0; ; pages++ {
				if pages > 2*gateway.MaxListPage {
					return errors.New("the friend's list does not end")
				}
				cctx, cancel := context.WithTimeout(ctx, cmp.Or(r.Timeout, time.Minute))
				page, err := list(cctx, f, sealed, after)
				cancel()
				var ce *control.CallError
				if errors.As(err, &ce) && ce.Code == control.CodeNoSuchBucket {
					return nil // holds nothing of it
				}
				if err != nil {
					return err
				}
				for _, o := range page.Objects {
					switch {
					case o.TombstonedAt != nil && !o.TombstonedAt.Before(time.Unix(p.Since, 0)):
						if prof.Classify(o.Key) == profile.LocalOnly || reject(prof, o) != "" {
							continue
						}
						fp.Keys++
						fp.Bytes += o.Size
						all[o.Key] = max(all[o.Key], o.Size)
						if prof.Classify(o.Key) == profile.Snapshot {
							snaps[o.Key] = true
						}
					case o.TombstonedAt == nil && q.Rewrites && len(o.Replaced) > 0:
						fp.Rewritten++
					}
				}
				if page.Next == "" {
					return nil
				}
				after = page.Next
			}
		}()
		if err != nil {
			fp.Error = err.Error()
		}
		p.Rewritten = max(p.Rewritten, fp.Rewritten)
		p.Friends = append(p.Friends, fp)
	}
	tgs, err := r.targets(ctx)
	if err != nil {
		return p, err
	}
	for _, tg := range tgs {
		p.Friends = append(p.Friends, r.previewTarget(ctx, tg, q, prof, time.Unix(p.Since, 0), all, snaps))
	}
	for k, n := range all {
		p.Keys++
		p.Bytes += n
		p.keys = append(p.keys, k)
	}
	p.Snapshots = len(snaps)
	for _, k := range slices.Sorted(maps.Keys(snaps)) {
		if len(p.Named) == MaxNamed {
			break
		}
		p.Named = append(p.Named, k)
	}
	return p, nil
}

// Undelete asks every friend to restore the request's bucket since its
// time, page by page, and records what each restored. A friend that
// fails is reported and the others still asked; asking again is safe.
func (r *Runner) Undelete(ctx context.Context, q state.UndeleteRequest) (Result, error) {
	var res Result
	prof, err := r.bucketProfile(ctx, q.Bucket)
	if err != nil {
		return res, err
	}
	fs, err := r.holders(ctx)
	if err != nil {
		return res, err
	}
	sealed := r.Self.SealedName(q.Bucket)
	var since int64
	if !q.Since.IsZero() {
		since = q.Since.Unix()
	}
	for _, f := range fs {
		fr := FriendResult{Peer: f.Peer}
		if err := r.undeleteAt(ctx, f, q, prof, sealed, since, &fr); err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			fr.Error = err.Error()
			r.log().Warn("undelete: a friend's undelete did not finish; run `lendisk undelete` again to finish it",
				"peer", f.Peer, "bucket", q.Bucket, "err", err)
		}
		if fr.Restored > 0 {
			r.log().Info("undelete: a friend restored deleted objects", "peer", f.Peer, "bucket", q.Bucket,
				"restored", fr.Restored, "bytes", fr.Bytes, "conflicts", fr.ConflictCount)
		}
		res.Friends = append(res.Friends, fr)
	}
	// Keys a friend revived whose answer never arrived (a call that timed
	// out after the lender's work, a restart): live in its list now,
	// deleted here. Adopted, so an earlier failed run heals too.
	for i, f := range fs {
		if res.Friends[i].Error != "" {
			continue
		}
		n, err := r.adopt(ctx, f, q, prof, sealed)
		res.Friends[i].Adopted += n
		if err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			res.Friends[i].Error = "listing what it holds after the undelete: " + err.Error()
		}
		if n > 0 {
			r.log().Info("undelete: adopted objects a friend restored whose answer was lost", "peer", f.Peer, "bucket", q.Bucket, "adopted", n)
		}
	}
	tgs, err := r.targets(ctx)
	if err != nil {
		return res, err
	}
	for _, tg := range tgs {
		fr := FriendResult{Peer: tg.o.Place()}
		err := r.undeleteTarget(ctx, tg, q, prof, time.Unix(since, 0), &fr)
		if err == nil {
			var n int
			n, err = r.adoptTarget(ctx, tg, q, prof)
			fr.Adopted += n
		}
		if err != nil {
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			fr.Error = err.Error()
			r.log().Warn("undelete: an overflow target's undelete did not finish; run `lendisk undelete` again to finish it",
				"place", fr.Peer, "bucket", q.Bucket, "err", err)
		}
		if fr.Restored > 0 {
			r.log().Info("undelete: an overflow target restored deleted objects", "place", fr.Peer, "bucket", q.Bucket,
				"restored", fr.Restored, "bytes", fr.Bytes)
		}
		res.Friends = append(res.Friends, fr)
	}
	return res, nil
}

// adopt reads f's list of the bucket and records the live keys an
// undelete revived there that the index still holds deleted
// (state.DB.AdoptRestored).
func (r *Runner) adopt(ctx context.Context, f state.Friendship, q state.UndeleteRequest, prof *profile.Profile, sealed string) (int, error) {
	list, done := r.lister(f)
	defer done()
	after, n := "", 0
	for pages := 0; ; pages++ {
		if pages > 2*gateway.MaxListPage {
			return n, errors.New("the friend's list does not end")
		}
		cctx, cancel := context.WithTimeout(ctx, cmp.Or(r.Timeout, time.Minute))
		page, err := list(cctx, f, sealed, after)
		cancel()
		var ce *control.CallError
		if errors.As(err, &ce) && ce.Code == control.CodeNoSuchBucket {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		var objs []state.Restored
		for _, o := range page.Objects {
			if o.TombstonedAt != nil || reject(prof, o) != "" || prof.Classify(o.Key) == profile.LocalOnly {
				continue
			}
			objs = append(objs, state.Restored{Key: o.Key, Size: o.Size, SHA256: o.SHA256,
				Class: string(prof.Classify(o.Key)), Rewritable: prof.Rewritable(o.Key)})
		}
		a, err := r.DB.AdoptRestored(ctx, f.Peer, q.Bucket, q.ID, objs)
		n += a
		if err != nil {
			return n, err
		}
		if page.Next == "" {
			return n, nil
		}
		after = page.Next
	}
}

// record records a page f restored, trying again on failure before the
// next page is asked for: the friend has revived those keys, and only
// this answer says so (#32 review).
func (r *Runner) record(ctx context.Context, place, bucket string, request int64, objs []state.Restored) (state.RestoreResult, error) {
	rec := r.Record
	if rec == nil {
		rec = r.DB.RecordRestored
	}
	var err error
	for try := 0; try < 5; try++ {
		var rr state.RestoreResult
		if rr, err = rec(ctx, place, bucket, request, objs); err == nil {
			return rr, nil
		}
		r.log().Warn("undelete: recording what a friend restored failed; trying again", "peer", place, "bucket", bucket, "err", err)
		if werr := r.wait(ctx, time.Second); werr != nil {
			return rr, werr
		}
	}
	return state.RestoreResult{}, err
}

func (r *Runner) undeleteAt(ctx context.Context, f state.Friendship, q state.UndeleteRequest, prof *profile.Profile,
	sealed string, since int64, fr *FriendResult) error {
	call, done := r.caller(f)
	defer done()
	after, waits := "", 0
	for pages := 0; ; pages++ {
		if pages > 2*gateway.MaxListPage {
			return errors.New("the friend's undelete does not end")
		}
		req := control.UndeleteRequest{Bucket: sealed, Since: since, Rewrites: q.Rewrites, After: after}
		cctx, cancel := context.WithTimeout(ctx, cmp.Or(r.Timeout, time.Minute))
		page, err := call(cctx, f, req)
		cancel()
		var ce *control.CallError
		switch {
		case errors.As(err, &ce) && ce.Code == control.CodeNoSuchBucket:
			return nil // holds nothing of it
		case errors.As(err, &ce) && ce.Status == http.StatusServiceUnavailable && waits < maxWaits:
			// The friend's pace (LEND-5): wait as long as it asks.
			waits++
			if err := r.wait(ctx, min(max(ce.RetryAfter, time.Second), time.Minute)); err != nil {
				return err
			}
			pages--
			continue
		case err != nil:
			return err
		}
		objs := make([]state.Restored, 0, len(page.Restored))
		for _, o := range page.Restored {
			if why := reject(prof, o); why != "" {
				fr.Rejected++
				r.log().Warn("undelete: a friend restored an entry that cannot be right; not recorded",
					"peer", f.Peer, "bucket", q.Bucket, "key", o.Key, "why", why)
				continue
			}
			fr.Restored++
			fr.Bytes += o.Size
			if prof.Classify(o.Key) == profile.LocalOnly {
				continue // never replicated; nothing to record
			}
			objs = append(objs, state.Restored{Key: o.Key, Size: o.Size, SHA256: o.SHA256,
				Class: string(prof.Classify(o.Key)), Rewritable: prof.Rewritable(o.Key)})
		}
		rr, err := r.record(ctx, f.Peer, q.Bucket, q.ID, objs)
		if err != nil {
			return fmt.Errorf("recording what the friend restored: %w", err)
		}
		r.tally(fr, f.Peer, q.Bucket, rr)
		for _, s := range page.Skipped {
			if s.Reason == gateway.SkipLive {
				fr.Live++
				continue
			}
			if s.Reason == gateway.SkipFailed {
				fr.Failed++
				if len(fr.FailedKeys) < MaxNamed {
					fr.FailedKeys = append(fr.FailedKeys, s)
				}
				continue
			}
			fr.ConflictCount++
			if len(fr.Conflicts) < MaxNamed {
				fr.Conflicts = append(fr.Conflicts, s)
			}
		}
		if page.Next == "" {
			return nil
		}
		after = page.Next
	}
}

// tally adds what the index made of a page place restored to fr.
func (r *Runner) tally(fr *FriendResult, place, bucket string, rr state.RestoreResult) {
	fr.Recorded += rr.Recorded
	fr.Acked += rr.Acked
	fr.Busy += len(rr.Busy)
	fr.Unknown += len(rr.Unknown)
	for _, k := range rr.Busy {
		if len(fr.BusyKeys) < MaxNamed {
			fr.BusyKeys = append(fr.BusyKeys, k)
		}
	}
	for _, k := range rr.Unknown {
		r.log().Warn("undelete: a place restored a key this node never had and the preview did not list; not recorded",
			"peer", place, "bucket", bucket, "key", k)
	}
	fr.KeptCount += len(rr.Kept)
	for _, k := range rr.Kept {
		if len(fr.Kept) < MaxNamed {
			fr.Kept = append(fr.Kept, k)
		}
	}
}

func (r *Runner) wait(ctx context.Context, d time.Duration) error {
	if r.Wait != nil {
		return r.Wait(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// client is a control client at f over Dial; done closes its connections.
func (r *Runner) client(f state.Friendship) (*control.Client, func()) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		peer, err := identity.ParseNodeID(f.Peer)
		if err != nil {
			return nil, err
		}
		return r.Dial(ctx, peer, f.Address)
	}}
	return &control.Client{HTTP: &http.Client{Transport: tr}}, tr.CloseIdleConnections
}

func (r *Runner) lister(f state.Friendship) (func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error), func()) {
	if r.List != nil {
		return r.List, func() {}
	}
	c, done := r.client(f)
	return func(ctx context.Context, _ state.Friendship, sealed, after string) (control.ListPage, error) {
		return c.ListPage(ctx, sealed, after, 0)
	}, done
}

func (r *Runner) caller(f state.Friendship) (func(ctx context.Context, f state.Friendship, req control.UndeleteRequest) (control.UndeleteResponse, error), func()) {
	if r.Call != nil {
		return r.Call, func() {}
	}
	c, done := r.client(f)
	return func(ctx context.Context, _ state.Friendship, req control.UndeleteRequest) (control.UndeleteResponse, error) {
		return c.Undelete(ctx, req)
	}, done
}

// reject says why an entry a friend sent cannot be a copy of the
// borrower's object, or "" if it can: recovery's checks (ID-5).
func reject(prof *profile.Profile, o control.ListObject) string {
	switch {
	case gateway.CheckKey(o.Key) != nil:
		return "a key a borrower cannot name"
	case o.Size < 0 || o.Size > gateway.MaxObjectSize:
		return fmt.Sprintf("size %d", o.Size)
	case !isSHA256(o.SHA256):
		return "not a SHA-256 in lower-case hex"
	case prof.HashNamed(o.Key) && profile.NameHash(o.Key) != o.SHA256:
		return "its hash is not the one its name gives"
	}
	return ""
}

func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}
