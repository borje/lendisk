package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// Lister reads the lists still to read (state.Relists) from each place
// and records them in placement (ID-5): `recover` makes one round, and
// the daemon runs it until every place has been listed, so a place that
// was offline at recovery is listed when it returns. Its objects count
// as held where other lists found them until then.
type Lister struct {
	DB   *state.DB
	Self *identity.Identity
	// Dial reaches a friend's tunnel port: the recovery's friend dialler,
	// or the daemon's transport.
	Dial manifest.DialFunc
	// Page reads one page of the list of sealed at friend f after the
	// key after; nil means the control API list call over Dial. Tests
	// set it.
	Page func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error)
	// Profiles classify each listed key by its bucket's profile (EP-7);
	// nil means the compiled-in ones.
	Profiles profile.Set
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Timeout bounds one page (default 1 min); Poll is the time between
	// the daemon's rounds (default 1 min); after failures in a row a
	// place's list is due again only MinBackoff after the last, doubling
	// per failure to at most MaxBackoff (defaults 1 min and 1 h).
	Timeout, Poll          time.Duration
	MinBackoff, MaxBackoff time.Duration
	// MaxKeys bounds one place's list of one bucket, in entries and
	// pages read (default DefaultMaxKeys): past it the list is abandoned,
	// so a friend that lists without end cannot keep recovery listing.
	MaxKeys int64
	Log     *slog.Logger
}

// DefaultMaxKeys is Lister.MaxKeys's default: far above any backup
// repository's object count (restic's 16 MiB packs make 10 million
// objects 160 TB).
const DefaultMaxKeys = 10_000_000

// Result is a list read in a round, whole or until it failed.
type Result struct {
	Place, Bucket string
	state.ListedCount
	// Rejected counts entries refused: a key a borrower may not name, a
	// size no object has, a malformed hash, or a hash that is not the
	// name of a key named by its hash. Each is logged.
	Rejected int
	Err      error // nil when the list was read to its end
}

func or(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func (l *Lister) now() time.Time {
	if l.Now == nil {
		return time.Now()
	}
	return l.Now()
}

func (l *Lister) due(r state.Relist, now time.Time) bool {
	if r.Failures == 0 {
		return true
	}
	wait, ceil := or(l.MinBackoff, time.Minute), or(l.MaxBackoff, time.Hour)
	for i := int64(1); i < r.Failures && wait < ceil; i++ {
		wait *= 2
	}
	return now.Sub(r.TriedAt) >= min(wait, ceil)
}

// Pending reports how many lists are still to read.
func (l *Lister) Pending(ctx context.Context) (int, error) {
	rs, err := l.DB.Relists(ctx)
	return len(rs), err
}

// Round reads every list that is due, place by place. A place that
// fails one list is not asked for its others in this round: each is
// recorded as failed, and tried again after the backoff. A list past
// MaxKeys is abandoned (its result says so) and not tried again.
func (l *Lister) Round(ctx context.Context) ([]Result, error) {
	rs, err := l.DB.Relists(ctx)
	if err != nil {
		return nil, err
	}
	profiles := l.Profiles
	if profiles == nil {
		if profiles, err = profile.Load(""); err != nil {
			return nil, err
		}
	}
	now := l.now()
	var out []Result
	failed := map[string]error{}
	for _, r := range rs {
		if !l.due(r, now) {
			continue
		}
		res := Result{Place: r.Place, Bucket: r.Bucket}
		var abandoned bool
		if err, ok := failed[r.Place]; ok {
			res.Err = err
		} else {
			abandoned, res.Err = l.listOne(ctx, r, profiles, &res)
		}
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		switch {
		case abandoned:
			if err := l.DB.AbandonRelist(ctx, r.Place, r.Bucket); err != nil {
				return out, err
			}
			l.logger().Warn("recovery: a place's list is longer than any list may be; abandoned",
				"place", r.Place, "bucket", r.Bucket, "err", res.Err)
		case res.Err != nil:
			failed[r.Place] = res.Err
			if err := l.DB.RelistFailed(ctx, r.Place, r.Bucket, res.Err.Error(), l.now()); err != nil {
				return out, err
			}
		}
		out = append(out, res)
	}
	return out, nil
}

func (l *Lister) logger() *slog.Logger {
	if l.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l.Log
}

// errNoMoreThan is a list past MaxKeys.
type errNoMoreThan int64

func (e errNoMoreThan) Error() string {
	return fmt.Sprintf("the place lists more than %d keys and pages; its list was abandoned", int64(e))
}

// listOne reads one list from its resume point to its end, counting in
// res; abandoned reports a list past MaxKeys.
func (l *Lister) listOne(ctx context.Context, r state.Relist, profiles profile.Set, res *Result) (abandoned bool, err error) {
	f, err := l.DB.GetFriendship(ctx, r.Place)
	if errors.Is(err, state.ErrNotFound) {
		// No longer a friend: nothing to list there.
		_, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, nil, 0, "", true)
		return false, err
	} else if err != nil {
		return false, err
	}
	prof := profiles.Get("")
	if b, err := l.DB.GetBucket(ctx, r.Bucket); err == nil {
		prof = profiles.Get(b.Profile)
	} else if !errors.Is(err, state.ErrNotFound) {
		return false, err
	}
	page := l.Page
	if page == nil {
		var done func()
		page, done = l.controlPage(f)
		defer done()
	}
	maxKeys := l.MaxKeys
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	sealed, after, seen := l.Self.SealedName(r.Bucket), r.After, r.Seen
	for {
		pctx, cancel := context.WithTimeout(ctx, or(l.Timeout, time.Minute))
		p, err := page(pctx, f, sealed, after)
		cancel()
		var ce *control.CallError
		if errors.As(err, &ce) && ce.Code == control.CodeNoSuchBucket {
			// The place holds no bucket of this name: nothing of it. Any
			// other refusal, 404s included, is tried again.
			p, err = control.ListPage{}, nil
		}
		if err != nil {
			return false, err
		}
		// Every page counts, an empty one too, so a list ends.
		seen += int64(max(len(p.Objects), 1))
		if seen > maxKeys {
			return true, errNoMoreThan(maxKeys)
		}
		objs := make([]state.Listed, 0, len(p.Objects))
		last := after
		for _, o := range p.Objects {
			if o.Key <= last {
				return false, fmt.Errorf("the place's list does not rise past %q", last)
			}
			last = o.Key
			class := prof.Classify(o.Key)
			if class == profile.LocalOnly {
				continue // never replicated; a place should not hold it
			}
			if why := l.reject(prof, o); why != "" {
				res.Rejected++
				l.logger().Warn("recovery: a place listed an entry that cannot be right; not counted",
					"place", r.Place, "bucket", r.Bucket, "key", o.Key, "why", why)
				continue
			}
			objs = append(objs, state.Listed{Key: o.Key, Size: o.Size, SHA256: o.SHA256, Class: string(class),
				Tombstoned: o.TombstonedAt != nil, HashNamed: prof.HashNamed(o.Key)})
		}
		if p.Next != "" && (p.Next <= after || p.Next < last) {
			return false, fmt.Errorf("the place's list does not move on from %q", after)
		}
		n, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, objs, max(len(p.Objects), 1), p.Next, p.Next == "")
		if err != nil {
			return false, err
		}
		res.ListedCount.Add(n)
		if p.Next == "" {
			return false, nil
		}
		after = p.Next
	}
}

// reject says why a listed entry cannot be a copy of the borrower's
// object, or "" if it can.
func (l *Lister) reject(prof *profile.Profile, o control.ListObject) string {
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

// controlPage is the list call at f over Dial; done closes its
// connections.
func (l *Lister) controlPage(f state.Friendship) (func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error), func()) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		peer, err := identity.ParseNodeID(f.Peer)
		if err != nil {
			return nil, err
		}
		return l.Dial(ctx, peer, f.Address)
	}}
	c := &control.Client{HTTP: &http.Client{Transport: tr}}
	return func(ctx context.Context, _ state.Friendship, sealed, after string) (control.ListPage, error) {
		return c.ListPage(ctx, sealed, after, 0)
	}, tr.CloseIdleConnections
}

// Run makes rounds until every list is read, then waits for ctx to end:
// the daemon runs it, so a place offline at recovery is listed when it
// answers again.
func (l *Lister) Run(ctx context.Context) {
	tick := time.NewTicker(or(l.Poll, time.Minute))
	defer tick.Stop()
	for {
		n, err := l.Pending(ctx)
		if err != nil && ctx.Err() == nil {
			l.Log.Error("recovery: reading the lists still to read failed", "err", err)
		}
		if n > 0 {
			rs, err := l.Round(ctx)
			if err != nil && ctx.Err() == nil {
				l.Log.Error("recovery: recording a place's list failed", "err", err)
			}
			for _, r := range rs {
				if r.Err != nil {
					l.Log.Info("recovery: a place's list could not be read yet; trying again later",
						"place", r.Place, "bucket", r.Bucket, "err", r.Err)
					continue
				}
				l.Log.Info("recovery: a place's list read", "place", r.Place, "bucket", r.Bucket,
					"listed", r.Listed, "rejected", r.Rejected, "tombstoned", r.Tombstoned)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
