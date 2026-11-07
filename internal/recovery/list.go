package recovery

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/borje/lendisk/internal/control"
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
	Log                    *slog.Logger
}

// Result is a list read in a round, whole or until it failed.
type Result struct {
	Place, Bucket string
	state.ListedCount
	Err error // nil when the list was read to its end
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
// recorded as failed, and tried again after the backoff.
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
		if err, ok := failed[r.Place]; ok {
			res.Err = err
		} else {
			res.ListedCount, res.Err = l.listOne(ctx, r, profiles)
		}
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		if res.Err != nil {
			failed[r.Place] = res.Err
			if err := l.DB.RelistFailed(ctx, r.Place, r.Bucket, res.Err.Error(), l.now()); err != nil {
				return out, err
			}
		}
		out = append(out, res)
	}
	return out, nil
}

// listOne reads one list from its resume point to its end.
func (l *Lister) listOne(ctx context.Context, r state.Relist, profiles profile.Set) (state.ListedCount, error) {
	var total state.ListedCount
	f, err := l.DB.GetFriendship(ctx, r.Place)
	if errors.Is(err, state.ErrNotFound) {
		// No longer a friend: nothing to list there.
		_, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, nil, "", true)
		return total, err
	} else if err != nil {
		return total, err
	}
	prof := profiles.Get("")
	if b, err := l.DB.GetBucket(ctx, r.Bucket); err == nil {
		prof = profiles.Get(b.Profile)
	} else if !errors.Is(err, state.ErrNotFound) {
		return total, err
	}
	page := l.Page
	var done func()
	if page == nil {
		page, done = l.controlPage(f)
		defer done()
	}
	sealed, after := l.Self.SealedName(r.Bucket), r.After
	for {
		pctx, cancel := context.WithTimeout(ctx, or(l.Timeout, time.Minute))
		p, err := page(pctx, f, sealed, after)
		cancel()
		var ce *control.CallError
		if errors.As(err, &ce) && ce.Status == http.StatusNotFound {
			// The place holds no bucket of this name: nothing of it.
			p, err = control.ListPage{}, nil
		}
		if err != nil {
			return total, err
		}
		objs := make([]state.Listed, 0, len(p.Objects))
		for _, o := range p.Objects {
			class := prof.Classify(o.Key)
			if class == profile.LocalOnly {
				continue // never replicated; a place should not hold it
			}
			objs = append(objs, state.Listed{Key: o.Key, Size: o.Size, SHA256: o.SHA256, Class: string(class),
				Tombstoned: o.TombstonedAt != nil})
		}
		n, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, objs, p.Next, p.Next == "")
		if err != nil {
			return total, err
		}
		total.Add(n)
		if p.Next == "" {
			return total, nil
		}
		after = p.Next
	}
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
					"held", r.Held, "differ", r.Differ, "tombstoned", r.Tombstoned)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
