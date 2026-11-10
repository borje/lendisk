package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Lister reads the lists still to read (state.Relists) from each place
// and records them in placement (ID-5): `recover` makes one round, and
// the daemon runs it until every place has been listed, so a place that
// was offline at recovery is listed when it returns. Its objects count
// as held where other lists found them until then. A place gone past
// replace_after is not listed until it answers again (#71).
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
	// Overflow builds the client listing an overflow target's copies
	// (#96): replica.NewOverflow with the credential from the
	// environment. Nil leaves the targets' lists pending.
	Overflow func(o state.Overflow) (Target, error)
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
	// MaxKeys and MaxPages bound one place's list of one bucket, in
	// entries and in pages read (defaults DefaultMaxKeys and
	// DefaultMaxPages): past either the list is abandoned, so a friend
	// that lists without end cannot keep recovery listing.
	MaxKeys, MaxPages int64
	// Parallel is how many places are listed at once (default 4), so a
	// slow friend holds up no other.
	Parallel int
	// Skip are friends not to ask in a round: their lists stay pending,
	// untried, for the daemon (recover --skip).
	Skip map[string]bool
	Log  *slog.Logger

	mu     sync.Mutex
	rounds int // rounds made: where the next starts
}

// DefaultMaxKeys is Lister.MaxKeys's default: far above any backup
// repository's object count (restic's 16 MiB packs make 10 million
// objects 160 TB).
const DefaultMaxKeys = 10_000_000

// DefaultMaxPages is Lister.MaxPages's default. An honest lender fills
// its pages (gateway.MaxListPage keys), so a list of DefaultMaxKeys
// takes 10,000 pages; twice that and 1,000 more leave room for short
// pages (keys a lender skips) and still end a list of empty pages
// within hours, not years.
const DefaultMaxPages = 2*DefaultMaxKeys/1000 + 1000

// ErrOutOfTime is a list RoundWithin's budget ended before it was read:
// it stays pending, untried, with what it recorded.
var ErrOutOfTime = errors.New("not finished within the time allowed; the daemon lists the rest")

// ErrSkipped is a list of a friend in Skip: it stays pending, untried.
var ErrSkipped = errors.New("skipped; the daemon lists it")

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

// Pending reports how many lists are still to read at places not gone
// past replace_after (readable), or ended without their keys being
// decided again yet (state.Unsettled): the daemon makes rounds while
// any is.
func (l *Lister) Pending(ctx context.Context) (int, error) {
	rs, err := l.readable(ctx)
	if err != nil {
		return 0, err
	}
	n, err := l.DB.Unsettled(ctx)
	return len(rs) + n, err
}

// readable are the lists still to read (state.Relists) but those at a
// place gone past replace_after (state.GonePlaces, #71): a friend that
// never returns is not listed for ever. Its copies count as gone, as
// any gone place's do (re-placement, REPL-13), and its lists are read
// once it answers again.
func (l *Lister) readable(ctx context.Context) ([]state.Relist, error) {
	rs, err := l.DB.Relists(ctx)
	if err != nil || len(rs) == 0 {
		return rs, err
	}
	gone, err := l.DB.GonePlaces(ctx, l.now())
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(rs, func(r state.Relist) bool { _, ok := gone[r.Place]; return ok }), nil
}

// settle decides again what lists that ended affect, within ctx (the
// round's budget); what is left is for a later round.
func (l *Lister) settle(ctx context.Context) {
	if err := l.DB.SettleFinished(ctx); err != nil && ctx.Err() == nil {
		l.logger().Warn("recovery: deciding the keys of lists that ended failed; trying again next round", "err", err)
	}
}

// Round reads every list that is due, with no time budget.
func (l *Lister) Round(ctx context.Context) ([]Result, error) { return l.RoundWithin(ctx, 0) }

// RoundWithin reads every list that is due (readable, and past its
// backoff), Parallel places at a time,
// each place's lists one after another, for at most budget (0: no
// limit). A place that fails one list is not asked for its others in
// this round: each is recorded as failed, and tried again after the
// backoff. A list past MaxKeys or MaxPages is abandoned (its result says
// so) and not read again. A list the budget cut short (ErrOutOfTime) or
// of a friend in Skip (ErrSkipped) stays pending, untried. Each round
// starts one place further on than the last. Results are by place and
// bucket.
func (l *Lister) RoundWithin(ctx context.Context, budget time.Duration) ([]Result, error) {
	rs, err := l.readable(ctx)
	if err != nil {
		return nil, err
	}
	profiles := l.Profiles
	if profiles == nil {
		if profiles, err = profile.Load(""); err != nil {
			return nil, err
		}
	}
	listCtx, cancel := ctx, context.CancelFunc(func() {})
	if budget > 0 {
		listCtx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	// Lists that ended before (a crash, a failure) may still have keys
	// to decide; that too is bounded by the budget.
	l.settle(listCtx)
	now := l.now()
	var places []string
	byPlace := map[string][]state.Relist{}
	for _, r := range rs {
		if !l.due(r, now) {
			continue
		}
		if _, ok := byPlace[r.Place]; !ok {
			places = append(places, r.Place)
		}
		byPlace[r.Place] = append(byPlace[r.Place], r)
	}
	// Each round starts one place further on, so places that use every
	// round's budget cannot keep those after them waiting for ever.
	parallel := l.Parallel
	if parallel <= 0 {
		parallel = 4
	}
	l.mu.Lock()
	start := l.rounds * parallel
	l.rounds++
	l.mu.Unlock()
	order := make([]int, len(places))
	for i := range order {
		order[i] = (start + i) % len(places)
	}
	results := make([][]Result, len(places))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, i := range order {
		place := places[i]
		if l.Skip[place] {
			for _, r := range byPlace[place] {
				results[i] = append(results[i], Result{Place: r.Place, Bucket: r.Bucket, Err: ErrSkipped})
			}
			continue
		}
		sem <- struct{}{} // in order: a place starts only once one before it is done
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = l.listPlace(ctx, listCtx, byPlace[place], profiles)
		}()
	}
	wg.Wait()
	l.settle(listCtx) // what the lists that ended in this round affect
	var out []Result
	for i := range places {
		out = append(out, results[i]...)
	}
	return out, ctx.Err()
}

// listPlace reads one place's due lists, one after another: listCtx
// bounds the listing (the budget), ctx the recording. A failure to
// record a failure or an abandon (a busy database) is logged, not
// fatal: the list stays as it was and is tried again.
func (l *Lister) listPlace(ctx, listCtx context.Context, rs []state.Relist, profiles profile.Set) []Result {
	var out []Result
	var failed error
	for _, r := range rs {
		res := Result{Place: r.Place, Bucket: r.Bucket}
		var abandoned bool
		switch {
		case failed != nil:
			res.Err = failed
		case listCtx.Err() != nil:
			res.Err = ErrOutOfTime
		default:
			abandoned, res.Err = l.listOne(listCtx, ctx, r, profiles, &res)
		}
		if ctx.Err() != nil {
			return out
		}
		switch {
		case abandoned:
			l.logger().Warn("recovery: a place's list is longer than any list may be; abandoned",
				"place", r.Place, "bucket", r.Bucket, "err", res.Err)
			if err := l.DB.AbandonRelist(ctx, r.Place, r.Bucket); err != nil {
				l.logger().Warn("recovery: marking the list abandoned failed; it stays pending, so it is read and abandoned again later",
					"place", r.Place, "bucket", r.Bucket, "err", err)
			}
		case res.Err != nil && listCtx.Err() != nil:
			res.Err = ErrOutOfTime // left pending, untried
		case res.Err != nil:
			failed = res.Err
			if err := l.DB.RelistFailed(ctx, r.Place, r.Bucket, res.Err.Error(), l.now()); err != nil {
				l.logger().Warn("recovery: recording a failed list failed", "place", r.Place, "bucket", r.Bucket, "err", err)
			}
		}
		out = append(out, res)
	}
	return out
}

func (l *Lister) logger() *slog.Logger {
	if l.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l.Log
}

// listOne reads one list from its resume point to its end, counting in
// res; abandoned reports a list past MaxKeys or MaxPages. listCtx bounds
// the calls to the place, ctx the recording.
func (l *Lister) listOne(listCtx, ctx context.Context, r state.Relist, profiles profile.Set, res *Result) (abandoned bool, err error) {
	prof := profiles.Get("")
	if b, err := l.DB.GetBucket(ctx, r.Bucket); err == nil {
		prof = profiles.Get(b.Profile)
	} else if !errors.Is(err, state.ErrNotFound) {
		return false, err
	}
	var page func(ctx context.Context, after string) (control.ListPage, error)
	if name, ok := state.OverflowName(r.Place); ok {
		o, err := l.DB.GetOverflow(ctx, name)
		if errors.Is(err, state.ErrNotFound) {
			// No longer a target: nothing to list there.
			_, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, nil, 0, "", true)
			return false, err
		} else if err != nil {
			return false, err
		}
		if l.Overflow == nil {
			return false, errNoOverflow
		}
		t, err := l.Overflow(o)
		if err != nil {
			return false, err
		}
		page = func(ctx context.Context, after string) (control.ListPage, error) {
			return l.overflowPage(ctx, t, prof, r.Bucket, after)
		}
	} else {
		f, err := l.DB.GetFriendship(ctx, r.Place)
		if errors.Is(err, state.ErrNotFound) {
			// No longer a friend: nothing to list there.
			_, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, nil, 0, "", true)
			return false, err
		} else if err != nil {
			return false, err
		}
		fpage := l.Page
		if fpage == nil {
			var done func()
			fpage, done = l.controlPage(f)
			defer done()
		}
		sealed := l.Self.SealedName(r.Bucket)
		page = func(ctx context.Context, after string) (control.ListPage, error) {
			return fpage(ctx, f, sealed, after)
		}
	}
	maxKeys, maxPages := l.MaxKeys, l.MaxPages
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	if maxPages <= 0 {
		maxPages = DefaultMaxPages
	}
	after, seen, pages := r.After, r.Seen, r.Pages
	for {
		pctx, cancel := context.WithTimeout(listCtx, or(l.Timeout, time.Minute))
		p, err := page(pctx, after)
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
		seen += int64(len(p.Objects))
		pages++
		switch {
		case seen > maxKeys:
			return true, fmt.Errorf("the place lists more than %d keys; its list was abandoned", maxKeys)
		case pages > maxPages:
			return true, fmt.Errorf("the place's list runs to more than %d pages; it was abandoned", maxPages)
		}
		if len(p.Objects) > gateway.MaxListPage {
			return false, fmt.Errorf("the place's page holds %d keys, more than %d", len(p.Objects), gateway.MaxListPage)
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
				Tombstoned: o.TombstonedAt != nil, HashNamed: prof.HashNamed(o.Key),
				Rewritable: prof.Rewritable(o.Key), Replaced: o.Replaced})
		}
		if p.Next != "" && (p.Next <= after || p.Next < last) {
			return false, fmt.Errorf("the place's list does not move on from %q", after)
		}
		n, err := l.DB.RecordListed(ctx, r.Place, r.Bucket, objs, len(p.Objects), p.Next, p.Next == "")
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

// Target is an overflow target as recovery lists it (#96):
// replica.Client.
type Target interface {
	// ListPage lists up to limit live keys of the node's bucket at the
	// target after the key after, in key order; next is "" at the end.
	ListPage(ctx context.Context, bucket, after string, limit int) ([]replica.Listed, string, error)
	// Stat reads what the target stores about key, lendisk's hash
	// included; ok is false when it holds no object there.
	Stat(ctx context.Context, bucket, key string) (replica.ObjectStat, bool, error)
}

// errNoOverflow is an overflow target's list in a Lister that cannot
// reach targets: it stays pending for one that can.
var errNoOverflow = errors.New("overflow targets are listed by `lendisk serve`, with the credential in its environment")

// overflowPage is one page of what overflow target t holds of bucket
// after the key after, as a friend's list gives it (#96). The provider
// lists keys and sizes only: a key named by its hash has that hash
// (the list's checks hold it to it like a friend's), and any other is
// read with a HEAD for the hash lendisk stored with it
// (x-amz-meta-lendisk-sha256). An object without one is not lendisk's
// and is left out (it is listed with an empty hash, which reject
// refuses and logs).
func (l *Lister) overflowPage(ctx context.Context, t Target, prof *profile.Profile, bucket, after string) (control.ListPage, error) {
	keys, next, err := t.ListPage(ctx, bucket, after, gateway.MaxListPage)
	if err != nil {
		return control.ListPage{}, err
	}
	p := control.ListPage{Next: next, Objects: make([]control.ListObject, 0, len(keys))}
	for _, k := range keys {
		o := control.ListObject{Key: k.Key, Size: k.Size}
		switch {
		case gateway.CheckKey(k.Key) != nil || prof.Classify(k.Key) == profile.LocalOnly:
			// reject or the class drops it: no HEAD for it.
		case prof.HashNamed(k.Key):
			o.SHA256 = profile.NameHash(k.Key)
		default:
			st, ok, err := t.Stat(ctx, bucket, k.Key)
			if err != nil {
				return control.ListPage{}, err
			}
			if ok {
				o.SHA256, o.Size = st.SHA256, st.Size
			}
		}
		p.Objects = append(p.Objects, o)
	}
	return p, nil
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
	case len(o.Replaced) > gateway.MaxReplaced:
		return fmt.Sprintf("%d replaced versions, more than %d", len(o.Replaced), gateway.MaxReplaced)
	}
	for _, h := range o.Replaced {
		if !isSHA256(h) {
			return "a replaced version that is not a SHA-256 in lower-case hex"
		}
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
			// A round lasts at most 10 min: a list it cuts short goes on
			// from where it stopped in the next, so a friend that lists
			// slowly delays no other friend's retry for long.
			rs, err := l.RoundWithin(ctx, 10*time.Minute)
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
