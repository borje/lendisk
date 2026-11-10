// Package recovery rebuilds a node from its recovery phrase and one
// lender's address (ID-5, NET-7a, #29): it fetches the node manifest
// from every lender it can reach and keeps the newest, restores the
// friendships and the version count from it, and rebuilds placement and
// the hash index (REPL-12) from each place's list of what it holds
// (NET-7 list), a place that is offline being listed when it returns.
//
// Rules a caller must keep:
//
//   - One dialler for the whole recovery: transport.NewFriendDialer of
//     the node's identity, closed before the daemon starts, so no dial
//     key has two clients at a relay (#52, #69). Recovery is for a node
//     whose old machine is gone or stopped: the old one dials with the
//     same keys.
//   - Nothing here writes a credential: the per-pair S3 secrets are
//     derived when used (NET-7a), and an overflow target's credential
//     in the manifest is the caller's to show, never to write (S3-1).
//   - Restore runs before the daemon first runs on the database, so the
//     manifest version continues above every copy friends hold
//     (state.ContinueManifest).
//   - Nothing is downloaded: the lists record what each place holds;
//     objects are fetched on demand later (ID-6, #30).
package recovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// FetchFunc fetches and opens the node's manifest from the lender at
// addr (manifest.Fetch over the recovery's dialler).
type FetchFunc func(ctx context.Context, addr string) (*manifest.Manifest, error)

// Fetched is one lender asked for the manifest: the version it served,
// or why it served none.
type Fetched struct {
	Peer identity.NodeID
	Addr string
	Seq  int64 // 0 when Err is set
	Err  error
}

// Newest fetches the manifest from the lender at addr and then from every
// other friend a fetched manifest lists, and returns the one with the
// highest version, with what each lender answered, the first one first.
// A lender may serve an older copy (the envelope binds the node and the
// format, not freshness), so the newest across lenders wins; a newer
// manifest's friends are asked too. It fails only when no lender served
// one.
func Newest(ctx context.Context, addr string, fetch FetchFunc) (*manifest.Manifest, []Fetched, error) {
	return (&Fetcher{Fetch: fetch}).Newest(ctx, addr)
}

// DefaultRetryFor bounds how long Fetcher asks one lender again whose
// dial was refused or whose connection was reset (#107).
const DefaultRetryFor = 30 * time.Second

// Fetcher asks lenders for the node's manifest (Newest). A lender that
// refuses the dial or resets the connection is asked again, after 1 s,
// 2 s, 4 s and then every 8 s, for RetryFor in all, before Newest moves
// on to the next lender or fails: a friend restarting its daemon
// refuses for a moment, and that should not end a recovery. Any other
// failure (no answer within the fetch's own timeout, refused for want
// of a friendship, no manifest) is not asked again.
type Fetcher struct {
	Fetch FetchFunc
	// RetryFor is DefaultRetryFor when zero; negative asks once.
	RetryFor time.Duration
	// Now and Sleep are the clock and the wait between tries; nil is
	// the wall clock.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// Newest is the package's Newest through f.
func (f *Fetcher) Newest(ctx context.Context, addr string) (*manifest.Manifest, []Fetched, error) {
	fetch := f.fetch
	first, err := transport.KeyOf(addr)
	if err != nil {
		return nil, nil, fmt.Errorf("the lender's address: %w", err)
	}
	var best *manifest.Manifest
	var tried []Fetched
	asked := map[identity.NodeID]bool{first: true}
	todo := []Fetched{{Peer: first, Addr: addr}}
	for len(todo) > 0 {
		f := todo[0]
		todo = todo[1:]
		m, err := fetch(ctx, f.Addr)
		if err != nil {
			f.Err = err
			tried = append(tried, f)
			continue
		}
		f.Seq = m.Seq
		tried = append(tried, f)
		if best == nil || m.Seq > best.Seq {
			best = m
		}
		for _, fr := range m.Friends {
			id, err := identity.ParseNodeID(fr.ID)
			if err != nil || asked[id] || fr.Address == "" {
				continue
			}
			asked[id] = true
			todo = append(todo, Fetched{Peer: id, Addr: fr.Address})
		}
	}
	if best == nil {
		return nil, tried, tried[0].Err
	}
	return best, tried, nil
}

// fetch asks the lender at addr, again while it refuses or resets the
// connection, for RetryFor at the most.
func (f *Fetcher) fetch(ctx context.Context, addr string) (*manifest.Manifest, error) {
	now, sleep := f.Now, f.Sleep
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = sleepCtx
	}
	retryFor := cmp.Or(f.RetryFor, DefaultRetryFor)
	start := now()
	wait := time.Second
	for {
		m, err := f.Fetch(ctx, addr)
		if err == nil || !refused(err) || now().Sub(start)+wait > retryFor {
			return m, err
		}
		if sleep(ctx, wait) != nil {
			return nil, err
		}
		wait = min(2*wait, 8*time.Second)
	}
}

// refused reports whether err is a dial the lender refused or a
// connection it reset. The tunnel's errors carry only their text
// (gVisor's "connection was refused", "connection reset by peer").
func refused(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection was refused") || strings.Contains(s, "connection reset by peer")
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Restored is what Restore wrote.
type Restored struct {
	// Friends are the friendships restored, as the manifest lists them.
	Friends []manifest.Friend
	// NoDialKey are friends whose dial key the manifest does not carry
	// (one written before it did): they reach this node again only
	// through a new invite (ID-5).
	NoDialKey []string
}

// Restore writes the manifest's friendships, with each friend's terms
// and dial key, the node's relay region (so its address stays the
// same, SEC-14), the version to continue from, and a list to read for
// every friend and bucket. It may run again on the same database after
// a crash: what is there already is kept. The buckets themselves, with
// their own S3 credentials, are the caller's (SEC-3: not in the
// manifest).
func Restore(ctx context.Context, db *state.DB, m *manifest.Manifest) (Restored, error) {
	var r Restored
	if len(m.Settings.TunnelRegion) > 0 {
		if err := transport.RestoreRegion(ctx, db, m.Settings.TunnelRegion); err != nil {
			return r, err
		}
	}
	var places, buckets []string
	for _, f := range m.Friends {
		if _, err := identity.ParseNodeID(f.ID); err != nil {
			return r, fmt.Errorf("the manifest lists a friend %q that is no node ID: %w", f.ID, err)
		}
		if f.DialKey != "" {
			if _, err := identity.ParseNodeID(f.DialKey); err != nil {
				return r, fmt.Errorf("the manifest's dial key for %s: %w", f.ID, err)
			}
		}
		err := db.AddFriendship(ctx, state.Friendship{Peer: f.ID, Address: f.Address,
			OwnNumber: f.OwnNumber, PeerNumber: f.PeerNumber,
			OwnRetention:  time.Duration(f.OwnRetentionSeconds) * time.Second,
			PeerRetention: time.Duration(f.PeerRetentionSeconds) * time.Second,
			CreatedAt:     f.Since, DialKey: f.DialKey})
		if err != nil && !errors.Is(err, state.ErrExists) {
			return r, fmt.Errorf("restoring the friendship with %s: %w", f.ID, err)
		}
		r.Friends = append(r.Friends, f)
		if f.DialKey == "" {
			r.NoDialKey = append(r.NoDialKey, f.ID)
		}
		places = append(places, f.ID)
	}
	// Overflow targets come back as they were added, all but the
	// credential, which the user puts in the daemon's environment (S3-1,
	// SEC-3): so the rebuilt node's manifest still names them, and its
	// daemon refuses to start until the credential is there (#38 review).
	for _, o := range m.Overflow {
		if err := overflow.CheckName(o.Name); err != nil {
			return r, fmt.Errorf("the manifest lists an overflow target: %w", err)
		}
		prefix := cmp.Or(o.EnvPrefix, overflow.DefaultEnvPrefix(o.Name))
		if err := overflow.CheckEnvPrefix(prefix); err != nil {
			return r, fmt.Errorf("the manifest's overflow target %s: %w", o.Name, err)
		}
		err := db.AddOverflow(ctx, state.Overflow{Name: o.Name, Endpoint: o.Endpoint, Region: o.Region, Bucket: o.Bucket,
			Prefix: o.Prefix, MaxBytes: o.MaxBytes, EnvPrefix: prefix,
			Retention:  time.Duration(o.RetentionSeconds) * time.Second,
			PricePerGB: o.PricePerGB, AddedAt: time.Now()})
		if err != nil && !errors.Is(err, state.ErrExists) {
			return r, fmt.Errorf("restoring overflow target %s: %w", o.Name, err)
		}
		// One being removed stays so (#102): no copy goes there again,
		// and the daemon goes on moving its copies elsewhere.
		if o.RemovingAt > 0 {
			if err := db.RemoveOverflow(ctx, o.Name, o.KeepData, time.Unix(o.RemovingAt, 0)); err != nil {
				return r, fmt.Errorf("restoring overflow target %s as being removed: %w", o.Name, err)
			}
		}
		// Its copies are listed like a friend's (#96), so they count as
		// held there rather than missing.
		places = append(places, state.OverflowPlace(o.Name))
	}
	for _, b := range m.Buckets {
		buckets = append(buckets, b.Name)
	}
	if err := db.AddRelists(ctx, places, buckets); err != nil {
		return r, err
	}
	return r, db.ContinueManifest(ctx, m.Seq)
}
