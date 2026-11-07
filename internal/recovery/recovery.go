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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
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
	for _, b := range m.Buckets {
		buckets = append(buckets, b.Name)
	}
	if err := db.AddRelists(ctx, places, buckets); err != nil {
		return r, err
	}
	return r, db.ContinueManifest(ctx, m.Seq)
}
