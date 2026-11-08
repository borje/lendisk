package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// errNotServing: the gateway has not built its lending layer yet.
var errNotServing = errors.New("the gateway is not serving yet; trying again later")

// PurgeFormer deletes what former friend peer stored on this node, at the
// end of the friendship's retention window (PEER-5, #25): each of its
// lent buckets, with its tombs and unfinished uploads, from the store.
// The friend is no longer admitted, so nothing writes there meanwhile;
// the rows go after this, with state.DB.ForgetFormerFriend. A bucket
// already gone is no error, so a purge a crash interrupted is finished
// by the next one. Before each bucket it checks that the purge is still
// due (state.DB.PurgeDue); if the peer is a friend again, or its window
// has not ended, it stops with state.ErrNotFormer and keeps the rest.
func (g *Gateway) PurgeFormer(ctx context.Context, peer string) error {
	l := g.running.Load()
	if l == nil {
		return errNotServing
	}
	return l.purgeFormer(ctx, peer)
}

func (l *Lending) purgeFormer(ctx context.Context, peer string) error {
	id, err := identity.ParseNodeID(peer)
	if err != nil {
		return err
	}
	bs, err := l.DB.LentBuckets(ctx, peer)
	if err != nil {
		return err
	}
	for _, b := range bs {
		// Before each bucket: a new invite redeemed meanwhile makes the
		// peer a friend again, whose buckets are live (#25 review).
		due, err := l.DB.PurgeDue(ctx, peer, l.now())
		if err != nil {
			return err
		}
		if !due {
			return state.ErrNotFormer
		}
		if err := os.RemoveAll(filepath.Join(l.StoreDir, LentName(id, b.Sealed))); err != nil {
			return err
		}
		if l.afterPurgeBucket != nil {
			l.afterPurgeBucket()
		}
	}
	if due, err := l.DB.PurgeDue(ctx, peer, l.now()); err != nil {
		return err
	} else if !due {
		return state.ErrNotFormer
	}
	// Its count starts afresh if it ever becomes a friend again.
	l.mu.Lock()
	delete(l.peers, peer)
	l.mu.Unlock()
	l.log().Info("lending: purged a former friend's replica at the end of its retention window", "peer", peer, "buckets", len(bs))
	return nil
}

// Requota tells the lending layer that the friendship with peer may
// have a lower quota now (PEER-6, SEC-15, #24): each of peer's writes
// whose body is still streaming checks the quota again at its next read,
// and fails with 507 if it no longer fits. New requests read the quota
// anyway. Before the gateway serves there is nothing in flight.
func (g *Gateway) Requota(peer string) {
	if l := g.running.Load(); l != nil {
		l.Requota(peer)
	}
}
