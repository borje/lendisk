package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/borje/lendisk/internal/identity"
)

// errNotServing: the gateway has not built its lending layer yet.
var errNotServing = errors.New("the gateway is not serving yet; trying again later")

// PurgeFormer deletes what former friend peer stored on this node, at the
// end of the friendship's retention window (PEER-5, #25): each of its
// lent buckets, with its tombs and unfinished uploads, from the store.
// The friend is no longer admitted, so nothing writes there meanwhile;
// the rows go after this, with state.DB.ForgetFormerFriend. A bucket
// already gone is no error, so a purge a crash interrupted is finished
// by the next one.
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
		if err := os.RemoveAll(filepath.Join(l.StoreDir, LentName(id, b.Sealed))); err != nil {
			return err
		}
	}
	// Its count starts afresh if it ever becomes a friend again.
	l.mu.Lock()
	delete(l.peers, peer)
	l.mu.Unlock()
	l.log().Info("lending: purged a former friend's replica at the end of its retention window", "peer", peer, "buckets", len(bs))
	return nil
}
