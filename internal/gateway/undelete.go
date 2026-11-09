package gateway

import (
	"context"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// This file is the lender's side of undelete (RET-3, #32): a borrower
// asks, through the control API, for its tombs of one bucket deleted
// since a time to go back to their keys.
//
//   - Which version: of a key's delete tombs deleted at or after since,
//     the newest, the version the key held when it was last deleted (S3:
//     removing the delete marker makes the latest version before it
//     current; B2: unhiding a file). A rewrite's tomb (#74) is a version
//     the borrower replaced, not deleted, and is considered only when
//     asked (rewrites); then the key comes back as it was at since: its
//     oldest tomb, of either kind, deleted at or after since, which is
//     the version it held then.
//   - Never over a live object: a key live with other bytes is skipped
//     and reported with both hashes, and its tombs stay for their window
//     (RET-1: no version is lost). One live with the chosen version's
//     bytes is reported too, and changes nothing.
//   - The revival is revive's, as a re-put of the same hash does it: the
//     row pending, the tomb linked back, the tomb unlinked, the row
//     dropped, holding the key's lock and the borrower's publication
//     lock. The count moves from tomb to live unchanged (LEND-1).
//   - A page covers at most limit keys with such tombs, in key order,
//     after the key after; Next is where the next page starts.
//   - Every revival is reported: a key that cannot be revived is skipped
//     as SkipFailed and the page goes on; each key's steps run detached
//     from the request, and a cancelled request ends the page after the
//     key in hand, Next naming the last key handled (#32 review).

// Reasons a key is skipped by undelete.
const (
	// SkipConflict: the key is live with other bytes than the version
	// undelete would bring back.
	SkipConflict = "conflict"
	// SkipLive: the key is live with the version's own bytes already.
	SkipLive = "live"
	// SkipFailed: the lender could not revive the key now (Detail says
	// why); its tomb stays, and a later undelete tries again.
	SkipFailed = "failed"
)

// UndeleteSkip is a key undelete left as it was.
type UndeleteSkip struct {
	Key    string
	SHA256 string // the version undelete would have brought back
	Live   string // the hash of what the key holds
	Reason string // SkipConflict, SkipLive or SkipFailed
	Detail string // SkipFailed's reason
}

// Undeleted is a page of undelete: the keys moved back to the live set,
// each with the version's size, hash and when it was tombstoned; the
// keys left as they were; and the key the next page starts after, ""
// after the last.
type Undeleted struct {
	Restored []LentObject
	Skipped  []UndeleteSkip
	Next     string
}

// Undelete brings borrower's tombs of bucket sealed deleted at or after
// since back to their keys, at most limit keys after the key after
// (RET-3). It answers for borrower alone, which the control API takes
// from the caller, never from the request: a sealed name not borrower's
// is ErrNoLentBucket.
func (g *Gateway) Undelete(ctx context.Context, borrower identity.NodeID, sealed string, since time.Time, rewrites bool,
	after string, limit int) (Undeleted, error) {
	l := g.running.Load()
	if l == nil {
		return Undeleted{}, errNotServing
	}
	return l.undelete(ctx, borrower, sealed, since, rewrites, after, limit)
}

func (l *Lending) undelete(ctx context.Context, borrower identity.NodeID, sealed string, since time.Time, rewrites bool,
	after string, limit int) (Undeleted, error) {
	var u Undeleted
	if !identity.IsSealedName(sealed) {
		return u, ErrNoLentBucket
	}
	peer := borrower.String()
	ok, err := l.DB.HasLentBucket(ctx, peer, sealed)
	if err != nil {
		return u, err
	}
	if !ok {
		return u, ErrNoLentBucket
	}
	keys, err := l.DB.UndeleteKeys(ctx, peer, sealed, since, rewrites, after, limit)
	if err != nil {
		return u, err
	}
	if len(keys) == limit {
		u.Next = keys[len(keys)-1]
	}
	bucket := LentName(borrower, sealed)
	// Each key's steps run to their end once begun, whatever becomes of
	// the request: a revival the answer never reports would leave the
	// object live at the lender and deleted in the borrower's index
	// (#32 review). A cancelled request ends the page after the key in
	// hand, Next naming it.
	work := detach(ctx)
	for _, key := range keys {
		if ctx.Err() != nil {
			u.Next = key
			break
		}
		if err := l.undeleteKey(work, peer, sealed, bucket, key, since, rewrites, &u); err != nil {
			// Never the page: the key keeps its tomb and is reported, and
			// the keys after it are still undeleted.
			l.log().Error("lending: undeleting a borrower's object failed; its tomb stays", "peer", peer, "key", key, "err", err)
			u.Skipped = append(u.Skipped, UndeleteSkip{Key: key, Reason: SkipFailed, Detail: failure(err)})
		}
		if l.afterUndeleteKey != nil {
			l.afterUndeleteKey(key)
		}
	}
	if ctx.Err() != nil && u.Next != "" {
		// Next is the key not reached: the next page starts after the
		// last key handled.
		u.Next = lastHandled(u, after)
	}
	return u, nil
}

// lastHandled is the greatest key u restored or skipped, or after.
func lastHandled(u Undeleted, after string) string {
	last := after
	for _, o := range u.Restored {
		last = max(last, o.Key)
	}
	for _, s := range u.Skipped {
		last = max(last, s.Key)
	}
	return last
}

// failure says why a key could not be revived, for the borrower.
func failure(err error) string {
	if isCode(err, "InternalError") {
		return "the lender could not move it back now (see its log); its tombstone stays, and a later undelete tries again"
	}
	return err.Error()
}

// undeleteKey brings key's chosen tomb back, or records why not. An
// error leaves the key as it was, its tombstone kept or settled at the
// next start.
func (l *Lending) undeleteKey(ctx context.Context, peer, sealed, bucket, key string, since time.Time, rewrites bool, u *Undeleted) error {
	unlock, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer unlock()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	// Read under the lock: a purge, a re-put or another undelete may have
	// taken the tombs since the keys were chosen.
	ts, err := l.DB.TombstonesOf(ctx, peer, sealed, key)
	if err != nil {
		return err
	}
	t, ok := chooseTomb(ts, since, rewrites)
	if !ok {
		return nil
	}
	if l.afterUndeleteChoose != nil {
		l.afterUndeleteChoose()
	}
	cur, has, err := l.existing(ctx, bucket, key)
	if err != nil {
		return err
	}
	if has {
		reason := SkipConflict
		if cur.sha256 == t.SHA256 {
			reason = SkipLive
		}
		u.Skipped = append(u.Skipped, UndeleteSkip{Key: key, SHA256: t.SHA256, Live: cur.sha256, Reason: reason})
		return nil
	}
	if _, err := l.revive(ctx, bucket, t); err != nil {
		if isCode(err, "PreconditionFailed") {
			// Another write published the key meanwhile.
			cur, _, _ := l.existing(ctx, bucket, key)
			u.Skipped = append(u.Skipped, UndeleteSkip{Key: key, SHA256: t.SHA256, Live: cur.sha256, Reason: SkipConflict})
			return nil
		}
		return err
	}
	u.Restored = append(u.Restored, LentObject{Key: key, Size: t.Size, SHA256: t.SHA256, TombstonedAt: t.DeletedAt})
	l.log().Info("lending: undeleted a borrower's object", "peer", peer, "key", key, "deleted_at", t.DeletedAt)
	return nil
}

// chooseTomb picks, from a key's tombstones (oldest first), the one
// undelete brings back: the newest kept delete tomb deleted at or after
// since; with rewrites, the oldest kept tomb of either kind deleted at or
// after since (the version the key held at since).
func chooseTomb(ts []state.Tombstone, since time.Time, rewrites bool) (state.Tombstone, bool) {
	var out state.Tombstone
	found := false
	for _, t := range ts {
		if t.Pending || t.DeletedAt.Before(time.Unix(since.Unix(), 0)) || t.Rewrite && !rewrites {
			continue
		}
		if rewrites {
			return t, true
		}
		out, found = t, true
	}
	return out, found
}
