package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// This file is ending a friendship (PEER-5, PEER-12, SEC-15, #25).
//
// RemoveFriendship deletes the friendship row in one transaction with
// everything that goes with it, so both of the friend's keys are refused
// the moment it commits (TunnelPeer, PeerRole: SEC-10), and records a
// former friend for the retention window:
//   - the friend's manifest delivery row goes (#28); the pusher reads
//     friendships, so it pushes there no more;
//   - its placement rows stay, so nothing it may hold is forgotten, but
//     its unsent rows leave its FIFO: a put a sender took is abandoned
//     (it may have landed, REPL-2), every other queued row dropped. None
//     then holds the metadata barrier, and every query that joins the
//     friendship table (the pool, placement, the offsite holders,
//     BelowCopies) no longer counts it: its copies of this node's data
//     are no longer copies (REPL-11), and re-placement (#26) restores
//     `copies`;
//   - its lent buckets, tombs and manifest here stay, refused to it, until
//     the purge.
//
// The former friend row keeps both keys, so the daemon cuts their open
// connections (SEC-15), the address, so it tells the friend, and the
// purge time: removal plus the friendship's retention window, the
// longer of the two sides' offers, as the lender's tombs use. At the
// purge the daemon deletes the friend's lent data here and its manifest,
// then ForgetFormerFriend drops the rest. Becoming friends again
// (addFriendship) deletes the row: nothing is purged.

// DefaultRetention is the retention window when neither side of a
// friendship offered one (RET-1).
const DefaultRetention = 30 * 24 * time.Hour

// Retention is how long either side keeps the other's deletes, and its
// replica after the friendship ends (RET-1, PEER-5): the longer of the
// two offers, so a borrower can only make the window longer.
// DefaultRetention when neither was recorded.
func (f Friendship) Retention() time.Duration {
	if r := max(f.OwnRetention, f.PeerRetention); r > 0 {
		return r
	}
	return DefaultRetention
}

// FormerFriend is a friendship that ended and whose replicas are kept
// for the retention window (PEER-5).
type FormerFriend struct {
	Peer       string // node ID
	DialKey    string // the key it dialled this node with (#69); "" if none was recorded
	Address    string
	RemovedAt  time.Time
	PurgeAfter time.Time
	// ByPeer: the friend ended it, by its remove call.
	ByPeer bool
	// NotifiedAt is when the friend was told (or, ByPeer, told this
	// node); zero until then. NotifyError is why the last try failed.
	NotifiedAt  time.Time
	NotifyError string
}

// RemoveFriendship ends the friendship with peer at now (package
// comment above); byPeer records that the friend ended it, and so was
// told already. ErrNotFound if peer is not a friend.
func (db *DB) RemoveFriendship(ctx context.Context, peer string, byPeer bool, now time.Time) (FormerFriend, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return FormerFriend{}, err
	}
	defer tx.Rollback()
	ff, err := removeFriendship(ctx, tx, peer, byPeer, now)
	if err != nil {
		return FormerFriend{}, err
	}
	return ff, tx.Commit()
}

// removeFriendship is RemoveFriendship inside tx.
func removeFriendship(ctx context.Context, tx *sql.Tx, peer string, byPeer bool, now time.Time) (FormerFriend, error) {
	f, err := scanFriendship(tx.QueryRowContext(ctx, `SELECT `+friendshipCols+` FROM friendship WHERE peer_key = ?`, peer))
	if err != nil {
		return FormerFriend{}, err
	}
	at := time.Unix(now.Unix(), 0)
	ff := FormerFriend{Peer: peer, DialKey: f.DialKey, Address: f.Address, RemovedAt: at,
		PurgeAfter: at.Add(f.Retention()), ByPeer: byPeer}
	var notified any
	if byPeer {
		ff.NotifiedAt = at
		notified = at.Unix()
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO former_friend
		(peer_key, dial_key, address, removed_at, purge_after, by_peer, notified_at, notify_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULL)`,
		peer, nullString(f.DialKey), nullString(f.Address), at.Unix(), ceilUnix(ff.PurgeAfter), nullTrue(byPeer), notified); err != nil {
		return FormerFriend{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM friendship WHERE peer_key = ?`, peer); err != nil {
		return FormerFriend{}, err
	}
	if err := forgetManifestPush(ctx, tx, peer); err != nil {
		return FormerFriend{}, err
	}
	// Its replicas' verification ends with it (#27 review): befriended
	// again, it starts unchecked, not unverified.
	if _, err := tx.ExecContext(ctx, `DELETE FROM replica_check WHERE place = ?`, peer); err != nil {
		return FormerFriend{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementAbandoned+`'
		WHERE place = ? AND state = 'queued' AND op = 'put' AND started_at IS NOT NULL`, peer); err != nil {
		return FormerFriend{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDropped+`'
		WHERE place = ? AND state = 'queued'`, peer); err != nil {
		return FormerFriend{}, err
	}
	return ff, nil
}

const formerCols = `peer_key, coalesce(dial_key, ''), coalesce(address, ''), removed_at, purge_after,
	coalesce(by_peer, 0), notified_at, coalesce(notify_error, '')`

// ErrNotFormer means a peer is not a former friend whose purge is due:
// it is a friend again, was forgotten, or its window has not ended.
var ErrNotFormer = errors.New("not a former friend due for its purge")

// FormerFriends lists the friendships that ended and are not purged
// yet, by removal time.
func (db *DB) FormerFriends(ctx context.Context) ([]FormerFriend, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+formerCols+` FROM former_friend ORDER BY removed_at, peer_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FormerFriend
	for rows.Next() {
		var f FormerFriend
		var removed, purge int64
		var notified sql.NullInt64
		if err := rows.Scan(&f.Peer, &f.DialKey, &f.Address, &removed, &purge, &f.ByPeer, &notified, &f.NotifyError); err != nil {
			return nil, err
		}
		f.RemovedAt, f.PurgeAfter = time.Unix(removed, 0), time.Unix(purge, 0)
		if notified.Valid {
			f.NotifiedAt = time.Unix(notified.Int64, 0)
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// NoteFarewell records an attempt at now to tell former friend peer
// that the friendship ended: told, when failure is "", else why not.
func (db *DB) NoteFarewell(ctx context.Context, peer, failure string, now time.Time) error {
	if failure == "" {
		_, err := db.ExecContext(ctx, `UPDATE former_friend SET notified_at = ?, notify_error = NULL WHERE peer_key = ?`,
			now.Unix(), peer)
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE former_friend SET notify_error = ? WHERE peer_key = ? AND notified_at IS NULL`,
		failure, peer)
	return err
}

// ForgetFormerFriend drops what is left of former friend peer once its
// replica here is purged: its placement rows, its use and condition as
// a place, its lent buckets, tombstones, uploads and usage, and the
// former friend row. A peer that is a friend again, or no former friend,
// keeps everything.
func (db *DB) ForgetFormerFriend(ctx context.Context, peer string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var former bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM former_friend WHERE peer_key = ?1)
		AND NOT EXISTS (SELECT 1 FROM friendship WHERE peer_key = ?1)`, peer).Scan(&former); err != nil {
		return err
	}
	if !former {
		return nil
	}
	for _, q := range []string{
		`DELETE FROM placement WHERE place = ?`,
		`DELETE FROM place_use WHERE place = ?`,
		`DELETE FROM place_freed WHERE place = ?`,
		`DELETE FROM place WHERE place = ?`,
		`DELETE FROM lent_part WHERE upload_id IN (SELECT upload_id FROM lent_upload WHERE peer_key = ?)`,
		`DELETE FROM lent_upload WHERE peer_key = ?`,
		`DELETE FROM tombstone WHERE peer_key = ?`,
		`DELETE FROM lent_bucket WHERE peer_key = ?`,
		`DELETE FROM lent_usage WHERE peer_key = ?`,
		`DELETE FROM former_friend WHERE peer_key = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, peer); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PurgeDue reports whether former friend peer's replica may be purged at
// now: it is a former friend, its window has ended, and it is not a
// friend again. The purge asks before each bucket it deletes (#25
// review): a new invite redeemed meanwhile stops it.
func (db *DB) PurgeDue(ctx context.Context, peer string, now time.Time) (bool, error) {
	var due bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM former_friend WHERE peer_key = ?1 AND purge_after <= ?2)
		AND NOT EXISTS (SELECT 1 FROM friendship WHERE peer_key = ?1)`, peer, now.Unix()).Scan(&due)
	return due, err
}
