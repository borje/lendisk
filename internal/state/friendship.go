package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Friendship is a friendship row (PEER-3): mutual, one quota each way.
// Numbers are bytes. Columns added after the row was written read as zero.
type Friendship struct {
	Peer          string        // the friend's node ID in display form
	Address       string        // the friend's tc… address
	OwnNumber     int64         // what this node lends the friend (LEND-8)
	PeerNumber    int64         // what the friend lends, as last reported
	OwnRetention  time.Duration // how long this node keeps the friend's deletes (RET-1)
	PeerRetention time.Duration // how long the friend keeps this node's
	CreatedAt     time.Time
}

// Quota is the friendship's quota: the smaller number (LEND-8).
func (f Friendship) Quota() int64 { return min(f.OwnNumber, f.PeerNumber) }

// Invite is an invite this node issued. Key is the node ID of the key
// derived from the invite's secret; the secret itself is never stored.
type Invite struct {
	Key       string
	Number    int64         // this node's number for the friendship
	Retention time.Duration // this node's retention offer
	ExpiresAt time.Time
}

// ErrInviteSpent means an invite is unknown, already used or expired.
var ErrInviteSpent = errors.New("invite already used or expired")

// AddInvite stores an issued invite.
func (db *DB) AddInvite(ctx context.Context, inv Invite) error {
	_, err := db.ExecContext(ctx, `INSERT INTO invite (invite_key, expires_at, number, retention) VALUES (?, ?, ?, ?)`,
		inv.Key, inv.ExpiresAt.Unix(), inv.Number, int64(inv.Retention/time.Second))
	return err
}

// PurgeInvites deletes every invite expired at now, used or not, and
// reports how many (SEC-3: invites are kept only while unexpired).
func (db *DB) PurgeInvites(ctx context.Context, now time.Time) (int64, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM invite WHERE expires_at <= ?`, now.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RedeemInvite marks the invite with key used and stores f as a
// friendship, with the invite's number and retention as this side's, in
// one transaction (PEER-2, SEC-13). It fails with ErrInviteSpent when the
// invite is unknown, used or expired at now, and with ErrExists when f's
// peer is already a friend; either way nothing changes.
func (db *DB) RedeemInvite(ctx context.Context, key string, f Friendship, now time.Time) (Friendship, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Friendship{}, err
	}
	defer tx.Rollback()
	var number, retention sql.NullInt64
	err = tx.QueryRowContext(ctx, `UPDATE invite SET used_at = ?1
		WHERE invite_key = ?2 AND used_at IS NULL AND expires_at > ?1
		RETURNING number, retention`, now.Unix(), key).Scan(&number, &retention)
	if errors.Is(err, sql.ErrNoRows) {
		return Friendship{}, ErrInviteSpent
	} else if err != nil {
		return Friendship{}, err
	}
	f.OwnNumber = number.Int64
	f.OwnRetention = time.Duration(retention.Int64) * time.Second
	f.CreatedAt = now
	if err := addFriendship(ctx, tx, f); err != nil {
		return Friendship{}, err
	}
	return f, tx.Commit()
}

// AddFriendship stores a friendship; ErrExists if the peer is a friend.
func (db *DB) AddFriendship(ctx context.Context, f Friendship) error {
	return addFriendship(ctx, db, f)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func addFriendship(ctx context.Context, x execer, f Friendship) error {
	res, err := x.ExecContext(ctx, `INSERT INTO friendship
		(peer_key, created_at, address, own_number, peer_number, own_retention, peer_retention)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (peer_key) DO NOTHING`,
		f.Peer, f.CreatedAt.Unix(), f.Address, f.OwnNumber, f.PeerNumber,
		int64(f.OwnRetention/time.Second), int64(f.PeerRetention/time.Second))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrExists
	}
	return nil
}

const friendshipCols = `peer_key, created_at, address, own_number, peer_number, own_retention, peer_retention`

func scanFriendship(row interface{ Scan(...any) error }) (Friendship, error) {
	var f Friendship
	var created int64
	var addr sql.NullString
	var own, peer, ownRet, peerRet sql.NullInt64
	err := row.Scan(&f.Peer, &created, &addr, &own, &peer, &ownRet, &peerRet)
	if errors.Is(err, sql.ErrNoRows) {
		return f, ErrNotFound
	} else if err != nil {
		return f, err
	}
	f.CreatedAt = time.Unix(created, 0)
	f.Address = addr.String
	f.OwnNumber, f.PeerNumber = own.Int64, peer.Int64
	f.OwnRetention = time.Duration(ownRet.Int64) * time.Second
	f.PeerRetention = time.Duration(peerRet.Int64) * time.Second
	return f, nil
}

// GetFriendship reads the friendship with peer; ErrNotFound if none.
func (db *DB) GetFriendship(ctx context.Context, peer string) (Friendship, error) {
	return scanFriendship(db.QueryRowContext(ctx, `SELECT `+friendshipCols+` FROM friendship WHERE peer_key = ?`, peer))
}

// Friendships lists every friendship, oldest first.
func (db *DB) Friendships(ctx context.Context) ([]Friendship, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+friendshipCols+` FROM friendship ORDER BY created_at, peer_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fs []Friendship
	for rows.Next() {
		f, err := scanFriendship(rows)
		if err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	return fs, rows.Err()
}
