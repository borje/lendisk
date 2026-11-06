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
	// DialKey is the key the friend dials with (#69), node-ID form,
	// learnt at redeem; "" for a friendship made before it existed.
	DialKey string
}

// ErrDialKeyTaken means a dial key is already another friend's dial key
// or node ID, or a friend's node ID is another friend's dial key: each
// key names one friend.
var ErrDialKeyTaken = errors.New("the dial key already names another friend")

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

// RedeemInvite marks the invite with key used by f's peer and stores f
// as a friendship, with the invite's number and retention as this
// side's, in one transaction (PEER-2, SEC-13). It fails with
// ErrInviteSpent when the invite is unknown, expired at now, or used by
// another node; nothing then changes.
//
// The caller must have proven that f.Peer redeems (control.RedeemProof),
// because a friend may redeem again, since it may have lost the answer
// (#51, #56): when f's peer is already a friend, any unexpired invite,
// used or not, returns the stored friendship with f's address, number
// and retention applied as the friend's latest report, and an unused
// invite is marked used by it.
func (db *DB) RedeemInvite(ctx context.Context, key string, f Friendship, now time.Time) (Friendship, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Friendship{}, err
	}
	defer tx.Rollback()
	var number, retention, usedAt sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT number, retention, used_at FROM invite
		WHERE invite_key = ? AND expires_at > ?`, key, now.Unix()).Scan(&number, &retention, &usedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Friendship{}, ErrInviteSpent
	} else if err != nil {
		return Friendship{}, err
	}
	got, err := refreshFriendship(ctx, tx, f)
	switch {
	case err == nil:
		if _, err := tx.ExecContext(ctx, `UPDATE invite SET used_at = ?, redeemed_by = ?
			WHERE invite_key = ? AND used_at IS NULL`, now.Unix(), f.Peer, key); err != nil {
			return Friendship{}, err
		}
		return got, tx.Commit()
	case !errors.Is(err, ErrNotFound):
		return Friendship{}, err
	case usedAt.Valid:
		return Friendship{}, ErrInviteSpent
	}
	// Conditional, so a concurrent redemption cannot use it twice.
	res, err := tx.ExecContext(ctx, `UPDATE invite SET used_at = ?, redeemed_by = ?
		WHERE invite_key = ? AND used_at IS NULL`, now.Unix(), f.Peer, key)
	if err != nil {
		return Friendship{}, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return Friendship{}, err
	} else if n == 0 {
		return Friendship{}, ErrInviteSpent
	}
	f.OwnNumber = number.Int64
	f.OwnRetention = time.Duration(retention.Int64) * time.Second
	f.CreatedAt = now
	if err := addFriendship(ctx, tx, f); err != nil {
		return Friendship{}, err
	}
	return f, tx.Commit()
}

// refreshFriendship applies f's address, number and retention to the
// friendship with f's peer as the friend's latest report and returns
// the row; ErrNotFound if f's peer is not a friend.
func refreshFriendship(ctx context.Context, tx *sql.Tx, f Friendship) (Friendship, error) {
	if err := checkDialKey(ctx, tx, f); err != nil {
		return Friendship{}, err
	}
	return scanFriendship(tx.QueryRowContext(ctx, `UPDATE friendship
		SET address = ?, peer_number = ?, peer_retention = ?, dial_key = coalesce(?, dial_key)
		WHERE peer_key = ?
		RETURNING `+friendshipCols,
		f.Address, f.PeerNumber, int64(f.PeerRetention/time.Second), nullString(f.DialKey), f.Peer))
}

// checkDialKey refuses f when its dial key or node ID would name two
// friends (ErrDialKeyTaken).
func checkDialKey(ctx context.Context, q queryer, f Friendship) error {
	var taken bool
	err := q.QueryRowContext(ctx, `SELECT
		coalesce(?2 = ?1, 0)
		OR EXISTS (SELECT 1 FROM friendship WHERE peer_key != ?1 AND (peer_key = ?2 OR dial_key = ?2))
		OR EXISTS (SELECT 1 FROM friendship WHERE peer_key != ?1 AND dial_key = ?1)`,
		f.Peer, nullString(f.DialKey)).Scan(&taken)
	if err != nil {
		return err
	}
	if taken {
		return ErrDialKeyTaken
	}
	return nil
}

func nullString(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

// PendingRedeem is a redeem the invited node started and has not
// finished (PEER-2, #51): the terms it offered, so that running peer
// add again repeats them. It admits nothing.
type PendingRedeem struct {
	InviteKey string // node ID of the invite key
	Peer      string // the inviting node's ID
	Number    int64  // what this node lends
	Retention time.Duration
	StartedAt time.Time
}

// StartRedeem records p before the redeem call; ErrExists if one is
// recorded for the invite already.
func (db *DB) StartRedeem(ctx context.Context, p PendingRedeem) error {
	res, err := db.ExecContext(ctx, `INSERT INTO pending_redeem (invite_key, peer_key, number, retention, started_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (invite_key) DO NOTHING`,
		p.InviteKey, p.Peer, p.Number, int64(p.Retention/time.Second), p.StartedAt.Unix())
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

// PendingRedeemOf reads the pending redeem of the invite with key;
// ErrNotFound if none.
func (db *DB) PendingRedeemOf(ctx context.Context, key string) (PendingRedeem, error) {
	var p PendingRedeem
	var ret, started int64
	err := db.QueryRowContext(ctx, `SELECT invite_key, peer_key, number, retention, started_at
		FROM pending_redeem WHERE invite_key = ?`, key).Scan(&p.InviteKey, &p.Peer, &p.Number, &ret, &started)
	if errors.Is(err, sql.ErrNoRows) {
		return p, ErrNotFound
	} else if err != nil {
		return p, err
	}
	p.Retention = time.Duration(ret) * time.Second
	p.StartedAt = time.Unix(started, 0)
	return p, nil
}

// PendingRedeemsWith lists the pending redeems of invites from peer,
// oldest first.
func (db *DB) PendingRedeemsWith(ctx context.Context, peer string) ([]PendingRedeem, error) {
	rows, err := db.QueryContext(ctx, `SELECT invite_key, peer_key, number, retention, started_at
		FROM pending_redeem WHERE peer_key = ? ORDER BY started_at, invite_key`, peer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ps []PendingRedeem
	for rows.Next() {
		var p PendingRedeem
		var ret, started int64
		if err := rows.Scan(&p.InviteKey, &p.Peer, &p.Number, &ret, &started); err != nil {
			return nil, err
		}
		p.Retention = time.Duration(ret) * time.Second
		p.StartedAt = time.Unix(started, 0)
		ps = append(ps, p)
	}
	return ps, rows.Err()
}

// FinishRedeem stores f and drops the pending redeem of the invite with
// key, and every other one with f's peer (#56), in one transaction;
// ErrExists if f's peer is a friend already.
func (db *DB) FinishRedeem(ctx context.Context, key string, f Friendship) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := addFriendship(ctx, tx, f); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_redeem WHERE invite_key = ? OR peer_key = ?`, key, f.Peer); err != nil {
		return err
	}
	return tx.Commit()
}

// AbandonRedeem drops the pending redeem of the invite with key, after
// the inviting node refused it for good.
func (db *DB) AbandonRedeem(ctx context.Context, key string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM pending_redeem WHERE invite_key = ?`, key)
	return err
}

// AddFriendship stores a friendship; ErrExists if the peer is a friend.
func (db *DB) AddFriendship(ctx context.Context, f Friendship) error {
	return addFriendship(ctx, db, f)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func addFriendship(ctx context.Context, x interface {
	execer
	queryer
}, f Friendship) error {
	if err := checkDialKey(ctx, x, f); err != nil {
		return err
	}
	res, err := x.ExecContext(ctx, `INSERT INTO friendship
		(peer_key, created_at, address, own_number, peer_number, own_retention, peer_retention, dial_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (peer_key) DO NOTHING`,
		f.Peer, f.CreatedAt.Unix(), f.Address, f.OwnNumber, f.PeerNumber,
		int64(f.OwnRetention/time.Second), int64(f.PeerRetention/time.Second), nullString(f.DialKey))
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

const friendshipCols = `peer_key, created_at, address, own_number, peer_number, own_retention, peer_retention, dial_key`

func scanFriendship(row interface{ Scan(...any) error }) (Friendship, error) {
	var f Friendship
	var created int64
	var addr, dial sql.NullString
	var own, peer, ownRet, peerRet sql.NullInt64
	err := row.Scan(&f.Peer, &created, &addr, &own, &peer, &ownRet, &peerRet, &dial)
	if errors.Is(err, sql.ErrNoRows) {
		return f, ErrNotFound
	} else if err != nil {
		return f, err
	}
	f.CreatedAt = time.Unix(created, 0)
	f.Address = addr.String
	f.DialKey = dial.String
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
