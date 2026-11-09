package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Each side's number for a friendship and the quota call (LEND-1,
// LEND-7, LEND-8, PEER-6, #24). The quota is the smaller number
// (Friendship.Quota), read wherever it is enforced, so a change here is
// in force at the next request; a number is changed only by its own
// side (SetOwnNumber) or by the friend's report of its own
// (QuotaReported, QuotaAnswered).

// ErrOverMaxTotal means a number would take the sum of the node's
// numbers past lending.max_total (LEND-1). The error is a *MaxTotalError.
var ErrOverMaxTotal = errors.New("the sum of this node's numbers would exceed lending.max_total")

// MaxTotalError is ErrOverMaxTotal with its figures: what the node's
// other numbers already take, and the cap.
type MaxTotalError struct {
	Others, Max int64
}

func (e *MaxTotalError) Error() string {
	return fmt.Sprintf("%v: the others take %d bytes of the %d allowed", ErrOverMaxTotal, e.Others, e.Max)
}

func (e *MaxTotalError) Is(target error) bool { return target == ErrOverMaxTotal }

// lentNumbers is the sum of the node's numbers at now, but except's own:
// every friendship's own number, every unexpired, unused invite's
// number, and every redeem in progress (a friendship to be).
func lentNumbers(ctx context.Context, q querier, now time.Time, except string) (int64, error) {
	var n int64
	err := q.QueryRowContext(ctx, `SELECT
		(SELECT coalesce(sum(coalesce(own_number, 0)), 0) FROM friendship WHERE peer_key != ?1)
		+ (SELECT coalesce(sum(coalesce(number, 0)), 0) FROM invite WHERE used_at IS NULL AND expires_at > ?2)
		+ (SELECT coalesce(sum(number), 0) FROM pending_redeem WHERE peer_key != ?1)`,
		except, now.Unix()).Scan(&n)
	return n, err
}

// LentNumbers is the sum of the node's numbers at now (lending.max_total
// bounds it), leaving out the friendship with except, if any.
func (db *DB) LentNumbers(ctx context.Context, now time.Time, except string) (int64, error) {
	return lentNumbers(ctx, db, now, except)
}

// AddInviteWithin stores inv, refusing with a *MaxTotalError at now one
// whose number takes the sum of the node's numbers past max (none if
// max ≤ 0), checked in the transaction that stores it (LEND-1).
func (db *DB) AddInviteWithin(ctx context.Context, inv Invite, max int64, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := withinMax(ctx, tx, now, "", inv.Number, max); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO invite (invite_key, expires_at, number, retention) VALUES (?, ?, ?, ?)`,
		inv.Key, inv.ExpiresAt.Unix(), inv.Number, int64(inv.Retention/time.Second)); err != nil {
		return err
	}
	return tx.Commit()
}

// StartRedeemWithin is StartRedeem, refusing with a *MaxTotalError at
// now a number that takes the sum of the node's numbers past max (none
// if max ≤ 0), checked in the transaction that records it. Other
// redeems with the same inviter are left out of the sum: this one
// settles them (FinishRedeem).
func (db *DB) StartRedeemWithin(ctx context.Context, p PendingRedeem, max int64, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := withinMax(ctx, tx, now, p.Peer, p.Number, max); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO pending_redeem (invite_key, peer_key, number, retention, started_at)
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
	return tx.Commit()
}

// withinMax refuses adding n to the node's numbers at now, but except's,
// past max (none if max ≤ 0).
func withinMax(ctx context.Context, q querier, now time.Time, except string, n, max int64) error {
	if max <= 0 {
		return nil
	}
	others, err := lentNumbers(ctx, q, now, except)
	if err != nil {
		return err
	}
	if n > max-others {
		return &MaxTotalError{Others: others, Max: max}
	}
	return nil
}

// SetOwnNumber sets this node's number for the friendship with peer to n
// (PEER-6, LEND-8), refusing with a *MaxTotalError a value that takes
// the sum of the node's numbers past max (none if max ≤ 0), and returns
// the friendship before and after. ErrNotFound if peer is no friend.
func (db *DB) SetOwnNumber(ctx context.Context, peer string, n, max int64) (before, after Friendship, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return before, after, err
	}
	defer tx.Rollback()
	before, err = scanFriendship(tx.QueryRowContext(ctx, `SELECT `+friendshipCols+` FROM friendship WHERE peer_key = ?`, peer))
	if err != nil {
		return before, after, err
	}
	if err := withinMax(ctx, tx, db.now(), peer, n, max); err != nil {
		return before, after, err
	}
	after, err = scanFriendship(tx.QueryRowContext(ctx, `UPDATE friendship SET own_number = ? WHERE peer_key = ?
		RETURNING `+friendshipCols, n, peer))
	if err != nil {
		return before, after, err
	}
	return before, after, tx.Commit()
}

// QuotaReport is what a friend's quota call carries: its own number for
// the friendship, its count of what it stores at this node, its
// repository size, and the protocol version it spoke.
type QuotaReport struct {
	Number, Use, RepositorySize int64
	Protocol                    string
}

// QuotaReported applies the quota call of peer at now (the callee's
// side): its number is in force at once, here as on its side. It
// returns the friendship as it now stands; ErrNotFound if peer is no
// friend.
func (db *DB) QuotaReported(ctx context.Context, peer string, r QuotaReport, now time.Time) (Friendship, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Friendship{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE friendship
		SET peer_number = ?, peer_use = ?, peer_repo_size = ?, peer_protocol = ?, seen_at = ?, reported_at = ?
		WHERE peer_key = ?`,
		r.Number, r.Use, r.RepositorySize, nullString(r.Protocol), now.Unix(), now.UnixNano(), peer); err != nil {
		return Friendship{}, err
	}
	f, err := markLowUse(ctx, tx, peer, now)
	if err != nil {
		return f, err
	}
	return f, tx.Commit()
}

// markLowUse marks at now since when each side of the friendship with
// peer has used less than half of its quota (LEND-7, #37), keeping an
// earlier mark, and clears a side's mark once it uses half or more or
// the quota is zero. This node's use there is place_use (with what its
// queued deletes will free, as PlaceUse counts it); the friend's here
// is lent_usage. It returns the friendship as it now stands.
func markLowUse(ctx context.Context, tx *sql.Tx, peer string, now time.Time) (Friendship, error) {
	return scanFriendship(tx.QueryRowContext(ctx, `UPDATE friendship SET
		low_there_since = CASE WHEN q.quota > 0 AND 2 * q.there < q.quota THEN coalesce(low_there_since, ?2) END,
		low_here_since = CASE WHEN q.quota > 0 AND 2 * q.here < q.quota THEN coalesce(low_here_since, ?2) END
		FROM (SELECT min(coalesce(f.own_number, 0), coalesce(f.peer_number, 0)) AS quota,
			coalesce((SELECT u.bytes FROM place_use u WHERE u.place = f.peer_key), 0)
				+ coalesce((SELECT fr.bytes FROM place_freed fr WHERE fr.place = f.peer_key), 0) AS there,
			coalesce((SELECT l.bytes FROM lent_usage l WHERE l.peer_key = f.peer_key), 0) AS here
			FROM friendship f WHERE f.peer_key = ?1) AS q
		WHERE peer_key = ?1 RETURNING `+friendshipCols, peer, now.Unix()))
}

// QuotaAnswer is a friend's answer to this node's quota call: its
// number, its count of what it stores here, its count as lender of what
// this node stores there, its repository size, the protocol version it
// spoke, and how the call went (direct or relayed; "" unknown).
type QuotaAnswer struct {
	Number, Use, Lent, RepositorySize int64
	Protocol, Path                    string
}

// QuotaAnswered applies peer's answer at now to the call sent at sentAt
// with the own number sent (the caller's side), which is then told
// unless it has changed since, and returns the friendship as it now
// stands. The answer's number is not applied if peer's own call applied
// one at or after sentAt: that one is newer (#24 review).
//
// The answer's Lent replaces this node's count of what it stores at peer
// (place_use, §3.4, #68), plus what is queued there and not yet
// acknowledged: the lender's count holds what the borrower's cannot know
// (a copy whose answer was lost, deletes it keeps as tombs until the
// purge, its own overheads). A delete queued there frees nothing until a
// later answer: what deletes take off place_use from here is kept in
// place_freed and counted back until then. The placement triggers move
// both from there. So that they do, place_freed starts at what the
// place's queued and acknowledged deletes take off place_use, and
// place_use at the answer less that: a delete acknowledged, or sized
// anew by a late put (AckPlacement), after the answer then nets to
// nothing, where a place_freed of 0 would floor at 0 and count it back
// twice (#85).
func (db *DB) QuotaAnswered(ctx context.Context, peer string, sent int64, sentAt time.Time, a QuotaAnswer, now time.Time) (Friendship, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Friendship{}, err
	}
	defer tx.Rollback()
	f, err := scanFriendship(tx.QueryRowContext(ctx, `UPDATE friendship
		SET peer_number = CASE WHEN coalesce(reported_at, 0) >= ?10 THEN peer_number ELSE ?1 END, peer_use = ?2, lent_there = ?3, peer_repo_size = ?4, peer_protocol = ?5,
			path = ?6, seen_at = ?7, exchange_error = NULL,
			told_number = CASE WHEN own_number = ?8 THEN ?8 ELSE told_number END
		WHERE peer_key = ?9 RETURNING `+friendshipCols,
		a.Number, a.Use, a.Lent, a.RepositorySize, nullString(a.Protocol), nullString(a.Path), now.Unix(), sent, peer, sentAt.UnixNano()))
	if err != nil {
		return f, err
	}
	var freed int64
	if err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(-size), 0) FROM placement
		WHERE place = ? AND state IN ('queued', 'acked') AND size < 0`, peer).Scan(&freed); err != nil {
		return f, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO place_use (place, bytes) VALUES (?1,
		?2 + (SELECT coalesce(sum(max(size, 0)), 0) FROM placement WHERE place = ?1 AND state = 'queued') - ?3)
		ON CONFLICT (place) DO UPDATE SET bytes = excluded.bytes`, peer, capped(a.Lent), freed); err != nil {
		return f, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO place_freed (place, bytes) VALUES (?1, ?2)
		ON CONFLICT (place) DO UPDATE SET bytes = excluded.bytes`, peer, freed); err != nil {
		return f, err
	}
	if f, err = markLowUse(ctx, tx, peer, now); err != nil {
		return f, err
	}
	return f, tx.Commit()
}

// PlaceSending reports whether a quota answer from place to a call sent
// at since may or may not count a send (#85): place has a row its
// sender took that is still in its FIFO, or one taken at or after
// since (to the second). The lender's count may be taken before or
// after such a send lands.
func (db *DB) PlaceSending(ctx context.Context, place string, since time.Time) (bool, error) {
	var sending bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM placement WHERE place = ?1
		AND (state IN ('queued', 'dropping') AND started_at IS NOT NULL OR started_at >= ?2))`,
		place, since.Unix()).Scan(&sending)
	return sending, err
}

// PlaceIdle reports whether place's FIFO is empty: nothing queued or
// dropping there.
func (db *DB) PlaceIdle(ctx context.Context, place string) (bool, error) {
	var busy bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM placement WHERE place = ?1 AND state = 'queued')
		OR EXISTS (SELECT 1 FROM placement WHERE place = ?1 AND state = 'dropping')`, place).Scan(&busy)
	return !busy, err
}

// QuotaFailed records at now why this node's last quota call to peer
// failed.
func (db *DB) QuotaFailed(ctx context.Context, peer, why string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE friendship SET exchange_error = ? WHERE peer_key = ?`, why, peer)
	return err
}

// ResumePlace ends place's pause for quota at now when the quota rose
// (LEND-8): its condition goes from quota to ok, so placement counts it
// as room again at once rather than after it next acknowledges a row.
// LastOK is untouched. Any other condition stands.
func (db *DB) ResumePlace(ctx context.Context, place string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE place SET condition = 'ok', detail = NULL, since = ?, failing_since = NULL
		WHERE place = ? AND condition = 'quota'`, now.Unix(), place)
	return err
}

// RepositorySize is the node's repository size (LEND-7): the bytes of
// every own bucket's live objects, each key's latest done write if a
// put.
func (db *DB) RepositorySize(ctx context.Context) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(sum(q.size), 0) FROM queue q
		WHERE q.op = 'put' AND q.state = 'done'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`).Scan(&n)
	return n, err
}

// RepositoryUse is the node's repository as capacity planning counts it
// (LEND-7, #37): as places count it, each live object with
// ObjectOverhead, split into data and metadata (the metadata and
// snapshot classes, held at every place, §3.4), and the largest copies
// of a bucket holding live data.
type RepositoryUse struct {
	Data, Metadata int64
	Copies         int
}

// RepositoryUse reads the node's repository as capacity planning
// counts it: each key's latest done write, if a put.
func (db *DB) RepositoryUse(ctx context.Context) (RepositoryUse, error) {
	var u RepositoryUse
	err := db.QueryRowContext(ctx, `SELECT
		coalesce(sum(CASE WHEN q.class IN ('metadata', 'snapshot') THEN 0 ELSE q.size + ?1 END), 0),
		coalesce(sum(CASE WHEN q.class IN ('metadata', 'snapshot') THEN q.size + ?1 ELSE 0 END), 0),
		coalesce(max(CASE WHEN q.class IN ('metadata', 'snapshot') THEN NULL ELSE coalesce(b.copies, ?2) END), 0)
		FROM queue q LEFT JOIN buckets b ON b.name = q.bucket
		WHERE q.op = 'put' AND q.state = 'done'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`,
		ObjectOverhead, DefaultCopies).Scan(&u.Data, &u.Metadata, &u.Copies)
	return u, err
}
