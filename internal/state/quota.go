package state

import (
	"context"
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

// CheckMaxTotal refuses adding n to the node's numbers at now when that
// would exceed max; max ≤ 0 is no cap.
func (db *DB) CheckMaxTotal(ctx context.Context, now time.Time, n, max int64) error {
	if max <= 0 {
		return nil
	}
	others, err := lentNumbers(ctx, db, now, "")
	if err != nil {
		return err
	}
	if others+n > max {
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
	if max > 0 {
		others, err := lentNumbers(ctx, tx, db.now(), peer)
		if err != nil {
			return before, after, err
		}
		if others+n > max {
			return before, after, &MaxTotalError{Others: others, Max: max}
		}
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
	return scanFriendship(db.QueryRowContext(ctx, `UPDATE friendship
		SET peer_number = ?, peer_use = ?, peer_repo_size = ?, peer_protocol = ?, seen_at = ?
		WHERE peer_key = ? RETURNING `+friendshipCols,
		r.Number, r.Use, r.RepositorySize, nullString(r.Protocol), now.Unix(), peer))
}

// QuotaAnswer is a friend's answer to this node's quota call: its
// number, its count of what it stores here, its count as lender of what
// this node stores there, its repository size, the protocol version it
// spoke, and how the call went (direct or relayed; "" unknown).
type QuotaAnswer struct {
	Number, Use, Lent, RepositorySize int64
	Protocol, Path                    string
}

// QuotaAnswered applies peer's answer at now to the call that sent the
// own number sent (the caller's side), which is then told unless it has
// changed since, and returns the friendship as it now stands.
//
// The answer's Lent replaces this node's count of what it stores at peer
// (place_use, §3.4, #68), plus what is queued there and not yet
// acknowledged: the lender's count holds what the borrower's cannot know
// (a copy whose answer was lost, deletes it keeps as tombs until the
// purge, its own overheads). A delete queued there frees nothing until a
// later answer. The placement triggers move the count from there.
func (db *DB) QuotaAnswered(ctx context.Context, peer string, sent int64, a QuotaAnswer, now time.Time) (Friendship, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Friendship{}, err
	}
	defer tx.Rollback()
	f, err := scanFriendship(tx.QueryRowContext(ctx, `UPDATE friendship
		SET peer_number = ?1, peer_use = ?2, lent_there = ?3, peer_repo_size = ?4, peer_protocol = ?5,
			path = ?6, seen_at = ?7, exchange_error = NULL,
			told_number = CASE WHEN own_number = ?8 THEN ?8 ELSE told_number END
		WHERE peer_key = ?9 RETURNING `+friendshipCols,
		a.Number, a.Use, a.Lent, a.RepositorySize, nullString(a.Protocol), nullString(a.Path), now.Unix(), sent, peer))
	if err != nil {
		return f, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO place_use (place, bytes) VALUES (?1,
		?2 + (SELECT coalesce(sum(max(size, 0)), 0) FROM placement WHERE place = ?1 AND state = 'queued'))
		ON CONFLICT (place) DO UPDATE SET bytes = excluded.bytes`, peer, capped(a.Lent)); err != nil {
		return f, err
	}
	return f, tx.Commit()
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
	_, err := db.ExecContext(ctx, `UPDATE place SET condition = 'ok', detail = NULL, since = ?
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
