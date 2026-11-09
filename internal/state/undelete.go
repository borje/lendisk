package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Undelete, the borrower's side (RET-3, #32). `lendisk undelete` asks
// the daemon through table undelete_request, as `lendisk verify` does
// (§3.6 VER-3): the daemon dials the friends, so no second process
// dials with its keys. What each friend restored joins the index here
// (RecordRestored).

// Stages of an undelete request.
const (
	UndeleteAsked     = "asked"     // the daemon is to preview it
	UndeletePreviewed = "previewed" // preview written; the user decides
	UndeleteConfirmed = "confirmed" // the daemon is to ask the friends
	UndeleteDone      = "done"
	UndeleteFailed    = "failed"
)

// UndeleteKept is how long an undelete request is kept: older ones are
// dropped at the next request.
const UndeleteKept = 7 * 24 * time.Hour

// UndeleteRequest is an undelete `lendisk undelete` asked the daemon
// for: the bucket, since when (zero: every tomb in the window), whether
// rewrites' tombs count, its stage, and the preview and result as the
// daemon wrote them (JSON, the undelete package's).
type UndeleteRequest struct {
	ID              int64
	Bucket          string
	Since           time.Time
	Rewrites        bool
	RequestedAt     time.Time
	Stage           string
	Preview, Result string
}

// RequestUndelete records a request for the daemon to preview an
// undelete of bucket since since, and returns its ID. Requests older
// than UndeleteKept are dropped.
func (db *DB) RequestUndelete(ctx context.Context, bucket string, since time.Time, rewrites bool, now time.Time) (int64, error) {
	if _, err := db.ExecContext(ctx, `DELETE FROM undelete_request WHERE requested_at < ?`, now.Add(-UndeleteKept).Unix()); err != nil {
		return 0, err
	}
	var s int64
	if !since.IsZero() {
		s = max(since.Unix(), 0)
	}
	r, err := db.ExecContext(ctx, `INSERT INTO undelete_request (bucket, since, rewrites, requested_at, stage, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`, bucket, s, nullTrue(rewrites), now.Unix(), UndeleteAsked, now.Unix())
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

const undeleteCols = `id, bucket, since, coalesce(rewrites, 0), requested_at, stage, coalesce(preview, ''), coalesce(result, '')`

func scanUndelete(row interface{ Scan(...any) error }) (UndeleteRequest, error) {
	var r UndeleteRequest
	var since, at int64
	if err := row.Scan(&r.ID, &r.Bucket, &since, &r.Rewrites, &at, &r.Stage, &r.Preview, &r.Result); err != nil {
		return r, err
	}
	if since > 0 {
		r.Since = time.Unix(since, 0)
	}
	r.RequestedAt = time.Unix(at, 0)
	return r, nil
}

// GetUndelete returns request id; ErrNotFound if there is none.
func (db *DB) GetUndelete(ctx context.Context, id int64) (UndeleteRequest, error) {
	r, err := scanUndelete(db.QueryRowContext(ctx, `SELECT `+undeleteCols+` FROM undelete_request WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// PendingUndeletes returns the requests the daemon has work on: to
// preview or confirmed, oldest first.
func (db *DB) PendingUndeletes(ctx context.Context) ([]UndeleteRequest, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+undeleteCols+` FROM undelete_request
		WHERE stage IN (?, ?) ORDER BY id`, UndeleteAsked, UndeleteConfirmed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UndeleteRequest
	for rows.Next() {
		r, err := scanUndelete(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// advanceUndelete moves request id from stage from to stage to, setting
// column col (preview or result) when given; ErrNotFound if it is not at
// from (taken back, or moved on).
func (db *DB) advanceUndelete(ctx context.Context, id int64, from, to, col, val string, now time.Time) error {
	q := `UPDATE undelete_request SET stage = ?, updated_at = ?`
	args := []any{to, now.Unix()}
	if col != "" {
		q += `, ` + col + ` = ?`
		args = append(args, val)
	}
	q += ` WHERE id = ? AND stage = ?`
	args = append(args, id, from)
	res, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// PreviewUndelete records the daemon's preview of request id.
func (db *DB) PreviewUndelete(ctx context.Context, id int64, preview string, now time.Time) error {
	return db.advanceUndelete(ctx, id, UndeleteAsked, UndeletePreviewed, "preview", preview, now)
}

// ConfirmUndelete records the user's yes to a previewed request.
func (db *DB) ConfirmUndelete(ctx context.Context, id int64, now time.Time) error {
	return db.advanceUndelete(ctx, id, UndeletePreviewed, UndeleteConfirmed, "", "", now)
}

// FinishUndelete records the outcome of a request the daemon ran, or
// could not preview: done, or failed, with the result.
func (db *DB) FinishUndelete(ctx context.Context, id int64, failed bool, result string, now time.Time) error {
	to := UndeleteDone
	if failed {
		to = UndeleteFailed
	}
	res, err := db.ExecContext(ctx, `UPDATE undelete_request SET stage = ?, result = ?, updated_at = ?
		WHERE id = ? AND stage IN (?, ?)`, to, result, now.Unix(), id, UndeleteAsked, UndeleteConfirmed)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// CancelUndelete drops a request not yet confirmed: the user said no, or
// stopped waiting. A confirmed one runs to its end.
func (db *DB) CancelUndelete(ctx context.Context, id int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM undelete_request WHERE id = ? AND stage IN (?, ?)`, id, UndeleteAsked, UndeletePreviewed)
	return err
}

// Restored is an object a place moved back to the live set (RET-3): its
// key, the version's size and hash, and the class and rewritability the
// bucket's profile gives the key (EP-7).
type Restored struct {
	Key        string
	Size       int64
	SHA256     string
	Class      string
	Rewritable bool
}

// KeptVersion is a key a place restored while this node's index holds
// another version of it, which it keeps: SHA256 the place's, Ours this
// node's.
type KeptVersion struct {
	Key, SHA256, Ours string
}

// RestoreResult is what RecordRestored did: Recorded keys gained a done
// put of the restored version, acknowledged at the place; Acked keys'
// latest write was that version already, now acknowledged at the place;
// Kept keys keep this node's other version (EP-11); Busy keys were being
// written, and are left to that write.
type RestoreResult struct {
	Recorded, Acked int
	Kept            []KeptVersion
	Busy            []string
}

// RecordRestored records objects place restored of bucket (RET-3, #32),
// in one transaction, as recovery records a listed version (ID-5):
//
//   - a key whose latest done write is a delete, or that has none (a
//     recovered node's key every place held tombstoned), gains a done
//     put of the version, created now, with an acknowledged placement
//     row at place, counted in its use with the object overhead. The row
//     is not held locally (queue.local NULL): the local store is not
//     refilled, the read-through fetches the object when it is read, and
//     the store reconciler does not take its absence for a delete (EP-14);
//   - a key whose latest done write is a put of the same version gains
//     the acknowledgement at place (a queued or refused row there
//     becomes acknowledged; one re-placement is dropping keeps its
//     course);
//   - a key whose latest done write is a put of another version keeps
//     it: the local store is the user's (EP-11), and the version written
//     last is this node's; the place's copy is reported (Kept);
//   - a key with a write in flight (a pending row) is left to it (Busy).
//
// The offsite watermark then counts the restored rows as any put
// acknowledged at its places (REPL-11).
func (db *DB) RecordRestored(ctx context.Context, place, bucket string, objs []Restored) (RestoreResult, error) {
	var r RestoreResult
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	p := prepared(tx)
	now := db.now().Unix()
	for _, o := range objs {
		var pending int
		if err := p.QueryRowContext(ctx, `SELECT count(*) FROM queue WHERE bucket = ? AND key = ? AND state = 'pending'`,
			bucket, o.Key).Scan(&pending); err != nil {
			return r, err
		}
		if pending > 0 {
			r.Busy = append(r.Busy, o.Key)
			continue
		}
		var seq int64
		var op string
		var sha sql.NullString
		err := p.QueryRowContext(ctx, `SELECT seq, op, sha256 FROM queue
			WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, bucket, o.Key).Scan(&seq, &op, &sha)
		switch {
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return r, err
		case err == nil && op == OpPut && sha.String != o.SHA256:
			r.Kept = append(r.Kept, KeptVersion{Key: o.Key, SHA256: o.SHA256, Ours: sha.String})
			continue
		case err == nil && op == OpPut:
			// No upsert: place_use's triggers insert OR IGNORE, which an
			// upsert's own conflict handling overrides.
			if _, err := p.ExecContext(ctx, `UPDATE placement SET state = 'acked', sha256 = ?, acked_at = ?, size = ?
				WHERE seq = ? AND place = ? AND state IN ('queued', 'refused', 'dropped', 'abandoned')`,
				o.SHA256, now, o.Size+ObjectOverhead, seq, place); err != nil {
				return r, err
			}
			if _, err := p.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
				SELECT ?, ?, ?, ?, 'put', 'acked', ?, ?, ?
				WHERE NOT EXISTS (SELECT 1 FROM placement WHERE seq = ? AND place = ?)`,
				seq, place, bucket, o.Key, o.SHA256, now, o.Size+ObjectOverhead, seq, place); err != nil {
				return r, err
			}
			r.Acked++
			continue
		}
		res, err := p.ExecContext(ctx, `INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at, rewritable)
			VALUES (?, ?, 'put', ?, 'done', ?, ?, ?, ?)`, bucket, o.Key, o.Class, o.Size, o.SHA256, now, nullTrue(o.Rewritable))
		if err != nil {
			return r, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return r, err
		}
		if _, err := p.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			VALUES (?, ?, ?, ?, 'put', 'acked', ?, ?, ?)`, id, place, bucket, o.Key, o.SHA256, now, o.Size+ObjectOverhead); err != nil {
			return r, err
		}
		r.Recorded++
	}
	return r, tx.Commit()
}
