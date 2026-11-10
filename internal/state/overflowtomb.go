package state

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"
)

// This file is the client-side tombstones of overflow targets without
// versioning (S3-4, RET-3, #39). Such a provider keeps nothing a delete
// removes, so the worker never sends it a delete at once: it holds the
// delete here (HoldOverflowDelete) and issues it only once the target's
// retention window has passed since (DueOverflowTombs). Until then the
// object stays at the target, counted in its use, and an undelete
// cancels the tomb (ClaimOverflowTombs). Tombs live in lendisk.db, so a
// restart neither forgets nor hastens one.
//
// Rules a caller must keep:
//   - A tomb is issued (the delete sent) only after DueOverflowTombs
//     returned it, which claims it, and is dropped only after the
//     delete was acknowledged (DropOverflowTomb).
//   - A tomb whose key the target holds again (an acknowledged put of
//     it at or after the tomb's row: a re-put, or an undelete's record)
//     is never issued; it is dropped (DropSupersededTombs).
//   - A claim holds a key: the worker sends no held delete of a key an
//     undelete claimed, and an undelete claims no key the worker holds,
//     and restores only keys whose newest tomb is still inside the
//     window.

// tombClaim is how long a claim on a tomb holds: one older died with
// its process, and the tomb is free again.
const tombClaim = time.Hour

// ClientSide reports whether the target's retention is lendisk's own
// (S3-4): it has no versioning as last read (overflow add, then daily:
// RecordOverflowVersioning, #97), so deletes from now on are held as
// client-side tombstones for its retention window.
func (o Overflow) ClientSide() bool { return o.Versioning != VersioningEnabled }

// Retention labels for status (S3-4).
const (
	RetentionProvider   = "provider"
	RetentionClientSide = "client-side"
)

// RetentionMode is the target's retention as status labels it.
func (o Overflow) RetentionMode() string {
	if o.ClientSide() {
		return RetentionClientSide
	}
	return RetentionProvider
}

// OverflowTomb is a delete held for an overflow target without
// versioning.
type OverflowTomb struct {
	Place       string
	Seq         int64 // the placement row held: the delete, or a drained copy
	Bucket, Key string
	SHA256      string // the version the target holds; "" when unknown
	Size        int64  // what the target counts, with the object overhead
	Drained     bool   // re-placement's drop of a copy, not a delete of the key
	DeletedAt   time.Time
	DueAt       time.Time
}

// HoldOverflowDelete holds the delete of row seq at place, an overflow
// target, as a client-side tombstone due after the target's retention
// window from now, if the target has no versioning (S3-4); held is
// false for any other place, which is sent the delete. The row is a
// queued delete of a key, or a copy re-placement drops (dropping):
// drain-back deletes go the same way (S3-4). Holding again is harmless.
// A row gone since its sender took it is held as nothing: such a target
// is never sent a delete outside a tomb. The caller then acknowledges
// the row as for a sent delete.
// At a target being removed with keep_data, every row is held as
// nothing: no delete is sent there any more (#96).
func (db *DB) HoldOverflowDelete(ctx context.Context, place string, seq int64, now time.Time) (held bool, err error) {
	name, ok := OverflowName(place)
	if !ok {
		return false, nil
	}
	o, err := db.GetOverflow(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if o.KeepData {
		// Being removed with its data kept (#96): nothing more is
		// deleted there, and the row is acknowledged unsent.
		return true, nil
	}
	if !o.ClientSide() {
		return false, nil
	}
	return db.insertTomb(ctx, place, seq, now, now.Add(o.Retention), false)
}

// DeniedRetry is how long a delete an overflow target refused for want
// of permission waits before it is tried again (S3-5, #40).
const DeniedRetry = 24 * time.Hour

// DenyOverflowDelete records that overflow target place refused the
// delete of row seq for want of permission (S3-5, #40): a credential
// without delete permission leaves drained copies and deleted objects
// at the target. The copy stays there as a tomb marked denied, counted
// in the target's use and in status (TombHold.Denied), and the delete
// is tried again after DeniedRetry, so a permission granted later
// empties the target. A row held already (a client-side tomb whose
// delete was refused) is marked so and given back. For a row not held
// yet, the caller then acknowledges it as for a sent delete.
func (db *DB) DenyOverflowDelete(ctx context.Context, place string, seq int64, now time.Time) error {
	r, err := db.ExecContext(ctx, `UPDATE overflow_tomb SET denied_at = ?, due_at = ?, claimed_at = NULL
		WHERE place = ? AND seq = ?`, now.Unix(), now.Add(DeniedRetry).Unix(), place, seq)
	if err != nil {
		return err
	}
	if n, err := r.RowsAffected(); err != nil || n > 0 {
		return err
	}
	_, err = db.insertTomb(ctx, place, seq, now, now.Add(DeniedRetry), true)
	return err
}

// insertTomb holds row seq at place, a queued delete or a dropping copy,
// as a tomb deleted at now and due at due; denied marks it refused for
// want of permission. Holding again is harmless.
func (db *DB) insertTomb(ctx context.Context, place string, seq int64, now, due time.Time, denied bool) (bool, error) {
	var bucket, key, op, st string
	var size int64
	var sha sql.NullString
	err := db.QueryRowContext(ctx, `SELECT bucket, key, op, state, coalesce(size, 0), sha256 FROM placement
		WHERE place = ? AND seq = ?`, place, seq).Scan(&bucket, &key, &op, &st, &size, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		// The row went since its sender took it (the place removed or
		// rebuilt): nothing to hold, and its delete is never sent at
		// once to a target that keeps nothing a delete removes (S3-4).
		return true, nil
	}
	if err != nil {
		return false, err
	}
	drained := st == PlacementDropping
	if !drained {
		// A delete: it took off what the target held of the key, which
		// is the tomb's; the version is the last put the target took.
		size = max(-size, 0)
		sha = sql.NullString{}
		err = db.QueryRowContext(ctx, `SELECT sha256 FROM placement
			WHERE place = ? AND bucket = ? AND key = ? AND op = 'put' AND seq < ?
			AND state IN ('acked', 'drained', 'abandoned') AND sha256 IS NOT NULL
			ORDER BY seq DESC LIMIT 1`, place, bucket, key, seq).Scan(&sha)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	}
	var deniedAt any
	if denied {
		deniedAt = now.Unix()
	}
	_, err = db.ExecContext(ctx, `INSERT OR IGNORE INTO overflow_tomb (place, seq, bucket, key, sha256, size, drained, deleted_at, due_at,
		denied_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, place, seq, bucket, key, sha, size, nullTrue(drained),
		now.Unix(), due.Unix(), deniedAt)
	return err == nil, err
}

const oTombCols = `place, seq, bucket, key, coalesce(sha256, ''), size, coalesce(drained, 0), deleted_at, due_at`

// tombColsT is oTombCols of table alias t.
const tombColsT = `t.place, t.seq, t.bucket, t.key, coalesce(t.sha256, ''), t.size, coalesce(t.drained, 0), t.deleted_at, t.due_at`

func scanOTombs(rows *sql.Rows, err error) ([]OverflowTomb, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OverflowTomb
	for rows.Next() {
		var t OverflowTomb
		var del, due int64
		if err := rows.Scan(&t.Place, &t.Seq, &t.Bucket, &t.Key, &t.SHA256, &t.Size, &t.Drained, &del, &due); err != nil {
			return nil, err
		}
		t.DeletedAt, t.DueAt = time.Unix(del, 0), time.Unix(due, 0)
		out = append(out, t)
	}
	return out, rows.Err()
}

// DropSupersededTombs drops place's tombs whose key the target holds
// again: an acknowledged put of the key there at or after the tomb's
// row (written again, or restored by an undelete). Their deletes are
// never issued. It returns how many it dropped.
func (db *DB) DropSupersededTombs(ctx context.Context, place string) (int64, error) {
	r, err := db.ExecContext(ctx, `DELETE FROM overflow_tomb WHERE place = ?1 AND EXISTS (SELECT 1 FROM placement p
		WHERE p.place = ?1 AND p.bucket = overflow_tomb.bucket AND p.key = overflow_tomb.key
		AND p.op = 'put' AND p.state = 'acked' AND p.seq >= overflow_tomb.seq)`, place)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// keyClaimed is true for tomb t when any tomb of its key at its place
// has a live claim (newer than ?stale): a claim holds the key, not one
// tomb, since an undelete restoring a key and the worker sending any of
// its held deletes must never meet (#39 review).
const keyClaimed = `EXISTS (SELECT 1 FROM overflow_tomb c WHERE c.place = t.place AND c.bucket = t.bucket
	AND c.key = t.key AND c.claimed_at > :stale)`

// DueOverflowTombs drops place's superseded tombs, then claims and
// returns up to limit of those due at now (their window ended), oldest
// first, whose key no one holds a live claim on. The caller sends each
// delete and then drops the tomb.
func (db *DB) DueOverflowTombs(ctx context.Context, place string, now time.Time, limit int) ([]OverflowTomb, error) {
	if _, err := db.DropSupersededTombs(ctx, place); err != nil {
		return nil, err
	}
	return scanOTombs(db.QueryContext(ctx, `UPDATE overflow_tomb SET claimed_at = :now
		WHERE place = :place AND seq IN (SELECT t.seq FROM overflow_tomb t WHERE t.place = :place AND t.due_at <= :now
			AND NOT `+keyClaimed+` ORDER BY t.due_at, t.seq LIMIT :limit)
		RETURNING `+oTombCols, sql.Named("place", place), sql.Named("now", now.Unix()),
		sql.Named("stale", now.Add(-tombClaim).Unix()), sql.Named("limit", limit)))
}

// DropOverflowTomb drops a tomb whose delete the target acknowledged.
func (db *DB) DropOverflowTomb(ctx context.Context, place string, seq int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM overflow_tomb WHERE place = ? AND seq = ?`, place, seq)
	return err
}

// undeletable selects place's tombs t of bucket an undelete may restore:
// the newest tomb of its key, a delete of the key (not a copy
// re-placement drained, which the index holds elsewhere), made at or
// after :since and inside its window at :now (not due).
const undeletable = `FROM overflow_tomb t WHERE t.place = :place AND t.bucket = :bucket AND t.drained IS NULL
	AND t.deleted_at >= :since AND t.due_at > :now
	AND t.seq = (SELECT max(n.seq) FROM overflow_tomb n WHERE n.place = t.place AND n.bucket = t.bucket AND n.key = t.key)`

func undeletableArgs(place, bucket string, since, now time.Time) []any {
	return []any{sql.Named("place", place), sql.Named("bucket", bucket), sql.Named("since", unixOrZero(since)),
		sql.Named("now", now.Unix()), sql.Named("stale", now.Add(-tombClaim).Unix())}
}

// UndeletableTombs drops place's superseded tombs, then lists those of
// bucket an undelete since since would restore at now (RET-3): the
// newest of each key, by key.
func (db *DB) UndeletableTombs(ctx context.Context, place, bucket string, since, now time.Time) ([]OverflowTomb, error) {
	if _, err := db.DropSupersededTombs(ctx, place); err != nil {
		return nil, err
	}
	return scanOTombs(db.QueryContext(ctx, `SELECT `+tombColsT+` `+undeletable+` ORDER BY t.key`,
		undeletableArgs(place, bucket, since, now)...))
}

// ClaimOverflowTombs drops place's superseded tombs, then claims every
// tomb, drained and due ones included, of each key of bucket an
// undelete since since restores at now (UndeletableTombs) that no one
// holds a live claim on, and returns them, by key and then sequence: the
// worker sends none of them while the claim lives. The caller renews
// the claims at least hourly (RenewOverflowClaims), records what it
// restores, drops the tombs that record supersedes
// (DropSupersededTombs), and gives back the rest (UnclaimOverflowTombs).
func (db *DB) ClaimOverflowTombs(ctx context.Context, place, bucket string, since, now time.Time) ([]OverflowTomb, error) {
	if _, err := db.DropSupersededTombs(ctx, place); err != nil {
		return nil, err
	}
	ts, err := scanOTombs(db.QueryContext(ctx, `UPDATE overflow_tomb SET claimed_at = :now
		WHERE place = :place AND bucket = :bucket AND key IN (SELECT t.key `+undeletable+` AND NOT `+keyClaimed+`)
		RETURNING `+oTombCols, undeletableArgs(place, bucket, since, now)...))
	slices.SortFunc(ts, func(a, b OverflowTomb) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), cmp.Compare(a.Seq, b.Seq))
	})
	return ts, err
}

// RenewOverflowClaims renews the live claims on place's tombs seqs at
// now, so a long undelete keeps them.
func (db *DB) RenewOverflowClaims(ctx context.Context, place string, seqs []int64, now time.Time) error {
	for _, s := range seqs {
		if _, err := db.ExecContext(ctx, `UPDATE overflow_tomb SET claimed_at = ? WHERE place = ? AND seq = ? AND claimed_at IS NOT NULL`,
			now.Unix(), place, s); err != nil {
			return err
		}
	}
	return nil
}

// UnclaimOverflowTombs gives back claims on place's tombs seqs: they
// are issued when due, as before.
func (db *DB) UnclaimOverflowTombs(ctx context.Context, place string, seqs []int64) error {
	for _, s := range seqs {
		if _, err := db.ExecContext(ctx, `UPDATE overflow_tomb SET claimed_at = NULL WHERE place = ? AND seq = ?`, place, s); err != nil {
			return err
		}
	}
	return nil
}

// TombHold is what an overflow target holds as client-side tombstones:
// how many, their bytes (counted in its use), and when the next is due;
// and the deletes it refused for want of permission (S3-5, #40), whose
// copies stay there, counted in its use, until a daily retry succeeds.
type TombHold struct {
	Tombs   int64
	Bytes   int64
	NextDue time.Time // zero when no tomb waits for its window
	// Denied and DeniedBytes are the copies, drained or deleted, the
	// target refused to delete (DenyOverflowDelete).
	Denied      int64
	DeniedBytes int64
}

// OverflowTombHolds returns each overflow target's tombstones that has
// any, by name.
func (db *DB) OverflowTombHolds(ctx context.Context) (map[string]TombHold, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, count(*) FILTER (WHERE denied_at IS NULL),
		coalesce(sum(size) FILTER (WHERE denied_at IS NULL), 0), min(due_at) FILTER (WHERE denied_at IS NULL),
		count(*) FILTER (WHERE denied_at IS NOT NULL), coalesce(sum(size) FILTER (WHERE denied_at IS NOT NULL), 0)
		FROM overflow_tomb GROUP BY place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]TombHold{}
	for rows.Next() {
		var place string
		var h TombHold
		var due sql.NullInt64
		if err := rows.Scan(&place, &h.Tombs, &h.Bytes, &due, &h.Denied, &h.DeniedBytes); err != nil {
			return nil, err
		}
		if due.Valid {
			h.NextDue = time.Unix(due.Int64, 0)
		}
		if name, ok := OverflowName(place); ok {
			out[name] = h
		}
	}
	return out, rows.Err()
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
