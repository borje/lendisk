package state

import (
	"context"
	"database/sql"
	"errors"
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
//   - An undelete restores only tombs it claimed, still inside the
//     window, so it never meets the worker issuing the same tomb.

// tombClaim is how long a claim on a tomb holds: one older died with
// its process, and the tomb is free again.
const tombClaim = time.Hour

// ClientSide reports whether the target's retention is lendisk's own
// (S3-4): it has no versioning, so deletes are held as client-side
// tombstones for its retention window.
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
// The caller then acknowledges the row as for a sent delete.
func (db *DB) HoldOverflowDelete(ctx context.Context, place string, seq int64, now time.Time) (held bool, err error) {
	name, ok := OverflowName(place)
	if !ok {
		return false, nil
	}
	o, err := db.GetOverflow(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil || !o.ClientSide() {
		return false, err
	}
	var bucket, key, op, st string
	var size int64
	var sha sql.NullString
	err = db.QueryRowContext(ctx, `SELECT bucket, key, op, state, coalesce(size, 0), sha256 FROM placement
		WHERE place = ? AND seq = ?`, place, seq).Scan(&bucket, &key, &op, &st, &size, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
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
		err := db.QueryRowContext(ctx, `SELECT sha256 FROM placement
			WHERE place = ? AND bucket = ? AND key = ? AND op = 'put' AND seq < ?
			AND state IN ('acked', 'drained', 'abandoned') AND sha256 IS NOT NULL
			ORDER BY seq DESC LIMIT 1`, place, bucket, key, seq).Scan(&sha)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	}
	_, err = db.ExecContext(ctx, `INSERT OR IGNORE INTO overflow_tomb (place, seq, bucket, key, sha256, size, drained, deleted_at, due_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, place, seq, bucket, key, sha, size, nullTrue(drained),
		now.Unix(), now.Add(o.Retention).Unix())
	return err == nil, err
}

const oTombCols = `place, seq, bucket, key, coalesce(sha256, ''), size, coalesce(drained, 0), deleted_at, due_at`

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

// DueOverflowTombs drops place's superseded tombs, then claims and
// returns up to limit of those due at now (their window ended), oldest
// first, that no one else holds a live claim on. The caller sends each
// delete and then drops the tomb.
func (db *DB) DueOverflowTombs(ctx context.Context, place string, now time.Time, limit int) ([]OverflowTomb, error) {
	if _, err := db.DropSupersededTombs(ctx, place); err != nil {
		return nil, err
	}
	return scanOTombs(db.QueryContext(ctx, `UPDATE overflow_tomb SET claimed_at = ?2
		WHERE place = ?1 AND seq IN (SELECT seq FROM overflow_tomb WHERE place = ?1 AND due_at <= ?2
			AND (claimed_at IS NULL OR claimed_at <= ?3) ORDER BY due_at, seq LIMIT ?4)
		RETURNING `+oTombCols, place, now.Unix(), now.Add(-tombClaim).Unix(), limit))
}

// DropOverflowTomb drops a tomb whose delete the target acknowledged.
func (db *DB) DropOverflowTomb(ctx context.Context, place string, seq int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM overflow_tomb WHERE place = ? AND seq = ?`, place, seq)
	return err
}

// undeletable selects place ?1's tombs of bucket ?2 an undelete may
// restore: deletes of the key (not copies re-placement drained, which
// the index holds elsewhere), deleted at or after ?3, inside the window
// at ?4 (not due).
const undeletable = `FROM overflow_tomb WHERE place = ?1 AND bucket = ?2 AND drained IS NULL
	AND deleted_at >= ?3 AND due_at > ?4`

// UndeletableTombs lists place's tombs of bucket an undelete since
// since would restore at now (RET-3), by key.
func (db *DB) UndeletableTombs(ctx context.Context, place, bucket string, since, now time.Time) ([]OverflowTomb, error) {
	return scanOTombs(db.QueryContext(ctx, `SELECT `+oTombCols+` `+undeletable+` ORDER BY key, seq`,
		place, bucket, unixOrZero(since), now.Unix()))
}

// ClaimOverflowTombs claims and returns place's tombs of bucket an
// undelete since since restores at now, that no one else holds a live
// claim on. The caller records what it restores, drops the tombs that
// record supersedes (DropSupersededTombs), and gives back the rest
// (UnclaimOverflowTombs).
func (db *DB) ClaimOverflowTombs(ctx context.Context, place, bucket string, since, now time.Time) ([]OverflowTomb, error) {
	return scanOTombs(db.QueryContext(ctx, `UPDATE overflow_tomb SET claimed_at = ?4
		WHERE place = ?1 AND seq IN (SELECT seq `+undeletable+` AND (claimed_at IS NULL OR claimed_at <= ?5))
		RETURNING `+oTombCols, place, bucket, unixOrZero(since), now.Unix(), now.Add(-tombClaim).Unix()))
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
// how many, their bytes (counted in its use), and when the next is due.
type TombHold struct {
	Tombs   int64
	Bytes   int64
	NextDue time.Time
}

// OverflowTombHolds returns each overflow target's tombstones that has
// any, by name.
func (db *DB) OverflowTombHolds(ctx context.Context) (map[string]TombHold, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, count(*), sum(size), min(due_at) FROM overflow_tomb GROUP BY place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]TombHold{}
	for rows.Next() {
		var place string
		var h TombHold
		var due int64
		if err := rows.Scan(&place, &h.Tombs, &h.Bytes, &due); err != nil {
			return nil, err
		}
		h.NextDue = time.Unix(due, 0)
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
