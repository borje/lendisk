package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// This file is overflow targets' versioning over time (S3-4, #97). The
// verifier reads each target's versioning again daily
// (RecordOverflowVersioning); a change is kept in overflow_versioning
// and the target's row takes the new value, so its retention follows it
// from then on (Overflow.ClientSide): a delete after a suspension is
// held as a client-side tomb, a delete after versioning was enabled is
// sent as a delete marker. What was done before stays as it was: tombs
// already held stay held until their window ends, and a delete sent as a
// marker while versioning was enabled stays a marker the lifecycle rule
// expires, which keeps the bucket's metadata at the target for its
// window (holdsLiveData) and which undelete may bring back
// (MarkerDeleted).
//
// Rules a caller must keep:
//   - Only an answer from the provider is recorded; a read that failed
//     decides nothing.
//   - Whether a sent delete is a marker is judged by the versioning at
//     the time the target acknowledged it (versionedAt), never by the
//     versioning now.

// VersioningChange is a change of an overflow target's versioning found
// by a read: Previous before At, Versioning from then on.
type VersioningChange struct {
	Name                 string
	At                   time.Time
	Previous, Versioning string
}

// versionedAt is the SQL condition that overflow target name had
// versioning at t (unix seconds), current its versioning now: the
// previous versioning of the first change after t, or current if there
// is none, is enabled. A change is known to the second, so in the second
// a change was found either versioning counts: a marker is never missed,
// and a delete held then is told apart by its tomb (MarkerDeleted). name,
// t and current are SQL expressions.
func versionedAt(name, t, current string) string {
	at := func(cmp string) string {
		return `coalesce((SELECT c.previous FROM overflow_versioning c WHERE c.name = ` + name + ` AND c.at ` + cmp + ` ` + t + `
		ORDER BY c.at, c.id LIMIT 1), ` + current + `) = '` + VersioningEnabled + `'`
	}
	return `(` + at(">") + ` OR ` + at(">=") + `)`
}

// RecordOverflowVersioning records that overflow target name's
// versioning read versioning at now. It returns the change when that
// differs from the target's row, which then takes it; nil otherwise.
// ErrNotFound if there is no such target.
func (db *DB) RecordOverflowVersioning(ctx context.Context, name, versioning string, now time.Time) (*VersioningChange, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ch, err := recordVersioning(ctx, tx, name, versioning, now)
	if err != nil {
		return nil, err
	}
	return ch, tx.Commit()
}

// recordVersioning is RecordOverflowVersioning inside tx.
func recordVersioning(ctx context.Context, tx *sql.Tx, name, versioning string, now time.Time) (*VersioningChange, error) {
	var prev string
	err := tx.QueryRowContext(ctx, `UPDATE overflow SET versioning_checked_at = ? WHERE name = ?
		RETURNING versioning`, now.Unix(), name).Scan(&prev)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var ch *VersioningChange
	if prev != versioning {
		ch = &VersioningChange{Name: name, At: time.Unix(now.Unix(), 0), Previous: prev, Versioning: versioning}
		if _, err := tx.ExecContext(ctx, `UPDATE overflow SET versioning = ? WHERE name = ?`, versioning, name); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO overflow_versioning (name, at, previous, versioning) VALUES (?, ?, ?, ?)`,
			name, now.Unix(), prev, versioning); err != nil {
			return nil, err
		}
	}
	return ch, nil
}

// LastVersioningChanges returns each overflow target's latest change of
// versioning, by name; a target whose versioning never changed is
// absent.
func (db *DB) LastVersioningChanges(ctx context.Context) (map[string]VersioningChange, error) {
	rows, err := db.QueryContext(ctx, `SELECT c.name, c.at, c.previous, c.versioning FROM overflow_versioning c
		JOIN overflow o ON o.name = c.name
		WHERE c.id = (SELECT n.id FROM overflow_versioning n WHERE n.name = c.name ORDER BY n.at DESC, n.id DESC LIMIT 1)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]VersioningChange{}
	for rows.Next() {
		var c VersioningChange
		var at int64
		if err := rows.Scan(&c.Name, &at, &c.Previous, &c.Versioning); err != nil {
			return nil, err
		}
		c.At = time.Unix(at, 0)
		out[c.Name] = c
	}
	return out, rows.Err()
}

// MarkerDeleted reports whether overflow target place may hold key of
// bucket under a delete marker that is the user's delete (RET-3, #97):
// the target's last row of the key that holds or held it is a delete
// (PlaceDeleted: not a copy drained away), and, once acknowledged, it
// was sent while the target had versioning: not held as a client-side
// tomb (or refused, S3-5: it has a tomb row still) nor sent to a bucket
// without versioning.
func (db *DB) MarkerDeleted(ctx context.Context, place, bucket, key string) (bool, error) {
	name, ok := OverflowName(place)
	if !ok {
		return false, nil
	}
	var seq int64
	var op, st string
	var acked sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT seq, op, state, acked_at FROM placement WHERE place = ? AND bucket = ? AND key = ?
		AND state IN ('queued', 'acked', 'dropping', 'drained') ORDER BY seq DESC LIMIT 1`, place, bucket, key).Scan(&seq, &op, &st, &acked)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil || op != OpDelete || st != PlacementAcked || !acked.Valid {
		return op == OpDelete, err
	}
	var marker bool
	err = db.QueryRowContext(ctx, `SELECT NOT EXISTS (SELECT 1 FROM overflow_tomb WHERE place = ?3 AND seq = ?4)
		AND `+versionedAt("?1", "?2", "o.versioning")+` FROM overflow o WHERE o.name = ?1`,
		name, acked.Int64, place, seq).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return marker, err
}
