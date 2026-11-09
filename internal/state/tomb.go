package state

import (
	"context"
	"database/sql"
	"time"
)

// Tombstone is a borrower's deleted object kept for the retention window
// (RET-1): where its bytes are, what they were, and when they may go. A
// pending row's move into the tomb directory, or out of it, may not
// have happened; the gateway settles pending rows at startup.
type Tombstone struct {
	ID          int64
	Peer        string // the borrower's node ID
	Sealed, Key string
	Tomb        string // the bytes' path relative to the bucket directory
	SHA256      string // hex
	ETag        string
	Size        int64
	DeletedAt   time.Time
	PurgeAfter  time.Time
	Pending     bool
	// Rewrite: the version a borrower's rewrite replaced (LEND-4, #74),
	// not a deleted object. A pending one whose new version was never
	// published is moved back at startup.
	Rewrite bool
}

const tombCols = `id, peer_key, sealed, key, tomb, sha256, etag, size, deleted_at, purge_after, pending, coalesce(rewrite, 0)`

func scanTombs(rows *sql.Rows, err error) ([]Tombstone, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tombstone
	for rows.Next() {
		var t Tombstone
		var del, purge int64
		if err := rows.Scan(&t.ID, &t.Peer, &t.Sealed, &t.Key, &t.Tomb, &t.SHA256, &t.ETag, &t.Size, &del, &purge, &t.Pending, &t.Rewrite); err != nil {
			return nil, err
		}
		t.DeletedAt, t.PurgeAfter = time.Unix(del, 0), time.Unix(purge, 0)
		out = append(out, t)
	}
	return out, rows.Err()
}

// AddTombstone records t, pending, before its bytes are moved; it
// returns the row's ID.
func (db *DB) AddTombstone(ctx context.Context, t Tombstone) (int64, error) {
	res, err := db.ExecContext(ctx, `INSERT INTO tombstone (peer_key, sealed, key, tomb, sha256, etag, size, deleted_at, purge_after, rewrite)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, t.Peer, t.Sealed, t.Key, t.Tomb, t.SHA256, t.ETag, t.Size,
		t.DeletedAt.Unix(), ceilUnix(t.PurgeAfter), nullTrue(t.Rewrite))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) setTombPending(ctx context.Context, id int64, pending bool) error {
	_, err := db.ExecContext(ctx, `UPDATE tombstone SET pending = ? WHERE id = ?`, pending, id)
	return err
}

// KeepTombstone marks a tombstone's bytes as moved into its tomb.
func (db *DB) KeepTombstone(ctx context.Context, id int64) error {
	return db.setTombPending(ctx, id, false)
}

// PendTombstone marks a tombstone as about to be moved back (a revival).
func (db *DB) PendTombstone(ctx context.Context, id int64) error {
	return db.setTombPending(ctx, id, true)
}

// DropTombstone forgets a tombstone whose bytes are live again or were
// never moved; nothing is freed.
func (db *DB) DropTombstone(ctx context.Context, id int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM tombstone WHERE id = ?`, id)
	return err
}

// PurgeTombstone forgets a purged tombstone and takes freed bytes off
// peer's usage (LEND-1), in one transaction.
func (db *DB) PurgeTombstone(ctx context.Context, id int64, peer string, freed int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM tombstone WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `UPDATE lent_usage SET bytes = MAX(bytes - ?, 0) WHERE peer_key = ?`, freed, peer); err != nil {
		return err
	}
	return tx.Commit()
}

// TombstonesOf returns the tombstones of one key, oldest first.
func (db *DB) TombstonesOf(ctx context.Context, peer, sealed, key string) ([]Tombstone, error) {
	return scanTombs(db.QueryContext(ctx, `SELECT `+tombCols+` FROM tombstone
		WHERE peer_key = ? AND sealed = ? AND key = ? ORDER BY deleted_at, id`, peer, sealed, key))
}

// PendingTombstones returns every pending tombstone.
func (db *DB) PendingTombstones(ctx context.Context) ([]Tombstone, error) {
	return scanTombs(db.QueryContext(ctx, `SELECT `+tombCols+` FROM tombstone WHERE pending = 1 ORDER BY id`))
}

// DueTombstones returns up to limit kept tombstones whose purge time is
// at or before now, earliest first.
func (db *DB) DueTombstones(ctx context.Context, now time.Time, limit int) ([]Tombstone, error) {
	return scanTombs(db.QueryContext(ctx, `SELECT `+tombCols+` FROM tombstone
		WHERE pending = 0 AND purge_after <= ? ORDER BY purge_after, id LIMIT ?`, now.Unix(), limit))
}

// TombstoneUsage returns how many tombstones peer has here and their
// bytes.
func (db *DB) TombstoneUsage(ctx context.Context, peer string) (int64, int64, error) {
	var n, bytes int64
	err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM tombstone WHERE peer_key = ?`, peer).Scan(&n, &bytes)
	return n, bytes, err
}

// TombstonedKeys returns up to limit keys of peer's bucket sealed after
// the key after that have a tombstone, in key order, each with its
// newest tombstone (NET-7 list).
func (db *DB) TombstonedKeys(ctx context.Context, peer, sealed, after string, limit int) ([]Tombstone, error) {
	return scanTombs(db.QueryContext(ctx, `SELECT `+tombCols+` FROM tombstone t
		WHERE peer_key = ? AND sealed = ? AND key > ? AND id = (SELECT id FROM tombstone u
			WHERE u.peer_key = t.peer_key AND u.sealed = t.sealed AND u.key = t.key ORDER BY deleted_at DESC, id DESC LIMIT 1)
		ORDER BY key LIMIT ?`, peer, sealed, after, limit))
}

// ReplacedVersions returns, for each key of peer's bucket sealed after
// the key after and up to the key upto, the hashes of the versions
// rewrites replaced there and whose tombs are kept, newest first (#74).
func (db *DB) ReplacedVersions(ctx context.Context, peer, sealed, after, upto string) (map[string][]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT key, sha256 FROM tombstone
		WHERE peer_key = ? AND sealed = ? AND key > ? AND key <= ? AND rewrite = 1 AND pending = 0
		ORDER BY key, deleted_at DESC, id DESC`, peer, sealed, after, upto)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var k, h string
		if err := rows.Scan(&k, &h); err != nil {
			return nil, err
		}
		out[k] = append(out[k], h)
	}
	return out, rows.Err()
}

// ceilUnix is t in whole seconds, rounded up, so a stored purge time
// is never earlier than the one asked for (RET-2).
func ceilUnix(t time.Time) int64 {
	s := t.Unix()
	if t.Nanosecond() > 0 {
		s++
	}
	return s
}

// UndeleteKeys returns up to limit keys of peer's bucket sealed after the
// key after, in key order, that have a kept tombstone deleted at or after
// since: a delete's, or with rewrites a rewrite's too (RET-3, #32).
func (db *DB) UndeleteKeys(ctx context.Context, peer, sealed string, since time.Time, rewrites bool, after string, limit int) ([]string, error) {
	return column(ctx, db, `SELECT DISTINCT key FROM tombstone
		WHERE peer_key = ? AND sealed = ? AND key > ? AND pending = 0 AND deleted_at >= ? AND (? OR rewrite IS NULL OR rewrite = 0)
		ORDER BY key LIMIT ?`, peer, sealed, after, since.Unix(), rewrites, limit)
}
