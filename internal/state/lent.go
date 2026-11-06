package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// LentUsage returns the bytes peer stores here (LEND-1) and when the
// count was last set from a listing (zero if never).
func (db *DB) LentUsage(ctx context.Context, peer string) (int64, time.Time, error) {
	var n int64
	var at sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT bytes, reconciled_at FROM lent_usage WHERE peer_key = ?`, peer).Scan(&n, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, time.Time{}, nil
	}
	if err != nil || !at.Valid {
		return n, time.Time{}, err
	}
	return n, time.Unix(at.Int64, 0), nil
}

// AddLentUsage moves peer's count by delta bytes, never below zero.
func (db *DB) AddLentUsage(ctx context.Context, peer string, delta int64) error {
	_, err := db.ExecContext(ctx, `INSERT INTO lent_usage (peer_key, bytes) VALUES (?, MAX(?, 0))
		ON CONFLICT (peer_key) DO UPDATE SET bytes = MAX(bytes + ?, 0)`, peer, delta, delta)
	return err
}

// SetLentUsage sets peer's count from a listing taken at at.
func (db *DB) SetLentUsage(ctx context.Context, peer string, bytes int64, at time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO lent_usage (peer_key, bytes, reconciled_at) VALUES (?, ?, ?)
		ON CONFLICT (peer_key) DO UPDATE SET bytes = excluded.bytes, reconciled_at = excluded.reconciled_at`, peer, bytes, at.Unix())
	return err
}

// LentUpload is a borrower's multipart upload in progress, with the hash
// it declared (LEND-4).
type LentUpload struct {
	UploadID, Peer, Sealed, Key string
	SHA256                      string // hex
	CreatedAt                   time.Time
	Counted                     int64 // part bytes counted in lent_usage
}

// AddLentUpload records an upload a borrower began.
func (db *DB) AddLentUpload(ctx context.Context, u LentUpload) error {
	_, err := db.ExecContext(ctx, `INSERT INTO lent_upload (upload_id, peer_key, sealed, key, sha256, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, u.UploadID, u.Peer, u.Sealed, u.Key, u.SHA256, u.CreatedAt.Unix())
	return err
}

// GetLentUpload reads an upload; ErrNotFound if it was never begun here
// or is finished.
func (db *DB) GetLentUpload(ctx context.Context, uploadID string) (LentUpload, error) {
	u := LentUpload{UploadID: uploadID}
	var created int64
	err := db.QueryRowContext(ctx, `SELECT peer_key, sealed, key, sha256, created_at, counted FROM lent_upload WHERE upload_id = ?`,
		uploadID).Scan(&u.Peer, &u.Sealed, &u.Key, &u.SHA256, &created, &u.Counted)
	if errors.Is(err, sql.ErrNoRows) {
		return LentUpload{}, ErrNotFound
	}
	u.CreatedAt = time.Unix(created, 0)
	return u, err
}

// SetLentUploadCounted records the part bytes counted for an upload.
func (db *DB) SetLentUploadCounted(ctx context.Context, uploadID string, n int64) error {
	_, err := db.ExecContext(ctx, `UPDATE lent_upload SET counted = ? WHERE upload_id = ?`, n, uploadID)
	return err
}

// DropLentUploadsBefore forgets peer's uploads begun before cutoff
// (abandoned, LEND-1).
func (db *DB) DropLentUploadsBefore(ctx context.Context, peer string, cutoff time.Time) error {
	_, err := db.ExecContext(ctx, `DELETE FROM lent_upload WHERE peer_key = ? AND created_at < ?`, peer, cutoff.Unix())
	return err
}

// DropLentUpload forgets a completed or aborted upload.
func (db *DB) DropLentUpload(ctx context.Context, uploadID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM lent_upload WHERE upload_id = ?`, uploadID)
	return err
}

// LentBucket is a bucket a borrower created on this node (PEER-8): the
// borrower's node ID and the bucket's sealed name (SEC-16).
type LentBucket struct {
	Peer      string
	Sealed    string
	CreatedAt time.Time
}

// AddLentBucket records peer's bucket sealed. A row already there is
// kept, with its creation time.
func (db *DB) AddLentBucket(ctx context.Context, peer, sealed string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO lent_bucket (peer_key, sealed, created_at) VALUES (?, ?, ?)
		ON CONFLICT DO NOTHING`, peer, sealed, now.Unix())
	return err
}

// HasLentBucket reports whether peer has a bucket called sealed here.
func (db *DB) HasLentBucket(ctx context.Context, peer, sealed string) (bool, error) {
	var ok bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM lent_bucket WHERE peer_key = ? AND sealed = ?)`,
		peer, sealed).Scan(&ok)
	return ok, err
}

// LentBuckets lists peer's buckets here, by sealed name.
func (db *DB) LentBuckets(ctx context.Context, peer string) ([]LentBucket, error) {
	rows, err := db.QueryContext(ctx, `SELECT sealed, created_at FROM lent_bucket WHERE peer_key = ? ORDER BY sealed`, peer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LentBucket
	for rows.Next() {
		b := LentBucket{Peer: peer}
		var created int64
		if err := rows.Scan(&b.Sealed, &created); err != nil {
			return nil, err
		}
		b.CreatedAt = time.Unix(created, 0)
		out = append(out, b)
	}
	return out, rows.Err()
}
