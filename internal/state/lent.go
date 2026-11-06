package state

import (
	"context"
	"time"
)

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
