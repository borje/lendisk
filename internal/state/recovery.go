package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Recovery (ID-5, #29): placement and the hash index are rebuilt from
// each place's list of what it holds of each bucket (NET-7 list), one
// page at a time, and the lists still to read are kept in the relist
// table, so a place that is offline at recovery is listed when it
// returns, and a list cut short resumes after its last recorded key.

// Listed is one entry of a place's list of a bucket: a key it holds,
// with the size and hash it stored (REPL-10), the class the bucket's
// profile gives the key (EP-7), and whether it is tombstoned there.
type Listed struct {
	Key        string
	Size       int64
	SHA256     string
	Class      string
	Tombstoned bool
}

// ListedCount is what a page of a list added up to: keys held (and so
// recorded at the place), keys whose hash differs from the version this
// node already counts (not held), and tombstoned keys (not live).
type ListedCount struct {
	Held, Differ, Tombstoned int
}

// Add sums n into c.
func (c *ListedCount) Add(n ListedCount) {
	c.Held += n.Held
	c.Differ += n.Differ
	c.Tombstoned += n.Tombstoned
}

// Relist is a list still to read: place's copy of bucket.
type Relist struct {
	Place, Bucket string
	After         string    // the next page starts after this key
	TriedAt       time.Time // the last failed attempt; zero if none
	Error         string    // its failure
	Failures      int64     // failed attempts in a row
}

// AddRelists records that every place's copy of every bucket is to be
// listed. A list already pending keeps its progress and failures.
func (db *DB) AddRelists(ctx context.Context, places, buckets []string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range places {
		for _, b := range buckets {
			if _, err := tx.ExecContext(ctx, `INSERT INTO relist (place, bucket) VALUES (?, ?) ON CONFLICT DO NOTHING`, p, b); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// Relists returns the lists still to read, by place and bucket.
func (db *DB) Relists(ctx context.Context) ([]Relist, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, bucket, after, tried_at, coalesce(error, ''), failures
		FROM relist ORDER BY place, bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Relist
	for rows.Next() {
		var r Relist
		var tried sql.NullInt64
		if err := rows.Scan(&r.Place, &r.Bucket, &r.After, &tried, &r.Error, &r.Failures); err != nil {
			return nil, err
		}
		if tried.Valid {
			r.TriedAt = time.Unix(tried.Int64, 0)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RelistFailed records a failed attempt to list place's copy of bucket.
func (db *DB) RelistFailed(ctx context.Context, place, bucket, msg string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE relist SET tried_at = ?, error = ?, failures = failures + 1
		WHERE place = ? AND bucket = ?`, now.Unix(), msg, place, bucket)
	return err
}

// RecordListed records a page of place's list of bucket, in one
// transaction: each live key whose latest done row this node has is
// recorded as acknowledged at place if the hash matches; a key with no
// row gets a done put row, with the listed size and hash, acknowledged
// at place. Recording a key again changes nothing. The list's resume
// point moves to next, or, when last, the list is dropped from relist.
//
// The first list of a key decides the version the node counts; a place
// holding another hash holds a stale or different copy and is not
// counted, and nor is a place holding a key this node has deleted
// since.
func (db *DB) RecordListed(ctx context.Context, place, bucket string, objs []Listed, next string, last bool) (ListedCount, error) {
	var c ListedCount
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return c, err
	}
	defer tx.Rollback()
	now := db.now().Unix()
	for _, o := range objs {
		if o.Tombstoned {
			c.Tombstoned++
			continue
		}
		var seq int64
		var op string
		var hash sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT seq, op, sha256 FROM queue
			WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, bucket, o.Key).Scan(&seq, &op, &hash)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			r, err := tx.ExecContext(ctx, `INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
				VALUES (?, ?, 'put', ?, 'done', ?, ?, ?)`, bucket, o.Key, o.Class, o.Size, o.SHA256, now)
			if err != nil {
				return c, err
			}
			if seq, err = r.LastInsertId(); err != nil {
				return c, err
			}
		case err != nil:
			return c, err
		case op != OpPut || hash.String != o.SHA256:
			c.Differ++
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			VALUES (?, ?, ?, ?, 'put', 'acked', ?, ?, ?) ON CONFLICT (place, seq) DO NOTHING`,
			seq, place, bucket, o.Key, o.SHA256, now, o.Size+ObjectOverhead); err != nil {
			return c, err
		}
		c.Held++
	}
	if last {
		_, err = tx.ExecContext(ctx, `DELETE FROM relist WHERE place = ? AND bucket = ?`, place, bucket)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE relist SET after = ?, tried_at = NULL, error = NULL, failures = 0
			WHERE place = ? AND bucket = ?`, next, place, bucket)
	}
	if err != nil {
		return c, err
	}
	return c, tx.Commit()
}
