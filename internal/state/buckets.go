package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Bucket is one of the node's own buckets.
type Bucket struct {
	Name      string
	Profile   string
	Copies    int
	AccessKey string
	SecretKey string
	CreatedAt time.Time
	// Format1At is when the bucket first captured a key only an
	// unsupported old repository format writes, Format1Key that key
	// (COMPAT-1, #79); UpgradedAt is when it captured the key the
	// tool's upgrade out of that format writes. Zero when not.
	Format1At  time.Time
	Format1Key string
	UpgradedAt time.Time
}

// Format1 reports whether the bucket holds a repository in an
// unsupported old format (Kopia format 1): a format-1 key was captured
// and the upgrade's key was not (COMPAT-1).
func (b Bucket) Format1() bool { return !b.Format1At.IsZero() && b.UpgradedAt.IsZero() }

// MarkFormat1 records that bucket captured key, which only an
// unsupported old repository format writes, at now: once, and never
// after the upgrade's key (MarkUpgraded). It reports whether this call
// marked the bucket.
func (db *DB) MarkFormat1(ctx context.Context, bucket, key string, now time.Time) (bool, error) {
	res, err := db.ExecContext(ctx, `UPDATE buckets SET format1_at = ?, format1_key = ?
		WHERE name = ? AND format1_at IS NULL AND upgraded_at IS NULL`, now.Unix(), key, bucket)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// MarkUpgraded records that bucket captured the key its tool writes when
// it upgrades a repository out of the old format, at now, once: the
// bucket no longer holds a format-1 repository (COMPAT-1).
func (db *DB) MarkUpgraded(ctx context.Context, bucket string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE buckets SET upgraded_at = ? WHERE name = ? AND upgraded_at IS NULL`,
		now.Unix(), bucket)
	return err
}

// ErrNotFound means no row matched.
var ErrNotFound = errors.New("not found")

// ErrExists means a row with that key already exists.
var ErrExists = errors.New("already exists")

// AddBucket inserts a bucket row.
func (db *DB) AddBucket(ctx context.Context, b Bucket) error {
	if _, err := db.GetBucket(ctx, b.Name); err == nil {
		return ErrExists
	}
	_, err := db.ExecContext(ctx, `INSERT INTO buckets
		(name, profile, copies, access_key, secret_key, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		b.Name, b.Profile, b.Copies, b.AccessKey, b.SecretKey, b.CreatedAt.UTC().Format(time.RFC3339))
	return err
}

// DeleteBucket removes a bucket row.
func (db *DB) DeleteBucket(ctx context.Context, name string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	return err
}

const bucketCols = `name, profile, copies, access_key, secret_key, created_at, format1_at, format1_key, upgraded_at`

func scanBucket(row interface{ Scan(...any) error }) (Bucket, error) {
	var b Bucket
	var created string
	var format1At, upgradedAt sql.NullInt64
	var format1Key sql.NullString
	err := row.Scan(&b.Name, &b.Profile, &b.Copies, &b.AccessKey, &b.SecretKey, &created, &format1At, &format1Key, &upgradedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, err
	}
	b.CreatedAt, _ = time.Parse(time.RFC3339, created)
	if format1At.Valid {
		b.Format1At = time.Unix(format1At.Int64, 0)
	}
	if upgradedAt.Valid {
		b.UpgradedAt = time.Unix(upgradedAt.Int64, 0)
	}
	b.Format1Key = format1Key.String
	return b, nil
}

// GetBucket reads a bucket by name.
func (db *DB) GetBucket(ctx context.Context, name string) (Bucket, error) {
	return scanBucket(db.QueryRowContext(ctx, `SELECT `+bucketCols+` FROM buckets WHERE name = ?`, name))
}

// BucketByAccessKey reads the bucket a credential belongs to.
func (db *DB) BucketByAccessKey(ctx context.Context, access string) (Bucket, error) {
	return scanBucket(db.QueryRowContext(ctx, `SELECT `+bucketCols+` FROM buckets WHERE access_key = ?`, access))
}

// Buckets lists every bucket by name.
func (db *DB) Buckets(ctx context.Context) ([]Bucket, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+bucketCols+` FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		b, err := scanBucket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
