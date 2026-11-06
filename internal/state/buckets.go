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

const bucketCols = `name, profile, copies, access_key, secret_key, created_at`

func scanBucket(row interface{ Scan(...any) error }) (Bucket, error) {
	var b Bucket
	var created string
	err := row.Scan(&b.Name, &b.Profile, &b.Copies, &b.AccessKey, &b.SecretKey, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return b, ErrNotFound
	}
	if err != nil {
		return b, err
	}
	b.CreatedAt, _ = time.Parse(time.RFC3339, created)
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
