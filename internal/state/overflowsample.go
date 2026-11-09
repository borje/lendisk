package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// The overflow verification sample (VER-2, S3-6, #39): an overflow
// target cannot answer a challenge, so a share of the bytes it holds is
// downloaded each week and hashed against the index. What a step owes
// and when it ran are kept per target (overflow_sample), so a restart
// neither forgives nor repeats a share.

// SampleObject is an object of the node's that an overflow target
// acknowledged and that is its key's latest write: the bytes the index
// says it holds.
type SampleObject struct {
	Bucket string
	Challengeable
}

// sampleable selects place ?1's objects a sample may download.
const sampleable = `FROM placement p JOIN queue q ON q.seq = p.seq
	WHERE p.place = ?1 AND p.state = 'acked' AND p.op = 'put'
	AND q.state = 'done' AND q.immutable IS NULL AND ` + liveQ

// Sampleable returns how many objects a sample may download at place,
// and their bytes.
func (db *DB) Sampleable(ctx context.Context, place string) (objects, bytes int64, err error) {
	err = db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(q.size), 0) `+sampleable, place).Scan(&objects, &bytes)
	return objects, bytes, err
}

// SampleableAt returns the i-th object a sample may download at place,
// in write order; ErrNotFound past the last.
func (db *DB) SampleableAt(ctx context.Context, place string, i int64) (SampleObject, error) {
	var o SampleObject
	err := db.QueryRowContext(ctx, `SELECT q.bucket, q.seq, q.key, coalesce(q.size, 0), coalesce(q.sha256, '') `+sampleable+`
		ORDER BY q.seq LIMIT 1 OFFSET ?2`, place, i).Scan(&o.Bucket, &o.Seq, &o.Key, &o.Size, &o.SHA256)
	if errors.Is(err, sql.ErrNoRows) {
		return o, ErrNotFound
	}
	return o, err
}

// SampleStep returns when place's sample last stepped and what it owes
// (bytes; negative when paid ahead); ok is false before its first step.
func (db *DB) SampleStep(ctx context.Context, place string) (at time.Time, owed int64, ok bool, err error) {
	var s int64
	err = db.QueryRowContext(ctx, `SELECT step_at, owed FROM overflow_sample WHERE place = ?`, place).Scan(&s, &owed)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, 0, false, nil
	}
	return time.Unix(s, 0), owed, err == nil, err
}

// SetSampleStep records place's sample step at at, owing owed.
func (db *DB) SetSampleStep(ctx context.Context, place string, at time.Time, owed int64) error {
	_, err := db.ExecContext(ctx, `INSERT INTO overflow_sample (place, step_at, owed) VALUES (?1, ?2, ?3)
		ON CONFLICT (place) DO UPDATE SET step_at = ?2, owed = ?3`, place, at.Unix(), owed)
	return err
}
