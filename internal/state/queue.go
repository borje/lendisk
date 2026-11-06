package state

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Queue operations (REPL-1, REPL-2).
const (
	OpPut    = "put"
	OpDelete = "delete"
)

// Queue row states (EP-3).
const (
	StatePending = "pending" // the backend call is in flight
	StateDone    = "done"    // the backend call succeeded
)

// Capture is a write or delete about to be passed to the backend.
type Capture struct {
	Bucket, Key string
	Op          string // OpPut or OpDelete
	Class       string // the key's class by the bucket's profile (EP-7)
	// Token is the lendisk-capture metadata a PutObject carries, which
	// tells startup whether the object on disk is this write's.
	Token string
	// ETag and UploadID identify a CompleteMultipartUpload, and Parts
	// are the parts it names, in order.
	ETag, UploadID string
	Parts          []Part
}

// Entry is a queue row.
type Entry struct {
	Seq                         int64
	Bucket, Key, Op, Class      string
	State                       string
	Size                        int64
	SHA256, Token, ETag, Upload string
}

// Part is one part of a multipart object, hashed as it was written.
type Part struct {
	Number int32
	Size   int64
	SHA256 string
}

// QueueStat is a bucket's queue size for status.
type QueueStat struct {
	Entries int64 // done rows
	Bytes   int64 // bytes of done puts
	Pending int64 // rows whose backend call has not returned
}

// BeginCaptures writes pending rows, in one transaction, and returns
// their sequence numbers in order. Call it before the backend call.
func (db *DB) BeginCaptures(ctx context.Context, cs []Capture) ([]int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().Unix()
	seqs := make([]int64, 0, len(cs))
	for _, c := range cs {
		r, err := tx.ExecContext(ctx, `INSERT INTO queue
			(bucket, key, op, class, state, token, etag, upload_id, created_at)
			VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
			c.Bucket, c.Key, c.Op, c.Class, nullStr(c.Token), nullStr(c.ETag), nullStr(c.UploadID), now)
		if err != nil {
			return nil, err
		}
		seq, err := r.LastInsertId()
		if err != nil {
			return nil, err
		}
		for _, p := range c.Parts {
			if _, err := tx.ExecContext(ctx, `INSERT INTO queue_part (seq, number, size, sha256) VALUES (?, ?, ?, ?)`,
				seq, p.Number, p.Size, p.SHA256); err != nil {
				return nil, err
			}
		}
		seqs = append(seqs, seq)
	}
	return seqs, tx.Commit()
}

// FinishPut marks a pending put done with its size and hash, and for a
// multipart object forgets the upload's part hashes. If the
// key's latest done row is a put with the same hash, the row is dropped
// instead and dup is true: an identical re-upload, such as a client's
// retry after a crash, is not queued twice (EP-3, EP-11).
func (db *DB) FinishPut(ctx context.Context, seq, size int64, sha256 string) (dup bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var bucket, key string
	var upload sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT bucket, key, upload_id FROM queue WHERE seq = ? AND state = 'pending'`, seq).
		Scan(&bucket, &key, &upload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, err
	}
	var op string
	var prev sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT op, sha256 FROM queue
		WHERE bucket = ? AND key = ? AND seq < ? AND state = 'done' ORDER BY seq DESC LIMIT 1`,
		bucket, key, seq).Scan(&op, &prev)
	switch {
	case err == nil && op == OpPut && prev.String == sha256:
		dup = true
		_, err = tx.ExecContext(ctx, `DELETE FROM queue WHERE seq = ?`, seq)
	case err == nil || errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `UPDATE queue SET state = 'done', size = ?, sha256 = ?, token = NULL WHERE seq = ?`,
			size, sha256, seq)
	}
	if err != nil {
		return false, err
	}
	if upload.Valid {
		if _, err := tx.ExecContext(ctx, `DELETE FROM upload_part WHERE upload_id = ?`, upload.String); err != nil {
			return false, err
		}
	}
	return dup, tx.Commit()
}

// FinishDelete marks pending deletes done.
func (db *DB) FinishDelete(ctx context.Context, seqs ...int64) error {
	return db.eachSeq(ctx, `UPDATE queue SET state = 'done' WHERE seq = ? AND state = 'pending'`, seqs)
}

// DropCaptures removes pending rows whose backend call failed or never
// took effect.
func (db *DB) DropCaptures(ctx context.Context, seqs ...int64) error {
	return db.eachSeq(ctx, `DELETE FROM queue WHERE seq = ? AND state = 'pending'`, seqs)
}

func (db *DB) eachSeq(ctx context.Context, q string, seqs []int64) error {
	if len(seqs) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range seqs {
		if _, err := tx.ExecContext(ctx, q, s); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const entryCols = `seq, bucket, key, op, class, state, coalesce(size, 0), coalesce(sha256, ''),
	coalesce(token, ''), coalesce(etag, ''), coalesce(upload_id, '')`

func (db *DB) entries(ctx context.Context, where string, args ...any) ([]Entry, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+entryCols+` FROM queue WHERE `+where+` ORDER BY seq`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Seq, &e.Bucket, &e.Key, &e.Op, &e.Class, &e.State, &e.Size, &e.SHA256,
			&e.Token, &e.ETag, &e.Upload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// PendingCaptures lists the rows whose backend call has not returned, in
// sequence order. At startup these are left by a crash.
func (db *DB) PendingCaptures(ctx context.Context) ([]Entry, error) {
	return db.entries(ctx, `state = 'pending'`)
}

// QueueEntries lists a bucket's rows in sequence order.
func (db *DB) QueueEntries(ctx context.Context, bucket string) ([]Entry, error) {
	return db.entries(ctx, `bucket = ?`, bucket)
}

// EntryParts lists a multipart entry's parts in order.
func (db *DB) EntryParts(ctx context.Context, seq int64) ([]Part, error) {
	return db.parts(ctx, `SELECT number, size, sha256 FROM queue_part WHERE seq = ? ORDER BY number`, seq)
}

func (db *DB) parts(ctx context.Context, q string, arg any) ([]Part, error) {
	rows, err := db.QueryContext(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Part
	for rows.Next() {
		var p Part
		if err := rows.Scan(&p.Number, &p.Size, &p.SHA256); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SaveUploadPart records a part's hash once the backend has stored it; a
// part uploaded again replaces the earlier one, as in the store.
func (db *DB) SaveUploadPart(ctx context.Context, uploadID string, p Part) error {
	_, err := db.ExecContext(ctx, `INSERT INTO upload_part (upload_id, number, size, sha256) VALUES (?, ?, ?, ?)
		ON CONFLICT (upload_id, number) DO UPDATE SET size = excluded.size, sha256 = excluded.sha256`,
		uploadID, p.Number, p.Size, p.SHA256)
	return err
}

// UploadParts returns an open upload's part hashes by part number.
func (db *DB) UploadParts(ctx context.Context, uploadID string) (map[int32]Part, error) {
	ps, err := db.parts(ctx, `SELECT number, size, sha256 FROM upload_part WHERE upload_id = ?`, uploadID)
	if err != nil {
		return nil, err
	}
	m := make(map[int32]Part, len(ps))
	for _, p := range ps {
		m[p.Number] = p
	}
	return m, nil
}

// DropUpload forgets an aborted upload's part hashes.
func (db *DB) DropUpload(ctx context.Context, uploadID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM upload_part WHERE upload_id = ?`, uploadID)
	return err
}

// UploadCaptured reports whether a completed upload of key is already
// queued, so a client's retried CompleteMultipartUpload is not queued
// twice.
func (db *DB) UploadCaptured(ctx context.Context, bucket, key, uploadID string) (bool, error) {
	var ok bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM queue
		WHERE bucket = ? AND key = ? AND upload_id = ? AND state = 'done')`, bucket, key, uploadID).Scan(&ok)
	return ok, err
}

// QueueStats returns each bucket's queue size; buckets with no rows are
// absent.
func (db *DB) QueueStats(ctx context.Context) (map[string]QueueStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket,
		count(*) FILTER (WHERE state = 'done'),
		coalesce(sum(size) FILTER (WHERE state = 'done' AND op = 'put'), 0),
		count(*) FILTER (WHERE state = 'pending')
		FROM queue GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]QueueStat{}
	for rows.Next() {
		var b string
		var s QueueStat
		if err := rows.Scan(&b, &s.Entries, &s.Bytes, &s.Pending); err != nil {
			return nil, err
		}
		out[b] = s
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
