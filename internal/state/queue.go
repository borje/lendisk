package state

import (
	"cmp"
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
	// Rewritable: the bucket's profile names the key rewritable (EP-7),
	// so a changed version is sent to each place as a rewrite (#74).
	Rewritable bool
	// Token is the lendisk-capture metadata a PutObject carries, which
	// tells startup whether the object on disk is this write's.
	Token string
	// ETag and UploadID identify a CompleteMultipartUpload.
	ETag, UploadID string
}

// Entry is a queue row.
type Entry struct {
	Seq                         int64
	Bucket, Key, Op, Class      string
	State                       string
	Size                        int64
	SHA256, Token, ETag, Upload string
	Rewritable                  bool // rewritable in the bucket's profile (EP-7)
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
	now := db.now().Unix()
	seqs := make([]int64, 0, len(cs))
	for _, c := range cs {
		r, err := tx.ExecContext(ctx, `INSERT INTO queue
			(bucket, key, op, class, state, token, etag, upload_id, created_at, rewritable)
			VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)`,
			c.Bucket, c.Key, c.Op, c.Class, nullStr(c.Token), nullStr(c.ETag), nullStr(c.UploadID), now, nullTrue(c.Rewritable))
		if err != nil {
			return nil, err
		}
		seq, err := r.LastInsertId()
		if err != nil {
			return nil, err
		}
		seqs = append(seqs, seq)
	}
	return seqs, tx.Commit()
}

// FinishPut marks a pending put done with its size and hash and places
// it (REPL-12), in one transaction. If the key's latest done row is a
// put with the same hash, the row is dropped instead and dup is true: an
// identical re-upload, such as a client's retry after a crash, is not
// queued twice (EP-3, EP-11). Unless that row was refused at some place
// for another reason than immutability (which the place would repeat):
// then the re-upload is queued and placed, which heals the refusal
// (#57), but acked at once where the place acknowledged the very row it
// heals (placeTx, #61).
//
// The immutability policy (EP-11, #22): a put of a key that is not
// rewritable (EP-7), whose bytes differ from what a place acknowledged
// for the key since the key's last delete, is done (the local write
// stands) but placed nowhere, and marked with the acknowledged hash
// (queue.immutable). ChangedKeys reports it while it is the key's latest
// write, and KeyChanged hears of it once committed. A write back to the
// acknowledged bytes after such a change heals the row the change
// replaced, so a place holding them is acked at once and nothing is sent
// again.
func (db *DB) FinishPut(ctx context.Context, seq, size int64, sha256 string) (dup bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var bucket, key string
	var rewritable bool
	var at int64
	if err := tx.QueryRowContext(ctx, `SELECT bucket, key, coalesce(rewritable, 0), created_at FROM queue
		WHERE seq = ? AND state = 'pending'`, seq).Scan(&bucket, &key, &rewritable, &at); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, err
	}
	var op string
	var prev sql.NullString
	var refused bool
	err = tx.QueryRowContext(ctx, `SELECT op, sha256,
		EXISTS (SELECT 1 FROM placement p WHERE p.seq = q.seq AND p.state = 'refused'
			AND p.cause IS NOT 'immutable')
		FROM queue q
		WHERE bucket = ? AND key = ? AND seq < ? AND state = 'done' ORDER BY seq DESC LIMIT 1`,
		bucket, key, seq).Scan(&op, &prev, &refused)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil && op == OpPut && prev.String == sha256 && !refused {
		if _, err := tx.ExecContext(ctx, `DELETE FROM queue WHERE seq = ?`, seq); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE queue SET state = 'done', size = ?, sha256 = ?, token = NULL WHERE seq = ?`,
		size, sha256, seq); err != nil {
		return false, err
	}
	var changed *ChangedKey
	if !rewritable {
		places, acked, err := ackedOther(ctx, tx, bucket, key, seq, sha256)
		if err != nil {
			return false, err
		}
		if len(places) > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE queue SET immutable = ? WHERE seq = ?`, acked, seq); err != nil {
				return false, err
			}
			changed = &ChangedKey{Bucket: bucket, Key: key, Seq: seq, CapturedAt: time.Unix(at, 0), SHA256: sha256,
				Acknowledged: acked, Places: places}
		}
	}
	if changed == nil {
		// A healing re-upload carries the bytes of the row it heals: the
		// key's latest done row that no change held back.
		var base sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT CASE WHEN op = 'put' THEN sha256 END FROM queue
			WHERE bucket = ? AND key = ? AND seq < ? AND state = 'done' AND immutable IS NULL
			ORDER BY seq DESC LIMIT 1`, bucket, key, seq).Scan(&base); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if err := db.placeTx(ctx, tx, seq, base.Valid && base.String == sha256); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	if changed != nil && db.KeyChanged != nil {
		db.KeyChanged(*changed)
	}
	return false, nil
}

// FinishDelete marks pending deletes done and places them, in one
// transaction.
func (db *DB) FinishDelete(ctx context.Context, seqs ...int64) error {
	if len(seqs) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range seqs {
		r, err := tx.ExecContext(ctx, `UPDATE queue SET state = 'done' WHERE seq = ? AND state = 'pending'`, s)
		if err != nil {
			return err
		}
		if n, err := r.RowsAffected(); err != nil {
			return err
		} else if n == 1 {
			if err := db.placeTx(ctx, tx, s, false); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// MarkUnmatchable records on pending row seq why it cannot be matched
// to the object in the store (#62): it may never settle, and it holds
// its bucket's metadata (Held), so status counts it as stuck and an
// alert names it (#20). The mark counts only while the row is pending.
func (db *DB) MarkUnmatchable(ctx context.Context, seq int64, detail string) error {
	_, err := db.ExecContext(ctx, `UPDATE queue SET unmatchable = ? WHERE seq = ? AND state = 'pending'`,
		cmp.Or(detail, "unmatchable"), seq)
	return err
}

// Unmatchable counts a bucket's pending rows marked unmatchable, with
// the first one's key and reason.
type Unmatchable struct {
	Objects     int64
	Key, Detail string
}

// UnmatchableRows returns, per bucket, the pending rows marked
// unmatchable; buckets with none are absent.
func (db *DB) UnmatchableRows(ctx context.Context) (map[string]Unmatchable, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, key, unmatchable FROM queue
		WHERE state = 'pending' AND unmatchable IS NOT NULL ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Unmatchable{}
	for rows.Next() {
		var b, k, d string
		if err := rows.Scan(&b, &k, &d); err != nil {
			return nil, err
		}
		u := out[b]
		if u.Objects == 0 {
			u.Key, u.Detail = k, d
		}
		u.Objects++
		out[b] = u
	}
	return out, rows.Err()
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
	coalesce(token, ''), coalesce(etag, ''), coalesce(upload_id, ''), coalesce(rewritable, 0)`

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
			&e.Token, &e.ETag, &e.Upload, &e.Rewritable); err != nil {
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

// PendingCapture returns row seq if it is still pending; ok is false if
// it is done or gone.
func (db *DB) PendingCapture(ctx context.Context, seq int64) (e Entry, ok bool, err error) {
	es, err := db.entries(ctx, `seq = ? AND state = 'pending'`, seq)
	if err != nil || len(es) == 0 {
		return Entry{}, false, err
	}
	return es[0], true, nil
}

// QueueEntries lists a bucket's rows in sequence order.
func (db *DB) QueueEntries(ctx context.Context, bucket string) ([]Entry, error) {
	return db.entries(ctx, `bucket = ?`, bucket)
}

// UploadEntry finds the queue row of a completed upload of key, so a
// client's retried CompleteMultipartUpload is not queued twice. ok is
// false if there is none.
func (db *DB) UploadEntry(ctx context.Context, bucket, key, uploadID string) (seq int64, state string, ok bool, err error) {
	err = db.QueryRowContext(ctx, `SELECT seq, state FROM queue
		WHERE bucket = ? AND key = ? AND upload_id = ? ORDER BY seq DESC LIMIT 1`, bucket, key, uploadID).Scan(&seq, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, nil
	}
	return seq, state, err == nil, err
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

// nullTrue is 1 for true and NULL for false: a flag column added later
// is NULL on older rows.
func nullTrue(b bool) any {
	if b {
		return 1
	}
	return nil
}

func nullStr(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
