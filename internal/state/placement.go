package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Placement row states (D6). A queued row is an entry in its place's
// FIFO; acked keeps the hash the place verified (REPL-10); refused is a
// refusal the place would repeat (EP-11, LEND-4), out of the FIFO and
// shown in status.
const (
	PlacementQueued  = "queued"
	PlacementAcked   = "acked"
	PlacementRefused = "refused"
)

// Place conditions: how a place answered last (#18).
const (
	ConditionOK       = "ok"
	ConditionRetrying = "retrying" // an error worth retrying with backoff
	ConditionQuota    = "quota"    // paused for quota (LEND-2)
	ConditionRemoved  = "removed"  // the friend refuses this node (PEER-5)
)

// Placement is a placement row joined with its queue row: what the
// worker sends to the place.
type Placement struct {
	Seq         int64
	Place       string
	Bucket, Key string
	Op          string // OpPut or OpDelete
	Size        int64
	SHA256      string // the capture's whole-object SHA-256 (REPL-10)
	State       string
	CreatedAt   time.Time // when the write was captured
}

// placeTx places a completed queue row. Placement is trivial until
// pooled placement (#19): every friend gets every row.
func placeTx(ctx context.Context, tx *sql.Tx, seq int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state)
		SELECT q.seq, f.peer_key, q.bucket, q.key, q.op, 'queued'
		FROM queue q, friendship f
		WHERE q.seq = ? AND q.state = 'done' AND coalesce(f.address, '') != ''`, seq)
	return err
}

// NextPlacement returns the head of place's FIFO: its queued row with
// the lowest sequence. ok is false when the FIFO is empty.
func (db *DB) NextPlacement(ctx context.Context, place string) (p Placement, ok bool, err error) {
	var created int64
	err = db.QueryRowContext(ctx, `SELECT p.seq, p.place, p.bucket, p.key, p.op, coalesce(q.size, 0),
		coalesce(q.sha256, ''), p.state, q.created_at
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.state = 'queued' ORDER BY p.seq LIMIT 1`, place).
		Scan(&p.Seq, &p.Place, &p.Bucket, &p.Key, &p.Op, &p.Size, &p.SHA256, &p.State, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	p.CreatedAt = time.Unix(created, 0)
	return p, true, nil
}

// AckPlacement records that place acknowledged the row, with the hash it
// verified (empty for a delete). Acknowledging again is harmless.
func (db *DB) AckPlacement(ctx context.Context, place string, seq int64, sha256 string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = 'acked', sha256 = ?, acked_at = ?
		WHERE place = ? AND seq = ?`, nullStr(sha256), now.Unix(), place, seq)
	return err
}

// CancelPlacement removes a queued row that will not be sent.
func (db *DB) CancelPlacement(ctx context.Context, place string, seq int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`, place, seq)
	return err
}

// RefusePlacement takes a queued row out of the FIFO as refused, with
// the reason.
func (db *DB) RefusePlacement(ctx context.Context, place string, seq int64, detail string) error {
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = 'refused', detail = ?
		WHERE place = ? AND seq = ? AND state = 'queued'`, detail, place, seq)
	return err
}

// Superseded reports whether a later completed write or delete of the
// same key follows the queue row seq: the store no longer holds that
// row's bytes, so a put of it is cancelled, not sent.
func (db *DB) Superseded(ctx context.Context, seq int64) (bool, error) {
	var sup bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM queue q, queue l
		WHERE q.seq = ? AND l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`, seq).Scan(&sup)
	return sup, err
}

// LaterCapture reports whether any later capture of the same key
// exists, done or still in flight.
func (db *DB) LaterCapture(ctx context.Context, seq int64) (bool, error) {
	var later bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM queue q, queue l
		WHERE q.seq = ? AND l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq)`, seq).Scan(&later)
	return later, err
}

// AckedHash returns the hash place last acknowledged for a key; ok is
// false when it holds none (never acknowledged, or deleted since).
func (db *DB) AckedHash(ctx context.Context, place, bucket, key string) (string, bool, error) {
	var op string
	var h sql.NullString
	err := db.QueryRowContext(ctx, `SELECT op, sha256 FROM placement
		WHERE place = ? AND bucket = ? AND key = ? AND state = 'acked' ORDER BY seq DESC LIMIT 1`,
		place, bucket, key).Scan(&op, &h)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && op != OpPut) {
		return "", false, nil
	}
	return h.String, err == nil, err
}

// PlaceStat is a place's FIFO for status.
type PlaceStat struct {
	Queued      int64         // rows waiting
	QueuedBytes int64         // bytes of queued puts
	Lag         time.Duration // age of the oldest queued row; zero when none
	Oldest      time.Time     // capture time of the oldest queued row
	Refused     int64         // rows the place refused for good
}

// PlaceStats returns each place's FIFO size and lag at now; places with
// no placement rows are absent.
func (db *DB) PlaceStats(ctx context.Context, now time.Time) (map[string]PlaceStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.place,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued' AND p.op = 'put'), 0),
		min(q.created_at) FILTER (WHERE p.state = 'queued'),
		count(*) FILTER (WHERE p.state = 'refused')
		FROM placement p JOIN queue q ON q.seq = p.seq GROUP BY p.place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PlaceStat{}
	for rows.Next() {
		var place string
		var s PlaceStat
		var oldest sql.NullInt64
		if err := rows.Scan(&place, &s.Queued, &s.QueuedBytes, &oldest, &s.Refused); err != nil {
			return nil, err
		}
		if oldest.Valid {
			s.Oldest = time.Unix(oldest.Int64, 0)
			s.Lag = max(now.Sub(s.Oldest), 0)
		}
		out[place] = s
	}
	return out, rows.Err()
}

// PlaceCondition is how a place answered last.
type PlaceCondition struct {
	Condition string    // ConditionOK, …
	Detail    string    // the error, for anything but ok
	Since     time.Time // when the place entered this condition
	LastOK    time.Time // when it last acknowledged; zero if never
}

// SetPlaceCondition records how place answered at now. Since changes only
// when the condition does; ConditionOK also sets LastOK.
func (db *DB) SetPlaceCondition(ctx context.Context, place, condition, detail string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO place (place, condition, detail, since, last_ok)
		VALUES (?1, ?2, ?3, ?4, CASE WHEN ?2 = 'ok' THEN ?4 END)
		ON CONFLICT (place) DO UPDATE SET
			since = CASE WHEN place.condition = excluded.condition THEN place.since ELSE excluded.since END,
			condition = excluded.condition, detail = excluded.detail,
			last_ok = coalesce(excluded.last_ok, place.last_ok)`,
		place, condition, nullStr(detail), now.Unix())
	return err
}

// PlaceConditions returns every place's last condition.
func (db *DB) PlaceConditions(ctx context.Context) (map[string]PlaceCondition, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, condition, coalesce(detail, ''), since, last_ok FROM place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PlaceCondition{}
	for rows.Next() {
		var place string
		var c PlaceCondition
		var since int64
		var lastOK sql.NullInt64
		if err := rows.Scan(&place, &c.Condition, &c.Detail, &since, &lastOK); err != nil {
			return nil, err
		}
		c.Since = time.Unix(since, 0)
		if lastOK.Valid {
			c.LastOK = time.Unix(lastOK.Int64, 0)
		}
		out[place] = c
	}
	return out, rows.Err()
}
