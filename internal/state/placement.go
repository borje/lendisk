package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Placement row states (D6). A queued row is an entry in its place's
// FIFO; acked keeps the hash the place verified (REPL-10); refused is a
// refusal that would repeat, out of the FIFO and shown in status: by
// the place (LEND-4, cause NULL; a 412 to a key that is not rewritable,
// EP-11, cause CauseImmutable, RefuseChanged), or because the node's own
// store cannot supply the object as captured (RefuseObject, #50; cause
// CauseStore or CauseOversize). An identical re-upload of the key heals
// a refusal, except an immutability refusal, which the place would
// repeat (#22): FinishPut queues it again except where the place
// acknowledged the very row it heals, and a refusal a later done write
// replaced no longer counts (#57, #61). dropped is a copy never sent:
// unsent at a friend gone past replace_after, or full for
// reroute_after while it held a metadata barrier, with no other friend
// with room (Reroute, #19); out of the FIFO, not counted in the place's
// use, and the object is below copies until re-placement (#26).
// abandoned is a taken put (StartPlacement) that will not be sent again
// and was never acknowledged: cancelled as superseded, or re-routed
// elsewhere. Its send may have landed, so it stays the place's row of
// the key and a later delete follows it (REPL-2, #21); out of the FIFO
// and not counted in the place's use.
const (
	PlacementQueued    = "queued"
	PlacementAcked     = "acked"
	PlacementRefused   = "refused"
	PlacementDropped   = "dropped"
	PlacementAbandoned = "abandoned"
)

// Place conditions: how a place answered last (#18).
const (
	ConditionOK       = "ok"
	ConditionRetrying = "retrying" // an error worth retrying with backoff
	ConditionQuota    = "quota"    // paused for quota (LEND-2)
	ConditionRemoved  = "removed"  // the friend refuses this node (PEER-5)
	// ConditionStore: the place waits because this node's own store
	// cannot be read now (a disk not mounted, EMFILE, EIO; #57). Not the
	// place's fault; the detail says what failed.
	ConditionStore = "store"
)

// Causes of a refusal against the bucket (placement.cause): the node's
// own store could not supply the object as captured. CauseOversize is
// only that it is larger than one PUT carries, which multipart
// replication (#23) lifts: RequeueOversize puts those rows back.
const (
	CauseStore    = "store"
	CauseOversize = "oversize"
)

// CauseImmutable is a place's 412 to a put of a key that is not
// rewritable: it holds the key with another hash (EP-11, LEND-4, #22).
// It is the place's refusal, counted in its refused rows, but reported
// as a changed key (ChangedKeys) rather than a place_refused one.
const CauseImmutable = "immutable"

// current keeps placement rows p whose queue row is the key's latest
// done write: a refusal a later write or delete of the key replaced
// (by healing it, #57) no longer counts.
const current = `NOT EXISTS (SELECT 1 FROM queue l
	WHERE l.bucket = p.bucket AND l.key = p.key AND l.seq > p.seq AND l.state = 'done')`

// Placement is a placement row joined with its queue row: what the
// worker sends to the place.
type Placement struct {
	Seq         int64
	Place       string
	Bucket, Key string
	Op          string // OpPut or OpDelete
	Class       string // the key's class (EP-7): metadata rows wait for the barrier (Held)
	Size        int64
	SHA256      string // the capture's whole-object SHA-256 (REPL-10)
	// Rewritable: the key is rewritable in its bucket's profile (EP-7),
	// so a changed version is sent as a rewrite of the one the place
	// holds (#74).
	Rewritable bool
	State      string
	CreatedAt  time.Time // when the write was captured
}

// NextPlacement returns the head of place's FIFO: its queued row with
// the lowest sequence. ok is false when the FIFO is empty.
func (db *DB) NextPlacement(ctx context.Context, place string) (p Placement, ok bool, err error) {
	var created int64
	err = db.QueryRowContext(ctx, `SELECT p.seq, p.place, p.bucket, p.key, p.op, q.class, coalesce(q.size, 0),
		coalesce(q.sha256, ''), p.state, q.created_at, coalesce(q.rewritable, 0)
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.state = 'queued' ORDER BY p.seq LIMIT 1`, place).
		Scan(&p.Seq, &p.Place, &p.Bucket, &p.Key, &p.Op, &p.Class, &p.Size, &p.SHA256, &p.State, &created, &p.Rewritable)
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
	// A put acknowledged after a delete of its key was placed there (it
	// was in flight) no longer counts: that delete removes it, and its
	// own count did not include it (#21).
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = 'acked', sha256 = ?1, acked_at = ?2,
		size = CASE WHEN op = 'put' AND state != 'acked' AND EXISTS (SELECT 1 FROM placement d
			WHERE d.place = placement.place AND d.bucket = placement.bucket AND d.key = placement.key
			AND d.op = 'delete' AND d.seq > placement.seq) THEN 0 ELSE size END
		WHERE place = ?3 AND seq = ?4`, nullStr(sha256), now.Unix(), place, seq)
	return err
}

// StartPlacement records that place's sender took row seq to send it,
// at now. ok is false when the row is no longer queued: a delete of the
// key cancelled it (placeTx), or it was moved or refused, and it must
// not be sent. A row taken once stays taken, as its send may have
// landed: a later delete of the key follows it instead of cancelling it
// (REPL-2, #21).
func (db *DB) StartPlacement(ctx context.Context, place string, seq int64, now time.Time) (ok bool, err error) {
	r, err := db.ExecContext(ctx, `UPDATE placement SET started_at = coalesce(started_at, ?)
		WHERE place = ? AND seq = ? AND state = 'queued'`, now.Unix(), place, seq)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// CancelPlacement takes a queued row that will not be sent out of the
// FIFO: removed if no sender ever took it, else abandoned, as a send of
// it may have landed and a later delete must still reach the place
// (#21 review).
func (db *DB) CancelPlacement(ctx context.Context, place string, seq int64) error {
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementAbandoned+`'
		WHERE place = ? AND seq = ? AND state = 'queued' AND started_at IS NOT NULL`, place, seq)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`, place, seq)
	return err
}

// RefusePlacement takes a queued row out of the FIFO as refused, with
// the reason.
func (db *DB) RefusePlacement(ctx context.Context, place string, seq int64, detail string) error {
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = 'refused', detail = ?
		WHERE place = ? AND seq = ? AND state = 'queued'`, detail, place, seq)
	return err
}

// RefuseChanged takes a queued put out of place's FIFO as refused
// because the place holds the key with another hash (a 412, EP-11,
// LEND-4, #22): held is that hash as the place reported it, empty if
// unknown. ChangedKeys reports it while the row is the key's latest
// write.
func (db *DB) RefuseChanged(ctx context.Context, place string, seq int64, held, detail string) error {
	_, err := db.ExecContext(ctx, `UPDATE placement SET state = 'refused', detail = ?, cause = ?, held = ?
		WHERE place = ? AND seq = ? AND state = 'queued'`, detail, CauseImmutable, held, place, seq)
	return err
}

// RefuseObject takes the queue row seq out of every place's FIFO as
// refused because the node's own store cannot supply the object as
// captured (#50), for cause (CauseStore or CauseOversize);
// RefusedObjects counts it against its bucket, and no place's refusals
// include it. Rows already acknowledged stay so. refused is false when
// no row was still queued: another place's sender refused it first, and
// only one alert is raised (#57).
func (db *DB) RefuseObject(ctx context.Context, seq int64, cause, detail string) (refused bool, err error) {
	r, err := db.ExecContext(ctx, `UPDATE placement SET state = 'refused', detail = ?, cause = ?
		WHERE seq = ? AND state = 'queued'`, detail, cause, seq)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n > 0, err
}

// RequeueOversize puts the rows refused only for being larger than one
// PUT carries, and not replaced by a later write of the key, back into
// their places' FIFOs, and returns how many. Multipart replication (#23)
// calls it once it can send them, with putLimit the most one PUT
// carried (replica.MaxPutSize). Those rows have CauseOversize, or, if
// refused before the cause was told apart (#61), CauseStore and a size
// above putLimit.
func (db *DB) RequeueOversize(ctx context.Context, putLimit int64) (int64, error) {
	r, err := db.ExecContext(ctx, `UPDATE placement SET state = 'queued', detail = NULL, cause = NULL
		WHERE state = 'refused'
		AND (cause = 'oversize' OR cause = 'store' AND op = 'put'
			AND (SELECT q.size FROM queue q WHERE q.seq = placement.seq) > ?)
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = placement.bucket AND l.key = placement.key AND l.seq > placement.seq AND l.state = 'done')`,
		putLimit)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// RefusedObjects returns, per bucket, how many objects were refused
// because the store could not supply them (any cause) and not written
// again since; buckets with none are absent.
func (db *DB) RefusedObjects(ctx context.Context) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, count(DISTINCT seq) FROM placement p
		WHERE state = 'refused' AND cause IN ('store', 'oversize') AND `+current+` GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var b string
		var n int64
		if err := rows.Scan(&b, &n); err != nil {
			return nil, err
		}
		out[b] = n
	}
	return out, rows.Err()
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

// AckedPutHash returns the hash of the last put place acknowledged for a
// key, even if a delete acknowledged since followed it: the version a
// lender keeps in its tomb (RET-1), which a rewritable key written again
// names as the version it replaces (#74). ok is false when the place
// never acknowledged a put of it.
func (db *DB) AckedPutHash(ctx context.Context, place, bucket, key string) (string, bool, error) {
	var h sql.NullString
	err := db.QueryRowContext(ctx, `SELECT sha256 FROM placement
		WHERE place = ? AND bucket = ? AND key = ? AND state = 'acked' AND op = 'put' ORDER BY seq DESC LIMIT 1`,
		place, bucket, key).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return h.String, err == nil && h.Valid, err
}

// ReplacedHash returns the version place keeps in a tomb of bucket's
// key, which a put of row seq must name to be stored over it (LEND-4,
// RET-1; #22 review): when the place's latest acknowledged row of the
// key before seq is a delete, the hash of the last put before that
// delete that the place acknowledged, or refused as holding other bytes
// (CauseImmutable) with the hash it reported. ok is false when there is
// no such delete or no such put. A key that is not rewritable deleted
// and written again is a new object to the borrower; the lender, which
// still holds the old version in its tomb, takes it only so marked.
func (db *DB) ReplacedHash(ctx context.Context, place, bucket, key string, seq int64) (string, bool, error) {
	var h string
	err := db.QueryRowContext(ctx, `WITH d AS (SELECT max(seq) AS seq FROM placement
			WHERE place = ?1 AND bucket = ?2 AND key = ?3 AND seq < ?4 AND state = 'acked' AND op = 'delete'
			AND seq = (SELECT max(x.seq) FROM placement x WHERE x.place = ?1 AND x.bucket = ?2 AND x.key = ?3
				AND x.seq < ?4 AND x.state = 'acked'))
		SELECT CASE WHEN p.state = 'acked' THEN p.sha256 ELSE p.held END FROM placement p, d
		WHERE p.place = ?1 AND p.bucket = ?2 AND p.key = ?3 AND p.seq < d.seq AND p.op = 'put'
		AND (p.state = 'acked' OR p.state = 'refused' AND p.cause = '`+CauseImmutable+`' AND coalesce(p.held, '') != '')
		ORDER BY p.seq DESC LIMIT 1`, place, bucket, key, seq).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return h, err == nil && h != "", err
}

// PlaceStat is a place's FIFO for status.
type PlaceStat struct {
	Queued      int64         // rows waiting
	QueuedBytes int64         // bytes of queued puts
	Lag         time.Duration // age of the oldest queued row; zero when none
	Oldest      time.Time     // capture time of the oldest queued row
	Refused     int64         // rows the place itself refused for good (not RefuseObject's), not written again since
}

// PlaceStats returns each place's FIFO size and lag at now; places with
// no placement rows are absent.
func (db *DB) PlaceStats(ctx context.Context, now time.Time) (map[string]PlaceStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.place,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued' AND p.op = 'put'), 0),
		min(q.created_at) FILTER (WHERE p.state = 'queued'),
		count(*) FILTER (WHERE p.state = 'refused' AND (p.cause IS NULL OR p.cause = 'immutable') AND `+current+`)
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

// ResetPlaceCondition sets place's condition back to ok at now when it
// is ConditionStore and its FIFO emptied: the rows it waited on went
// (refused for the store, #57), so this node's store no longer holds it
// up. LastOK is untouched: nothing was acknowledged. Every other
// condition is the place's own last answer (retrying, quota, removed)
// and stands until the place answers again (#61). A place with no
// condition yet gets none.
func (db *DB) ResetPlaceCondition(ctx context.Context, place string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE place SET condition = 'ok', detail = NULL, since = ?
		WHERE place = ? AND condition = 'store'`, now.Unix(), place)
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
