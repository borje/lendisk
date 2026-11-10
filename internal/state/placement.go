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
// and not counted in the place's use. Re-placement's dropping and
// drained (#26) are in replace.go.
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
	// ConditionBusy: the place asked this node to send again shortly
	// (503 SlowDown: the write fits, but not beside the writes in flight
	// there, or this node has too many requests in flight; LEND-2, #92).
	// It answered, so it is not failing: LastOK advances, as at a probe's
	// answer, and a run of failing answers ends.
	ConditionBusy = "busy"
)

// Causes of a refusal against the bucket (placement.cause): the node's
// own store could not supply the object as captured. CauseOversize is
// only that it is larger than the worker could send (one PUT before
// multipart replication, #23; 5 TiB since): RequeueOversize puts those
// rows back once it can.
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
	// DrainBack: a queued copy moving back from an overflow target to
	// this friend (#40), the lowest priority in its FIFO.
	DrainBack bool
}

// drainBack is true for placement row p while it is a queued drain-back
// copy: one that replaces a copy at an overflow target (REPL-13, #40).
// It comes last in its place's FIFO and holds no metadata barrier.
const drainBack = `(p.state = 'queued' AND coalesce(p.replaces, '') GLOB '` + OverflowPrefix + `*')`

// NextPlacement returns the head of place's FIFO: its queued or
// dropping row with the lowest sequence. A dropping row (re-placement,
// #26) is returned as a delete of its key. A queued row of a bucket
// with a pending seed at place is held (REPL-8, seed.go). A drain-back
// copy (drainBack: a copy moving back from an overflow target, #40) has
// the lowest priority: it comes after every other row, in sequence
// order among its kind. ok is false when the FIFO is empty.
func (db *DB) NextPlacement(ctx context.Context, place string) (p Placement, ok bool, err error) {
	head := func(state string) (Placement, bool, error) {
		var p Placement
		var created int64
		err := db.QueryRowContext(ctx, `SELECT p.seq, p.place, p.bucket, p.key, p.op, q.class, coalesce(q.size, 0),
			coalesce(q.sha256, ''), p.state, q.created_at, coalesce(q.rewritable, 0), `+drainBack+`
			FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ?1 AND p.state = ?2 AND (?2 != 'queued' OR p.bucket NOT IN
				(SELECT s.bucket FROM seed s WHERE s.place = ?1 AND s.done_at IS NULL))
			ORDER BY `+drainBack+`, p.seq LIMIT 1`, place, state).
			Scan(&p.Seq, &p.Place, &p.Bucket, &p.Key, &p.Op, &p.Class, &p.Size, &p.SHA256, &p.State, &created, &p.Rewritable,
				&p.DrainBack)
		if errors.Is(err, sql.ErrNoRows) {
			return p, false, nil
		}
		p.CreatedAt = time.Unix(created, 0)
		return p, err == nil, err
	}
	p, ok, err = head(PlacementQueued)
	if err != nil {
		return p, false, err
	}
	d, dok, err := head(PlacementDropping)
	if err != nil {
		return p, false, err
	}
	if dok && (!ok || p.DrainBack || d.Seq < p.Seq) {
		d.Op, d.Size = OpDelete, 0
		return d, true, nil
	}
	return p, ok, nil
}

// AckPlacement records that place acknowledged the row, with the hash it
// verified (empty for a delete). Acknowledging again is harmless. A
// dropping row's delete acknowledged leaves it drained (#26). A copy
// re-placement placed in place of another (placement.replaces) makes
// that other copy dropping once acknowledged, in the same transaction:
// the object keeps the copies it had, and the drained place no longer
// counts as holding it (REPL-13, REPL-11).
func (db *DB) AckPlacement(ctx context.Context, place string, seq int64, sha256 string, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := db.ackPlacement(ctx, tx, place, seq, sha256, now, false); err != nil {
		return err
	}
	return tx.Commit()
}

// ackPlacement is AckPlacement in tx. With onlyQueued it acknowledges
// the row only if it is still queued, in the statement that marks it
// (ConfirmSeeded, #95), and reports whether it did.
func (db *DB) ackPlacement(ctx context.Context, tx *sql.Tx, place string, seq int64, sha256 string, now time.Time, onlyQueued bool) (bool, error) {
	if !onlyQueued {
		r, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDrained+`', acked_at = ?
			WHERE place = ? AND seq = ? AND state = '`+PlacementDropping+`'`, now.Unix(), place, seq)
		if err != nil {
			return false, err
		}
		if n, err := r.RowsAffected(); err != nil {
			return false, err
		} else if n > 0 {
			return true, nil
		}
	}
	var bucket, key, op, state string
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT bucket, key, op, state, coalesce(size, 0) FROM placement
		WHERE place = ? AND seq = ?`, place, seq).Scan(&bucket, &key, &op, &state, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	guard := `state != '` + PlacementDrained + `'`
	if onlyQueued {
		guard = `state = '` + PlacementQueued + `'`
	}
	r, err := tx.ExecContext(ctx, `UPDATE placement SET state = 'acked', sha256 = ?1, acked_at = ?2
		WHERE place = ?3 AND seq = ?4 AND `+guard, nullStr(sha256), now.Unix(), place, seq)
	if err != nil {
		return false, err
	}
	if n, err := r.RowsAffected(); err != nil {
		return false, err
	} else if n == 0 && onlyQueued {
		return false, nil
	}
	if op == OpPut && state != PlacementAcked && state != PlacementDrained && size != 0 {
		if err := freeLatePut(ctx, tx, place, bucket, key, seq, size); err != nil {
			return false, err
		}
	}
	var replaces sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT replaces FROM placement WHERE place = ? AND seq = ? AND state = 'acked' AND op = 'put'`,
		place, seq).Scan(&replaces); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if replaces.Valid && replaces.String != place {
		keep, err := db.keepReplaced(ctx, tx, bucket, seq, replaces.String)
		if err != nil {
			return false, err
		}
		if !keep {
			if _, err := tx.ExecContext(ctx, markDropping, replaces.String, seq); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// keepReplaced reports whether the copy of row seq at replaced must stay
// although its replacement was acknowledged (#38 review): replaced is
// an intact place (in the pool, not removed, not gone past
// replace_after, its replica of the bucket not unverified), as it is
// when re-placement moved a copy rather than drained one, and without
// it the object would have fewer than `copies` acknowledged copies at
// such places. Re-placement then drops it when that is safe (decide).
func (db *DB) keepReplaced(ctx context.Context, tx *sql.Tx, bucket string, seq int64, replaced string) (bool, error) {
	ps, err := poolPlaces(ctx, tx)
	if err != nil {
		return false, err
	}
	unverified, err := unverifiedAt(ctx, tx, bucket)
	if err != nil {
		return false, err
	}
	now := db.now()
	intact := map[string]bool{}
	for _, p := range ps {
		if p.cond != ConditionRemoved && !p.gone(db, now) && !p.removing && !unverified[p.peer] {
			intact[p.peer] = true
		}
	}
	if !intact[replaced] {
		return false, nil // drained: it never counted
	}
	acked, err := column(ctx, tx, `SELECT place FROM placement WHERE seq = ? AND state = 'acked' AND place != ?`, seq, replaced)
	if err != nil {
		return false, err
	}
	n := 0
	for _, a := range acked {
		if intact[a] {
			n++
		}
	}
	var copies int
	if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
		bucket, DefaultCopies).Scan(&copies); err != nil {
		return false, err
	}
	return n < copies, nil
}

// freeLatePut handles a put of size bytes (row seq) acknowledged after a
// delete of its key was placed at place (it was in flight, #21): it
// landed, and the lender counts it, then its tomb, until the purge. The
// put keeps its size, and the first delete after it at the place frees
// it as well, as if it had been acknowledged before that delete was
// placed (insertPlacement sized the delete then); the place_freed
// triggers count it back until the place's next quota answer (#24,
// #85).
func freeLatePut(ctx context.Context, tx *sql.Tx, place, bucket, key string, seq, size int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE placement SET size = coalesce(size, 0) - ?1
		WHERE place = ?2 AND seq = (SELECT min(d.seq) FROM placement d
			WHERE d.place = ?2 AND d.bucket = ?3 AND d.key = ?4 AND d.op = 'delete' AND d.seq > ?5)`,
		size, place, bucket, key, seq)
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
		WHERE place = ? AND seq = ? AND state IN ('queued', 'dropping')`, now.Unix(), place, seq)
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
// PUT carries (CauseOversize), no larger than sendLimit and not replaced
// by a later write of the key, back into their places' FIFOs, and
// returns how many. Multipart replication (#23) calls it once at start,
// with sendLimit the largest object it sends. Rows refused before #61
// told the causes apart were given CauseOversize by migration 31, so the
// cause alone decides: a store refusal of a large object stays refused.
func (db *DB) RequeueOversize(ctx context.Context, sendLimit int64) (int64, error) {
	r, err := db.ExecContext(ctx, `UPDATE placement SET state = 'queued', detail = NULL, cause = NULL
		WHERE state = 'refused' AND cause = 'oversize'
		AND (SELECT q.size FROM queue q WHERE q.seq = placement.seq) <= ?
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = placement.bucket AND l.key = placement.key AND l.seq > placement.seq AND l.state = 'done')`,
		sendLimit)
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
// false when it holds none (never acknowledged, or deleted since, by a
// delete or by re-placement's, #26).
func (db *DB) AckedHash(ctx context.Context, place, bucket, key string) (string, bool, error) {
	var op string
	var h sql.NullString
	err := db.QueryRowContext(ctx, `SELECT CASE WHEN state = 'acked' THEN op ELSE 'delete' END, sha256 FROM placement
		WHERE place = ? AND bucket = ? AND key = ? AND state IN ('acked', 'drained') ORDER BY seq DESC LIMIT 1`,
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
		WHERE place = ? AND bucket = ? AND key = ? AND state IN ('acked', 'dropping', 'drained') AND op = 'put'
		ORDER BY seq DESC LIMIT 1`,
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
	// A copy re-placement deleted there (drained, #26) is in the lender's
	// tomb as it was acknowledged.
	var state string
	var drained sql.NullString
	err := db.QueryRowContext(ctx, `SELECT state, sha256 FROM placement
		WHERE place = ? AND bucket = ? AND key = ? AND seq < ? AND state IN ('acked', 'drained')
		ORDER BY seq DESC LIMIT 1`, place, bucket, key, seq).Scan(&state, &drained)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	case state == PlacementDrained:
		return drained.String, drained.String != "", nil
	}
	var h string
	err = db.QueryRowContext(ctx, `WITH d AS (SELECT max(seq) AS seq FROM placement
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
	Dropping    int64         // copies re-placement is deleting there (#26), also in its FIFO
}

// PlaceStats returns each place's FIFO size and lag at now; places with
// no placement rows are absent. A row re-placement queued counts its
// lag from then (replaced_at), not from its capture (#26).
func (db *DB) PlaceStats(ctx context.Context, now time.Time) (map[string]PlaceStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.place,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued' AND p.op = 'put'), 0),
		min(coalesce(p.replaced_at, q.created_at)) FILTER (WHERE p.state = 'queued'),
		count(*) FILTER (WHERE p.state = 'refused' AND (p.cause IS NULL OR p.cause = 'immutable') AND `+current+`),
		count(*) FILTER (WHERE p.state = 'dropping')
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
		if err := rows.Scan(&place, &s.Queued, &s.QueuedBytes, &oldest, &s.Refused, &s.Dropping); err != nil {
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
	// RefusedAt is when it last answered ConditionRemoved; zero if
	// never (#83).
	RefusedAt time.Time
	// FailingSince is when its current run of failing answers
	// (retrying, quota, removed) began, kept through changes among them
	// and through a wait on this node's store; zero when its condition
	// is ok or it never failed (#65). Away (unreachable or full for
	// reroute_after) is counted from it, not from Since.
	FailingSince time.Time
	// BusySince is its first busy answer since its last ok answer, kept
	// through failures and probes' answers between busy ones; zero when
	// it has not answered busy since (#93). A busy run is counted from
	// it, not from Since.
	BusySince time.Time
}

// SetPlaceCondition records how place answered at now. Since changes only
// when the condition does; ConditionOK and ConditionBusy also set
// LastOK, and ConditionRemoved RefusedAt. FailingSince starts with the first
// failing answer (retrying, quota, removed) after an ok, is kept while
// the answers keep failing whatever their kind and through
// ConditionStore (this node's failure, not the place's answer), and is
// cleared by ok (#65). BusySince starts with the first busy answer
// after an ok, and only ok clears it (#93).
func (db *DB) SetPlaceCondition(ctx context.Context, place, condition, detail string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO place (place, condition, detail, since, last_ok, refused_at, failing_since, busy_since)
		VALUES (?1, ?2, ?3, ?4, CASE WHEN ?2 IN ('ok', 'busy') THEN ?4 END, CASE WHEN ?2 = 'removed' THEN ?4 END,
			CASE WHEN ?2 IN ('retrying', 'quota', 'removed') THEN ?4 END, CASE WHEN ?2 = 'busy' THEN ?4 END)
		ON CONFLICT (place) DO UPDATE SET
			since = CASE WHEN place.condition = excluded.condition THEN place.since ELSE excluded.since END,
			condition = excluded.condition, detail = excluded.detail,
			last_ok = coalesce(excluded.last_ok, place.last_ok),
			refused_at = coalesce(excluded.refused_at, place.refused_at),
			busy_since = CASE WHEN excluded.condition != 'ok' THEN coalesce(place.busy_since, excluded.busy_since) END,
			failing_since = CASE
				WHEN excluded.condition IN ('retrying', 'quota', 'removed') THEN coalesce(`+carriedFailing+`, excluded.since)
				WHEN excluded.condition = 'store' THEN `+carriedFailing+`
			END`,
		place, condition, nullStr(detail), now.Unix())
	return err
}

// carriedFailing is, in an update of place, the start of the run of
// failing answers its current condition continues: NULL after ok.
const carriedFailing = `(CASE
	WHEN place.condition IN ('retrying', 'quota', 'removed') THEN coalesce(place.failing_since, place.since)
	WHEN place.condition = 'store' THEN place.failing_since END)`

// ProbeAnswered records that place answered the reachability probe at
// now (#65): LastOK advances, and a network failure or a refusal
// (retrying, removed) ends in ok. A place full by its last answer (507)
// stays full, and its run of failing answers goes on, since the probe
// stores nothing; one waiting on this node's store stays waiting. A
// place with no condition yet becomes ok.
func (db *DB) ProbeAnswered(ctx context.Context, place string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO place (place, condition, since, last_ok) VALUES (?1, 'ok', ?2, ?2)
		ON CONFLICT (place) DO UPDATE SET last_ok = ?2,
			since = CASE WHEN place.condition IN ('ok', 'quota', 'store') THEN place.since ELSE ?2 END,
			detail = CASE WHEN place.condition IN ('quota', 'store') THEN place.detail END,
			failing_since = CASE WHEN place.condition IN ('quota', 'store') THEN `+carriedFailing+` END,
			condition = CASE WHEN place.condition IN ('quota', 'store') THEN place.condition ELSE 'ok' END`,
		place, now.Unix())
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
	_, err := db.ExecContext(ctx, `UPDATE place SET condition = 'ok', detail = NULL, since = ?, failing_since = NULL
		WHERE place = ? AND condition = 'store'`, now.Unix(), place)
	return err
}

// PlaceConditions returns every place's last condition.
func (db *DB) PlaceConditions(ctx context.Context) (map[string]PlaceCondition, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, condition, coalesce(detail, ''), since, last_ok, refused_at,
		CASE WHEN condition IN ('retrying', 'quota', 'removed') THEN coalesce(failing_since, since) ELSE failing_since END,
		busy_since FROM place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PlaceCondition{}
	for rows.Next() {
		var place string
		var c PlaceCondition
		var since int64
		var lastOK, refused, failing, busy sql.NullInt64
		if err := rows.Scan(&place, &c.Condition, &c.Detail, &since, &lastOK, &refused, &failing, &busy); err != nil {
			return nil, err
		}
		c.Since = time.Unix(since, 0)
		if lastOK.Valid {
			c.LastOK = time.Unix(lastOK.Int64, 0)
		}
		if refused.Valid {
			c.RefusedAt = time.Unix(refused.Int64, 0)
		}
		if failing.Valid {
			c.FailingSince = time.Unix(failing.Int64, 0)
		}
		if busy.Valid {
			c.BusySince = time.Unix(busy.Int64, 0)
		}
		out[place] = c
	}
	return out, rows.Err()
}
