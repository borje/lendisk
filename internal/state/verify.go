package state

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"time"
)

// This file is verification's state (VER-1, VER-3, VER-4, #27): when
// each friend's replica of each bucket was last challenged and with what
// outcome, the objects a challenge may name, and the requests of
// `lendisk verify`. A replica marked unverified is drained by
// re-placement (replace.go) as a removed friend is, for that bucket
// alone, and gets no new data copies of it, until a full check passes.

// replicaPlaces is every place a replica can be at, as a table of
// (peer_key, address): friends, and overflow targets, whose address is
// never empty (VER-2, S3-6, #39).
const replicaPlaces = `(SELECT peer_key, address FROM friendship
	UNION ALL SELECT '` + OverflowPrefix + `' || name, 'overflow' FROM overflow)`

// ReplicaKey names one place's replica of one bucket: a friend's, or an
// overflow target's (#39).
type ReplicaKey struct {
	Place, Bucket string
}

// ReplicaCheck is a replica's verification record; zero times are
// never.
type ReplicaCheck struct {
	ReplicaKey
	CheckedAt    time.Time // the last answer, passed or failed
	VerifiedAt   time.Time // the last pass
	FullAt       time.Time // the last pass of a full check
	UnverifiedAt time.Time // the failure that marked it unverified; zero while it is not
	Detail       string    // what failed
	Error        string    // the last check that got no answer
	ErrorAt      time.Time
	// HeldSince is the first acknowledgement the friend still holds of
	// the bucket; zero when it holds none.
	HeldSince time.Time
	// AllCheckedAt is when every object the replica holds was last
	// checked whole without a failure: the rolling full check's last
	// completed pass (VER-1a, #105), or FullAt if later.
	AllCheckedAt time.Time
}

// LastPassOrHeld is when the replica last passed a challenge, or, if
// never, since when it has held acknowledged objects: what
// replica_unchecked counts from. Zero when it holds nothing and never
// passed.
func (c ReplicaCheck) LastPassOrHeld() time.Time {
	if !c.VerifiedAt.IsZero() {
		return c.VerifiedAt
	}
	return c.HeldSince
}

// Unverified reports whether the replica is marked unverified.
func (c ReplicaCheck) Unverified() bool { return !c.UnverifiedAt.IsZero() }

func timeOf(n sql.NullInt64) time.Time {
	if !n.Valid {
		return time.Time{}
	}
	return time.Unix(n.Int64, 0)
}

// RecordVerified records a passed check of place's replica of bucket at
// now: it is due again a day later. A full check also clears the
// unverified mark (VER-3: the user checked every object) and counts as
// a completed full cycle, the rolling pass starting over (VER-1a); a
// ranged one leaves it, and leaves the time it last passed as it was,
// so an unverified replica never looks freshly verified (#39 review).
func (db *DB) RecordVerified(ctx context.Context, place, bucket string, full bool, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, checked_at, verified_at, full_at, cycle_full_at)
		VALUES (?1, ?2, ?3, ?3, CASE WHEN ?4 THEN ?3 END, CASE WHEN ?4 THEN ?3 END)
		ON CONFLICT (place, bucket) DO UPDATE SET checked_at = ?3,
			verified_at = CASE WHEN ?4 OR unverified_at IS NULL THEN ?3 ELSE verified_at END,
			full_at = CASE WHEN ?4 THEN ?3 ELSE full_at END,
			cycle_full_at = CASE WHEN ?4 THEN ?3 ELSE cycle_full_at END,
			cycle_after = CASE WHEN ?4 THEN NULL ELSE cycle_after END,
			cycle_started_at = CASE WHEN ?4 THEN NULL ELSE cycle_started_at END,
			cycle_checked = CASE WHEN ?4 THEN NULL ELSE cycle_checked END,
			cycle_skipped = CASE WHEN ?4 THEN NULL ELSE cycle_skipped END,
			unverified_at = CASE WHEN ?4 THEN NULL ELSE unverified_at END,
			detail = CASE WHEN ?4 THEN NULL ELSE detail END,
			error = NULL, error_at = NULL`, place, bucket, now.Unix(), full)
	return err
}

// RecordUnverified records a failed challenge of place's replica of
// bucket at now (VER-1): the replica is marked unverified, which
// re-placement reads (PoolFingerprint changes), and the alert service
// raises KindReplicaUnverified. An earlier mark keeps its time. The
// rolling full check's pass ends unfinished (VER-1a): once a full check
// clears the mark, a new pass starts.
func (db *DB) RecordUnverified(ctx context.Context, place, bucket, detail string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, checked_at, unverified_at, detail)
		VALUES (?1, ?2, ?3, ?3, ?4)
		ON CONFLICT (place, bucket) DO UPDATE SET checked_at = ?3,
			unverified_at = coalesce(unverified_at, ?3), detail = ?4, error = NULL, error_at = NULL,
			cycle_after = NULL, cycle_started_at = NULL, cycle_checked = NULL, cycle_skipped = NULL`,
		place, bucket, now.Unix(), detail)
	return err
}

// RecordCheckError records a check of place's replica of bucket that
// got no answer at now (a network error, the friend busy or failing): no
// failure, and the replica is due again an hour later.
func (db *DB) RecordCheckError(ctx context.Context, place, bucket, msg string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, error, error_at) VALUES (?1, ?2, ?3, ?4)
		ON CONFLICT (place, bucket) DO UPDATE SET error = ?3, error_at = ?4`, place, bucket, msg, now.Unix())
	return err
}

// Overdue is how long past its daily challenge a replica waits for its
// FIFO to empty before it is challenged anyway.
const Overdue = 24 * time.Hour

// fifoBusy holds when f.peer_key has copies of bucket b.name still to
// send or delete (queued or dropping), read by the partial indexes of
// those rows, not every copy the place holds (#118).
const fifoBusy = `(EXISTS (SELECT 1 FROM placement p INDEXED BY placement_queued
		WHERE p.place = f.peer_key AND p.state = 'queued' AND p.bucket = b.name)
	OR EXISTS (SELECT 1 FROM placement p INDEXED BY placement_dropping
		WHERE p.place = f.peer_key AND p.state = 'dropping' AND p.bucket = b.name))`

// heldOverdue reports whether place's first acknowledgement of bucket
// still held, as the offsite fold stored it (heldOf, #119), is more
// than Overdue before now: when a replica never answered whose FIFO is
// busy is due anyway. db should be a Snapshot when the caller read the
// replicas in one.
func (db *DB) heldOverdue(ctx context.Context, place, bucket string, now time.Time) (bool, error) {
	h, err := db.heldOf(ctx, place, bucket, math.MaxInt64)
	if err != nil {
		return false, err
	}
	return h.at.Valid && h.at.Int64 <= now.Add(-Overdue).Unix(), nil
}

// ReplicaChecks returns every replica that a friend with an address
// holds acknowledged objects of, or that has a record, by bucket and
// then friend; a replica never checked has zero times. It reads in one
// read transaction unless db is already a Snapshot; HeldSince comes
// from what the fold stored (heldOf, #116).
func (db *DB) ReplicaChecks(ctx context.Context) ([]ReplicaCheck, error) {
	if db.snap == nil {
		view, done, err := db.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		defer done()
		return view.ReplicaChecks(ctx)
	}
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name, c.checked_at, c.verified_at, c.full_at,
		max(coalesce(c.cycle_full_at, 0), coalesce(c.full_at, 0)), c.unverified_at, coalesce(c.detail, ''), coalesce(c.error, ''), c.error_at
		FROM `+replicaPlaces+` f CROSS JOIN buckets b
		LEFT JOIN replica_check c ON c.place = f.peer_key AND c.bucket = b.name
		WHERE c.place IS NOT NULL OR coalesce(f.address, '') != '' AND EXISTS (SELECT 1 FROM placement p
			WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state = 'acked' AND p.op = 'put')
		ORDER BY b.name, f.peer_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicaCheck
	for rows.Next() {
		var c ReplicaCheck
		var checked, verified, full, all, unverified, errAt sql.NullInt64
		if err := rows.Scan(&c.Place, &c.Bucket, &checked, &verified, &full, &all, &unverified, &c.Detail, &c.Error, &errAt); err != nil {
			return nil, err
		}
		c.CheckedAt, c.VerifiedAt, c.FullAt = timeOf(checked), timeOf(verified), timeOf(full)
		c.UnverifiedAt, c.ErrorAt = timeOf(unverified), timeOf(errAt)
		if all.Int64 > 0 {
			c.AllCheckedAt = timeOf(all)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i, c := range out {
		h, err := db.heldOf(ctx, c.Place, c.Bucket, math.MaxInt64)
		if err != nil {
			return nil, err
		}
		out[i].HeldSince = timeOf(h.at)
	}
	return out, nil
}

// DueReplicas returns the replicas due for their daily challenge at now
// (VER-1): held at a friend with an address that acknowledged objects of
// the bucket, not marked unverified, last answered every or more ago
// (or never), and with no error within retry. A replica with copies of
// the bucket still to send or delete there waits until its FIFO is
// empty: a backup in progress is not slowed by its challenge, until it
// is more than Overdue past due (from its last answer, or, never
// answered, from its first acknowledgement), when it is challenged
// anyway, so a FIFO that never empties (a friend at quota, a slow
// uplink) does not starve it (#27 review). The FIFO is read by the
// partial indexes of queued and dropping rows, not every copy the place
// holds (#118). The first acknowledgement of a replica never answered
// whose FIFO is busy is what the offsite fold stored (heldOf, #119), as
// ReplicaChecks reads it; only such a replica needs it, and then the
// whole answer is read in one read transaction unless db is already a
// Snapshot.
func (db *DB) DueReplicas(ctx context.Context, now time.Time, every, retry time.Duration) ([]ReplicaKey, error) {
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name, c.checked_at IS NULL AND `+fifoBusy+`
		FROM `+replicaPlaces+` f CROSS JOIN buckets b
		LEFT JOIN replica_check c ON c.place = f.peer_key AND c.bucket = b.name
		WHERE coalesce(f.address, '') != '' AND c.unverified_at IS NULL
		AND (c.checked_at IS NULL OR c.checked_at <= ?1) AND (c.error_at IS NULL OR c.error_at <= ?2)
		AND EXISTS (SELECT 1 FROM placement p
			WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state = 'acked' AND p.op = 'put')
		AND (NOT `+fifoBusy+` OR c.checked_at <= ?3 OR c.checked_at IS NULL)
		ORDER BY b.name, f.peer_key`, now.Add(-every).Unix(), now.Add(-retry).Unix(),
		now.Add(-every-Overdue).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicaKey
	var busy []bool
	for rows.Next() {
		var k ReplicaKey
		var b bool
		if err := rows.Scan(&k.Place, &k.Bucket, &b); err != nil {
			return nil, err
		}
		out, busy = append(out, k), append(busy, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if !slices.Contains(busy, true) {
		return out, nil
	}
	if db.snap == nil {
		view, done, err := db.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		defer done()
		return view.DueReplicas(ctx, now, every, retry)
	}
	// Never answered, busy: due a day after its first acknowledgement.
	var due []ReplicaKey
	for i, k := range out {
		if busy[i] {
			ok, err := db.heldOverdue(ctx, k.Place, k.Bucket, now)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		due = append(due, k)
	}
	return due, nil
}

// Challengeable is an object a challenge may name at a place: the place
// acknowledged it, and it is its key's latest write, so the node's store
// holds the same bytes (Size of them, with SHA256 at capture).
type Challengeable struct {
	Seq    int64
	Key    string
	Size   int64
	SHA256 string
}

// challengeable selects the objects of place ?1 and bucket ?2 a challenge
// may name.
const challengeable = `FROM placement p JOIN queue q ON q.seq = p.seq
	WHERE p.place = ?1 AND p.bucket = ?2 AND p.state = 'acked' AND p.op = 'put'
	AND q.state = 'done' AND q.immutable IS NULL AND coalesce(q.size, 0) > 0 AND ` + liveQ

// StillChallengeable reports whether queue row seq is still an object a
// challenge may name at place: its row there acked, and the key's
// latest done write. A verifier reads it again before it records a
// failure, since a delete, a rewrite or re-placement's drop that reached
// the place mid-challenge explains a refusal (#27 review).
func (db *DB) StillChallengeable(ctx context.Context, place string, seq int64) (bool, error) {
	var ok bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ?1 AND p.seq = ?2 AND p.state = 'acked' AND p.op = 'put'
		AND q.state = 'done' AND q.immutable IS NULL AND `+liveQ+`)`, place, seq).Scan(&ok)
	return ok, err
}

// ChallengeableCount returns how many objects a challenge may name at
// place in bucket.
func (db *DB) ChallengeableCount(ctx context.Context, place, bucket string) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT count(*) `+challengeable, place, bucket).Scan(&n)
	return n, err
}

// ChallengeableAt returns the i-th object a challenge may name at place
// in bucket, in write order; ErrNotFound past the last.
func (db *DB) ChallengeableAt(ctx context.Context, place, bucket string, i int64) (Challengeable, error) {
	var c Challengeable
	err := db.QueryRowContext(ctx, `SELECT q.seq, q.key, q.size, coalesce(q.sha256, '') `+challengeable+`
		ORDER BY q.seq LIMIT 1 OFFSET ?3`, place, bucket, i).Scan(&c.Seq, &c.Key, &c.Size, &c.SHA256)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	return c, err
}

// ChallengeablePage returns up to limit objects a challenge may name at
// place in bucket after sequence after, in write order (a full check,
// VER-3).
func (db *DB) ChallengeablePage(ctx context.Context, place, bucket string, after int64, limit int) ([]Challengeable, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.seq, q.key, q.size, coalesce(q.sha256, '') `+challengeable+`
		AND q.seq > ?3 ORDER BY q.seq LIMIT ?4`, place, bucket, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Challengeable
	for rows.Next() {
		var c Challengeable
		if err := rows.Scan(&c.Seq, &c.Key, &c.Size, &c.SHA256); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// unverifiedAt returns the friends whose replica of bucket is marked
// unverified: no new data copies of the bucket go there, and
// re-placement moves those it holds.
func unverifiedAt(ctx context.Context, q querier, bucket string) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT place FROM replica_check WHERE bucket = ? AND unverified_at IS NOT NULL`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out[p] = true
	}
	return out, rows.Err()
}

// Outcomes of a verify request.
const (
	VerifyPassed  = "verified" // every object checked answered right
	VerifyFailed  = "failed"   // the replica is marked unverified
	VerifyError   = "error"    // no answer: nothing was decided
	VerifyNothing = "nothing"  // the friend holds nothing this node can check
)

// VerifyRequest is a check `lendisk verify` asked the daemon for.
type VerifyRequest struct {
	ID            int64
	Place, Bucket string
	Full          bool
	RequestedAt   time.Time
}

// VerifyOutcome is what a check found: how many objects it checked and
// how many it could not (no local copy to compare with), and what
// failed or went wrong.
type VerifyOutcome struct {
	Outcome          string
	Checked, Skipped int64
	Detail           string
}

// VerifyKept is how long a verify request is kept: requests older are
// dropped at the next one.
const VerifyKept = 7 * 24 * time.Hour

// RequestVerify records a request for the daemon to check place's
// replica of bucket (VER-3), and returns its ID. Requests older than
// VerifyKept, done or not, are dropped.
func (db *DB) RequestVerify(ctx context.Context, place, bucket string, full bool, now time.Time) (int64, error) {
	if _, err := db.ExecContext(ctx, `DELETE FROM verify_request WHERE requested_at < ?`, now.Add(-VerifyKept).Unix()); err != nil {
		return 0, err
	}
	r, err := db.ExecContext(ctx, `INSERT INTO verify_request (place, bucket, full, requested_at) VALUES (?, ?, ?, ?)`,
		place, bucket, full, now.Unix())
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// CancelVerify drops a request no daemon will run.
func (db *DB) CancelVerify(ctx context.Context, id int64) error {
	_, err := db.ExecContext(ctx, `DELETE FROM verify_request WHERE id = ? AND done_at IS NULL`, id)
	return err
}

// PendingVerifies returns the requests not yet done, oldest first.
func (db *DB) PendingVerifies(ctx context.Context) ([]VerifyRequest, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, place, bucket, full, requested_at FROM verify_request
		WHERE done_at IS NULL ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VerifyRequest
	for rows.Next() {
		var r VerifyRequest
		var at int64
		if err := rows.Scan(&r.ID, &r.Place, &r.Bucket, &r.Full, &at); err != nil {
			return nil, err
		}
		r.RequestedAt = time.Unix(at, 0)
		out = append(out, r)
	}
	return out, rows.Err()
}

// FinishVerify records the outcome of request id at now.
func (db *DB) FinishVerify(ctx context.Context, id int64, o VerifyOutcome, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE verify_request SET done_at = ?, outcome = ?, checked = ?, skipped = ?, detail = ?
		WHERE id = ?`, now.Unix(), o.Outcome, o.Checked, o.Skipped, nullStr(o.Detail), id)
	return err
}

// VerifyResult returns request id's outcome; done is false while the
// daemon has not finished it. ErrNotFound if there is no such request.
func (db *DB) VerifyResult(ctx context.Context, id int64) (o VerifyOutcome, done bool, err error) {
	var at sql.NullInt64
	var outcome, detail sql.NullString
	var checked, skipped sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT done_at, outcome, checked, skipped, detail FROM verify_request WHERE id = ?`, id).
		Scan(&at, &outcome, &checked, &skipped, &detail)
	if errors.Is(err, sql.ErrNoRows) {
		return o, false, ErrNotFound
	}
	if err != nil || !at.Valid {
		return o, false, err
	}
	return VerifyOutcome{Outcome: outcome.String, Checked: checked.Int64, Skipped: skipped.Int64, Detail: detail.String}, true, nil
}
