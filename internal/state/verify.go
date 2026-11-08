package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// This file is verification's state (VER-1, VER-3, VER-4, #27): when
// each friend's replica of each bucket was last challenged and with what
// outcome, the objects a challenge may name, and the requests of
// `lendisk verify`. A replica marked unverified is drained by
// re-placement (replace.go) as a removed friend is, for that bucket
// alone, and gets no new data copies of it, until a full check passes.

// ReplicaKey names one friend's replica of one bucket.
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
// unverified mark (VER-3: the user checked every object); a ranged one
// leaves it.
func (db *DB) RecordVerified(ctx context.Context, place, bucket string, full bool, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, checked_at, verified_at, full_at)
		VALUES (?1, ?2, ?3, ?3, CASE WHEN ?4 THEN ?3 END)
		ON CONFLICT (place, bucket) DO UPDATE SET checked_at = ?3, verified_at = ?3,
			full_at = CASE WHEN ?4 THEN ?3 ELSE full_at END,
			unverified_at = CASE WHEN ?4 THEN NULL ELSE unverified_at END,
			detail = CASE WHEN ?4 THEN NULL ELSE detail END,
			error = NULL, error_at = NULL`, place, bucket, now.Unix(), full)
	return err
}

// RecordUnverified records a failed challenge of place's replica of
// bucket at now (VER-1): the replica is marked unverified, which
// re-placement reads (PoolFingerprint changes), and the alert service
// raises KindReplicaUnverified. An earlier mark keeps its time.
func (db *DB) RecordUnverified(ctx context.Context, place, bucket, detail string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, checked_at, unverified_at, detail)
		VALUES (?1, ?2, ?3, ?3, ?4)
		ON CONFLICT (place, bucket) DO UPDATE SET checked_at = ?3,
			unverified_at = coalesce(unverified_at, ?3), detail = ?4, error = NULL, error_at = NULL`,
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

// heldSince is the first acknowledgement still held at f.peer_key of
// bucket b.name (unix seconds), NULL when it holds none.
const heldSince = `(SELECT min(h.acked_at) FROM placement h
	WHERE h.place = f.peer_key AND h.bucket = b.name AND h.state = 'acked' AND h.op = 'put')`

// ReplicaChecks returns every replica that a friend with an address
// holds acknowledged objects of, or that has a record, by bucket and
// then friend; a replica never checked has zero times.
func (db *DB) ReplicaChecks(ctx context.Context) ([]ReplicaCheck, error) {
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name, c.checked_at, c.verified_at, c.full_at,
		c.unverified_at, coalesce(c.detail, ''), coalesce(c.error, ''), c.error_at, `+heldSince+`
		FROM friendship f CROSS JOIN buckets b
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
		var checked, verified, full, unverified, errAt, held sql.NullInt64
		if err := rows.Scan(&c.Place, &c.Bucket, &checked, &verified, &full, &unverified, &c.Detail, &c.Error, &errAt,
			&held); err != nil {
			return nil, err
		}
		c.CheckedAt, c.VerifiedAt, c.FullAt = timeOf(checked), timeOf(verified), timeOf(full)
		c.UnverifiedAt, c.ErrorAt, c.HeldSince = timeOf(unverified), timeOf(errAt), timeOf(held)
		out = append(out, c)
	}
	return out, rows.Err()
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
// uplink) does not starve it (#27 review).
func (db *DB) DueReplicas(ctx context.Context, now time.Time, every, retry time.Duration) ([]ReplicaKey, error) {
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name FROM friendship f CROSS JOIN buckets b
		LEFT JOIN replica_check c ON c.place = f.peer_key AND c.bucket = b.name
		WHERE coalesce(f.address, '') != '' AND c.unverified_at IS NULL
		AND (c.checked_at IS NULL OR c.checked_at <= ?1) AND (c.error_at IS NULL OR c.error_at <= ?2)
		AND EXISTS (SELECT 1 FROM placement p
			WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state = 'acked' AND p.op = 'put')
		AND (NOT EXISTS (SELECT 1 FROM placement p
				WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state IN ('queued', 'dropping'))
			OR c.checked_at <= ?3
			OR c.checked_at IS NULL AND `+heldSince+` <= ?4)
		ORDER BY b.name, f.peer_key`, now.Add(-every).Unix(), now.Add(-retry).Unix(),
		now.Add(-every-Overdue).Unix(), now.Add(-Overdue).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicaKey
	for rows.Next() {
		var k ReplicaKey
		if err := rows.Scan(&k.Place, &k.Bucket); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
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

// RequestVerify records a request for the daemon to check place's
// replica of bucket (VER-3), and returns its ID.
func (db *DB) RequestVerify(ctx context.Context, place, bucket string, full bool, now time.Time) (int64, error) {
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
