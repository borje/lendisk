package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// The store reconciler's state (EP-14, #8). The reconciler compares each
// own bucket's listing in the store with the index: an object the index
// does not have is enqueued as a write, and an indexed key whose object
// is gone is enqueued as a delete only if the key is held locally
// (queue.local): this node knew its object to be in its own store. A
// recovered node's store is empty by design (ID-5, ID-6): its recovered
// keys are not held until the read-through fetches them, so their
// absence is never a delete.

// NodeMarkerMissing is the node-table key holding the store marker's
// path while a pass found it missing, "" (or unset) otherwise.
const NodeMarkerMissing = "store_marker_missing"

// IndexedKey is a key whose latest done write is a put, as the
// reconciler compares it with the store.
type IndexedKey struct {
	Key   string
	Seq   int64 // the latest done row
	Local bool  // held locally: its object was in this node's store
}

// IndexedKeys returns up to limit keys of bucket whose latest done write
// is a put, sorting after after and, unless upto is "", at most upto,
// in key order (bytes, as S3 orders them). A key with only pending rows
// is not indexed.
func (db *DB) IndexedKeys(ctx context.Context, bucket, after, upto string, limit int) ([]IndexedKey, error) {
	q := `SELECT q.key, q.seq, q.local IS NOT NULL FROM queue q
		WHERE q.bucket = ?1 AND q.key > ?2 AND (?3 = '' OR q.key <= ?3) AND q.state = 'done' AND q.op = 'put'
			AND NOT EXISTS (SELECT 1 FROM queue l WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		ORDER BY q.key LIMIT ?4`
	rows, err := db.QueryContext(ctx, q, bucket, after, upto, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IndexedKey
	for rows.Next() {
		var k IndexedKey
		if err := rows.Scan(&k.Key, &k.Seq, &k.Local); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// KeyNow is what the index holds of one key: its latest done row (Seq
// 0 if none) and whether any row is pending (a capture in flight, or
// one left for ResolveStranded or startup).
type KeyNow struct {
	Seq     int64
	Op      string
	Local   bool
	Pending bool
}

// Indexed reports whether the key's latest done write is a put.
func (k KeyNow) Indexed() bool { return k.Seq != 0 && k.Op == OpPut }

// KeyNow reads what the index holds of key now.
func (db *DB) KeyNow(ctx context.Context, bucket, key string) (KeyNow, error) {
	var k KeyNow
	err := db.QueryRowContext(ctx, `SELECT seq, op, local IS NOT NULL FROM queue
		WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, bucket, key).Scan(&k.Seq, &k.Op, &k.Local)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return k, err
	}
	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM queue WHERE bucket = ? AND key = ? AND state = 'pending')`,
		bucket, key).Scan(&k.Pending)
	return k, err
}

// MarkLocal marks key held locally if its latest done row is a put of
// sha256: the read-through has just stored that version.
func (db *DB) MarkLocal(ctx context.Context, bucket, key, sha256 string) error {
	_, err := db.ExecContext(ctx, `UPDATE queue SET local = ? WHERE seq = (SELECT seq FROM queue
		WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1) AND op = 'put' AND sha256 = ?`,
		db.now().Unix(), bucket, key, sha256)
	return err
}

// MarkLocalSeqs marks done put rows held locally: a reconcile pass
// listed their objects in the store.
func (db *DB) MarkLocalSeqs(ctx context.Context, seqs ...int64) error {
	now := db.now().Unix()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range seqs {
		if _, err := tx.ExecContext(ctx, `UPDATE queue SET local = ? WHERE seq = ? AND state = 'done' AND op = 'put'
			AND local IS NULL`, now, s); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ReconcileState is a bucket's reconciler record (EP-14).
type ReconcileState struct {
	// Writes and Deletes count the done rows the reconciler wrote.
	Writes, Deletes int64
	// Held deletes the last pass held back for the user's confirmation,
	// of HeldOf objects held locally, since HeldAt; zero when none.
	Held, HeldOf int64
	HeldAt       time.Time
	// Allowed is how many deletes the user allowed the next pass.
	Allowed  int64
	PassedAt time.Time // the last finished pass; zero before one
}

// Errors of AllowDeletes.
var (
	ErrNothingHeld = errors.New("no deletes are held back")
	ErrAllowTooFew = errors.New("fewer than the deletes held back")
)

// ReconcileStates returns every bucket's reconciler record that has one.
func (db *DB) ReconcileStates(ctx context.Context) (map[string]ReconcileState, error) {
	out := map[string]ReconcileState{}
	rows, err := db.QueryContext(ctx, `SELECT bucket, count(*) FILTER (WHERE op = 'put'), count(*) FILTER (WHERE op = 'delete')
		FROM queue WHERE reconciled = 1 AND state = 'done' GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b string
		var r ReconcileState
		if err := rows.Scan(&b, &r.Writes, &r.Deletes); err != nil {
			return nil, err
		}
		out[b] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	rs, err := db.QueryContext(ctx, `SELECT bucket, passed_at, coalesce(held, 0), coalesce(held_of, 0), held_at,
		coalesce(allowed, 0) FROM reconcile`)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	for rs.Next() {
		var b string
		var passed, heldAt sql.NullInt64
		r0 := ReconcileState{}
		if err := rs.Scan(&b, &passed, &r0.Held, &r0.HeldOf, &heldAt, &r0.Allowed); err != nil {
			return nil, err
		}
		r := out[b]
		r.Held, r.HeldOf, r.Allowed = r0.Held, r0.HeldOf, r0.Allowed
		if passed.Valid {
			r.PassedAt = time.Unix(passed.Int64, 0)
		}
		if heldAt.Valid {
			r.HeldAt = time.Unix(heldAt.Int64, 0)
		}
		out[b] = r
	}
	return out, rs.Err()
}

// ReconcileState returns bucket's reconciler record; zero if none.
func (db *DB) ReconcileState(ctx context.Context, bucket string) (ReconcileState, error) {
	st, err := db.ReconcileStates(ctx)
	return st[bucket], err
}

// FinishReconcile records a finished pass of bucket at at: held deletes
// held back of heldOf objects held locally (0 for none: the hold ends),
// and the allowance the pass read (0 for none), which it consumed: it is
// cleared unless the user has set another since.
func (db *DB) FinishReconcile(ctx context.Context, bucket string, at time.Time, held, heldOf, consumed int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO reconcile (bucket) VALUES (?) ON CONFLICT (bucket) DO NOTHING`, bucket); err != nil {
		return err
	}
	if held > 0 {
		_, err = tx.ExecContext(ctx, `UPDATE reconcile SET passed_at = ?1, held = ?2, held_of = ?3,
			held_at = CASE WHEN held IS NULL THEN ?1 ELSE held_at END WHERE bucket = ?4`, at.Unix(), held, heldOf, bucket)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE reconcile SET passed_at = ?, held = NULL, held_of = NULL, held_at = NULL
			WHERE bucket = ?`, at.Unix(), bucket)
	}
	if err != nil {
		return err
	}
	if consumed > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE reconcile SET allowed = NULL WHERE bucket = ? AND allowed = ?`,
			bucket, consumed); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AllowDeletes lets bucket's next pass enqueue up to n deletes of
// objects missing from the store, though more than the limit's share of
// what it holds: the user's confirmation. It refuses when no deletes
// are held back (ErrNothingHeld) or n is fewer than those held
// (ErrAllowTooFew), and returns how many are held.
func (db *DB) AllowDeletes(ctx context.Context, bucket string, n int64) (held int64, err error) {
	err = db.QueryRowContext(ctx, `SELECT coalesce(held, 0) FROM reconcile WHERE bucket = ?`, bucket).Scan(&held)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	switch {
	case held == 0:
		return 0, ErrNothingHeld
	case n < held:
		return held, ErrAllowTooFew
	}
	_, err = db.ExecContext(ctx, `UPDATE reconcile SET allowed = ? WHERE bucket = ?`, n, bucket)
	return held, err
}

// AllowedBuckets lists the buckets whose next pass has an allowance.
func (db *DB) AllowedBuckets(ctx context.Context) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket FROM reconcile WHERE allowed IS NOT NULL ORDER BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
