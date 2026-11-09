package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Bit-rot sampling (VER-5, #31). The sampler sweeps the store, object by
// object, and hashes each against what the index holds: an own bucket's
// key against its latest done put (OwnVersion), a lent bucket's against
// the hash the borrower stored it with (its xattr, in the gateway).
// Sweep is its place, kept across restarts so a sweep covers every
// object; Rot is an object found rotten, which the alert service reads.

// RotShown is how long a repaired own object's record is kept, shown
// in status and alerted, after its repair.
const RotShown = 7 * 24 * time.Hour

// OwnVersion is what the index holds of an own bucket's key.
type OwnVersion struct {
	Seq    int64 // the latest done row; 0 if none
	Op     string
	SHA256 string
	Size   int64
	// Local is when this node last knew the version to be in its store
	// (queue.local); zero when it is not held locally.
	Local time.Time
	// Changed: EP-11 held the version back (another was acknowledged).
	Changed bool
	// Pending: a write of the key is in flight.
	Pending bool
}

// Held reports whether the key's latest done write is a put held
// locally: the store should hold exactly its bytes.
func (v OwnVersion) Held() bool { return v.Seq != 0 && v.Op == OpPut && !v.Local.IsZero() }

// OwnVersion reads what the index holds of bucket's key now.
func (db *DB) OwnVersion(ctx context.Context, bucket, key string) (OwnVersion, error) {
	var v OwnVersion
	var sha sql.NullString
	var size, local sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT q.seq, q.op, q.sha256, q.size, q.local,
			q.immutable IS NOT NULL OR EXISTS (SELECT 1 FROM placement p WHERE p.seq = q.seq AND p.state = 'refused'
				AND p.cause = '`+CauseImmutable+`')
		FROM queue q WHERE q.bucket = ? AND q.key = ? AND q.state = 'done' ORDER BY q.seq DESC LIMIT 1`, bucket, key).
		Scan(&v.Seq, &v.Op, &sha, &size, &local, &v.Changed)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return v, err
	}
	v.SHA256, v.Size = sha.String, size.Int64
	if local.Valid {
		v.Local = time.Unix(local.Int64, 0)
	}
	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM queue WHERE bucket = ? AND key = ? AND state = 'pending')`,
		bucket, key).Scan(&v.Pending)
	return v, err
}

// ClearLocal marks row seq not held locally: its object is about to
// leave the store without a captured delete (VER-5's re-fetch), so the
// store reconciler must not take its absence for one (EP-14).
func (db *DB) ClearLocal(ctx context.Context, seq int64) error {
	_, err := db.ExecContext(ctx, `UPDATE queue SET local = NULL WHERE seq = ?`, seq)
	return err
}

// StoredBytes is what the store holds, as the index counts it: the own
// objects held locally and what every friend stores here (lent_usage,
// tombs and overhead included).
func (db *DB) StoredBytes(ctx context.Context) (int64, error) {
	var own, lent int64
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(q.size), 0) FROM queue q
		WHERE q.state = 'done' AND q.op = 'put' AND q.local IS NOT NULL AND `+live).Scan(&own); err != nil {
		return 0, err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(sum(bytes), 0) FROM lent_usage`).Scan(&lent); err != nil {
		return 0, err
	}
	return own + lent, nil
}

// AllLentBuckets lists every friend's buckets here, by friend and sealed
// name.
func (db *DB) AllLentBuckets(ctx context.Context) ([]LentBucket, error) {
	rows, err := db.QueryContext(ctx, `SELECT peer_key, sealed, created_at FROM lent_bucket ORDER BY peer_key, sealed`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LentBucket
	for rows.Next() {
		var b LentBucket
		var created int64
		if err := rows.Scan(&b.Peer, &b.Sealed, &created); err != nil {
			return nil, err
		}
		b.CreatedAt = time.Unix(created, 0)
		out = append(out, b)
	}
	return out, rows.Err()
}

// Sweep is the sampler's place in its sweep of the store.
type Sweep struct {
	Bucket, Key string    // the last object checked
	CycleAt     time.Time // when the current cycle began
	Cycles      int64     // cycles finished
	// Objects and Bytes checked in the current cycle.
	Objects, Bytes int64
	CheckedAt      time.Time // the last check; zero before one
}

// Sweep reads the sampler's place; ok is false before its first check.
func (db *DB) Sweep(ctx context.Context) (Sweep, bool, error) {
	var s Sweep
	var cycleAt int64
	var checked sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT bucket, key, cycle_at, cycles, objects, bytes, checked_at FROM bitrot_sweep
		WHERE id = 1`).Scan(&s.Bucket, &s.Key, &cycleAt, &s.Cycles, &s.Objects, &s.Bytes, &checked)
	if errors.Is(err, sql.ErrNoRows) {
		return s, false, nil
	}
	if err != nil {
		return s, false, err
	}
	s.CycleAt = time.Unix(cycleAt, 0)
	if checked.Valid {
		s.CheckedAt = time.Unix(checked.Int64, 0)
	}
	return s, true, nil
}

// SaveSweep records the sampler's place.
func (db *DB) SaveSweep(ctx context.Context, s Sweep) error {
	_, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO bitrot_sweep (id, bucket, key, cycle_at, cycles, objects, bytes,
		checked_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?)`, s.Bucket, s.Key, s.CycleAt.Unix(), s.Cycles, s.Objects, s.Bytes,
		unixOrNull(s.CheckedAt))
	return err
}

// Rot is an object whose bytes no longer hash to what the index holds.
type Rot struct {
	Bucket, Key string // the store bucket: an own bucket or a lent one
	// Borrower and Sealed name a lent object's friend and bucket; ""
	// for an own one.
	Borrower, Sealed string
	Seq              int64 // an own object's queue row
	// LocalAt is the row's queue.local when found: a later one means the
	// version was stored again since (an identical re-upload).
	LocalAt time.Time
	SHA256  string // what the index holds
	Found   string // what the bytes hash to
	Size    int64
	FoundAt time.Time
	// An own object's repair: removed for its re-fetch, and stored again
	// verified (or written since); Detail says why it is not, yet.
	RemovedAt, RepairedAt time.Time
	Detail                string
}

// Lent reports whether the rotten object is a friend's.
func (r Rot) Lent() bool { return r.Borrower != "" }

// RecordRot writes r, replacing any row of its bucket and key.
func (db *DB) RecordRot(ctx context.Context, r Rot) error {
	_, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO bitrot (bucket, key, borrower, sealed, seq, local_at, sha256, found,
		size, found_at, removed_at, repaired_at, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Bucket, r.Key, nullStr(r.Borrower), nullStr(r.Sealed), r.Seq, unixOrNull(r.LocalAt), r.SHA256, r.Found, r.Size,
		r.FoundAt.Unix(),
		unixOrNull(r.RemovedAt), unixOrNull(r.RepairedAt), nullStr(r.Detail))
	return err
}

// DropRot forgets the rotten object at bucket's key.
func (db *DB) DropRot(ctx context.Context, bucket, key string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM bitrot WHERE bucket = ? AND key = ?`, bucket, key)
	return err
}

// Rots lists the rotten objects, by bucket and key.
func (db *DB) Rots(ctx context.Context) ([]Rot, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, key, coalesce(borrower, ''), coalesce(sealed, ''), coalesce(seq, 0),
		local_at, sha256, found, size, found_at, removed_at, repaired_at, coalesce(detail, '') FROM bitrot ORDER BY bucket, key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rot
	for rows.Next() {
		var r Rot
		var found int64
		var local, removed, repaired sql.NullInt64
		if err := rows.Scan(&r.Bucket, &r.Key, &r.Borrower, &r.Sealed, &r.Seq, &local, &r.SHA256, &r.Found, &r.Size, &found,
			&removed, &repaired, &r.Detail); err != nil {
			return nil, err
		}
		r.FoundAt = time.Unix(found, 0)
		if local.Valid {
			r.LocalAt = time.Unix(local.Int64, 0)
		}
		if removed.Valid {
			r.RemovedAt = time.Unix(removed.Int64, 0)
		}
		if repaired.Valid {
			r.RepairedAt = time.Unix(repaired.Int64, 0)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// unixOrNull is t in unix seconds, or NULL when zero.
func unixOrNull(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Unix()
}
