package state

import (
	"context"
	"database/sql"
	"time"
)

// This file is the rolling full check's state (VER-1a, #105): per
// replica, the pass that challenges every object it holds whole once a
// cycle, a daily slice at a time, in write order (queue seq). The
// cursor is the last object the pass decided, kept in replica_check, so
// a restart goes on where the pass stopped, and an object written since
// the pass began joins it where the cursor reaches it.

// FullCycle is one replica's rolling full check as it stands.
type FullCycle struct {
	ReplicaKey
	// Cycle is an overflow target's own cycle; 0 for a friend's replica,
	// whose cycle is the node's (verify.full_cycle).
	Cycle time.Duration
	// After is the cursor: the sequence of the last object the pass
	// decided, 0 before the first.
	After int64
	// StartedAt is when the pass began; zero while none is under way,
	// so the next slice starts one.
	StartedAt time.Time
	// Checked and Skipped count what the pass checked and skipped (no
	// local copy to compare with) so far.
	Checked, Skipped int64
}

// cyclePlaces is every place whose replicas have a rolling full check,
// as (peer_key, address, cycle seconds): friends, with NULL (the node's
// cycle), and overflow targets given one with `overflow set
// --full-cycle`.
const cyclePlaces = `(SELECT peer_key, address, NULL AS cycle FROM friendship
	UNION ALL SELECT '` + OverflowPrefix + `' || name, 'overflow', full_cycle FROM overflow WHERE full_cycle IS NOT NULL)`

// DueCycles returns the replicas due for their next slice at now: as
// DueReplicas for the daily challenge (held at a place with an address
// that acknowledged objects of the bucket, not marked unverified, no
// error within retry, and waiting while the replica's FIFO is busy until
// more than Overdue past due), each a slice every after the last
// (cycle_at), or at once when it never had one.
func (db *DB) DueCycles(ctx context.Context, now time.Time, every, retry time.Duration) ([]FullCycle, error) {
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name, coalesce(f.cycle, 0), coalesce(c.cycle_after, 0),
		c.cycle_started_at, coalesce(c.cycle_checked, 0), coalesce(c.cycle_skipped, 0)
		FROM `+cyclePlaces+` f CROSS JOIN buckets b
		LEFT JOIN replica_check c ON c.place = f.peer_key AND c.bucket = b.name
		WHERE coalesce(f.address, '') != '' AND c.unverified_at IS NULL
		AND (c.cycle_at IS NULL OR c.cycle_at <= ?1) AND (c.error_at IS NULL OR c.error_at <= ?2)
		AND EXISTS (SELECT 1 FROM placement p
			WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state = 'acked' AND p.op = 'put')
		AND (NOT EXISTS (SELECT 1 FROM placement p
				WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state IN ('queued', 'dropping'))
			OR c.cycle_at <= ?3
			OR c.cycle_at IS NULL AND `+heldSince+` <= ?4)
		ORDER BY b.name, f.peer_key`, now.Add(-every).Unix(), now.Add(-retry).Unix(),
		now.Add(-every-Overdue).Unix(), now.Add(-Overdue).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FullCycle
	for rows.Next() {
		var c FullCycle
		var cycle int64
		var started sql.NullInt64
		if err := rows.Scan(&c.Place, &c.Bucket, &cycle, &c.After, &started, &c.Checked, &c.Skipped); err != nil {
			return nil, err
		}
		c.Cycle, c.StartedAt = time.Duration(cycle)*time.Second, timeOf(started)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ChallengeableAfter returns how many objects a challenge may name at
// place in bucket after sequence after: what the pass has still to
// check.
func (db *DB) ChallengeableAfter(ctx context.Context, place, bucket string, after int64) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT count(*) `+challengeable+` AND q.seq > ?3`, place, bucket, after).Scan(&n)
	return n, err
}

// SaveCycle records c's pass as it stands. A slice that ended (passed,
// or skipped what it named) records sliced, from which the next is due
// a day later; one that got no answer passes the zero time, keeping the
// progress it made, and is due again with the error's retry.
func (db *DB) SaveCycle(ctx context.Context, c FullCycle, sliced time.Time) error {
	var at any
	if !sliced.IsZero() {
		at = sliced.Unix()
	}
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, cycle_after, cycle_started_at, cycle_at,
		cycle_checked, cycle_skipped) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
		ON CONFLICT (place, bucket) DO UPDATE SET cycle_after = ?3, cycle_started_at = ?4,
			cycle_at = coalesce(?5, cycle_at), cycle_checked = ?6, cycle_skipped = ?7`,
		c.Place, c.Bucket, c.After, c.StartedAt.Unix(), at, c.Checked, c.Skipped)
	return err
}

// CompleteCycle records that place's replica of bucket completed its
// pass at now, every object challenged whole with no failure: its
// cycle_full_at. The next slice, a day later, starts a new pass.
func (db *DB) CompleteCycle(ctx context.Context, place, bucket string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, cycle_at, cycle_full_at) VALUES (?1, ?2, ?3, ?3)
		ON CONFLICT (place, bucket) DO UPDATE SET cycle_at = ?3, cycle_full_at = ?3,
			cycle_after = NULL, cycle_started_at = NULL, cycle_checked = NULL, cycle_skipped = NULL`,
		place, bucket, now.Unix())
	return err
}

// EndCycle ends place's replica of bucket's pass at now without
// recording it complete: it checked nothing, every object skipped. The
// next slice, a day later, starts a new pass.
func (db *DB) EndCycle(ctx context.Context, place, bucket string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO replica_check (place, bucket, cycle_at) VALUES (?1, ?2, ?3)
		ON CONFLICT (place, bucket) DO UPDATE SET cycle_at = ?3,
			cycle_after = NULL, cycle_started_at = NULL, cycle_checked = NULL, cycle_skipped = NULL`,
		place, bucket, now.Unix())
	return err
}
