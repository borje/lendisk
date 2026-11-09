package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Seeding from a removable disk (REPL-8, #36). `lendisk export <bucket>
// --for <friend>` starts a seed: the friend's queued rows of the bucket
// are held from the network (NextPlacement skips them) while the disk
// travels, so nothing on it is sent twice. The daemon lists the friend's
// copy (control API list) and acknowledges each held put that the list
// shows with the captured hash (ConfirmSeeded); a seed ends when none of
// its puts is left (confirmed), or as SeedQuietAfter and SeedExpiry say, and
// the place's FIFO goes on from there.

// Seed outcomes (seed.outcome).
const (
	SeedConfirmed = "confirmed" // every put the export covered is acknowledged
	SeedQuiet     = "quiet"     // the import confirmed some, then nothing more for SeedQuietAfter
	SeedExpired   = "expired"   // nothing confirmed within SeedExpiry of the export
	SeedRemoved   = "removed"   // the friend is no longer a friend
)

// SeedQuietAfter is how long a seed that confirmed some objects waits for
// more before the rest go by the network: an import that ended or
// stopped part way.
const SeedQuietAfter = time.Hour

// SeedExpiry is how long a seed nothing has confirmed holds the
// friend's rows: a disk lost or never imported.
const SeedExpiry = 30 * 24 * time.Hour

// SeedObject is a key an export writes for a place: its latest done
// write, a put placed there (queued or acknowledged).
type SeedObject struct {
	Seq    int64
	Key    string
	Size   int64
	SHA256 string
}

// SeedObjects returns the keys of bucket placed at place, in key order:
// each key whose latest done write is a put with a queued or
// acknowledged placement row at place.
func (db *DB) SeedObjects(ctx context.Context, place, bucket string) ([]SeedObject, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.seq, q.key, coalesce(q.size, 0), coalesce(q.sha256, '')
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.bucket = ? AND p.op = 'put' AND p.state IN ('queued', 'acked') AND q.state = 'done'
		AND `+current+` ORDER BY q.key`, place, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SeedObject
	for rows.Next() {
		var o SeedObject
		if err := rows.Scan(&o.Seq, &o.Key, &o.Size, &o.SHA256); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Seed is a seed row.
type Seed struct {
	Place, Bucket string
	Through       int64 // the highest queue seq the export covered
	Objects       int64
	Bytes         int64
	ExportedAt    time.Time
	Confirmed     int64
	ProgressAt    time.Time // zero until a listing confirmed any
	CheckedAt     time.Time // zero until listed
	Failures      int64
	Error         string
	DoneAt        time.Time // zero while pending
	Outcome       string
}

// StartSeed records that an export of bucket for place is under way,
// covering rows up to through: from now on place's queued rows of the
// bucket are held. A seed of the same place and bucket is replaced.
func (db *DB) StartSeed(ctx context.Context, place, bucket string, through int64, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO seed (place, bucket, through, objects, bytes, exported_at)
		VALUES (?, ?, ?, 0, 0, ?)`, place, bucket, through, now.Unix())
	return err
}

// SeedWritten records what the export wrote, once it finished.
func (db *DB) SeedWritten(ctx context.Context, place, bucket string, objects, bytes int64, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE seed SET objects = ?, bytes = ?, exported_at = ? WHERE place = ? AND bucket = ?`,
		objects, bytes, now.Unix(), place, bucket)
	return err
}

// DropSeed removes a seed: an export that failed holds nothing.
func (db *DB) DropSeed(ctx context.Context, place, bucket string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM seed WHERE place = ? AND bucket = ?`, place, bucket)
	return err
}

const seedCols = `place, bucket, through, objects, bytes, exported_at, confirmed, progress_at, checked_at,
	failures, coalesce(error, ''), done_at, coalesce(outcome, '')`

func scanSeed(sc interface{ Scan(...any) error }) (Seed, error) {
	var s Seed
	var exported int64
	var progress, checked, done sql.NullInt64
	err := sc.Scan(&s.Place, &s.Bucket, &s.Through, &s.Objects, &s.Bytes, &exported, &s.Confirmed, &progress, &checked,
		&s.Failures, &s.Error, &done, &s.Outcome)
	s.ExportedAt = time.Unix(exported, 0)
	at := func(v sql.NullInt64) time.Time {
		if !v.Valid {
			return time.Time{}
		}
		return time.Unix(v.Int64, 0)
	}
	s.ProgressAt, s.CheckedAt, s.DoneAt = at(progress), at(checked), at(done)
	return s, err
}

// Seeds returns every seed, pending or done, by place and bucket.
func (db *DB) Seeds(ctx context.Context) ([]Seed, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+seedCols+` FROM seed ORDER BY place, bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Seed
	for rows.Next() {
		s, err := scanSeed(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSeed reads place's seed of bucket.
func (db *DB) GetSeed(ctx context.Context, place, bucket string) (Seed, error) {
	s, err := scanSeed(db.QueryRowContext(ctx, `SELECT `+seedCols+` FROM seed WHERE place = ? AND bucket = ?`, place, bucket))
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

// SeedListed is a live key a place's list shows, with its hash.
type SeedListed struct {
	Key, SHA256 string
}

// ConfirmSeeded acknowledges, at now, each of place's queued puts of
// bucket up to through whose key listed shows with the captured hash
// (REPL-10: the hash recorded is the one the place verified). It
// returns how many it acknowledged.
func (db *DB) ConfirmSeeded(ctx context.Context, place, bucket string, through int64, listed []SeedListed, now time.Time) (int64, error) {
	var n int64
	for _, l := range listed {
		rows, err := db.QueryContext(ctx, `SELECT p.seq FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ? AND p.bucket = ? AND p.key = ? AND p.op = 'put' AND p.state = 'queued'
			AND p.seq <= ? AND q.sha256 = ?`, place, bucket, l.Key, through, l.SHA256)
		if err != nil {
			return n, err
		}
		var seqs []int64
		for rows.Next() {
			var s int64
			if err := rows.Scan(&s); err != nil {
				rows.Close()
				return n, err
			}
			seqs = append(seqs, s)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return n, err
		}
		for _, s := range seqs {
			if err := db.AckPlacement(ctx, place, s, l.SHA256, now); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// SeedLeft counts place's queued puts of bucket up to through that are
// still their key's latest done write: what the seed has yet to
// confirm.
func (db *DB) SeedLeft(ctx context.Context, place, bucket string, through int64) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM placement p WHERE p.place = ? AND p.bucket = ?
		AND p.op = 'put' AND p.state = 'queued' AND p.seq <= ? AND `+current, place, bucket, through).Scan(&n)
	return n, err
}

// SeedChecked records a listing of the seed: confirmed more rows (and
// progress when it is more than none), or failed with msg.
func (db *DB) SeedChecked(ctx context.Context, place, bucket string, confirmed int64, msg string, now time.Time) error {
	if msg != "" {
		_, err := db.ExecContext(ctx, `UPDATE seed SET checked_at = ?, failures = failures + 1, error = ?
			WHERE place = ? AND bucket = ?`, now.Unix(), msg, place, bucket)
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE seed SET checked_at = ?1, failures = 0, error = NULL,
		confirmed = confirmed + ?2, progress_at = CASE WHEN ?2 > 0 THEN ?1 ELSE progress_at END
		WHERE place = ?3 AND bucket = ?4`, now.Unix(), confirmed, place, bucket)
	return err
}

// EndSeed ends a pending seed with outcome: place's FIFO goes on with
// what it holds of the bucket.
func (db *DB) EndSeed(ctx context.Context, place, bucket, outcome string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE seed SET done_at = ?, outcome = ? WHERE place = ? AND bucket = ? AND done_at IS NULL`,
		now.Unix(), outcome, place, bucket)
	return err
}
