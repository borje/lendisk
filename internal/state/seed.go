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
	SeedCancelled = "cancelled" // the user cancelled it (`lendisk export --cancel`, #95)
)

// SeedQuietAfter is how long a seed that confirmed some objects waits for
// more before the rest go by the network: an import that ended or
// stopped part way.
const SeedQuietAfter = time.Hour

// SeedExpiry is how long a seed nothing has confirmed holds the
// friend's rows: a disk lost or never imported.
const SeedExpiry = 30 * 24 * time.Hour

// SeedStaleAfter is how long a pending seed may confirm nothing before
// it is alerted (#95): a disk lost, or never imported, delays that
// friend's copies of the bucket until SeedExpiry. A week covers a disk
// sent by post or carried at the next weekly visit, and leaves three
// weeks of SeedExpiry to import it or cancel the seed; it is
// replication.replace_after's default too.
const SeedStaleAfter = 7 * 24 * time.Hour

// SeedObject is a key an export writes for a place: its latest done
// write, a put placed there (queued or acknowledged).
type SeedObject struct {
	Seq    int64
	Key    string
	Size   int64
	SHA256 string
	Class  string // data, metadata or snapshot (EP-7)
}

// SeedObjects returns the keys of bucket placed at place, in key order:
// each key whose latest done write is a put with a queued or
// acknowledged placement row at place.
func (db *DB) SeedObjects(ctx context.Context, place, bucket string) ([]SeedObject, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.seq, q.key, coalesce(q.size, 0), coalesce(q.sha256, ''), q.class
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
		if err := rows.Scan(&o.Seq, &o.Key, &o.Size, &o.SHA256, &o.Class); err != nil {
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
// bucket are held. A seed of the same place and bucket is replaced,
// with its skips.
func (db *DB) StartSeed(ctx context.Context, place, bucket string, through int64, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO seed (place, bucket, through, objects, bytes, exported_at)
		VALUES (?, ?, ?, 0, 0, ?)`, place, bucket, through, now.Unix()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM seed_skip WHERE place = ? AND bucket = ?`, place, bucket); err != nil {
		return err
	}
	return tx.Commit()
}

// SeedWritten records what the export wrote, once it finished, and the
// rows up to through it skipped (a directory marker, a key whose copy
// changed while it ran): the seed does not wait for the friend's list
// to show them (#95). They stay held while it is pending, and go by the
// network after, in order.
func (db *DB) SeedWritten(ctx context.Context, place, bucket string, objects, bytes int64, skipped []int64, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE seed SET objects = ?, bytes = ?, exported_at = ? WHERE place = ? AND bucket = ?`,
		objects, bytes, now.Unix(), place, bucket); err != nil {
		return err
	}
	for _, seq := range skipped {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO seed_skip (place, bucket, seq) VALUES (?, ?, ?)`,
			place, bucket, seq); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DropSeed removes a seed: an export that failed holds nothing.
func (db *DB) DropSeed(ctx context.Context, place, bucket string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM seed WHERE place = ? AND bucket = ?`, place, bucket); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM seed_skip WHERE place = ? AND bucket = ?`, place, bucket); err != nil {
		return err
	}
	return tx.Commit()
}

// CancelSeed ends place's pending seed of bucket as cancelled (#95):
// its rows go by the network from now. ErrNotFound if none is pending.
func (db *DB) CancelSeed(ctx context.Context, place, bucket string, now time.Time) error {
	r, err := db.ExecContext(ctx, `UPDATE seed SET done_at = ?, outcome = ? WHERE place = ? AND bucket = ? AND done_at IS NULL`,
		now.Unix(), SeedCancelled, place, bucket)
	if err != nil {
		return err
	}
	if n, err := r.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNotFound
	}
	return nil
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
// returns how many it acknowledged. The rows are found and acknowledged
// in one transaction, each only while it is still queued (#95): a row
// re-placement abandoned or dropped meanwhile is left as it is.
func (db *DB) ConfirmSeeded(ctx context.Context, place, bucket string, through int64, listed []SeedListed, now time.Time) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var n int64
	for _, l := range listed {
		seqs, err := seqColumn(ctx, tx, `SELECT p.seq FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ? AND p.bucket = ? AND p.key = ? AND p.op = 'put' AND p.state = 'queued'
			AND p.seq <= ? AND q.sha256 = ?`, place, bucket, l.Key, through, l.SHA256)
		if err != nil {
			return 0, err
		}
		if db.beforeSeedAck != nil && len(seqs) > 0 {
			db.beforeSeedAck(tx)
		}
		for _, s := range seqs {
			acked, err := db.ackPlacement(ctx, tx, place, s, l.SHA256, now, true)
			if err != nil {
				return 0, err
			}
			if acked {
				n++
			}
		}
	}
	return n, tx.Commit()
}

// seqColumn reads one integer column of q in tx.
func seqColumn(ctx context.Context, tx *sql.Tx, q string, args ...any) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SeedLeft counts place's queued puts of bucket up to through that are
// still their key's latest done write and that the export did not skip
// (SeedWritten): what the seed has yet to confirm.
func (db *DB) SeedLeft(ctx context.Context, place, bucket string, through int64) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM placement p WHERE p.place = ?1 AND p.bucket = ?2
		AND p.op = 'put' AND p.state = 'queued' AND p.seq <= ?3 AND `+current+`
		AND p.seq NOT IN (SELECT k.seq FROM seed_skip k WHERE k.place = ?1 AND k.bucket = ?2)`, place, bucket, through).Scan(&n)
	return n, err
}

// ErrOlderSeed is a seeding disk older than one already imported here
// from the same borrower's bucket (#95).
var ErrOlderSeed = errors.New("an export of this bucket newer than this disk was imported here")

// SeedImport records, on the lender, that an export of borrower's bucket
// sealed made at exported (the borrower's clock, as its manifest says)
// is being imported. It refuses, with ErrOlderSeed and the newest
// export imported, an export older than one imported before: an old
// disk would bring back keys the borrower has deleted since (#95). The
// same export again (an import run again to finish) and a newer one
// pass.
func (db *DB) SeedImport(ctx context.Context, borrower, sealed string, exported, now time.Time) (time.Time, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback()
	var newest int64
	err = tx.QueryRowContext(ctx, `SELECT exported_at FROM seed_import WHERE borrower = ? AND sealed = ?`,
		borrower, sealed).Scan(&newest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, err
	}
	if err == nil && exported.UnixNano() < newest {
		return time.Unix(0, newest), ErrOlderSeed
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO seed_import (borrower, sealed, exported_at, imported_at)
		VALUES (?, ?, ?, ?)`, borrower, sealed, exported.UnixNano(), now.Unix()); err != nil {
		return time.Time{}, err
	}
	return exported, tx.Commit()
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
