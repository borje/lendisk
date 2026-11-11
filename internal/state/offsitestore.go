package state

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// This file stores what the offsite watermark (REPL-11, offsite.go) is
// computed from, so a read no longer costs a look-up per put and place
// (#81: 7.9 s for a million objects on the development machine).
//
// The watermark is a function of a bucket's short puts (doneShorts and
// the pending ones), and a short put's row reads only the pool's
// holders, the bucket's copies, and queue and placement rows of its own
// key. So:
//   - offsite_short holds the rows doneShorts gave for every key as of
//     the pool and copies in offsite_basis;
//   - triggers on queue and placement (migration 53) mark the key of
//     every row inserted, updated or deleted in offsite_dirty, in the
//     writer's own transaction, whichever code writes it;
//   - a read (storedShorts) takes the stored rows of keys not marked and
//     computes the marked keys' puts afresh, so it equals the full
//     computation at every commit, folded or not;
//   - the daemon's fold (FoldOffsite) stores the marked keys' rows and
//     clears the marks it read.
//
// Nothing is advanced or lowered by hand: a rewrite acknowledged at one
// place lowers the watermark as surely as a refusal, and both are a
// marked key. The holders change with the clock (a friend gone past
// replace_after) as well as with writes, so they are compared, not
// marked: a read whose pool or copies differ from the basis computes
// every put, as before #81, until the next fold stores the bucket again
// for the pool as it is. Pending puts are always read: they are few,
// and never stored.

// storedShorts returns bucket's short puts from offsite_short, with the
// keys offsite_dirty marks computed afresh, and the pending puts; ok is
// false when the basis is missing or is not holders and copies (then
// nothing is returned). db must be a Snapshot, so the stored rows and
// marks are read as of one commit. latest is the bucket's latest done
// write.
//
// Of the stored rows it reads only those the watermark w can depend on:
// a put superseded at or below w (next <= w) rules out nothing at or
// above w. Every deleted or rewritten object leaves such a row, as many
// as there are live objects in a pruned repository, so they are not
// read: w is first found from the rows of keys' latest writes (next 0,
// none superseded, bound latest), and while it comes out below the
// bound the rows were read for, they are read again with w as the
// bound, until it does not. Each round reads what is short or
// superseded above w, the bucket's backlog.
func (db *DB) storedShorts(ctx context.Context, bucket string, copies int, holders string, latest int64) (out []short, ok bool, err error) {
	var bh string
	var bc int
	err = db.QueryRowContext(ctx, `SELECT holders, copies FROM offsite_basis WHERE bucket = ?`, bucket).Scan(&bh, &bc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil || bh != holders || bc != copies {
		return nil, false, err
	}
	changed, err := doneShorts(ctx, db, bucket, copies, holders, dirtyKeys, int64(math.MaxInt64))
	if err != nil {
		return nil, false, err
	}
	if changed, err = db.withPending(ctx, bucket, changed); err != nil {
		return nil, false, err
	}
	for bound := latest; ; {
		if out, err = db.storedRows(ctx, bucket, bound); err != nil {
			return nil, false, err
		}
		out = append(out, changed...)
		slices.SortFunc(out, func(a, b short) int { return cmp.Compare(a.seq, b.seq) })
		w := watermark(out, latest)
		if w >= bound {
			return out, true, nil
		}
		bound = w
	}
}

// storedRows reads bucket's stored rows whose key is not marked, and
// that are their key's latest done put (next 0) or superseded above
// bound.
func (db *DB) storedRows(ctx context.Context, bucket string, bound int64) ([]short, error) {
	rows, err := db.QueryContext(ctx, `SELECT s.seq, s.key, s.size, s.next, s.stuck, s.refused, s.dropped, s.gone, s.immutable
		FROM offsite_short s WHERE s.bucket = ?1 AND s.next = 0
		AND NOT EXISTS (SELECT 1 FROM offsite_dirty d WHERE d.bucket = s.bucket AND d.key = s.key)
		UNION ALL
		SELECT s.seq, s.key, s.size, s.next, s.stuck, s.refused, s.dropped, s.gone, s.immutable
		FROM offsite_short s WHERE s.bucket = ?1 AND s.next > ?2
		AND NOT EXISTS (SELECT 1 FROM offsite_dirty d WHERE d.bucket = s.bucket AND d.key = s.key)`, bucket, bound)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []short
	for rows.Next() {
		var s short
		if err := rows.Scan(&s.seq, &s.key, &s.size, &s.next, &s.stuck, &s.refused, &s.dropped, &s.gone, &s.immutable); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// dirtyKeys selects the bucket's (?1) keys offsite_dirty marks, with a
// mark no newer than ?4.
const dirtyKeys = `SELECT key FROM offsite_dirty WHERE bucket = ?1 AND id <= ?4`

// FoldOffsite stores, for every bucket, the short puts of the keys
// changed since the last fold, and clears their marks (#81); a bucket
// never stored, or whose pool or copies changed since, is stored in
// full. The daemon calls it every few seconds; a read is correct
// without it, only slower the more keys are marked. A bucket whose
// fold fails is skipped; the others are folded, and the errors joined.
//
// Each bucket is read in a read transaction (a full store takes as long
// as a full read, and must not hold the write lock), then written in
// write transactions of SettleBudget each, SettlePause apart, so the
// gateway's captures wait one batch at most (writeFold). Only the
// marks the read saw are cleared: a key changed after the read keeps a
// newer mark, so its rows stay unread until the next fold. Set the DB's
// Clock and ReplaceAfter as the daemon's.
func (db *DB) FoldOffsite(ctx context.Context) error {
	if db.snap != nil {
		return errSnapshot
	}
	buckets, err := keysOf(ctx, db, `SELECT name FROM buckets UNION SELECT bucket FROM offsite_dirty
		UNION SELECT bucket FROM offsite_basis ORDER BY 1`)
	if err != nil {
		return err
	}
	var errs []error
	for _, b := range buckets {
		if err := db.foldBucket(ctx, b); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, fmt.Errorf("bucket %s: %w", b, err))
		}
	}
	return errors.Join(errs...)
}

// fold is what foldBucket read: the rows to store, the marks they
// answer, and the basis.
type fold struct {
	mark    int64         // the newest mark read; 0 when none
	full    bool          // every key: the basis is new
	folds   sql.NullInt64 // the basis's count of folds claimed; NULL when none
	holders string
	copies  int
	rows    []short
	keys    []string // the keys marked, when not full
}

func (db *DB) foldBucket(ctx context.Context, bucket string) error {
	f, err := db.readFold(ctx, bucket)
	if err != nil {
		return err
	}
	return db.writeFold(ctx, bucket, f)
}

// errFoldStale stops a fold another fold claimed the bucket from.
var errFoldStale = errors.New("another fold claimed the bucket")

// writeFold stores what readFold read of bucket, in batches.
//
// The first transaction claims the bucket: it counts the fold in
// offsite_basis.folds, if no fold was claimed since the read (another
// fold's write may have cleared marks of changes this read did not
// see), and for a full fold it empties the basis's holders, which match
// no pool, so every read does the full read until the last batch. Each
// batch then checks the claim is still this fold's, and replaces the
// stored rows: a full fold all the bucket's, an incremental one the
// marked keys'. Between batches, a read ignores the stored rows of
// marked keys (their marks stay until the last batch) and, during a
// full fold, the stored rows altogether: what it reads is never more
// than the rows say. The last batch stores the basis and clears the
// marks the read saw. A fold cut short (an error, a crash, a newer
// claim) leaves the marks and, if full, the emptied basis: the next
// fold reads again.
func (db *DB) writeFold(ctx context.Context, bucket string, f fold) error {
	if !f.full && f.mark == 0 {
		return nil
	}
	var claim int64
	err := db.foldBatch(ctx, func(tx *sql.Tx) error {
		var folds sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT folds FROM offsite_basis WHERE bucket = ?)`,
			bucket).Scan(&folds); err != nil {
			return err
		}
		if folds != f.folds {
			return errFoldStale
		}
		claim = folds.Int64 + 1
		holders := f.holders
		if f.full {
			holders = "" // no pool's: until the last batch
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO offsite_basis (bucket, holders, copies, folds) VALUES (?, ?, ?, ?)
			ON CONFLICT (bucket) DO UPDATE SET holders = excluded.holders, copies = excluded.copies, folds = excluded.folds`,
			bucket, holders, f.copies, claim)
		return err
	})
	if errors.Is(err, errFoldStale) {
		return nil
	}
	if err != nil {
		return err
	}
	// The work, in order: each step does a little and says whether it
	// has more.
	keys, rows := f.keys, f.rows
	cleared := !f.full
	step := func(tx *sql.Tx) (bool, error) {
		switch {
		case !cleared:
			r, err := tx.ExecContext(ctx, `DELETE FROM offsite_short WHERE rowid IN
				(SELECT rowid FROM offsite_short WHERE bucket = ? LIMIT 500)`, bucket)
			if err != nil {
				return false, err
			}
			n, err := r.RowsAffected()
			cleared = err == nil && n == 0
			return true, err
		case len(keys) > 0:
			// The keys the read saw marked, by name: a key marked again
			// since has a newer mark, but its rows go too.
			_, err := tx.ExecContext(ctx, `DELETE FROM offsite_short WHERE bucket = ? AND key = ?`, bucket, keys[0])
			keys = keys[1:]
			return true, err
		case len(rows) > 0:
			s := rows[0]
			_, err := tx.ExecContext(ctx, `INSERT INTO offsite_short
				(bucket, key, seq, size, next, stuck, refused, dropped, gone, immutable) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				bucket, s.key, s.seq, s.size, s.next, s.stuck, s.refused, s.dropped, s.gone, s.immutable)
			rows = rows[1:]
			return true, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE offsite_basis SET holders = ? WHERE bucket = ?`, f.holders, bucket); err != nil {
			return false, err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM offsite_dirty WHERE bucket = ? AND id <= ?`, bucket, f.mark)
		return false, err
	}
	for more, first := true, true; more; first = false {
		if db.betweenFoldBatches != nil {
			db.betweenFoldBatches()
		}
		if !first {
			if err := pause(ctx); err != nil {
				return err
			}
		}
		err := db.foldBatch(ctx, func(tx *sql.Tx) error {
			var folds int64
			if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT folds FROM offsite_basis WHERE bucket = ?), 0)`,
				bucket).Scan(&folds); err != nil {
				return err
			}
			if folds != claim {
				return errFoldStale
			}
			start := time.Now()
			for more = true; more && time.Since(start) < SettleBudget; {
				var err error
				if more, err = step(tx); err != nil {
					return err
				}
			}
			return nil
		})
		if errors.Is(err, errFoldStale) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// foldBatch runs do in one write transaction.
func (db *DB) foldBatch(ctx context.Context, do func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if db.foldTook != nil {
		start := time.Now()
		defer func() { db.foldTook(time.Since(start)) }()
	}
	defer tx.Rollback()
	if err := do(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// readFold reads, as of one commit, what foldBucket stores.
func (db *DB) readFold(ctx context.Context, bucket string) (fold, error) {
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		return fold{}, err
	}
	defer done()
	f := fold{copies: DefaultCopies}
	var holders sql.NullString
	var copies sql.NullInt64
	if err := view.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?1), ?2),
		coalesce((SELECT max(id) FROM offsite_dirty WHERE bucket = ?1), 0),
		b.holders, b.copies, b.folds
		FROM (SELECT 1) LEFT JOIN offsite_basis b ON b.bucket = ?1`,
		bucket, DefaultCopies).Scan(&f.copies, &f.mark, &holders, &copies, &f.folds); err != nil {
		return f, err
	}
	if f.holders, err = view.holders(ctx); err != nil {
		return f, err
	}
	f.full = !holders.Valid || holders.String != f.holders || copies.Int64 != int64(f.copies)
	if f.full {
		f.rows, err = doneShorts(ctx, view, bucket, f.copies, f.holders, "")
		return f, err
	}
	if f.keys, err = keysOf(ctx, view, `SELECT key FROM offsite_dirty WHERE bucket = ? AND id <= ?`, bucket, f.mark); err != nil {
		return f, err
	}
	f.rows, err = doneShorts(ctx, view, bucket, f.copies, f.holders, dirtyKeys, f.mark)
	return f, err
}

// keysOf returns the first column of query's rows.
func keysOf(ctx context.Context, db querier, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
