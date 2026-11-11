package state

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
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
// without it, only slower the more keys are marked.
//
// Each bucket is read in a read transaction (a full store takes as long
// as a full read, and must not hold the write lock), then written in a
// short write transaction that clears only the marks the read saw: a
// key changed after the read keeps a newer mark, so its rows stay
// unread until the next fold. Two folds at once are safe for the same
// reason; a fold whose basis another replaced meanwhile writes nothing.
// Set the DB's Clock and ReplaceAfter as the daemon's.
func (db *DB) FoldOffsite(ctx context.Context) error {
	if db.snap != nil {
		return errSnapshot
	}
	rows, err := db.QueryContext(ctx, `SELECT name FROM buckets UNION SELECT bucket FROM offsite_dirty
		UNION SELECT bucket FROM offsite_basis ORDER BY 1`)
	if err != nil {
		return err
	}
	var buckets []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		buckets = append(buckets, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, b := range buckets {
		if err := db.foldBucket(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// fold is what foldBucket read: the rows to store, the marks they
// answer, and the basis.
type fold struct {
	mark    int64         // the newest mark read; 0 when none
	full    bool          // every key: the basis is new
	folds   sql.NullInt64 // the basis's count of folds written; NULL when none
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

// writeFold stores what readFold read of bucket.
func (db *DB) writeFold(ctx context.Context, bucket string, f fold) error {
	if !f.full && f.mark == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Another fold written since the read may have cleared marks of
	// changes this read did not see: this one is then stale, and the
	// next fold reads again.
	var folds sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT folds FROM offsite_basis WHERE bucket = ?)`,
		bucket).Scan(&folds); err != nil {
		return err
	}
	if folds != f.folds {
		return nil
	}
	if f.full {
		if _, err := tx.ExecContext(ctx, `DELETE FROM offsite_short WHERE bucket = ?`, bucket); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO offsite_basis (bucket, holders, copies, folds) VALUES (?, ?, ?, 1)
		ON CONFLICT (bucket) DO UPDATE SET holders = excluded.holders, copies = excluded.copies, folds = folds + 1`,
		bucket, f.holders, f.copies); err != nil {
		return err
	}
	// The keys the read saw marked, by name: a key marked again since
	// has a newer mark, not one at or below f.mark, but its rows go too.
	for _, k := range f.keys {
		if _, err := tx.ExecContext(ctx, `DELETE FROM offsite_short WHERE bucket = ? AND key = ?`, bucket, k); err != nil {
			return err
		}
	}
	ins, err := tx.PrepareContext(ctx, `INSERT INTO offsite_short
		(bucket, key, seq, size, next, stuck, refused, dropped, gone, immutable) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer ins.Close()
	for _, s := range f.rows {
		if _, err := ins.ExecContext(ctx, bucket, s.key, s.seq, s.size, s.next, s.stuck, s.refused, s.dropped, s.gone, s.immutable); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM offsite_dirty WHERE bucket = ? AND id <= ?`, bucket, f.mark); err != nil {
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
