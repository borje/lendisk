package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
)

// This file stores what BelowCopies counts (REPL-4, OPS-2, #116) beside
// the offsite watermark's short puts (offsitestore.go), on the same
// marks.
//
// A live put is below copies when fewer than its bucket's copies of its
// placement rows are queued or acknowledged at a friend or overflow
// target, whatever their condition (below). Such a row reads the places
// (every friendship and target), the bucket's copies, and queue and
// placement rows of the put's own key: the inputs of an offsite short
// put, with the places for the holders. So:
//   - below_short holds belowRows's rows for every key as of the places
//     and copies in below_basis;
//   - the fold (FoldOffsite) stores them with the offsite rows, from the
//     same read, and its last batch clears the marks both answer;
//   - a read (storedBelow) takes the stored rows of keys offsite_dirty
//     does not mark and computes the marked keys afresh, so it equals
//     the full count at every commit. A bucket whose places or copies
//     differ from the basis, or whose basis is not the latest fold's
//     (below_basis.folds against offsite_basis.folds: a fold by an
//     older lendisk stores no below rows, and a full fold empties the
//     basis until its last batch), is counted in full.
//
// The fold stores two more reads that would walk every row the same
// way, on the same terms: Offsite's Through (latestPut) and
// ReplicaChecks' HeldSince (heldOf).

// belowRow is a live put below copies.
type belowRow struct {
	key  string
	seq  int64
	size sql.NullInt64
}

// belowPlaces are the places whose rows below counts, as JSON for
// json_each: every friend and overflow target.
func belowPlaces(ctx context.Context, q querier) (string, error) {
	ps, err := keysOf(ctx, q, `SELECT peer_key FROM friendship UNION `+overflowPlaces+` ORDER BY 1`)
	if err != nil {
		return "", err
	}
	if ps == nil {
		ps = []string{}
	}
	b, err := json.Marshal(ps)
	return string(b), err
}

// belowRows lists bucket's live puts that fewer than copies of places
// hold or will hold. keys, when not "", restricts
// them to the keys it selects (a subquery, which may name ?1, the
// bucket, and args after the first three), as in doneShorts. Every row
// reads only places, copies, and queue and placement rows of its own
// key: a change that breaks that must widen migration 53's marks.
func belowRows(ctx context.Context, q querier, bucket string, copies int, places, keys string, args ...any) ([]belowRow, error) {
	from, filter := `queue q`, ""
	if keys != "" {
		// By key: the bucket's rows in sequence order would read all.
		from, filter = `queue q INDEXED BY queue_key`, ` AND q.key IN (`+keys+`)`
	}
	rows, err := q.QueryContext(ctx, `WITH places AS MATERIALIZED (SELECT value AS place FROM json_each(?3))
		SELECT q.key, q.seq, q.size FROM `+from+` WHERE q.bucket = ?1 AND q.state = 'done' AND q.op = 'put'`+filter+`
		AND `+liveQ+`
		AND (SELECT count(*) FROM placement p WHERE p.seq = q.seq AND p.state IN ('queued', 'acked')
			AND p.place IN (SELECT place FROM places)) < ?2`, append([]any{bucket, copies, places}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []belowRow
	for rows.Next() {
		var r belowRow
		if err := rows.Scan(&r.key, &r.seq, &r.size); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// queueBuckets selects every bucket with queue rows, one index look-up
// each rather than a walk of every row.
const queueBuckets = `WITH RECURSIVE b (bucket) AS (SELECT (SELECT min(bucket) FROM queue)
	UNION ALL SELECT (SELECT min(q.bucket) FROM queue q WHERE q.bucket > b.bucket) FROM b WHERE b.bucket IS NOT NULL)
	SELECT bucket FROM b WHERE bucket IS NOT NULL`

// storedBelow is BelowCopies: per bucket with queue rows, its stored
// rows and its marked keys' afresh, or every live put when the basis
// does not match. It reads in one read transaction unless db already
// is a Snapshot.
func (db *DB) storedBelow(ctx context.Context) (map[string]BelowStat, error) {
	if db.snap == nil {
		view, done, err := db.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		defer done()
		return view.storedBelow(ctx)
	}
	buckets, err := keysOf(ctx, db, queueBuckets)
	if err != nil {
		return nil, err
	}
	places, err := belowPlaces(ctx, db)
	if err != nil {
		return nil, err
	}
	out := map[string]BelowStat{}
	for _, b := range buckets {
		rows, _, err := db.belowOf(ctx, b, places)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			s := out[b]
			s.Objects++
			s.Bytes += r.size.Int64
			out[b] = s
		}
	}
	return out, nil
}

// belowOf returns bucket's live puts below copies; stored reports
// whether they came from the stored rows.
func (db *DB) belowOf(ctx context.Context, bucket, places string) (rows []belowRow, stored bool, err error) {
	var copies int
	if err := db.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
		bucket, DefaultCopies).Scan(&copies); err != nil {
		return nil, false, err
	}
	var bp string
	var bc int
	var latest bool
	err = db.QueryRowContext(ctx, `SELECT b.places, b.copies, b.folds = o.folds
		FROM below_basis b JOIN offsite_basis o ON o.bucket = b.bucket WHERE b.bucket = ?`, bucket).Scan(&bp, &bc, &latest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	if err != nil || bp != places || bc != copies || !latest {
		rows, err := belowRows(ctx, db, bucket, copies, places, "")
		return rows, false, err
	}
	rs, err := db.QueryContext(ctx, `SELECT s.key, s.seq, s.size FROM below_short s WHERE s.bucket = ?
		AND NOT EXISTS (SELECT 1 FROM offsite_dirty d WHERE d.bucket = s.bucket AND d.key = s.key)`, bucket)
	if err != nil {
		return nil, false, err
	}
	defer rs.Close()
	for rs.Next() {
		var r belowRow
		if err := rs.Scan(&r.key, &r.seq, &r.size); err != nil {
			return nil, false, err
		}
		rows = append(rows, r)
	}
	if err := rs.Err(); err != nil {
		return nil, false, err
	}
	marked, err := belowRows(ctx, db, bucket, copies, places, dirtyKeys, int64(math.MaxInt64))
	return append(rows, marked...), true, err
}

// readBelowFold reads, into f, what a fold stores of bucket's below
// rows: all of them when the basis is not f's places, copies and fold,
// else the keys marked up to f.mark (marked).
func readBelowFold(ctx context.Context, view *DB, bucket string, f *fold, marked []string) error {
	var err error
	if f.belowPlaces, err = belowPlaces(ctx, view); err != nil {
		return err
	}
	var bp sql.NullString
	var bc, bf sql.NullInt64
	if err := view.QueryRowContext(ctx, `SELECT b.places, b.copies, b.folds
		FROM (SELECT 1) LEFT JOIN below_basis b ON b.bucket = ?`, bucket).Scan(&bp, &bc, &bf); err != nil {
		return err
	}
	f.belowFull = !bp.Valid || bp.String != f.belowPlaces || bc.Int64 != int64(f.copies) ||
		!f.folds.Valid || bf.Int64 != f.folds.Int64
	if f.belowFull {
		f.below, err = belowRows(ctx, view, bucket, f.copies, f.belowPlaces, "")
		return err
	}
	f.belowKeys = marked
	f.below, err = belowRows(ctx, view, bucket, f.copies, f.belowPlaces, dirtyKeys, f.mark)
	return err
}

// latestPut returns the sequence of bucket's latest done put at or
// below bound, 0 when none, counting marks up to mark (Offsite's
// Through). A bucket's puts are mostly far behind a prune's deletes,
// which the walk back by sequence reads one by one (#116), so the fold
// stores the latest as of its read (below_basis.through, on the below
// rows' terms): every done put after it is of a key marked since. The
// stored one counts if it is still a done put at or below bound; the
// marked keys' puts at or below bound are read by key. Otherwise, or
// without a basis of the latest fold, the bucket's rows are walked
// back from bound.
func (db *DB) latestPut(ctx context.Context, bucket string, bound, mark int64) (int64, error) {
	var stored sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT b.through FROM below_basis b JOIN offsite_basis o ON o.bucket = b.bucket
		WHERE b.bucket = ? AND b.folds = o.folds`, bucket).Scan(&stored)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err == nil && stored.Valid && stored.Int64 <= bound {
		var ok bool
		if err := db.QueryRowContext(ctx, `SELECT ?2 = 0 OR EXISTS (SELECT 1 FROM queue
			WHERE seq = ?2 AND bucket = ?1 AND state = 'done' AND op = 'put')`, bucket, stored.Int64).Scan(&ok); err != nil {
			return 0, err
		}
		if ok {
			var marked int64
			err := db.QueryRowContext(ctx, `SELECT coalesce(max(q.seq), 0) FROM queue q INDEXED BY queue_key
				WHERE q.bucket = ?1 AND q.key IN (`+dirtyKeys+`) AND q.state = 'done' AND q.op = 'put' AND q.seq <= ?2`,
				bucket, bound, nil, mark).Scan(&marked)
			return max(stored.Int64, marked), err
		}
	}
	return db.latestPutWalked(ctx, bucket, bound)
}

// latestPutWalked is latestPut from the bucket's rows alone.
func (db *DB) latestPutWalked(ctx context.Context, bucket string, bound int64) (int64, error) {
	var seq int64
	err := db.QueryRowContext(ctx, `SELECT coalesce((SELECT seq FROM queue INDEXED BY queue_bucket_seq
		WHERE bucket = ? AND state = 'done' AND op = 'put' AND seq <= ? ORDER BY seq DESC LIMIT 1), 0)`,
		bucket, bound).Scan(&seq)
	return seq, err
}

// held is a replica's first acknowledgement still held: the time
// (NULL when it holds none) and its row.
type held struct {
	at, seq sql.NullInt64
}

// heldOf returns place's first acknowledgement of a put of bucket it
// still holds (the least acked_at, then seq), counting marks up to
// mark (ReplicaChecks' HeldSince). Reading it from the rows reads
// every copy the place holds (#116), so the fold stores it as of its
// read (held_basis, on the below rows' terms): every acknowledgement
// changed since is of a key marked since. The stored one counts if its
// row is still that acknowledgement; the marked keys' rows are read by
// key. Otherwise, or without a basis of the latest fold, the place's
// rows are read.
func (db *DB) heldOf(ctx context.Context, place, bucket string, mark int64) (held, error) {
	var h held
	err := db.QueryRowContext(ctx, `SELECT h.acked_at, h.seq FROM held_basis h
		JOIN offsite_basis o ON o.bucket = h.bucket AND o.folds = h.folds
		WHERE h.bucket = ? AND h.place = ?`, bucket, place).Scan(&h.at, &h.seq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return h, err
	}
	if err == nil {
		ok := !h.at.Valid
		if !ok {
			if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM placement WHERE place = ? AND seq = ?
				AND bucket = ? AND state = 'acked' AND op = 'put' AND acked_at = ?)`,
				place, h.seq, bucket, h.at).Scan(&ok); err != nil {
				return h, err
			}
		}
		if ok {
			var m held
			err := db.QueryRowContext(ctx, `SELECT p.acked_at, p.seq FROM placement p INDEXED BY placement_key
				WHERE p.place = ?2 AND p.bucket = ?1 AND p.key IN (`+dirtyKeys+`)
				AND p.state = 'acked' AND p.op = 'put' AND p.acked_at IS NOT NULL
				ORDER BY p.acked_at, p.seq LIMIT 1`, bucket, place, nil, mark).Scan(&m.at, &m.seq)
			if errors.Is(err, sql.ErrNoRows) {
				return h, nil
			}
			if err == nil && (!h.at.Valid || m.at.Int64 < h.at.Int64 || m.at.Int64 == h.at.Int64 && m.seq.Int64 < h.seq.Int64) {
				h = m
			}
			return h, err
		}
	}
	return db.heldWalked(ctx, place, bucket)
}

// heldWalked is heldOf from every copy place holds of bucket.
func (db *DB) heldWalked(ctx context.Context, place, bucket string) (held, error) {
	var h held
	err := db.QueryRowContext(ctx, `SELECT acked_at, seq FROM placement
		WHERE place = ? AND bucket = ? AND state = 'acked' AND op = 'put' AND acked_at IS NOT NULL
		ORDER BY acked_at, seq LIMIT 1`, place, bucket).Scan(&h.at, &h.seq)
	if errors.Is(err, sql.ErrNoRows) {
		return held{}, nil
	}
	return h, err
}

// readHeldFold reads, into f, each friend's and target's held of
// bucket, as of the fold's read.
func readHeldFold(ctx context.Context, view *DB, bucket string, f *fold) error {
	places, err := keysOf(ctx, view, `SELECT peer_key FROM friendship UNION `+overflowPlaces+` ORDER BY 1`)
	if err != nil {
		return err
	}
	f.held = map[string]held{}
	for _, p := range places {
		if f.held[p], err = view.heldOf(ctx, p, bucket, f.mark); err != nil {
			return err
		}
	}
	return nil
}
