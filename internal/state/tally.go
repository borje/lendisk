package state

import (
	"cmp"
	"context"
	"database/sql"
	"slices"
)

// This file repairs queue_tally (schema 54, #116) when it no longer
// equals the rows (#118), and the overflow targets' stored holds
// (overflow_live, overflow_hold, schema 57, #119) likewise. The tally
// drives decisions, not only status: RepositoryUse feeds the overflow
// split and drain and capacity, RepositorySize the quota exchange; the
// holds feed the alerts and `capacity`. Their triggers keep them in
// every writer's transaction, so they drift only by something outside
// them: a manual SQL fix, a writer on a connection without
// recursive_triggers (an older lendisk's REPLACE onto a taken seq), or
// a migration that rebuilds queue or placement and drops the triggers
// (the package comment's rule). Nothing else would ever correct such
// drift, so the daemon checks.

// Tally is one bucket's queue_tally row: the done rows and their puts'
// bytes, and the live puts (each key's latest done write, if a put) of
// the data class and of the metadata and snapshot classes, with how
// many have a size and their bytes.
type Tally struct {
	Done, DoneBytes                        int64
	LiveData, LiveDataSized, LiveDataBytes int64
	LiveMeta, LiveMetaSized, LiveMetaBytes int64
}

// TallyDrift is a bucket whose stored tally differed from its rows, as
// of one read, or (Place set, Bucket empty) an overflow target whose
// stored hold did (#119): RepairQueueTally corrected it.
type TallyDrift struct {
	Bucket   string
	Stored   Tally
	Computed Tally
	// Place is the target; StoredHeld its overflow_hold row,
	// ComputedHeld its live data copies counted from the rows, and
	// Copies how many of its overflow_live rows were wrong or missing.
	Place                    string
	StoredHeld, ComputedHeld TargetHeld
	Copies                   int
}

// TargetHeld is an overflow target's live data copies and their bytes.
type TargetHeld struct {
	Objects, Bytes int64
}

// RepairQueueTally recomputes every bucket's tally from the queue rows
// and corrects the stored one where it differs, and every overflow
// target's stored hold likewise (#119), returning what it corrected
// (none when the triggers kept them, the normal case). The daemon
// calls it at start and then hourly, and logs any drift.
//
// The stored and computed tallies are read in one read transaction
// (the computation reads every done row, so it must not hold the write
// lock), and each drifted bucket is then corrected by the difference,
// in one short write transaction: the triggers have added every write
// since the read to both sides alike, so the stored tally plus the
// difference is the rows' as of the write. It is never set to a stale
// value, and a bucket without drift is not written: what reads the
// tally meanwhile sees it as it was, or right. A target's hold is two
// stores: its copies (overflow_live) and their total (overflow_hold).
// The copies that differ from the rows are recomputed, key by key, from
// the rows as of the write, by the triggers' own statements; the total
// is corrected by its difference from the stored copies as of the
// read, which the triggers also keep alike.
func (db *DB) RepairQueueTally(ctx context.Context) ([]TallyDrift, error) {
	if db.snap != nil {
		return nil, errSnapshot
	}
	drift, fix, err := db.tallyDrift(ctx)
	if err != nil || len(drift) == 0 {
		return nil, err
	}
	if db.beforeTallyRepair != nil {
		db.beforeTallyRepair()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, d := range drift {
		if d.Place != "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO queue_tally (bucket) SELECT ?1
			WHERE NOT EXISTS (SELECT 1 FROM queue_tally WHERE bucket = ?1)`, d.Bucket); err != nil {
			return nil, err
		}
		c, s := d.Computed, d.Stored
		if _, err := tx.ExecContext(ctx, `UPDATE queue_tally SET done = done + ?2, done_bytes = done_bytes + ?3,
			live_data = live_data + ?4, live_data_sized = live_data_sized + ?5, live_data_bytes = live_data_bytes + ?6,
			live_meta = live_meta + ?7, live_meta_sized = live_meta_sized + ?8, live_meta_bytes = live_meta_bytes + ?9
			WHERE bucket = ?1`, d.Bucket, c.Done-s.Done, c.DoneBytes-s.DoneBytes,
			c.LiveData-s.LiveData, c.LiveDataSized-s.LiveDataSized, c.LiveDataBytes-s.LiveDataBytes,
			c.LiveMeta-s.LiveMeta, c.LiveMetaSized-s.LiveMetaSized, c.LiveMetaBytes-s.LiveMetaBytes); err != nil {
			return nil, err
		}
	}
	if err := fix.apply(ctx, tx); err != nil {
		return nil, err
	}
	return drift, tx.Commit()
}

// tallyDrift reads, as of one commit, every bucket whose stored tally
// differs from its rows, and every overflow target whose stored hold
// does, with what correcting the holds takes. A bucket or target
// missing on either side counts as zeros there.
func (db *DB) tallyDrift(ctx context.Context) ([]TallyDrift, heldFix, error) {
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		return nil, heldFix{}, err
	}
	defer done()
	out, err := bucketDrift(ctx, view)
	if err != nil {
		return nil, heldFix{}, err
	}
	held, fix, err := heldDrift(ctx, view)
	return append(out, held...), fix, err
}

func bucketDrift(ctx context.Context, view *DB) ([]TallyDrift, error) {
	stored, err := readTallies(ctx, view, `SELECT bucket, done, done_bytes, live_data, live_data_sized, live_data_bytes,
		live_meta, live_meta_sized, live_meta_bytes FROM queue_tally`)
	if err != nil {
		return nil, err
	}
	// Each key's latest done row, by queue_key in key order, then that
	// row if a put; the done rows by bucket.
	computed, err := readTallies(ctx, view, `WITH latest AS (SELECT q.bucket, q.class, q.size FROM
			(SELECT bucket, key, max(seq) AS seq FROM queue INDEXED BY queue_key WHERE state = 'done' GROUP BY bucket, key) l
			JOIN queue q ON q.seq = l.seq WHERE q.op = 'put'),
		live AS (SELECT bucket,
			count(*) FILTER (WHERE class NOT IN ('metadata', 'snapshot')) AS data,
			count(size) FILTER (WHERE class NOT IN ('metadata', 'snapshot')) AS data_sized,
			coalesce(sum(size) FILTER (WHERE class NOT IN ('metadata', 'snapshot')), 0) AS data_bytes,
			count(*) FILTER (WHERE class IN ('metadata', 'snapshot')) AS meta,
			count(size) FILTER (WHERE class IN ('metadata', 'snapshot')) AS meta_sized,
			coalesce(sum(size) FILTER (WHERE class IN ('metadata', 'snapshot')), 0) AS meta_bytes
			FROM latest GROUP BY bucket),
		done AS (SELECT bucket, count(*) AS n, coalesce(sum(size) FILTER (WHERE op = 'put'), 0) AS bytes
			FROM queue WHERE state = 'done' GROUP BY bucket)
		SELECT d.bucket, d.n, d.bytes, coalesce(l.data, 0), coalesce(l.data_sized, 0), coalesce(l.data_bytes, 0),
			coalesce(l.meta, 0), coalesce(l.meta_sized, 0), coalesce(l.meta_bytes, 0)
		FROM done d LEFT JOIN live l ON l.bucket = d.bucket`)
	if err != nil {
		return nil, err
	}
	var out []TallyDrift
	for b, c := range computed {
		if s := stored[b]; s != c {
			out = append(out, TallyDrift{Bucket: b, Stored: s, Computed: c})
		}
	}
	for b, s := range stored {
		if _, ok := computed[b]; !ok && s != (Tally{}) {
			out = append(out, TallyDrift{Bucket: b, Stored: s})
		}
	}
	slices.SortFunc(out, func(a, b TallyDrift) int { return cmp.Compare(a.Bucket, b.Bucket) })
	return out, nil
}

func readTallies(ctx context.Context, q querier, query string) (map[string]Tally, error) {
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Tally{}
	for rows.Next() {
		var b string
		var t Tally
		if err := rows.Scan(&b, &t.Done, &t.DoneBytes, &t.LiveData, &t.LiveDataSized, &t.LiveDataBytes,
			&t.LiveMeta, &t.LiveMetaSized, &t.LiveMetaBytes); err != nil {
			return nil, err
		}
		out[b] = t
	}
	return out, rows.Err()
}

// heldFix is what correcting the targets' stored holds takes: what each
// target's total lacks of the sum of its stored copies, as of the read,
// to add; and the keys whose stored copies differ from the rows, to
// recompute.
type heldFix struct {
	short map[string]TargetHeld
	keys  [][2]string
}

// heldDrift reads every target whose stored hold differs from its rows:
// its total from its copies counted afresh, or any of its stored
// copies from the copies that count (overflowLiveRows).
func heldDrift(ctx context.Context, view *DB) ([]TallyDrift, heldFix, error) {
	fix := heldFix{short: map[string]TargetHeld{}}
	read := func(q string) (map[string]TargetHeld, error) {
		out := map[string]TargetHeld{}
		err := eachRow(ctx, view, q, func(rows *sql.Rows) error {
			var place string
			var h TargetHeld
			if err := rows.Scan(&place, &h.Objects, &h.Bytes); err != nil {
				return err
			}
			out[place] = h
			return nil
		})
		return out, err
	}
	stored, err := read(`SELECT place, objects, bytes FROM overflow_hold`)
	if err != nil {
		return nil, fix, err
	}
	sums, err := read(`SELECT place, count(*), coalesce(sum(size), 0) FROM overflow_live GROUP BY place`)
	if err != nil {
		return nil, fix, err
	}
	// The copies that count, in one pass, each looked up among the stored
	// copies by its key (place, seq): per target, how many and their
	// bytes, and how many have no stored copy alike. Both sides are unique
	// by place and seq, so when every copy that counts is stored alike
	// and a target stores as many as count, its stored copies are
	// exactly the rows' (#120: the hourly check walked the copies that
	// count three times, 1.8 s -> 5.8 s on a million objects).
	computed := map[string]TargetHeld{}
	unalike := false
	err = eachRow(ctx, view, `SELECT r.place, count(*), coalesce(sum(r.size), 0),
			count(*) FILTER (WHERE s.seq IS NULL OR s.bucket IS NOT r.bucket OR s.key IS NOT r.key OR s.size IS NOT r.size)
		FROM (`+overflowLiveRows+`) r LEFT JOIN overflow_live s ON s.place = r.place AND s.seq = r.seq
		GROUP BY r.place`, func(rows *sql.Rows) error {
		var place string
		var h TargetHeld
		var n int64
		if err := rows.Scan(&place, &h.Objects, &h.Bytes, &n); err != nil {
			return err
		}
		computed[place] = h
		unalike = unalike || n > 0
		return nil
	})
	if err != nil {
		return nil, fix, err
	}
	for p, s := range sums {
		unalike = unalike || s.Objects != computed[p].Objects
	}
	for p, c := range computed {
		unalike = unalike || c.Objects != sums[p].Objects
	}
	// Only then: the stored copies the rows lack, and the rows' copies
	// not stored as they are.
	const cols = `SELECT place, seq, bucket, key, size`
	copies := map[string]map[int64]bool{}
	keys := map[[2]string]bool{}
	if unalike {
		err = eachRow(ctx, view, `SELECT place, seq, bucket, key FROM (`+cols+` FROM overflow_live
			EXCEPT `+cols+` FROM (`+overflowLiveRows+`))
		UNION ALL SELECT place, seq, bucket, key FROM (`+cols+` FROM (`+overflowLiveRows+`)
			EXCEPT `+cols+` FROM overflow_live)`, func(rows *sql.Rows) error {
			var place string
			var seq int64
			var bucket, key sql.NullString
			if err := rows.Scan(&place, &seq, &bucket, &key); err != nil {
				return err
			}
			if copies[place] == nil {
				copies[place] = map[int64]bool{}
			}
			copies[place][seq] = true
			k := [2]string{bucket.String, key.String}
			if !keys[k] {
				keys[k] = true
				fix.keys = append(fix.keys, k)
			}
			return nil
		})
	}
	if err != nil {
		return nil, fix, err
	}
	places := map[string]bool{}
	for _, m := range []map[string]TargetHeld{stored, sums, computed} {
		for p := range m {
			places[p] = true
		}
	}
	var out []TallyDrift
	for p := range places {
		if d := (TargetHeld{sums[p].Objects - stored[p].Objects, sums[p].Bytes - stored[p].Bytes}); d != (TargetHeld{}) {
			fix.short[p] = d
		}
		if stored[p] != computed[p] || len(copies[p]) > 0 {
			out = append(out, TallyDrift{Place: p, StoredHeld: stored[p], ComputedHeld: computed[p], Copies: len(copies[p])})
		}
	}
	slices.SortFunc(out, func(a, b TallyDrift) int { return cmp.Compare(a.Place, b.Place) })
	return out, fix, nil
}

// apply corrects each target's total by its difference from its stored
// copies, then recomputes each key's copies from the rows: every key's
// copies dropped first, then added, so a copy stored under another key
// than its row's is gone before its row's key adds it.
func (f heldFix) apply(ctx context.Context, tx *sql.Tx) error {
	for p, d := range f.short {
		if _, err := tx.ExecContext(ctx, `INSERT INTO overflow_hold (place) SELECT ?1
			WHERE NOT EXISTS (SELECT 1 FROM overflow_hold WHERE place = ?1)`, p); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE overflow_hold SET objects = objects + ?2, bytes = bytes + ?3
			WHERE place = ?1`, p, d.Objects, d.Bytes); err != nil {
			return err
		}
	}
	for _, stmts := range []func(bucket, key, cond string) []string{overflowKeyDrop, overflowKeyAdd58} {
		for _, k := range f.keys {
			for _, s := range stmts("?1", "?2", "") {
				if _, err := tx.ExecContext(ctx, s, k[0], k[1]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// eachRow runs query and calls f for each row.
func eachRow(ctx context.Context, q querier, query string, f func(*sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := f(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
