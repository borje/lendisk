package state

import (
	"cmp"
	"context"
	"slices"
)

// This file repairs queue_tally (schema 54, #116) when it no longer
// equals the rows (#118). The tally drives decisions, not only status:
// RepositoryUse feeds the overflow split and drain and capacity,
// RepositorySize the quota exchange. Its triggers keep it in every
// writer's transaction, so it drifts only by something outside them: a
// manual SQL fix, a writer on a connection without recursive_triggers
// (an older lendisk's REPLACE onto a taken seq), or a migration that
// rebuilds queue and drops the triggers (the package comment's rule).
// Nothing else would ever correct such drift, so the daemon checks.

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
// of one read: RepairQueueTally added the difference.
type TallyDrift struct {
	Bucket   string
	Stored   Tally
	Computed Tally
}

// RepairQueueTally recomputes every bucket's tally from the queue rows
// and corrects the stored one where it differs, returning what it
// corrected (none when the triggers kept it, the normal case). The
// daemon calls it at start and then hourly, and logs any drift.
//
// The stored and computed tallies are read in one read transaction
// (the computation reads every done row, so it must not hold the write
// lock), and each drifted bucket is then corrected by the difference,
// in one short write transaction: the triggers have added every write
// since the read to both sides alike, so the stored tally plus the
// difference is the rows' as of the write. It is never set to a stale
// value, and a bucket without drift is not written: what reads the
// tally meanwhile sees it as it was, or right.
func (db *DB) RepairQueueTally(ctx context.Context) ([]TallyDrift, error) {
	if db.snap != nil {
		return nil, errSnapshot
	}
	drift, err := db.tallyDrift(ctx)
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
	return drift, tx.Commit()
}

// tallyDrift reads, as of one commit, every bucket whose stored tally
// differs from its rows. A bucket missing on either side counts as
// zeros there.
func (db *DB) tallyDrift(ctx context.Context) ([]TallyDrift, error) {
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
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
