package state

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"
)

// What status's reads computed from every row before #116: each read
// that now comes from queue_tally, below_short or a partial index must
// equal its query here.

func queueStatsFull(ctx context.Context, db *DB) (map[string]QueueStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket,
		count(*) FILTER (WHERE state = 'done'),
		coalesce(sum(size) FILTER (WHERE state = 'done' AND op = 'put'), 0),
		count(*) FILTER (WHERE state = 'pending')
		FROM queue GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]QueueStat{}
	for rows.Next() {
		var b string
		var s QueueStat
		if err := rows.Scan(&b, &s.Entries, &s.Bytes, &s.Pending); err != nil {
			return nil, err
		}
		out[b] = s
	}
	return out, rows.Err()
}

func repositorySizeFull(ctx context.Context, db *DB) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, `SELECT coalesce(sum(q.size), 0) FROM queue q
		WHERE q.op = 'put' AND q.state = 'done' AND `+liveQ).Scan(&n)
	return n, err
}

func repositoryUseFull(ctx context.Context, db *DB) (RepositoryUse, error) {
	var u RepositoryUse
	err := db.QueryRowContext(ctx, `SELECT
		coalesce(sum(CASE WHEN q.class IN ('metadata', 'snapshot') THEN 0 ELSE q.size + ?1 END), 0),
		coalesce(sum(CASE WHEN q.class IN ('metadata', 'snapshot') THEN q.size + ?1 ELSE 0 END), 0),
		coalesce(max(CASE WHEN q.class IN ('metadata', 'snapshot') THEN NULL ELSE coalesce(b.copies, ?2) END), 0)
		FROM queue q LEFT JOIN buckets b ON b.name = q.bucket
		WHERE q.op = 'put' AND q.state = 'done' AND `+liveQ,
		ObjectOverhead, DefaultCopies).Scan(&u.Data, &u.Metadata, &u.Copies)
	return u, err
}

// droppedBelowFull is DroppedBelowCopies as it read every live put
// before #118.
func droppedBelowFull(ctx context.Context, db *DB) (map[string]BelowStat, error) {
	return db.below(ctx, `AND EXISTS (SELECT 1 FROM placement p WHERE p.seq = q.seq AND p.state = '`+PlacementDropped+`')`)
}

// dueReplicasFull is DueReplicas as it read every copy at the place
// before #118, at every and retry of a day and an hour.
func dueReplicasFull(ctx context.Context, db *DB, now time.Time) ([]ReplicaKey, error) {
	rows, err := db.QueryContext(ctx, `SELECT f.peer_key, b.name FROM `+replicaPlaces+` f CROSS JOIN buckets b
		LEFT JOIN replica_check c ON c.place = f.peer_key AND c.bucket = b.name
		WHERE coalesce(f.address, '') != '' AND c.unverified_at IS NULL
		AND (c.checked_at IS NULL OR c.checked_at <= ?1) AND (c.error_at IS NULL OR c.error_at <= ?2)
		AND EXISTS (SELECT 1 FROM placement p
			WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state = 'acked' AND p.op = 'put')
		AND (NOT EXISTS (SELECT 1 FROM placement p
				WHERE p.place = f.peer_key AND p.bucket = b.name AND p.state IN ('queued', 'dropping'))
			OR c.checked_at <= ?3
			OR c.checked_at IS NULL AND `+heldSince+` <= ?4)
		ORDER BY b.name, f.peer_key`, now.Add(-24*time.Hour).Unix(), now.Add(-time.Hour).Unix(),
		now.Add(-24*time.Hour-Overdue).Unix(), now.Add(-Overdue).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicaKey
	for rows.Next() {
		var k ReplicaKey
		if err := rows.Scan(&k.Place, &k.Bucket); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// heldSince is the first acknowledgement still held at f.peer_key of
// bucket b.name (unix seconds), NULL when it holds none, from every
// copy at the place: what DueReplicas (#119) and DueCycles (#120) read
// before the fold's value (heldOf).
const heldSince = `(SELECT min(h.acked_at) FROM placement h
	WHERE h.place = f.peer_key AND h.bucket = b.name AND h.state = 'acked' AND h.op = 'put')`

// dueCyclesFull is DueCycles as it read every copy at the place before
// #120, at every and retry of a day and an hour.
func dueCyclesFull(ctx context.Context, db *DB, now time.Time) ([]FullCycle, error) {
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
		ORDER BY b.name, f.peer_key`, now.Add(-24*time.Hour).Unix(), now.Add(-time.Hour).Unix(),
		now.Add(-24*time.Hour-Overdue).Unix(), now.Add(-Overdue).Unix())
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

func placeStatsFull(ctx context.Context, db *DB, now time.Time) (map[string]PlaceStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.place,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued' AND p.op = 'put'), 0),
		min(coalesce(p.replaced_at, q.created_at)) FILTER (WHERE p.state = 'queued'),
		count(*) FILTER (WHERE p.state = 'refused' AND (p.cause IS NULL OR p.cause = 'immutable') AND `+current+`),
		count(*) FILTER (WHERE p.state = 'dropping')
		FROM placement p JOIN queue q ON q.seq = p.seq GROUP BY p.place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PlaceStat{}
	for rows.Next() {
		var place string
		var s PlaceStat
		var oldest sql.NullInt64
		if err := rows.Scan(&place, &s.Queued, &s.QueuedBytes, &oldest, &s.Refused, &s.Dropping); err != nil {
			return nil, err
		}
		if oldest.Valid {
			s.Oldest = time.Unix(oldest.Int64, 0)
			s.Lag = max(now.Sub(s.Oldest), 0)
		}
		out[place] = s
	}
	return out, rows.Err()
}

func replacingFull(ctx context.Context, db *DB) (map[string]ReplacingStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.bucket,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued'), 0),
		count(*) FILTER (WHERE p.state = 'dropping')
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE (p.state = 'dropping' OR p.state = 'queued' AND p.replaced_at IS NOT NULL)
		AND p.place IN (SELECT peer_key FROM friendship UNION ALL `+overflowPlaces+`)
		GROUP BY p.bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ReplacingStat{}
	for rows.Next() {
		var b string
		var s ReplacingStat
		if err := rows.Scan(&b, &s.Queued, &s.QueuedBytes, &s.Dropping); err != nil {
			return nil, err
		}
		out[b] = s
	}
	return out, rows.Err()
}

// heldSinceFull is each replica's first acknowledgement still held, by
// place and bucket.
func heldSinceFull(ctx context.Context, db *DB) (map[[2]string]time.Time, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, bucket, min(acked_at) FROM placement
		WHERE state = 'acked' AND op = 'put' GROUP BY place, bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[[2]string]time.Time{}
	for rows.Next() {
		var k [2]string
		var at sql.NullInt64
		if err := rows.Scan(&k[0], &k[1], &at); err != nil {
			return nil, err
		}
		out[k] = timeOf(at)
	}
	return out, rows.Err()
}

// overflowHoldsFull is OverflowHolds as it read every copy at each
// target before #119.
func overflowHoldsFull(ctx context.Context, db *DB) (map[string]OverflowHold, error) {
	rows, err := db.QueryContext(ctx, `SELECT o.name, coalesce(u.bytes, 0),
		(SELECT count(*) FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = '`+OverflowPrefix+`' || o.name AND p.state IN ('queued', 'acked') AND p.op = 'put'
			AND q.class = 'data' AND `+liveQ+`),
		(SELECT coalesce(sum(q.size), 0) FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = '`+OverflowPrefix+`' || o.name AND p.state IN ('queued', 'acked') AND p.op = 'put'
			AND q.class = 'data' AND `+liveQ+`)
		FROM overflow o LEFT JOIN place_use u ON u.place = '`+OverflowPrefix+`' || o.name ORDER BY o.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]OverflowHold{}
	for rows.Next() {
		var name string
		var h OverflowHold
		if err := rows.Scan(&name, &h.Use, &h.DataObjects, &h.DataBytes); err != nil {
			return nil, err
		}
		h.Use = max(h.Use, 0)
		out[name] = h
	}
	return out, rows.Err()
}

// sameStatusReads compares, in one snapshot, each of status's reads
// that #116, #119 and #120 stored or indexed with its computation from every row.
// storedBelow counts the buckets whose below rows came from the stored
// ones; diff is "" when everything agrees.
func sameStatusReads(db *DB, now time.Time) (storedBelow int, diff string, err error) {
	ctx := context.Background()
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		return 0, "", err
	}
	defer done()
	differs := func(name string, got, want any) bool {
		if reflect.DeepEqual(got, want) {
			return false
		}
		diff = fmt.Sprintf("%s: read %+v, computed %+v", name, got, want)
		return true
	}
	qs, err := view.QueueStats(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := queueStatsFull(ctx, view); err != nil || differs("queue stats", qs, want) {
		return 0, diff, err
	}
	size, err := view.RepositorySize(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := repositorySizeFull(ctx, view); err != nil || differs("repository size", size, want) {
		return 0, diff, err
	}
	use, err := view.RepositoryUse(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := repositoryUseFull(ctx, view); err != nil || differs("repository use", use, want) {
		return 0, diff, err
	}
	below, err := view.BelowCopies(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := view.below(ctx, ""); err != nil || differs("below copies", below, want) {
		return 0, diff, err
	}
	dropped, err := view.DroppedBelowCopies(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := droppedBelowFull(ctx, view); err != nil || differs("dropped below copies", dropped, want) {
		return 0, diff, err
	}
	due, err := view.DueReplicas(ctx, now, 24*time.Hour, time.Hour)
	if err != nil {
		return 0, "", err
	}
	if want, err := dueReplicasFull(ctx, view, now); err != nil || differs("due replicas", due, want) {
		return 0, diff, err
	}
	cycles, err := view.DueCycles(ctx, now, 24*time.Hour, time.Hour)
	if err != nil {
		return 0, "", err
	}
	if want, err := dueCyclesFull(ctx, view, now); err != nil || differs("due cycles", cycles, want) {
		return 0, diff, err
	}
	holds, err := view.OverflowHolds(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := overflowHoldsFull(ctx, view); err != nil || differs("overflow holds", holds, want) {
		return 0, diff, err
	}
	places, err := belowPlaces(ctx, view)
	if err != nil {
		return 0, "", err
	}
	buckets, err := keysOf(ctx, view, queueBuckets)
	if err != nil {
		return 0, "", err
	}
	for _, b := range buckets {
		_, stored, err := view.belowOf(ctx, b, places)
		if err != nil {
			return 0, "", err
		}
		if stored {
			storedBelow++
		}
	}
	ps, err := view.PlaceStats(ctx, now)
	if err != nil {
		return 0, "", err
	}
	if want, err := placeStatsFull(ctx, view, now); err != nil || differs("place stats", ps, want) {
		return 0, diff, err
	}
	rs, err := view.Replacing(ctx)
	if err != nil {
		return 0, "", err
	}
	if want, err := replacingFull(ctx, view); err != nil || differs("replacing", rs, want) {
		return 0, diff, err
	}
	cs, err := view.ReplicaChecks(ctx)
	if err != nil {
		return 0, "", err
	}
	held, err := heldSinceFull(ctx, view)
	if err != nil {
		return 0, "", err
	}
	for _, c := range cs {
		if differs(fmt.Sprintf("replica %s/%s held since", c.Place, c.Bucket), c.HeldSince, held[[2]string{c.Place, c.Bucket}]) {
			return 0, diff, nil
		}
	}
	return storedBelow, "", nil
}

// #116: queue_tally, kept by triggers, equals the sums over the rows
// after every one of random writes to queue by raw SQL: done and
// pending puts and deletes of keys in two buckets, with and without a
// size, settled, resized, reclassed, moved to another key or bucket,
// renumbered and deleted, written under OR IGNORE (skipped on a taken
// seq) and OR REPLACE (deleting the row on a taken seq, which fires
// the delete trigger since #118), so no writer can leave it wrong.
// A tally changed by hand is caught (the check is not vacuous).
func TestQueueTallyEqualsTheRowsWhateverWritesThem(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := rand.New(rand.NewPCG(116, 1))
	buckets := []string{"laptop", "photos", "gone"}
	if err := db.AddBucket(ctx, Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "a1"}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, Bucket{Name: "photos", Profile: "restic", Copies: 3, AccessKey: "a2"}); err != nil {
		t.Fatal(err)
	}
	pick := func(xs ...string) string { return xs[r.IntN(len(xs))] }
	size := func() any {
		if r.IntN(5) == 0 {
			return nil
		}
		return r.IntN(1000)
	}
	anySeq := func() (int64, bool) {
		var seq int64
		err := db.QueryRow(`SELECT seq FROM queue ORDER BY random() LIMIT 1`).Scan(&seq)
		return seq, err == nil
	}
	check := func(step int, did string) {
		t.Helper()
		_, diff, err := sameStatusReads(db, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if diff != "" {
			t.Fatalf("step %d (%s): %s", step, did, diff)
		}
	}
	for step := 1; step <= 600; step++ {
		b, k := pick(buckets...), fmt.Sprintf("k%d", r.IntN(8))
		var q string
		args := []any{}
		switch n := r.IntN(14); n {
		case 0, 1, 2:
			q = `INSERT INTO queue (bucket, key, op, class, state, size, created_at) VALUES (?, ?, ?, ?, ?, ?, 1)`
			args = []any{b, k, pick("put", "put", "delete"), pick("data", "metadata", "snapshot"), pick("done", "pending"), size()}
		case 3:
			q = `INSERT OR IGNORE INTO queue (seq, bucket, key, op, class, state, size, created_at)
				VALUES ((SELECT seq FROM queue ORDER BY random() LIMIT 1), ?, ?, 'put', 'data', 'done', ?, 1)`
			args = []any{b, k, size()}
		case 4:
			// OR REPLACE meets no conflict (seq is AUTOINCREMENT); OR
			// IGNORE skips a renumbering onto a taken seq.
			q = `INSERT OR REPLACE INTO queue (bucket, key, op, class, state, size, created_at) VALUES (?, ?, 'put', 'data', 'done', ?, 1)`
			args = []any{b, k, size()}
			if r.IntN(2) == 0 {
				q = `UPDATE OR IGNORE queue SET seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1), size = 1
					WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
				args = nil
			}
		case 5:
			q = `UPDATE queue SET state = 'done', size = ? WHERE seq = (SELECT seq FROM queue WHERE state = 'pending' ORDER BY random() LIMIT 1)`
			args = []any{size()}
		case 6:
			q = `UPDATE queue SET size = ? WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
			args = []any{size()}
		case 7:
			q = `UPDATE queue SET class = ?, op = ? WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
			args = []any{pick("data", "metadata", "snapshot"), pick("put", "delete")}
		case 8:
			q = `UPDATE queue SET key = ?, bucket = ? WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
			args = []any{k, b}
		case 9:
			// Renumbered past every row: the key's latest write moves.
			q = `UPDATE queue SET seq = (SELECT max(seq) + 1 FROM queue) WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
		case 10:
			q = `UPDATE queue SET state = 'pending' WHERE seq = (SELECT seq FROM queue WHERE state = 'done' ORDER BY random() LIMIT 1)`
		case 11:
			q = `DELETE FROM queue WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
		case 13:
			// REPLACE onto a taken seq deletes that row (#118).
			q = `INSERT OR REPLACE INTO queue (seq, bucket, key, op, class, state, size, created_at)
				VALUES ((SELECT seq FROM queue ORDER BY random() LIMIT 1), ?, ?, ?, 'data', ?, ?, 1)`
			args = []any{b, k, pick("put", "delete"), pick("done", "pending"), size()}
			if r.IntN(2) == 0 {
				q = `UPDATE OR REPLACE queue SET seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)
					WHERE seq = (SELECT seq FROM queue ORDER BY random() LIMIT 1)`
				args = nil
			}
		case 12:
			// Columns no contribution reads.
			if seq, ok := anySeq(); ok {
				q = `UPDATE queue SET local = 1, stamp = 'x', reconciled = 1 WHERE seq = ?`
				args = []any{seq}
			}
		}
		if q == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("step %d: %s: %v", step, q, err)
		}
		check(step, fmt.Sprintf("%s %v", q, args))
	}
	if _, err := db.Exec(`UPDATE queue_tally SET live_data_bytes = live_data_bytes + 1 WHERE bucket = 'laptop'`); err != nil {
		t.Fatal(err)
	}
	if _, diff, err := sameStatusReads(db, time.Now()); err != nil || diff == "" {
		t.Fatalf("a tally changed by hand was not caught: %q %v", diff, err)
	}
}

// #119: each overflow target's stored hold (overflow_live and
// overflow_hold, kept by triggers on queue and placement) equals the
// count and sum over its live data copies after every one of random
// writes by raw SQL: copies queued, acknowledged, refused, dropped,
// moved between targets and a friend, renumbered, deleted and written
// under OR IGNORE and OR REPLACE; and queue rows written, settled,
// reclassed, resized, moved to another key, renumbered and deleted, so
// that a key's latest write, and with it which copies are live,
// changes. A hold changed by hand is caught.
func TestOverflowHoldsEqualTheRowsWhateverWritesThem(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	r := rand.New(rand.NewPCG(119, 1))
	for _, n := range []string{"s3", "b2"} {
		mustExec(t, db, `INSERT INTO overflow (name, endpoint, bucket, max_bytes, env_prefix, retention, added_at)
			VALUES (?, 'https://x', 'b', 1000000, 'X', 0, 0)`, n)
	}
	pick := func(xs ...string) string { return xs[r.IntN(len(xs))] }
	places := []string{"overflow:s3", "overflow:s3", "overflow:b2", "overflow:gone", "B"}
	size := func() any {
		if r.IntN(5) == 0 {
			return nil
		}
		return r.IntN(1000)
	}
	const anySeq = `(SELECT seq FROM queue ORDER BY random() LIMIT 1)`
	const anyCopy = `(SELECT rowid FROM placement ORDER BY random() LIMIT 1)`
	held := 0 // steps after which some copy counted
	for step := 1; step <= 800; step++ {
		b, k := pick("laptop", "photos"), fmt.Sprintf("k%d", r.IntN(6))
		var q string
		var args []any
		switch r.IntN(16) {
		case 0, 1:
			q = `INSERT INTO queue (bucket, key, op, class, state, size, created_at) VALUES (?, ?, ?, ?, ?, ?, 1)`
			args = []any{b, k, pick("put", "put", "delete"), pick("data", "data", "metadata"), pick("done", "done", "pending"), size()}
		case 2, 3, 4:
			q = `INSERT OR IGNORE INTO placement (seq, place, bucket, key, op, state)
				SELECT seq, ?, bucket, key, ?, ? FROM queue ORDER BY random() LIMIT 1`
			args = []any{pick(places...), pick("put", "put", "delete"), pick("queued", "acked", "refused")}
		case 5:
			q = `UPDATE placement SET state = ? WHERE rowid = ` + anyCopy
			args = []any{pick("queued", "acked", "dropped", "dropping", "refused")}
		case 6:
			q = `UPDATE OR IGNORE placement SET place = ? WHERE rowid = ` + anyCopy
			args = []any{pick(places...)}
		case 7:
			q = `UPDATE OR ` + pick("IGNORE", "REPLACE") + ` placement SET seq = ` + anySeq + ` WHERE rowid = ` + anyCopy
		case 8:
			q = `UPDATE placement SET op = ? WHERE rowid = ` + anyCopy
			args = []any{pick("put", "delete")}
		case 9:
			q = `DELETE FROM placement WHERE rowid = ` + anyCopy
		case 10:
			q = `INSERT OR REPLACE INTO placement (seq, place, bucket, key, op, state)
				SELECT seq, ?, bucket, key, 'put', 'acked' FROM queue ORDER BY random() LIMIT 1`
			args = []any{pick(places...)}
		case 11:
			q = `UPDATE queue SET state = ?, size = ? WHERE seq = ` + anySeq
			args = []any{pick("done", "pending"), size()}
		case 12:
			q = `UPDATE queue SET class = ? WHERE seq = ` + anySeq
			args = []any{pick("data", "metadata", "snapshot")}
		case 13:
			q = `UPDATE queue SET key = ?, bucket = ? WHERE seq = ` + anySeq
			args = []any{k, b}
		case 14:
			q = `UPDATE OR ` + pick("IGNORE", "REPLACE") + ` queue SET seq = ` +
				pick(anySeq, `(SELECT max(seq) + 1 FROM queue)`) + ` WHERE seq = ` + anySeq
		case 15:
			q = `DELETE FROM queue WHERE seq = ` + anySeq
		}
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("step %d: %s: %v", step, q, err)
		}
		if _, diff, err := sameStatusReads(db, time.Now()); err != nil || diff != "" {
			t.Fatalf("step %d (%s %v): %s %v", step, q, args, diff, err)
		}
		// The stored copies too, not only their totals (#120).
		view, done, err := db.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		drift, _, err := heldDrift(ctx, view)
		done()
		if err != nil || len(drift) != 0 {
			t.Fatalf("step %d (%s %v): stored copies drifted: %+v %v", step, q, args, drift, err)
		}
		var some bool
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM overflow_live)`).Scan(&some); err != nil {
			t.Fatal(err)
		}
		if some {
			held++
		}
	}
	if held < 100 {
		t.Fatalf("a live copy at a target after only %d steps: the check is nearly vacuous", held)
	}
	mustExec(t, db, `INSERT INTO overflow_hold (place, bytes) VALUES ('overflow:s3', 1)
		ON CONFLICT (place) DO UPDATE SET bytes = bytes + 1`)
	if _, diff, err := sameStatusReads(db, time.Now()); err != nil || diff == "" {
		t.Fatalf("a hold changed by hand was not caught: %q %v", diff, err)
	}
}

// #116: schema 54 fills queue_tally from the rows already there, so an
// upgraded node counts what it captured before.
func TestTheUpgradeTalliesTheRowsAlreadyThere(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	old, err := open(ctx, dir, migrations[:53], quiet)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO buckets (name, profile, copies, access_key, secret_key, created_at) VALUES ('laptop', 'restic', 3, 'a', 's', '')`,
		`INSERT INTO queue (bucket, key, op, class, state, size, created_at) VALUES
			('laptop', 'a', 'put', 'data', 'done', 10, 1), ('laptop', 'a', 'put', 'data', 'done', 20, 1),
			('laptop', 'b', 'put', 'metadata', 'done', 5, 1), ('laptop', 'c', 'put', 'data', 'done', 7, 1),
			('laptop', 'c', 'delete', 'data', 'done', NULL, 1), ('laptop', 'd', 'put', 'data', 'pending', NULL, 1),
			('other', 'e', 'put', 'snapshot', 'done', NULL, 1)`,
	} {
		if _, err := old.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	old.Close()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, diff, err := sameStatusReads(db, time.Now()); err != nil || diff != "" {
		t.Fatalf("after the upgrade: %s %v", diff, err)
	}
	if n, err := db.RepositorySize(ctx); err != nil || n != 25 {
		t.Fatalf("repository size %d, want 25 (%v)", n, err)
	}
}

// #119: schema 57 fills each overflow target's hold from the copies
// already there, so an upgraded node counts what its targets held
// before: live data puts queued or acknowledged there, not a
// metadata copy, a refused or dropped one, or the put of a key written
// since.
func TestTheUpgradeCountsWhatOverflowTargetsAlreadyHold(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	old, err := open(ctx, dir, migrations[:56], quiet)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO overflow (name, endpoint, bucket, max_bytes, env_prefix, retention, added_at) VALUES ('s3', 'https://x', 'b', 1000, 'X', 0, 0)`,
		`INSERT INTO queue (seq, bucket, key, op, class, state, size, created_at) VALUES
			(1, 'laptop', 'a', 'put', 'data', 'done', 10, 1), (2, 'laptop', 'b', 'put', 'data', 'done', 20, 1),
			(3, 'laptop', 'c', 'put', 'data', 'done', 40, 1), (4, 'laptop', 'c', 'put', 'data', 'done', 80, 1),
			(5, 'laptop', 'm', 'put', 'metadata', 'done', 5, 1), (6, 'laptop', 'd', 'put', 'data', 'done', 160, 1),
			(7, 'laptop', 'e', 'put', 'data', 'done', NULL, 1)`,
		`INSERT INTO placement (seq, place, bucket, key, op, state) VALUES
			(1, 'overflow:s3', 'laptop', 'a', 'put', 'acked'), (2, 'overflow:s3', 'laptop', 'b', 'put', 'queued'),
			(3, 'overflow:s3', 'laptop', 'c', 'put', 'acked'), (4, 'B', 'laptop', 'c', 'put', 'acked'),
			(5, 'overflow:s3', 'laptop', 'm', 'put', 'acked'), (6, 'overflow:s3', 'laptop', 'd', 'put', 'dropped'),
			(7, 'overflow:s3', 'laptop', 'e', 'put', 'acked'), (1, 'B', 'laptop', 'a', 'put', 'acked')`,
	} {
		if _, err := old.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	old.Close()
	db, err := Open(ctx, dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, diff, err := sameStatusReads(db, time.Now()); err != nil || diff != "" {
		t.Fatalf("after the upgrade: %s %v", diff, err)
	}
	if h, err := db.OverflowHolds(ctx); err != nil || h["s3"].DataObjects != 3 || h["s3"].DataBytes != 30 {
		t.Fatalf("holds %+v (%v), want a, b and e: 3 objects of 30 bytes", h, err)
	}
}

// #116: a fold by an older lendisk (schema 53's, which stores no below
// rows yet counts a fold and clears the marks) leaves the stored below
// rows unread: BelowCopies counts the bucket in full rather than trust
// rows that miss the changes it cleared, until the next fold stores
// them again.
func TestAnOlderLendisksFoldLeavesTheStoredBelowRowsUnread(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	p.friend("B", 1000)
	p.friend("C", 1000)
	seq := p.put("laptop", "k", "data", 0.01)
	p.ackAll(seq)
	p.fold()
	if below, err := p.db.BelowCopies(ctx); err != nil || len(below) != 0 {
		t.Fatalf("below copies %v %v, want none", below, err)
	}
	// The copy at C is refused, and an older lendisk's fold clears the
	// mark without storing below rows.
	if _, err := p.db.Exec(`UPDATE placement SET state = 'refused' WHERE place = 'C' AND seq = ?`, seq); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Exec(`UPDATE offsite_basis SET folds = folds + 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Exec(`DELETE FROM offsite_dirty`); err != nil {
		t.Fatal(err)
	}
	below, err := p.db.BelowCopies(ctx)
	if err != nil || below["laptop"].Objects != 1 {
		t.Fatalf("below copies %+v %v, want the refused object", below, err)
	}
	if n, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" || n != 0 {
		t.Fatalf("stored %d, %s %v: want the full count", n, diff, err)
	}
	p.fold()
	if n, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" || n != 1 {
		t.Fatalf("after a fold: stored %d, %s %v: want the stored rows again", n, diff, err)
	}
}
