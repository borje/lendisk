package state

import (
	"fmt"
	"testing"
)

// placeUseDiff compares place_use with what it counts (D6, #68): each
// place's queued and acked placement rows' size, and its overflow
// tombs' (#39). "" when they agree.
func placeUseDiff(db *DB) (string, error) {
	rows, err := db.Query(`WITH want (place, bytes) AS (
			SELECT place, sum(coalesce(size, 0)) FROM placement WHERE state IN ('queued', 'acked') GROUP BY place
			UNION ALL SELECT place, sum(size) FROM overflow_tomb GROUP BY place),
		w (place, bytes) AS (SELECT place, sum(bytes) FROM want GROUP BY place)
		SELECT w.place, w.bytes, coalesce(u.bytes, 0) FROM w LEFT JOIN place_use u ON u.place = w.place
		WHERE w.bytes != coalesce(u.bytes, 0)
		UNION ALL SELECT u.place, 0, u.bytes FROM place_use u
		WHERE u.bytes != 0 AND u.place NOT IN (SELECT place FROM w)`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	diff := ""
	for rows.Next() {
		var place string
		var want, got int64
		if err := rows.Scan(&place, &want, &got); err != nil {
			return "", err
		}
		diff += fmt.Sprintf("%s: place_use %d, rows %d; ", place, got, want)
	}
	return diff, rows.Err()
}

// D6, #117: place_use counts each place's rows whatever conflict policy
// the statement writing placement or overflow_tomb names. SQLite hands
// the outer statement's policy to the statements inside a trigger, so
// a trigger that relied on its own OR IGNORE failed an upsert or an OR
// FAIL write with "UNIQUE constraint failed: place_use.place", and
// reset the place's count to 0 under an OR REPLACE insert.
func TestPlaceUseIsKeptWhateverTheWritersConflictPolicy(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d := p.put("laptop", "data/1", "data", 1)
	e := p.put("laptop", "data/2", "data", 1)
	if diff, err := placeUseDiff(p.db); err != nil || diff != "" {
		t.Fatalf("before any write: %s %v", diff, err)
	}
	for i, w := range []struct {
		name, sql string
		args      []any
	}{
		{"an upsert acknowledging a row", `INSERT INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'B', 'laptop', 'data/1', 'put', 'acked', 5000)
			ON CONFLICT (place, seq) DO UPDATE SET state = excluded.state, size = excluded.size`, []any{d}},
		{"an upsert inserting a row", `INSERT INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'D', 'laptop', 'data/1', 'put', 'queued', 700)
			ON CONFLICT (place, seq) DO UPDATE SET size = excluded.size`, []any{d}},
		{"an upsert moving a row to a place with a count", `INSERT INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'D', 'laptop', 'data/1', 'put', 'queued', 700)
			ON CONFLICT (place, seq) DO UPDATE SET place = 'C', seq = ?`, []any{d, e + 100}},
		{"an OR FAIL insert", `INSERT OR FAIL INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'B', 'laptop', 'data/2', 'put', 'queued', 300)`, []any{e + 1}},
		{"an OR ABORT insert", `INSERT OR ABORT INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'C', 'laptop', 'data/2', 'put', 'acked', 400)`, []any{e + 1}},
		{"an OR ROLLBACK insert", `INSERT OR ROLLBACK INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'B', 'laptop', 'data/2', 'put', 'acked', 40)`, []any{e + 2}},
		{"an OR IGNORE insert", `INSERT OR IGNORE INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'C', 'laptop', 'data/2', 'put', 'queued', 60)`, []any{e + 2}},
		{"an OR REPLACE insert", `INSERT OR REPLACE INTO placement (seq, place, bucket, key, op, state, size)
			VALUES (?, 'B', 'laptop', 'data/2', 'put', 'queued', 900)`, []any{e + 3}},
		{"an OR FAIL update", `UPDATE OR FAIL placement SET state = 'acked', size = 1234 WHERE place = 'C' AND seq = ?`, []any{e}},
		{"an OR IGNORE update", `UPDATE OR IGNORE placement SET size = 77 WHERE place = 'B' AND seq = ?`, []any{e}},
		{"an OR REPLACE update to a new place", `UPDATE OR REPLACE placement SET place = 'E' WHERE place = 'B' AND seq = ?`, []any{e}},
		{"an OR FAIL tomb", `INSERT OR FAIL INTO overflow_tomb (place, seq, bucket, key, size, deleted_at, due_at)
			VALUES ('B', ?, 'laptop', 'data/1', 111, 0, 0)`, []any{d}},
		{"an OR REPLACE tomb", `INSERT OR REPLACE INTO overflow_tomb (place, seq, bucket, key, size, deleted_at, due_at)
			VALUES ('C', ?, 'laptop', 'data/1', 222, 0, 0)`, []any{d}},
		{"an upsert tomb", `INSERT INTO overflow_tomb (place, seq, bucket, key, size, deleted_at, due_at)
			VALUES ('E', ?, 'laptop', 'data/1', 333, 0, 0) ON CONFLICT (place, seq) DO NOTHING`, []any{d}},
	} {
		if _, err := p.db.Exec(w.sql, w.args...); err != nil {
			t.Fatalf("%d: %s: %v", i, w.name, err)
		}
		if diff, err := placeUseDiff(p.db); err != nil || diff != "" {
			t.Fatalf("%d: after %s: %s %v", i, w.name, diff, err)
		}
	}
}
