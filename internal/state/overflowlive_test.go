package state

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #120: with a copy at an overflow target, a write to a key recomputes
// the key's copies from its latest done write on, found by queue_key's
// range on (bucket, key, seq), not from every version of the key: a
// generic S3 key overwritten very often does not get slower to write.
// Each queue trigger of overflow_live names the bound, and SQLite plans
// the recompute's read as that range.
func TestAWriteToAKeyReadsItsVersionsOnlyFromTheLatestDoneOne(t *testing.T) {
	db := openTest(t)
	for _, name := range []string{"overflow_live_queue_insert", "overflow_live_queue_update", "overflow_live_queue_delete"} {
		var sql string
		if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?`, name).Scan(&sql); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.Contains(sql, "AND q.seq >= coalesce((SELECT l.seq FROM queue l") {
			t.Errorf("%s recomputes every version of the key:\n%s", name, sql)
		}
	}
	add := overflowKeyAdd58("?1", "?2", "")[0]
	rows, err := db.Query(`EXPLAIN QUERY PLAN `+add, "laptop", "hot")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(plan, "\n"), "SEARCH q USING INDEX queue_key (bucket=? AND key=? AND seq>?)") {
		t.Errorf("the recompute's plan:\n%s", strings.Join(plan, "\n"))
	}
}

// #120: what a write to a key with many versions, each copied to an
// overflow target, costs at schema 57 (every version recomputed) and
// at schema 58 (from the latest done write on), against a write to a
// fresh key. Opt-in:
//
//	LENDISK_BIG_VERSIONS=100000 go test -run TestAWriteToAKeyOfManyVersionsCostsAsAFreshKeys -v ./internal/state
func TestAWriteToAKeyOfManyVersionsCostsAsAFreshKeys(t *testing.T) {
	versions, _ := strconv.Atoi(os.Getenv("LENDISK_BIG_VERSIONS"))
	if versions <= 0 {
		t.Skip("LENDISK_BIG_VERSIONS is not set")
	}
	ctx := context.Background()
	dir := t.TempDir()
	db, err := open(ctx, dir, migrations[:57], quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mustExec(t, db, `INSERT INTO overflow (name, endpoint, bucket, max_bytes, env_prefix, retention, added_at)
		VALUES ('s3', 'https://x', 'b', 1125899906842624, 'X', 0, 0)`)
	start := time.Now()
	mustExec(t, db, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?1)
		INSERT INTO queue (bucket, key, op, class, state, size, created_at)
		SELECT 'laptop', 'hot', 'put', 'data', 'done', 1000, 0 FROM n`, versions)
	mustExec(t, db, `INSERT INTO placement (seq, place, bucket, key, op, state, acked_at)
		SELECT seq, 'overflow:s3', bucket, key, 'put', 'acked', 0 FROM queue`)
	t.Logf("seeded %d versions of a key at a target in %v", versions, time.Since(start))
	const writes = 50
	write := func(what, key string) {
		t.Helper()
		start := time.Now()
		for range writes {
			mustExec(t, db, `INSERT INTO queue (bucket, key, op, class, state, size, created_at)
				VALUES ('laptop', ?, 'put', 'data', 'done', 1000, 0)`, key)
			mustExec(t, db, `INSERT INTO placement (seq, place, bucket, key, op, state, acked_at)
				SELECT max(seq), 'overflow:s3', bucket, key, 'put', 'acked', 0 FROM queue`)
		}
		t.Logf("%s: %v a write (and its copy)", what, time.Since(start)/writes)
	}
	write("schema 57, the key of many versions", "hot")
	write("schema 57, a fresh key", "cold")
	start = time.Now()
	if err := db.apply(ctx, migrations[57]); err != nil {
		t.Fatal(err)
	}
	t.Logf("migration 58 took %v", time.Since(start))
	write("schema 58, the key of many versions", "hot")
	write("schema 58, a fresh key", "fresh")
	var live int
	if err := db.QueryRow(`SELECT objects FROM overflow_hold WHERE place = 'overflow:s3'`).Scan(&live); err != nil || live != 3 {
		t.Fatalf("the target holds %d live copies (%v), want one per key", live, err)
	}
}
