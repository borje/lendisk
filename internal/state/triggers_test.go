package state

import (
	"regexp"
	"strings"
	"testing"
)

// REPL-11, D6, #118: a REPLACE that deletes a row on a taken key
// (queue's seq, placement's and overflow_tomb's place and seq) fires
// the delete triggers (recursive_triggers is on), so queue_tally,
// place_use and the offsite and below-copies marks drop the deleted
// row as they do for a DELETE. Before, the deleted row stayed counted
// in the tally and in place_use, and its key unmarked: the stored
// watermark and below-copies rows of a key that has no put any more
// were read as if it still had.
func TestAReplaceOntoATakenKeyCountsTheRowItDeletes(t *testing.T) {
	for _, w := range []struct {
		name, sql string
		args      func(a, b, c, d, e int64) []any
	}{
		{"an OR REPLACE insert of another key onto a queue row's seq", `INSERT OR REPLACE INTO queue
			(seq, bucket, key, op, class, state, size, created_at) VALUES (?, 'laptop', 'data/x', 'put', 'data', 'done', 7, 1)`,
			func(a, b, c, d, e int64) []any { return []any{c} }},
		{"an OR REPLACE renumbering onto a queue row's seq", `UPDATE OR REPLACE queue SET seq = ? WHERE seq = ?`,
			func(a, b, c, d, e int64) []any { return []any{d, e} }},
		{"an OR REPLACE insert onto a placement row", `INSERT OR REPLACE INTO placement
			(seq, place, bucket, key, op, state, size) VALUES (?, 'C', 'laptop', 'data/a', 'put', 'refused', 0)`,
			func(a, b, c, d, e int64) []any { return []any{a} }},
		{"an OR REPLACE move onto a placement row", `UPDATE OR REPLACE placement SET place = 'C' WHERE place = 'B' AND seq = ?`,
			func(a, b, c, d, e int64) []any { return []any{b} }},
		{"an OR REPLACE insert onto a tomb", `INSERT OR REPLACE INTO overflow_tomb (place, seq, bucket, key, size, deleted_at, due_at)
			VALUES ('B', ?, 'laptop', 'data/a', 9, 0, 0)`, func(a, b, c, d, e int64) []any { return []any{a} }},
	} {
		t.Run(w.name, func(t *testing.T) {
			p := newPool(t)
			p.bucket("laptop", 2)
			p.friend("B", 100)
			p.friend("C", 100)
			a := p.put("laptop", "data/a", "data", 1)
			b := p.put("laptop", "data/b", "data", 1)
			c := p.put("laptop", "data/c", "data", 1)
			d := p.put("laptop", "data/d", "data", 1)
			e := p.put("laptop", "data/e", "data", 1)
			// c, d and e unplaced, so a queue row written onto their seq
			// is not read with placement rows of another key.
			mustExec(t, p.db, `DELETE FROM placement WHERE seq IN (?, ?, ?)`, c, d, e)
			p.ack("B", a)
			p.ack("C", a)
			p.ack("B", b)
			mustExec(t, p.db, `INSERT INTO overflow_tomb (place, seq, bucket, key, size, deleted_at, due_at)
				VALUES ('B', ?, 'laptop', 'data/a', 500, 0, 0)`, a)
			// Stored first, so a key the write leaves unmarked is read
			// from rows that no longer hold.
			p.fold()
			if _, err := p.db.Exec(w.sql, w.args(a, b, c, d, e)...); err != nil {
				t.Fatal(err)
			}
			if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" {
				t.Errorf("%s %v", diff, err)
			}
			if _, diff, err := sameOffsite(p.db, "laptop", 0); err != nil || diff != "" {
				t.Errorf("offsite: %s %v", diff, err)
			}
			if diff, err := placeUseDiff(p.db); err != nil || diff != "" {
				t.Errorf("%s %v", diff, err)
			}
			// The fold stores what the write left.
			p.fold()
			if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" {
				t.Errorf("after the next fold: %s %v", diff, err)
			}
		})
	}
}

// #118: with recursive_triggers on, a trigger that wrote a table with
// triggers of its own could fire itself again. None does: every table a
// trigger writes (INSERT INTO, UPDATE, DELETE FROM) has no trigger, in
// the schema as migrated. And Open's connections have the pragma on.
func TestNoTriggerWritesATableThatHasTriggers(t *testing.T) {
	db := openTest(t)
	var on int
	if err := db.QueryRow(`PRAGMA recursive_triggers`).Scan(&on); err != nil || on != 1 {
		t.Fatalf("recursive_triggers %d (%v), want 1", on, err)
	}
	rows, err := db.Query(`SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type trigger struct{ name, table, sql string }
	var ts []trigger
	triggered := map[string]bool{}
	for rows.Next() {
		var tr trigger
		if err := rows.Scan(&tr.name, &tr.table, &tr.sql); err != nil {
			t.Fatal(err)
		}
		ts = append(ts, tr)
		triggered[strings.ToLower(tr.table)] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ts) < 10 {
		t.Fatalf("only %d triggers found", len(ts))
	}
	writes := regexp.MustCompile(`(?is)(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE(?:\s+OR\s+\w+)?|DELETE\s+FROM)\s+(\w+)`)
	written := map[string]bool{}
	for _, tr := range ts {
		body := tr.sql[strings.Index(strings.ToUpper(tr.sql), "BEGIN"):]
		for _, m := range writes.FindAllStringSubmatch(body, -1) {
			written[strings.ToLower(m[1])] = true
			if triggered[strings.ToLower(m[1])] {
				t.Errorf("trigger %s on %s writes %s, which has triggers", tr.name, tr.table, m[1])
			}
		}
	}
	for _, w := range []string{"place_use", "place_freed", "offsite_dirty", "queue_tally", "overflow_live", "overflow_hold"} {
		if !written[w] {
			t.Errorf("no trigger found writing %s: the check reads the triggers wrong (%v)", w, written)
		}
	}
}
