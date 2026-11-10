package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// tableColumn is one column of a table as SQLite reports it.
type tableColumn struct {
	Type    string
	NotNull bool
	Default bool // has a DEFAULT
	PK      int
}

// schema is a database's tables and their columns, and its unique
// indexes by table, as an older binary would meet them.
type schema struct {
	tables  map[string]map[string]tableColumn
	uniques map[string]map[string][]string // table -> unique index -> its columns
}

func readSchema(t *testing.T, db *DB) schema {
	t.Helper()
	s := schema{tables: map[string]map[string]tableColumn{}, uniques: map[string]map[string][]string{}}
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		names = append(names, n)
	}
	rows.Close()
	for _, n := range names {
		cols := map[string]tableColumn{}
		rows, err := db.Query(`SELECT name, type, "notnull", dflt_value IS NOT NULL, pk FROM pragma_table_info(?)`, n)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name string
			var c tableColumn
			if err := rows.Scan(&name, &c.Type, &c.NotNull, &c.Default, &c.PK); err != nil {
				t.Fatal(err)
			}
			cols[name] = c
		}
		rows.Close()
		s.tables[n] = cols
		rows, err = db.Query(`SELECT il.name, (SELECT group_concat(name) FROM pragma_index_info(il.name))
			FROM pragma_index_list(?) il WHERE il."unique" AND il.origin = 'c'`, n)
		if err != nil {
			t.Fatal(err)
		}
		s.uniques[n] = map[string][]string{}
		for rows.Next() {
			var idx, cols string
			rows.Scan(&idx, &cols)
			s.uniques[n][idx] = strings.Split(cols, ",")
		}
		rows.Close()
	}
	return s
}

// usableBy lists why a binary that knows only old cannot run on s: a
// table or column it reads is gone or changed, or a column or unique
// index it does not know would make its inserts fail. A new unique index
// that takes in a new column is harmless: the old binary leaves that
// column NULL, and NULLs never collide.
func (s schema) usableBy(old schema) []string {
	var why []string
	for tbl, oldCols := range old.tables {
		newCols, ok := s.tables[tbl]
		if !ok {
			why = append(why, "table "+tbl+" dropped")
			continue
		}
		for name, oc := range oldCols {
			nc, ok := newCols[name]
			switch {
			case !ok:
				why = append(why, fmt.Sprintf("column %s.%s dropped", tbl, name))
			case nc != oc:
				why = append(why, fmt.Sprintf("column %s.%s changed from %+v to %+v", tbl, name, oc, nc))
			}
		}
		for name, nc := range newCols {
			if _, ok := oldCols[name]; !ok && nc.NotNull && !nc.Default {
				why = append(why, fmt.Sprintf("new column %s.%s is NOT NULL without a default", tbl, name))
			}
		}
	indexes:
		for idx, cols := range s.uniques[tbl] {
			if _, ok := old.uniques[tbl][idx]; ok {
				continue
			}
			for _, c := range cols {
				if _, ok := oldCols[c]; !ok {
					continue indexes
				}
			}
			why = append(why, fmt.Sprintf("new unique index %s on %s(%s), columns the old binary writes", idx, tbl, strings.Join(cols, ", ")))
		}
	}
	return why
}

// OPS-8 on the real migration history (#44): upgrade migrates state, and
// the binary one step back still runs on the result. Each step takes a
// database forward from the schema before it, as an upgrade does. A
// step that keeps Compat below its version claims the previous schema's
// binary still works, so that binary must open it (downgrade mode, the
// schema untouched) and must find every table and column it knew
// unchanged, with nothing new that its inserts would trip on. A step
// that raises Compat must be refused by the binary it locks out. The
// previous minor release's binary is one such earlier schema, so this
// covers the one-minor downgrade for every release boundary; the
// release-level rule itself is TestReleasedMigrationsCompat.
func TestRealHistoryUpgradesAndKeepsTheEarlierSchemaUsable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var prev schema
	for v := 1; v <= len(migrations); v++ {
		db, err := open(ctx, dir, migrations[:v], quiet)
		if err != nil {
			t.Fatalf("upgrade to schema %d: %v", v, err)
		}
		if got, _, _ := db.Versions(ctx); got != v {
			t.Fatalf("upgrade to schema %d left version %d", v, got)
		}
		cur := readSchema(t, db)
		db.Close()
		if v == 1 {
			prev = cur
			continue
		}
		m := migrations[v-1]
		old, err := open(ctx, dir, migrations[:v-1], quiet)
		if m.Compat > v-1 {
			if !errors.Is(err, ErrTooNew) {
				t.Errorf("schema %d raises compat to %d, but the schema-%d binary was not refused: %v", v, m.Compat, v-1, err)
			}
			if old != nil {
				old.Close()
			}
			prev = cur
			continue
		}
		if err != nil {
			t.Fatalf("schema-%d binary on schema %d (compat %d): %v", v-1, v, m.Compat, err)
		}
		if got, _, _ := old.Versions(ctx); got != v {
			t.Errorf("downgrade to the schema-%d binary changed the version to %d", v-1, got)
		}
		old.Close()
		if why := cur.usableBy(prev); len(why) > 0 {
			t.Errorf("migration %d keeps compat %d but the schema-%d binary cannot use its result:\n  %s\n"+
				"make it additive (new tables, nullable columns), or raise Compat in a later minor release (state package comment)",
				v, m.Compat, v-1, strings.Join(why, "\n  "))
		}
		prev = cur
	}
}

// The schema comparison itself: what an older binary would trip on is
// found, and an additive step passes.
func TestSchemaComparisonFindsBreakingSteps(t *testing.T) {
	ctx := context.Background()
	base := Migration{Version: 1, Compat: 1, Release: "0.1",
		SQL: `CREATE TABLE a (x TEXT NOT NULL, y INTEGER); CREATE TABLE b (z TEXT);`}
	for _, tc := range []struct {
		sql    string
		breaks bool
	}{
		{`CREATE TABLE c (w TEXT NOT NULL); ALTER TABLE a ADD COLUMN n INTEGER; CREATE INDEX ai ON a (y);`, false},
		{`ALTER TABLE a ADD COLUMN d INTEGER NOT NULL DEFAULT 0;`, false},
		{`DROP TABLE b;`, true},
		{`ALTER TABLE a DROP COLUMN y;`, true},
		{`ALTER TABLE a RENAME COLUMN y TO yy;`, true},
		{`CREATE UNIQUE INDEX au ON a (y);`, true},
		{`ALTER TABLE a ADD COLUMN k TEXT; CREATE UNIQUE INDEX ak ON a (k);`, false},
		{`CREATE TABLE a2 (x TEXT NOT NULL, y TEXT); INSERT INTO a2 SELECT x, y FROM a; DROP TABLE a; ALTER TABLE a2 RENAME TO a;`, true},
	} {
		dir := t.TempDir()
		db, err := open(ctx, dir, []Migration{base}, quiet)
		if err != nil {
			t.Fatal(err)
		}
		before := readSchema(t, db)
		db.Close()
		db, err = open(ctx, dir, []Migration{base, {Version: 2, Compat: 1, Release: "0.2", SQL: tc.sql}}, quiet)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		why := readSchema(t, db).usableBy(before)
		db.Close()
		if (len(why) > 0) != tc.breaks {
			t.Errorf("%s: found %v, want breaking = %v", tc.sql, why, tc.breaks)
		}
	}
}
