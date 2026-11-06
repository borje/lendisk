package state

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The admission lookup (SEC-10): a friendship row is the peer role, an
// unexpired, unused invite key the invite role, anything else none, and
// a closed database answers with an error.
func TestPeerRole(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	role := func(k string) Role {
		t.Helper()
		r, err := db.PeerRole(ctx, k, now)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := role("friend"); r != RoleNone {
		t.Fatalf("empty database: role %q", r)
	}
	mustExec(t, db, `INSERT INTO friendship (peer_key, created_at) VALUES ('friend', 1)`)
	mustExec(t, db, `INSERT INTO invite (invite_key, expires_at) VALUES ('fresh', ?), ('stale', ?)`, now.Unix()+60, now.Unix())
	mustExec(t, db, `INSERT INTO invite (invite_key, expires_at, used_at) VALUES ('used', ?, 1)`, now.Unix()+60)
	for k, want := range map[string]Role{"friend": RolePeer, "fresh": RoleInvite, "stale": RoleNone, "used": RoleNone, "stranger": RoleNone} {
		if r := role(k); r != want {
			t.Errorf("%s: role %q, want %q", k, r, want)
		}
	}
	db.Close()
	if _, err := db.PeerRole(ctx, "friend", now); err == nil {
		t.Fatal("closed database answered the lookup")
	}
}

func mustExec(t *testing.T, db *DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestOpenCreatesAt0600(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fi, err := os.Stat(filepath.Join(dir, FileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %04o, want 0600", fi.Mode().Perm())
	}
	v, c, err := db.Versions(context.Background())
	if err != nil || v != SchemaVersion() || c < 1 {
		t.Fatalf("versions %d %d %v", v, c, err)
	}
	if err := db.SetNode(context.Background(), "id", "x"); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := db.GetNode(context.Background(), "id"); !ok || got != "x" {
		t.Fatalf("GetNode = %q %v", got, ok)
	}
}

// Simulated release history for upgrade/downgrade tests.
var (
	r1 = []Migration{
		{Version: 1, Compat: 1, Release: "0.1", SQL: `CREATE TABLE a (x TEXT);`},
	}
	// 0.2 adds a table: additive, so 0.1 binaries keep working (compat 1).
	r2 = append(r1[:1:1],
		Migration{Version: 2, Compat: 1, Release: "0.2", SQL: `CREATE TABLE b (y TEXT); INSERT INTO a VALUES ('kept');`})
	// 0.3 drops a: breaking, so it raises compat to 2 (0.2 still works, 0.1 does not).
	r3 = append(r2[:2:2],
		Migration{Version: 3, Compat: 2, Release: "0.3", SQL: `CREATE TABLE c (z TEXT);`})
)

func TestUpgradeAppliesForwardAndBacksUp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := open(ctx, dir, r1, quiet)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = open(ctx, dir, r3, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, c, _ := db.Versions(ctx)
	if v != 3 || c != 2 {
		t.Fatalf("after upgrade: version %d compat %d", v, c)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM a`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("data from migration 2 missing: %d %v", n, err)
	}
	if _, err := os.Stat(filepath.Join(dir, FileName+".schema-1.bak")); err != nil {
		t.Fatalf("no pre-migration backup: %v", err)
	}
}

func TestDowngradeOneMinor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := open(ctx, dir, r2, quiet)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	// A 0.1 binary on a 0.2 database: allowed, and the schema is untouched.
	db, err = open(ctx, dir, r1, quiet)
	if err != nil {
		t.Fatalf("one-minor downgrade refused: %v", err)
	}
	v, _, _ := db.Versions(ctx)
	db.Close()
	if v != 2 {
		t.Fatalf("downgrade changed schema version to %d", v)
	}
	// Upgrading again works as normal.
	db, err = open(ctx, dir, r3, quiet)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	// A 0.1 binary on a 0.3 database: refused.
	if _, err := open(ctx, dir, r1, quiet); !errors.Is(err, ErrTooNew) {
		t.Fatalf("two-minor downgrade: %v", err)
	}
	// A 0.2 binary on a 0.3 database: allowed.
	db, err = open(ctx, dir, r2, quiet)
	if err != nil {
		t.Fatalf("0.2 on 0.3: %v", err)
	}
	db.Close()
}

func TestCheckMigrations(t *testing.T) {
	bad := [][]Migration{
		{{Version: 2, Compat: 1}},
		{{Version: 1, Compat: 2}},
		{{Version: 1, Compat: 1}, {Version: 2, Compat: 2}, {Version: 3, Compat: 1}},
	}
	for i, ms := range bad {
		if checkMigrations(ms) == nil {
			t.Errorf("case %d accepted", i)
		}
	}
	if err := checkMigrations(migrations); err != nil {
		t.Fatalf("built-in migrations: %v", err)
	}
}

// TestReleasedMigrationsCompat enforces the one-minor-back rule on the real
// history: a migration may only lock out schemas from at least two minor
// releases before its own.
func TestReleasedMigrationsCompat(t *testing.T) {
	lastOfRelease := map[string]int{}
	var order []string
	for _, m := range migrations {
		if _, ok := lastOfRelease[m.Release]; !ok {
			order = append(order, m.Release)
		}
		lastOfRelease[m.Release] = m.Version
	}
	idx := map[string]int{}
	for i, r := range order {
		idx[r] = i
	}
	for _, m := range migrations {
		i := idx[m.Release]
		if i == 0 {
			continue
		}
		prevLast := lastOfRelease[order[i-1]]
		if m.Compat > prevLast {
			t.Errorf("migration %d (release %s) has compat %d, locking out release %s (schema %d)",
				m.Version, m.Release, m.Compat, order[i-1], prevLast)
		}
	}
}
