// Package state owns lendisk.db, the SQLite database holding everything the
// daemon creates or learns (OPS-6), and its schema migrations (OPS-8).
//
// Migration rule. Schema versions are integers, one per migration, applied
// forward automatically on open. Each migration declares Compat: the oldest
// schema version whose binary can still run safely on the database after it.
// A binary that finds a database newer than it knows opens it if the
// database's compat is at or below the binary's own schema, and refuses
// otherwise. Downgrade by one minor version (OPS-8) is kept by writing every
// migration within a minor release so that it stays usable by the previous
// minor's last schema: add tables and nullable columns, never drop or
// repurpose. A migration that cannot be written that way raises Compat, and
// may do so only in the release after the one whose schema it locks out.
//
// Trigger rule. Triggers on queue, placement and overflow_tomb keep
// queue_tally, place_use, place_freed and the offsite marks
// (offsite_dirty) in every writer's transaction; readers trust them. A
// migration that rebuilds one of those tables (create, copy, drop,
// rename) drops its triggers with it: it must create them again and
// refill what they keep from the rows, as migrations 12, 26, 53 and 54
// fill them, in the same migration. The daemon repairs queue_tally
// drift hourly (RepairQueueTally), but nothing else is checked. A
// trigger names no conflict policy and must meet no conflict (insert
// where missing, WHERE NOT EXISTS): the writer's policy overrides the
// trigger's own (#117). Open turns recursive_triggers on, so a REPLACE
// that deletes a row fires the delete triggers (#118), and a trigger
// that writes its own table (or one whose triggers write it back) fires
// itself again: none does.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// FileName is the database's file name inside the data directory.
const FileName = "lendisk.db"

// Migration is one forward schema step.
type Migration struct {
	Version int    // 1, 2, 3, … with no gaps
	Compat  int    // oldest schema that can use the database after this step
	Release string // the lendisk minor release that introduced it, e.g. "0.1"
	SQL     string
}

// DB is an open lendisk.db.
type DB struct {
	*sql.DB
	Path string
	// Clock stamps the rows the queue writes (created_at, acked_at); nil
	// means time.Now. Tests set it.
	Clock func() time.Time
	// RerouteAfter and ReplaceAfter are replication.reroute_after and
	// replication.replace_after (D6, §3.4): how long a friend may fail
	// before its unsent data goes elsewhere, and before it is no longer
	// room at all. Zero means 1 hour and 7 days.
	RerouteAfter, ReplaceAfter time.Duration
	// KeyChanged, if set, is called after FinishPut commits a write that
	// the immutability policy holds back (EP-11, #22), on the writer's
	// goroutine: it must not block. The daemon logs it and asks the
	// alert service to look.
	KeyChanged func(ChangedKey)

	// bucketLocks holds a *sync.Mutex per bucket, shared by the placer
	// and re-placement (lockBucket, #26).
	bucketLocks sync.Map

	// split caches overflowSplit (#38) for splitTTL (default
	// splitCacheTTL; below zero, no cache: tests).
	splitMu  sync.Mutex
	splitAt  time.Time
	splitVal bool
	splitKey string
	splitTTL time.Duration

	// beforeApply, if set, runs before each re-placement batch takes the
	// bucket's lock: a capture racing the pass. Tests only.
	beforeApply func(bucket string, meta bool)
	// beforeSeedAck, if set, runs in ConfirmSeeded's transaction between
	// finding a listed key's queued rows and acknowledging them. Tests
	// only.
	beforeSeedAck func(tx *sql.Tx)
	// betweenFoldBatches, if set, runs between the write transactions of
	// an offsite fold (#81); foldTook, if set, is told how long each of
	// them held the write lock. Tests only.
	betweenFoldBatches func()
	foldTook           func(time.Duration)
	// beforeTallyRepair, if set, runs between RepairQueueTally's read
	// and its write (#118). Tests only.
	beforeTallyRepair func()

	// snap, on a Snapshot, is the read transaction every query goes
	// through.
	snap *sql.Tx
}

// errSnapshot refuses a write through a Snapshot.
var errSnapshot = errors.New("state: a snapshot is read-only")

// Snapshot returns a read-only view of db: every read through it sees
// the database as of the view's first read, so a report built from many
// reads is never torn by a write committing between two of them (#110).
// It is one deferred read transaction (BEGIN, not the DSN's IMMEDIATE),
// so in WAL mode writers are not blocked while it is open. The view's
// own writes and transactions fail. Callers must call done, which ends
// the transaction, and must not use the view after it. Reads use the
// context methods (QueryContext, QueryRowContext); the package's own
// read methods all do.
func (db *DB) Snapshot(ctx context.Context) (view *DB, done func() error, err error) {
	if db.snap != nil {
		return nil, nil, errors.New("state: a snapshot within a snapshot")
	}
	tx, err := db.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	// No overflowSplit cache: it would outlive the transaction.
	view = &DB{DB: db.DB, Path: db.Path, Clock: db.Clock, RerouteAfter: db.RerouteAfter, ReplaceAfter: db.ReplaceAfter,
		splitTTL: -1, snap: tx}
	return view, tx.Rollback, nil
}

// QueryContext runs a query, in the snapshot's transaction on a
// Snapshot.
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if db.snap != nil {
		return db.snap.QueryContext(ctx, query, args...)
	}
	return db.DB.QueryContext(ctx, query, args...)
}

// QueryRowContext runs a one-row query, in the snapshot's transaction
// on a Snapshot.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if db.snap != nil {
		return db.snap.QueryRowContext(ctx, query, args...)
	}
	return db.DB.QueryRowContext(ctx, query, args...)
}

// ExecContext runs a statement; a Snapshot refuses it.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if db.snap != nil {
		return nil, errSnapshot
	}
	return db.DB.ExecContext(ctx, query, args...)
}

// BeginTx begins a transaction; a Snapshot refuses it.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if db.snap != nil {
		return nil, errSnapshot
	}
	return db.DB.BeginTx(ctx, opts)
}

func (db *DB) now() time.Time {
	if db.Clock == nil {
		return time.Now()
	}
	return db.Clock()
}

// ErrTooNew means the database was migrated by a release this binary
// cannot run against.
var ErrTooNew = errors.New("state database is too new for this lendisk version")

// Open opens (creating at mode 0600 if absent) dataDir/lendisk.db and
// brings its schema up to date with the built-in migrations.
func Open(ctx context.Context, dataDir string, log *slog.Logger) (*DB, error) {
	return open(ctx, dataDir, migrations, log)
}

func open(ctx context.Context, dataDir string, ms []Migration, log *slog.Logger) (*DB, error) {
	if err := checkMigrations(ms); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, FileName)
	// Create the file ourselves so its mode is 0600 from the first byte.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	// Write transactions begin IMMEDIATE: they take the write lock at
	// BEGIN, waiting up to busy_timeout for it, so a transaction that
	// reads and then writes cannot fail with SQLITE_BUSY_SNAPSHOT when
	// another connection commits in between (#47). Every write
	// transaction is short (no I/O but SQLite's own inside one), so the
	// timeout is a queue of writers, not a held lock.
	//
	// recursive_triggers is on so that a REPLACE that deletes a row to
	// make room fires the table's delete triggers (#118): without it
	// SQLite deletes the row silently, and queue_tally, place_use and the
	// offsite marks keep counting it. No trigger writes a table that has
	// triggers, so none recurses (the package comment's trigger rule).
	dsn := "file:" + path + "?_txlock=immediate&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)&_pragma=recursive_triggers(1)"
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db := &DB{DB: sqldb, Path: path}
	if err := db.migrate(ctx, ms, log); err != nil {
		sqldb.Close()
		return nil, err
	}
	return db, nil
}

func checkMigrations(ms []Migration) error {
	for i, m := range ms {
		if m.Version != i+1 {
			return fmt.Errorf("migration %d has version %d", i+1, m.Version)
		}
		if m.Compat < 1 || m.Compat > m.Version {
			return fmt.Errorf("migration %d: compat %d out of range", m.Version, m.Compat)
		}
		if i > 0 && m.Compat < ms[i-1].Compat {
			return fmt.Errorf("migration %d: compat goes backwards", m.Version)
		}
	}
	return nil
}

// SchemaVersion is the newest schema this binary knows.
func SchemaVersion() int { return len(migrations) }

func (db *DB) migrate(ctx context.Context, ms []Migration, log *slog.Logger) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_meta (
		id      INTEGER PRIMARY KEY CHECK (id = 1),
		version INTEGER NOT NULL,
		compat  INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	cur, compat, err := db.versions(ctx)
	if err != nil {
		return err
	}
	known := len(ms)
	if cur > known {
		if compat > known {
			return fmt.Errorf("%w: database schema %d requires schema %d or later, this binary has %d; upgrade lendisk",
				ErrTooNew, cur, compat, known)
		}
		log.Warn("state database is newer than this binary; running in downgrade mode",
			"db_schema", cur, "binary_schema", known)
		return nil
	}
	if cur == known {
		return nil
	}
	if cur > 0 {
		// The backup holds bucket secrets: create it empty at 0600, which
		// VACUUM INTO accepts, so it is never readable by others (#53).
		backup := fmt.Sprintf("%s.schema-%d.bak", db.Path, cur)
		if err := os.Remove(backup); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("backing up state before migration: removing the old backup: %w", err)
		}
		f, err := os.OpenFile(backup, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("backing up state before migration: %w", err)
		}
		f.Close()
		if _, err := db.ExecContext(ctx, `VACUUM INTO ?`, backup); err != nil {
			return fmt.Errorf("backing up state before migration: %w", err)
		}
		log.Info("backed up state database before migration", "path", backup)
	}
	for _, m := range ms[cur:] {
		if err := db.apply(ctx, m); err != nil {
			return fmt.Errorf("migration %d: %w", m.Version, err)
		}
		log.Info("applied state migration", "version", m.Version, "release", m.Release)
	}
	return nil
}

func (db *DB) versions(ctx context.Context) (version, compat int, err error) {
	err = db.QueryRowContext(ctx, `SELECT version, compat FROM schema_meta WHERE id = 1`).Scan(&version, &compat)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return version, compat, err
}

func (db *DB) apply(ctx context.Context, m Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta (id, version, compat) VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET version = excluded.version, compat = excluded.compat`,
		m.Version, m.Compat); err != nil {
		return err
	}
	return tx.Commit()
}

// Versions reports the database's schema version and compat level.
func (db *DB) Versions(ctx context.Context) (version, compat int, err error) {
	return db.versions(ctx)
}

// GetNode reads a value from the node table; ok is false if unset.
func (db *DB) GetNode(ctx context.Context, key string) (value string, ok bool, err error) {
	err = db.QueryRowContext(ctx, `SELECT value FROM node WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

// SetNode writes a value to the node table.
func (db *DB) SetNode(ctx context.Context, key, value string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO node (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}
