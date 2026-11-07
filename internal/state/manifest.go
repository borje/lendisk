package state

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
)

// Node manifest bookkeeping (ID-4): the current version and, per friend,
// what it acknowledged. The manifest itself is built from the other
// tables each time; only its hash is kept.

// Node table keys of the current manifest version.
const (
	manifestSeqKey     = "manifest_seq"
	manifestHashKey    = "manifest_hash"
	manifestUpdatedKey = "manifest_updated_at"
)

// ManifestVersion is the manifest's current version: Seq counts content
// changes from 1, Hash is the content's hash, UpdatedAt when it changed.
// Seq is 0 before the first manifest.
type ManifestVersion struct {
	Seq       int64
	Hash      string
	UpdatedAt time.Time
}

// ManifestVersion reads the current version.
func (db *DB) ManifestVersion(ctx context.Context) (ManifestVersion, error) {
	return manifestVersion(ctx, db)
}

func manifestVersion(ctx context.Context, q querier) (ManifestVersion, error) {
	var v ManifestVersion
	get := func(key string) (string, error) {
		var s string
		err := q.QueryRowContext(ctx, `SELECT value FROM node WHERE key = ?`, key).Scan(&s)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return s, err
	}
	seq, err := get(manifestSeqKey)
	if err != nil || seq == "" {
		return v, err
	}
	if v.Seq, err = strconv.ParseInt(seq, 10, 64); err != nil {
		return v, err
	}
	if v.Hash, err = get(manifestHashKey); err != nil {
		return v, err
	}
	at, err := get(manifestUpdatedKey)
	if err != nil {
		return v, err
	}
	unix, _ := strconv.ParseInt(at, 10, 64)
	v.UpdatedAt = time.Unix(unix, 0)
	return v, nil
}

// NoteManifest records that the manifest's content hashes to hash: if
// that differs from the current version's, the version counts up and
// is stamped now. It returns the version in force after the call.
func (db *DB) NoteManifest(ctx context.Context, hash string, now time.Time) (ManifestVersion, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ManifestVersion{}, err
	}
	defer tx.Rollback()
	v, err := manifestVersion(ctx, tx)
	if err != nil || (v.Seq > 0 && v.Hash == hash) {
		return v, err
	}
	v = ManifestVersion{Seq: v.Seq + 1, Hash: hash, UpdatedAt: time.Unix(now.Unix(), 0)}
	for k, val := range map[string]string{
		manifestSeqKey:     strconv.FormatInt(v.Seq, 10),
		manifestHashKey:    hash,
		manifestUpdatedKey: strconv.FormatInt(now.Unix(), 10),
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO node (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, val); err != nil {
			return ManifestVersion{}, err
		}
	}
	return v, tx.Commit()
}

// ManifestPush is one friend's manifest delivery: Seq is the version it
// acknowledged (0 if none), Error the last attempt's failure, if it
// failed.
type ManifestPush struct {
	Seq         int64
	DeliveredAt time.Time // zero if never
	TriedAt     time.Time
	Error       string
	Failures    int64 // failed attempts since the last success
}

// ContinueManifest makes the version count on from seq, the version of
// a manifest fetched from a friend: if seq is above the current
// version, it becomes the current one with no hash, so the next
// NoteManifest counts seq+1 whatever the content. The version thus never
// goes back, and a recovered node's next manifest is above every copy
// its friends hold (ID-5). `recover` (#29) must call it with the fetched
// manifest's seq before the daemon first runs.
func (db *DB) ContinueManifest(ctx context.Context, seq int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	v, err := manifestVersion(ctx, tx)
	if err != nil || seq <= v.Seq {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO node (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, manifestSeqKey, strconv.FormatInt(seq, 10)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM node WHERE key = ?`, manifestHashKey); err != nil {
		return err
	}
	return tx.Commit()
}

// forgetManifestPush drops what peer acknowledged, when a friendship
// with it is committed (new or again): it may have lost its store, so
// it is pushed to as if it held nothing.
func forgetManifestPush(ctx context.Context, x execer, peer string) error {
	_, err := x.ExecContext(ctx, `DELETE FROM manifest_push WHERE peer_key = ?`, peer)
	return err
}

// ManifestDelivered records that peer acknowledged version seq.
func (db *DB) ManifestDelivered(ctx context.Context, peer string, seq int64, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO manifest_push (peer_key, seq, delivered_at, tried_at, error, failures)
		VALUES (?1, ?2, ?3, ?3, NULL, 0)
		ON CONFLICT (peer_key) DO UPDATE SET seq = ?2, delivered_at = ?3, tried_at = ?3, error = NULL, failures = 0`,
		peer, seq, now.Unix())
	return err
}

// ManifestPushFailed records a failed push to peer, keeping the version
// it acknowledged before.
func (db *DB) ManifestPushFailed(ctx context.Context, peer, msg string, now time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO manifest_push (peer_key, tried_at, error, failures) VALUES (?1, ?2, ?3, 1)
		ON CONFLICT (peer_key) DO UPDATE SET tried_at = ?2, error = ?3, failures = failures + 1`,
		peer, now.Unix(), msg)
	return err
}

// ManifestPushes reads every friend's delivery, by node ID. Rows of
// former friends are kept and harmless.
func (db *DB) ManifestPushes(ctx context.Context) (map[string]ManifestPush, error) {
	rows, err := db.QueryContext(ctx, `SELECT peer_key, seq, delivered_at, tried_at, error, failures FROM manifest_push`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ManifestPush{}
	for rows.Next() {
		var peer string
		var seq, delivered sql.NullInt64
		var tried, failures int64
		var msg sql.NullString
		if err := rows.Scan(&peer, &seq, &delivered, &tried, &msg, &failures); err != nil {
			return nil, err
		}
		p := ManifestPush{Seq: seq.Int64, TriedAt: time.Unix(tried, 0), Error: msg.String, Failures: failures}
		if delivered.Valid {
			p.DeliveredAt = time.Unix(delivered.Int64, 0)
		}
		out[peer] = p
	}
	return out, rows.Err()
}
