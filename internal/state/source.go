package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// The read-through (EP-10, ID-6, #30): where an object missing from the
// local store can be fetched, and which keys a bucket holds although
// the local store lacks them (a recovered node lists them before it has
// read them).

// Source is where an object can be fetched from: the version of the key
// that counts (its latest done write, a put), and the places that
// acknowledged holding exactly that hash.
type Source struct {
	Size   int64
	SHA256 string
	// Places, most recently reachable first: by the place's last
	// acknowledgement (place.last_ok), then never-acknowledged ones, by
	// node ID.
	Places []SourcePlace
}

// SourcePlace is a place holding a source's version.
type SourcePlace struct {
	Place  string
	LastOK time.Time // zero if the place never answered
}

// Source returns where key of bucket can be fetched from. ok is false
// when no version counts: the key is unknown, its latest write is a
// delete, or recovery holds it in conflict (no queue row). A version
// with no place holding it has ok and no Places.
func (db *DB) Source(ctx context.Context, bucket, key string) (Source, bool, error) {
	var s Source
	var op string
	var sha sql.NullString
	var size sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT op, sha256, size FROM queue
		WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, bucket, key).Scan(&op, &sha, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return s, false, nil
	}
	if err != nil {
		return s, false, err
	}
	if op != OpPut || sha.String == "" {
		return s, false, nil
	}
	s.SHA256, s.Size = sha.String, size.Int64
	// A place holds the version when its latest acknowledged row of the
	// key is a put of that hash.
	rows, err := db.QueryContext(ctx, `SELECT p.place, pl.last_ok FROM placement p LEFT JOIN place pl ON pl.place = p.place
		WHERE p.bucket = ? AND p.key = ? AND p.state = 'acked' AND p.op = 'put' AND p.sha256 = ?
		AND NOT EXISTS (SELECT 1 FROM placement l WHERE l.place = p.place AND l.bucket = p.bucket AND l.key = p.key
			AND l.state IN ('acked', 'drained') AND l.seq > p.seq)
		GROUP BY p.place
		ORDER BY pl.last_ok IS NULL, pl.last_ok DESC, p.place`, bucket, key, s.SHA256)
	if err != nil {
		return s, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var p SourcePlace
		var last sql.NullInt64
		if err := rows.Scan(&p.Place, &last); err != nil {
			return s, false, err
		}
		if last.Valid {
			p.LastOK = time.Unix(last.Int64, 0)
		}
		s.Places = append(s.Places, p)
	}
	return s, true, rows.Err()
}

// LiveKey is a key of a bucket as the index knows it.
type LiveKey struct {
	Key      string
	Size     int64
	Modified time.Time // when the version was captured or listed; zero for a conflict
}

// LiveKeys returns up to limit keys of bucket beginning with prefix and
// sorting after after, in key order: each key whose latest done write is
// a put, and each recovered key in conflict (listed with the largest
// size a place listed), so a read of it is refused with the reason
// rather than not found. Keys are compared as bytes, as S3 orders them.
func (db *DB) LiveKeys(ctx context.Context, bucket, prefix, after string, limit int) ([]LiveKey, error) {
	// Each branch walks its index in key order and stops at limit, so a
	// page costs O(limit), not O(keys after the marker) (#30 review).
	rows, err := db.QueryContext(ctx, `SELECT key, size, modified FROM (
		SELECT * FROM (
			SELECT q.key AS key, coalesce(q.size, 0) AS size, q.created_at AS modified FROM queue q
			WHERE q.bucket = ?1 AND q.key >= ?3 AND q.key > ?2 AND substr(q.key, 1, length(?3)) = ?3
				AND q.state = 'done' AND q.op = 'put'
				AND NOT EXISTS (SELECT 1 FROM queue l WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
			ORDER BY q.key LIMIT ?4)
		UNION ALL
		SELECT * FROM (
			SELECT k.key AS key, coalesce((SELECT max(size) FROM listed l WHERE l.bucket = k.bucket AND l.key = k.key), 0) AS size, NULL AS modified
			FROM recovered_key k WHERE k.bucket = ?1 AND k.key >= ?3 AND k.key > ?2 AND substr(k.key, 1, length(?3)) = ?3
				AND `+inConflict+`
			ORDER BY k.key LIMIT ?4))
		ORDER BY key LIMIT ?4`, bucket, after, prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LiveKey
	for rows.Next() {
		var k LiveKey
		var mod sql.NullInt64
		if err := rows.Scan(&k.Key, &k.Size, &mod); err != nil {
			return nil, err
		}
		if mod.Valid {
			k.Modified = time.Unix(mod.Int64, 0)
		}
		if n := len(out); n > 0 && out[n-1].Key == k.Key {
			continue
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
