package state

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"time"
)

// This file is the immutability policy (EP-11, #22): a key a place has
// acknowledged is not replaced by other bytes, unless its bucket's
// profile names it rewritable (EP-7). A changed key is found in two
// ways:
//   - at capture: FinishPut finds a place whose latest acknowledgement of
//     the key, since the key's last delete, is a put of another hash; the
//     write is done locally and placed nowhere (queue.immutable holds the
//     acknowledged hash);
//   - at a place: a lender refuses the put with a 412 because it holds
//     the key with another hash the index does not show (an answer lost,
//     or another friend's version after recovery); the worker records it
//     with RefuseChanged (cause CauseImmutable, placement.held the hash
//     the place reported).
//
// Either is a condition, not an event: ChangedKeys reports it while the
// changed write is the key's latest done write. It ends when the key is
// written again (back to the acknowledged bytes, which heals the row the
// change replaced without a transfer) or deleted, after which a write is
// a new object. The alert (alert.KindKeyChanged) and status follow it.

// ChangedKey is a write of a key that changes what a place holds,
// replicated nowhere it would replace that (EP-11).
type ChangedKey struct {
	Bucket, Key string
	Seq         int64 // the changed write's queue row
	CapturedAt  time.Time
	SHA256      string // the changed write's hash, held locally
	// Acknowledged is the hash a place acknowledged (or, after a 412,
	// said it holds); empty when the place's answer did not say.
	Acknowledged string
	// Places maps each place holding another version to its hash, as
	// Acknowledged.
	Places map[string]string
}

// ackedOther returns the places whose latest acknowledgement of bucket's
// key before row seq is a put, since the key's last done delete before
// seq, of another hash than sha, with that hash; and the hash of the
// latest such acknowledgement. Only friendships count: a friend no
// longer one is no replica.
func ackedOther(ctx context.Context, q querier, bucket, key string, seq int64, sha string) (map[string]string, string, error) {
	rows, err := q.QueryContext(ctx, `SELECT a.place, a.sha256 FROM queue k
		JOIN placement a ON a.seq = k.seq
		JOIN friendship f ON f.peer_key = a.place
		WHERE k.bucket = ?1 AND k.key = ?2 AND k.seq < ?3 AND k.op = 'put' AND k.state = 'done'
		AND k.seq > coalesce((SELECT max(d.seq) FROM queue d WHERE d.bucket = ?1 AND d.key = ?2
			AND d.op = 'delete' AND d.state = 'done' AND d.seq < ?3), 0)
		AND a.state = 'acked' AND a.sha256 != ?4
		AND a.seq = (SELECT max(x.seq) FROM placement x WHERE x.place = a.place AND x.bucket = ?1 AND x.key = ?2
			AND x.state = 'acked' AND x.seq < ?3)
		ORDER BY a.seq DESC, a.place`, bucket, key, seq, sha)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out, latest := map[string]string{}, ""
	for rows.Next() {
		var place, h string
		if err := rows.Scan(&place, &h); err != nil {
			return nil, "", err
		}
		if len(out) == 0 {
			latest = h
		}
		out[place] = h
	}
	return out, latest, rows.Err()
}

// ChangedKeys returns the changed keys (EP-11) whose changed write is
// still the key's latest done write, by bucket, key and sequence.
func (db *DB) ChangedKeys(ctx context.Context) ([]ChangedKey, error) {
	bySeq := map[int64]*ChangedKey{}
	rows, err := db.QueryContext(ctx, `SELECT q.bucket, q.key, q.seq, q.created_at, q.sha256, q.immutable
		FROM queue q WHERE q.immutable IS NOT NULL AND q.state = 'done' AND `+live)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ChangedKey
		var at int64
		if err := rows.Scan(&c.Bucket, &c.Key, &c.Seq, &at, &c.SHA256, &c.Acknowledged); err != nil {
			rows.Close()
			return nil, err
		}
		c.CapturedAt = time.Unix(at, 0)
		bySeq[c.Seq] = &c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, c := range bySeq {
		// Where it is held now; the mark keeps the hash if, since, every
		// such friendship ended.
		places, _, err := ackedOther(ctx, db, c.Bucket, c.Key, c.Seq, c.SHA256)
		if err != nil {
			return nil, err
		}
		c.Places = places
	}
	rows, err = db.QueryContext(ctx, `SELECT q.bucket, q.key, q.seq, q.created_at, q.sha256, p.place, coalesce(p.held, '')
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.state = 'refused' AND p.cause = '`+CauseImmutable+`' AND `+current)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var c ChangedKey
		var at int64
		var place, held string
		if err := rows.Scan(&c.Bucket, &c.Key, &c.Seq, &at, &c.SHA256, &place, &held); err != nil {
			return nil, err
		}
		if bySeq[c.Seq] == nil {
			c.CapturedAt, c.Places = time.Unix(at, 0), map[string]string{}
			bySeq[c.Seq] = &c
		}
		bySeq[c.Seq].Places[place] = held
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]ChangedKey, 0, len(bySeq))
	for _, c := range bySeq {
		if c.Acknowledged == "" {
			for _, p := range slices.Sorted(maps.Keys(c.Places)) {
				if c.Acknowledged = c.Places[p]; c.Acknowledged != "" {
					break
				}
			}
		}
		out = append(out, *c)
	}
	slices.SortFunc(out, func(a, b ChangedKey) int {
		return cmp.Or(cmp.Compare(a.Bucket, b.Bucket), cmp.Compare(a.Key, b.Key), cmp.Compare(a.Seq, b.Seq))
	})
	return out, nil
}
