package state

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"slices"
	"time"
)

// This file is pooled placement (D6, REPL-1, REPL-12, #19): where each
// captured row goes, decided in the transaction that completes it; the
// metadata barrier; and re-routing unsent data from a friend that stopped
// answering.

const (
	// DefaultCopies is a bucket's copies when it has no row (D6, EP-6).
	DefaultCopies = 2
	// ObjectOverhead is what the lender counts per object besides its
	// bytes (gateway's itemOverhead, LEND-1), so the borrower's count of a
	// place's use matches the lender's.
	ObjectOverhead = 4096
)

func (db *DB) rerouteAfter() time.Duration { return cmp.Or(db.RerouteAfter, time.Hour) }
func (db *DB) replaceAfter() time.Duration { return cmp.Or(db.ReplaceAfter, 7*24*time.Hour) }

// poolPlace is a friend with an address as placement sees it.
type poolPlace struct {
	peer   string
	quota  int64 // the friendship's quota (LEND-8)
	use    int64 // place_use: what it holds or will hold
	cond   string
	since  time.Time // when cond began
	lastOK time.Time // zero if never
}

func (p poolPlace) failing() bool {
	return p.cond == ConditionRetrying || p.cond == ConditionRemoved
}

// unreachable: failing for reroute_after. A friend failing for less is
// still counted reachable: a blip is not an outage.
func (p poolPlace) unreachable(db *DB, now time.Time) bool {
	return p.failing() && now.Sub(p.since) >= db.rerouteAfter()
}

// gone: unreachable, and silent for replace_after since its last
// answer (or its first failure, if it never answered). It is no longer
// room (REPL-12); re-placement (REPL-13, #26) moves what it held.
func (p poolPlace) gone(db *DB, now time.Time) bool {
	from := p.lastOK
	if from.IsZero() || from.After(p.since) {
		from = p.since
	}
	return p.unreachable(db, now) && now.Sub(from) >= db.replaceAfter()
}

// full: its last answer was 507 (LEND-2). The lender's word corrects the
// borrower's count: no new copies go there until it acknowledges again,
// which the metadata every backup sends there finds out (§3.4).
func (p poolPlace) full() bool { return p.cond == ConditionQuota }

// reroutable: its unsent data should go elsewhere if there is room.
func (p poolPlace) reroutable(db *DB, now time.Time) bool {
	return p.unreachable(db, now) || p.full()
}

func poolPlaces(ctx context.Context, tx *sql.Tx) ([]poolPlace, error) {
	rows, err := tx.QueryContext(ctx, `SELECT f.peer_key,
		min(coalesce(f.own_number, 0), coalesce(f.peer_number, 0)), coalesce(u.bytes, 0),
		coalesce(c.condition, ''), coalesce(c.since, 0), c.last_ok
		FROM friendship f LEFT JOIN place_use u ON u.place = f.peer_key LEFT JOIN place c ON c.place = f.peer_key
		WHERE coalesce(f.address, '') != '' ORDER BY f.peer_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []poolPlace
	for rows.Next() {
		var p poolPlace
		var since int64
		var lastOK sql.NullInt64
		if err := rows.Scan(&p.peer, &p.quota, &p.use, &p.cond, &since, &lastOK); err != nil {
			return nil, err
		}
		p.since = time.Unix(since, 0)
		if lastOK.Valid {
			p.lastOK = time.Unix(lastOK.Int64, 0)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// unsentMetadata is the bytes of bucket's live metadata not placed at
// place: a friend added later receives them (REPL-13, #26), so they are
// not room for data (D6 step 1).
func unsentMetadata(ctx context.Context, tx *sql.Tx, bucket, place string) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(q.size + ?3), 0) FROM queue q
		WHERE q.bucket = ?1 AND q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = ?2 AND p.bucket = q.bucket AND p.key = q.key)`,
		bucket, place, ObjectOverhead).Scan(&n)
	return n, err
}

// choose is D6's choice of up to n places for an object of bucket that
// counts need bytes at a place, never one in exclude: friends with free
// capacity for it, reachable ones first, each by largest free fraction
// (free ÷ quota), ties by node ID. A friend gone past replace_after,
// removed, or full by its last answer is not room. Fewer than n means
// the pool lacks room: what fits is placed, and the deficit shows as
// bytes below copies (§3.4).
func (db *DB) choose(ctx context.Context, tx *sql.Tx, bucket string, need int64, n int, exclude []string) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	ps, err := poolPlaces(ctx, tx)
	if err != nil {
		return nil, err
	}
	now := db.now()
	type cand struct {
		peer      string
		reachable bool
		frac      float64
	}
	var cs []cand
	for _, p := range ps {
		if slices.Contains(exclude, p.peer) || p.quota <= 0 || p.cond == ConditionRemoved || p.full() || p.gone(db, now) {
			continue
		}
		meta, err := unsentMetadata(ctx, tx, bucket, p.peer)
		if err != nil {
			return nil, err
		}
		free := p.quota - p.use - meta
		if free < need {
			continue
		}
		cs = append(cs, cand{p.peer, !p.unreachable(db, now), float64(free) / float64(p.quota)})
	}
	slices.SortStableFunc(cs, func(a, b cand) int {
		if a.reachable != b.reachable {
			if a.reachable {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(b.frac, a.frac); c != 0 {
			return c
		}
		return cmp.Compare(a.peer, b.peer)
	})
	var out []string
	for _, c := range cs[:min(n, len(cs))] {
		out = append(out, c.peer)
	}
	return out, nil
}

// placeTx places the completed queue row seq (REPL-12), in the
// transaction that completes it:
//   - a delete goes to every friend whose latest row of the key is a
//     put (REPL-2): every place that holds or will hold it;
//   - a metadata or snapshot put goes to every friend;
//   - a data put goes to `copies` friends by choose. A healing re-upload
//     (heal, #57) goes where the row it heals was placed, topped up to
//     `copies`.
//
// A healing re-upload is acked at once, without sending, at a place that
// acknowledged the very row it heals (the key's previous done row, a put
// of the same hash) and has nothing of the key queued: exactly-once
// holds for honest places while another place keeps refusing (#61). An
// older acknowledgement of the hash is not enough: a row of other bytes
// sent since may have landed with its answer lost and then been
// cancelled as superseded, which leaves no row behind.
func (db *DB) placeTx(ctx context.Context, tx *sql.Tx, seq int64, heal bool) error {
	var bucket, key, op, class, sha string
	var size int64
	err := tx.QueryRowContext(ctx, `SELECT bucket, key, op, class, coalesce(size, 0), coalesce(sha256, '')
		FROM queue WHERE seq = ? AND state = 'done'`, seq).Scan(&bucket, &key, &op, &class, &size, &sha)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	var targets []string
	switch {
	case op == OpDelete:
		targets, err = column(ctx, tx, `SELECT DISTINCT p.place FROM placement p JOIN friendship f ON f.peer_key = p.place
			WHERE coalesce(f.address, '') != '' AND p.bucket = ?1 AND p.key = ?2 AND p.op = 'put'
			AND p.seq = (SELECT max(x.seq) FROM placement x
				WHERE x.place = p.place AND x.bucket = p.bucket AND x.key = p.key AND x.seq < ?3)
			ORDER BY p.place`, bucket, key, seq)
	case class == "metadata" || class == "snapshot":
		targets, err = column(ctx, tx, `SELECT peer_key FROM friendship WHERE coalesce(address, '') != '' ORDER BY peer_key`)
	default:
		if heal {
			targets, err = column(ctx, tx, `SELECT p.place FROM placement p JOIN friendship f ON f.peer_key = p.place
				WHERE coalesce(f.address, '') != '' AND p.seq = (SELECT max(l.seq) FROM queue l
					WHERE l.bucket = ?1 AND l.key = ?2 AND l.seq < ?3 AND l.state = 'done')
				ORDER BY p.place`, bucket, key, seq)
			if err != nil {
				return err
			}
		}
		var copies int
		if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
			bucket, DefaultCopies).Scan(&copies); err != nil {
			return err
		}
		var more []string
		more, err = db.choose(ctx, tx, bucket, size+ObjectOverhead, copies-len(targets), targets)
		targets = append(targets, more...)
	}
	if err != nil {
		return err
	}
	for _, place := range targets {
		if err := db.insertPlacement(ctx, tx, seq, place, heal); err != nil {
			return err
		}
	}
	return nil
}

// insertPlacement adds row seq to place's FIFO, or acked when heal and
// the place holds what it heals (placeTx), with the bytes it adds to the
// place's use.
func (db *DB) insertPlacement(ctx context.Context, tx *sql.Tx, seq int64, place string, heal bool) error {
	_, err := tx.ExecContext(ctx, `WITH c AS (
		SELECT q.seq, q.bucket, q.key, q.op, q.sha256,
			?3 AND q.op = 'put'
			AND EXISTS (SELECT 1 FROM placement a
				WHERE a.place = ?2 AND a.state = 'acked' AND a.sha256 = q.sha256
				AND a.seq = (SELECT max(l.seq) FROM queue l
					WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq < q.seq AND l.state = 'done'))
			AND NOT EXISTS (SELECT 1 FROM placement w
				WHERE w.place = ?2 AND w.bucket = q.bucket AND w.key = q.key
				AND w.state = 'queued' AND w.seq < q.seq) AS held,
			-- What this row adds to the place's use: a put its bytes,
			-- unless the place's latest row of the key acknowledged the
			-- same hash; a delete less what the place acknowledged of the
			-- key since its last delete there.
			CASE WHEN q.op = 'put' THEN
				CASE WHEN EXISTS (SELECT 1 FROM placement a
					WHERE a.place = ?2 AND a.bucket = q.bucket AND a.key = q.key AND a.op = 'put'
					AND a.state = 'acked' AND a.sha256 = q.sha256
					AND a.seq = (SELECT max(x.seq) FROM placement x
						WHERE x.place = a.place AND x.bucket = a.bucket AND x.key = a.key AND x.seq < q.seq))
				THEN 0 ELSE q.size + ?4 END
			ELSE -coalesce((SELECT sum(a.size) FROM placement a
				WHERE a.place = ?2 AND a.bucket = q.bucket AND a.key = q.key
				AND a.op = 'put' AND a.state = 'acked' AND a.seq < q.seq
				AND a.seq > coalesce((SELECT max(d.seq) FROM placement d WHERE d.place = a.place
					AND d.bucket = a.bucket AND d.key = a.key AND d.op = 'delete' AND d.seq < q.seq), 0)), 0)
			END AS size
		FROM queue q WHERE q.seq = ?1)
	INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
		SELECT seq, ?2, bucket, key, op,
			CASE WHEN held THEN 'acked' ELSE 'queued' END,
			CASE WHEN held THEN sha256 END,
			CASE WHEN held THEN ?5 END,
			size
		FROM c`, seq, place, heal, ObjectOverhead, db.now().Unix())
	return err
}

func column(ctx context.Context, tx *sql.Tx, q string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Held reports whether row seq is a metadata row the barrier holds back
// (REPL-1): a metadata or snapshot row, put or delete, is sent to any
// place only when no data put of its bucket before it has a queued
// placement row left, each acked at every place it is placed at. Rows
// refused for good are not queued, so a friend that refuses everything
// cannot stall the barrier (#20 review): the object is below copies,
// which status shows. A queued put a later done write of the key
// replaced will be cancelled, not sent, so it does not hold it either.
func (db *DB) Held(ctx context.Context, seq int64) (bool, error) {
	var held bool
	err := db.QueryRowContext(ctx, `SELECT q.class IN ('metadata', 'snapshot') AND EXISTS (
		SELECT 1 FROM placement p JOIN queue d ON d.seq = p.seq
		WHERE p.bucket = q.bucket AND p.seq < q.seq AND p.state = 'queued' AND p.op = 'put' AND d.class = 'data'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = p.bucket AND l.key = p.key AND l.seq > p.seq AND l.state = 'done'))
		FROM queue q WHERE q.seq = ?`, seq).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return held, err
}

// Reroute moves the unsent data rows of every friend that is failing
// for longer than reroute_after, or whose last answer was 507, to
// another friend with room by D6's choice, never to one that has or had
// a row of the object (§3.4). The row keeps its sequence, so it takes its
// place in the new FIFO before later rows. Moving an unsent row moves no
// bytes; should the old place's send still be in flight and land, it
// holds an unrecorded extra copy. With no friend with room the row
// waits, unless its place is gone past replace_after: then it is
// dropped, a deficit status shows as bytes below copies, so it cannot
// hold the metadata barrier for good. Re-placement (#26) restores the
// copies once there is room.
func (db *DB) Reroute(ctx context.Context) (moved, dropped int64, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	ps, err := poolPlaces(ctx, tx)
	if err != nil {
		return 0, 0, err
	}
	now := db.now()
	for _, p := range ps {
		if !p.reroutable(db, now) {
			continue
		}
		type row struct {
			seq    int64
			bucket string
			size   int64
		}
		var rs []row
		rows, err := tx.QueryContext(ctx, `SELECT p.seq, p.bucket, coalesce(q.size, 0)
			FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ? AND p.state = 'queued' AND p.op = 'put' AND q.class = 'data' ORDER BY p.seq`, p.peer)
		if err != nil {
			return 0, 0, err
		}
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.seq, &r.bucket, &r.size); err != nil {
				rows.Close()
				return 0, 0, err
			}
			rs = append(rs, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return 0, 0, err
		}
		for _, r := range rs {
			holders, err := column(ctx, tx, `SELECT place FROM placement WHERE seq = ?`, r.seq)
			if err != nil {
				return 0, 0, err
			}
			to, err := db.choose(ctx, tx, r.bucket, r.size+ObjectOverhead, 1, holders)
			if err != nil {
				return 0, 0, err
			}
			if len(to) == 0 && !p.gone(db, now) {
				continue
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`,
				p.peer, r.seq); err != nil {
				return 0, 0, err
			}
			if len(to) == 0 {
				dropped++
				continue
			}
			if err := db.insertPlacement(ctx, tx, r.seq, to[0], false); err != nil {
				return 0, 0, err
			}
			moved++
		}
	}
	return moved, dropped, tx.Commit()
}

// PoolSize is how many places the pool has for copies: friends with an
// address that do not refuse this node (OPS-2, EP-6).
func (db *DB) PoolSize(ctx context.Context) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM friendship f LEFT JOIN place c ON c.place = f.peer_key
		WHERE coalesce(f.address, '') != '' AND coalesce(c.condition, '') != 'removed'`).Scan(&n)
	return n, err
}

// PlaceUse returns the borrower's count of what each place holds or
// will hold: its queued and acked rows, objects at their size plus
// ObjectOverhead, less what deletes free.
func (db *DB) PlaceUse(ctx context.Context) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, bytes FROM place_use`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var p string
		var n int64
		if err := rows.Scan(&p, &n); err != nil {
			return nil, err
		}
		out[p] = n
	}
	return out, rows.Err()
}

// BelowStat counts a bucket's live objects at fewer than `copies` places.
type BelowStat struct {
	Objects int64
	Bytes   int64
}

// BelowCopies returns, per bucket, the live objects (the key's latest
// done write is a put) that fewer than the bucket's `copies` places hold
// or will hold (acked or queued rows): the deficit the pool lacks room
// for, refusals, and copies dropped at a friend gone past replace_after
// (REPL-4). Buckets with none are absent.
func (db *DB) BelowCopies(ctx context.Context) (map[string]BelowStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.bucket, count(*), coalesce(sum(q.size), 0) FROM queue q
		WHERE q.state = 'done' AND q.op = 'put'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		AND (SELECT count(*) FROM placement p WHERE p.seq = q.seq AND p.state IN ('queued', 'acked'))
			< coalesce((SELECT copies FROM buckets WHERE name = q.bucket), ?)
		GROUP BY q.bucket`, DefaultCopies)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]BelowStat{}
	for rows.Next() {
		var b string
		var s BelowStat
		if err := rows.Scan(&b, &s.Objects, &s.Bytes); err != nil {
			return nil, err
		}
		out[b] = s
	}
	return out, rows.Err()
}
