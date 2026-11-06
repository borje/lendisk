package state

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
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

	// MetadataReserveFactor and MetadataReservePercent size the headroom
	// data leaves for metadata at each friend (D6, §3.4): the larger of
	// MetadataReserveFactor times the live metadata of all the node's
	// buckets, and MetadataReservePercent of the friend's quota. Restic's
	// prune writes its new index before it deletes packs, so metadata
	// must always fit, or the deletes behind it never free room.
	MetadataReserveFactor  = 3
	MetadataReservePercent = 1
)

// liveMetadata is the bytes of every bucket's live metadata (the key's
// latest done write is a put), as a place counts them.
func liveMetadata(ctx context.Context, q querier) (int64, error) {
	var n int64
	err := q.QueryRowContext(ctx, `SELECT coalesce(sum(q.size + ?), 0) FROM queue q
		WHERE q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`,
		ObjectOverhead).Scan(&n)
	return n, err
}

// Room for data at a friend is counted in hundredths of a byte, so the
// reserve's percentage of the quota is exact and fractions that should
// tie do tie.

// freeRoom is the room for data, in hundredths of a byte, at a friend of
// quota bytes holding use and with unsent bytes of a bucket's metadata
// still to come, less the metadata reserve: the larger of
// MetadataReserveFactor times live (liveMetadata) and
// MetadataReservePercent of the quota. Metadata placement ignores the
// reserve: metadata may use it.
func freeRoom(quota, use, unsent, live int64) int64 {
	return 100*(quota-use-unsent) - max(100*MetadataReserveFactor*live, MetadataReservePercent*quota)
}

// byFraction orders candidates by largest free fraction (free ÷ quota,
// free in hundredths of a byte, quota above 0), compared exactly, then by
// node ID: a total order, so any scan or sort of the candidates agrees.
func byFraction(aPeer string, aFree, aQuota int64, bPeer string, bFree, bQuota int64) int {
	ah, al := mulSigned(aFree, bQuota)
	bh, bl := mulSigned(bFree, aQuota)
	switch {
	case ah != bh:
		return -cmp.Compare(ah, bh)
	case al != bl:
		return -cmp.Compare(al, bl)
	}
	return cmp.Compare(aPeer, bPeer)
}

// mulSigned is x × y, y > 0, as a 128-bit two's complement number: high
// word signed, low word unsigned, so (high, low) compare in order.
func mulSigned(x, y int64) (int64, uint64) {
	hi, lo := bits.Mul64(uint64(x), uint64(y))
	if x < 0 { // uint64(x) is x + 2^64: take y × 2^64 back off
		hi -= uint64(y)
	}
	return int64(hi), lo
}

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

// fullForLong: it has answered 507 for reroute_after. A 507 can pass (the
// lender counts writes in flight and its floor, LEND-2), so only a long
// one lets Reroute drop copies there.
func (p poolPlace) fullForLong(db *DB, now time.Time) bool {
	return p.full() && now.Sub(p.since) >= db.rerouteAfter()
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func poolPlaces(ctx context.Context, tx querier) ([]poolPlace, error) {
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
func unsentMetadata(ctx context.Context, tx querier, bucket, place string) (int64, error) {
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
// capacity for it (quota less use, the bucket's unsent metadata and the
// metadata reserve), reachable ones first, each by largest free fraction
// (free ÷ quota), ties by node ID. A friend gone past replace_after,
// removed, or full by its last answer is not room; with reachableOnly,
// neither is an unreachable one (re-routing, so rows do not move between
// friends that both stopped answering). Fewer than n means the pool
// lacks room: what fits is placed, and the deficit shows as bytes below
// copies (§3.4).
func (db *DB) choose(ctx context.Context, tx *sql.Tx, bucket string, need int64, n int, exclude []string, reachableOnly bool) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	ps, err := poolPlaces(ctx, tx)
	if err != nil {
		return nil, err
	}
	now := db.now()
	live, err := liveMetadata(ctx, tx)
	if err != nil {
		return nil, err
	}
	type cand struct {
		peer        string
		reachable   bool
		free, quota int64 // free in hundredths of a byte
	}
	var cs []cand
	for _, p := range ps {
		if slices.Contains(exclude, p.peer) || p.quota <= 0 || p.cond == ConditionRemoved || p.full() || p.gone(db, now) ||
			reachableOnly && p.unreachable(db, now) {
			continue
		}
		meta, err := unsentMetadata(ctx, tx, bucket, p.peer)
		if err != nil {
			return nil, err
		}
		free := freeRoom(p.quota, p.use, meta, live)
		if free < 100*need {
			continue
		}
		cs = append(cs, cand{p.peer, !p.unreachable(db, now), free, p.quota})
	}
	slices.SortStableFunc(cs, func(a, b cand) int {
		if a.reachable != b.reachable {
			if a.reachable {
				return -1
			}
			return 1
		}
		return byFraction(a.peer, a.free, a.quota, b.peer, b.free, b.quota)
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
				WHERE coalesce(f.address, '') != '' AND p.state != '`+PlacementDropped+`' AND p.seq = (SELECT max(l.seq) FROM queue l
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
		more, err = db.choose(ctx, tx, bucket, size+ObjectOverhead, copies-len(targets), targets, false)
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

func column(ctx context.Context, tx querier, q string, args ...any) ([]string, error) {
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
// which status shows. A queued put that a done write of the key before
// the metadata replaced will be cancelled, not sent, so it does not hold
// it either; one replaced only after the metadata still does, as the
// metadata was written while it was the key's bytes.
func (db *DB) Held(ctx context.Context, seq int64) (bool, error) {
	var held bool
	err := db.QueryRowContext(ctx, `SELECT q.class IN ('metadata', 'snapshot') AND EXISTS (
		SELECT 1 FROM placement p JOIN queue d ON d.seq = p.seq
		WHERE p.bucket = q.bucket AND p.seq < q.seq AND p.state = 'queued' AND p.op = 'put' AND d.class = 'data'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = p.bucket AND l.key = p.key AND l.seq > p.seq AND l.seq < q.seq AND l.state = 'done'))
		FROM queue q WHERE q.seq = ?`, seq).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return held, err
}

// Reroute moves the unsent data rows of every friend that is failing
// for longer than reroute_after, or whose last answer was 507, to
// another reachable friend with room by D6's choice, never to one that
// has or had a row of the object (§3.4). The row keeps its sequence, so
// it takes its place in the new FIFO before later rows. With no friend
// with room the row waits, unless its place is gone past replace_after,
// or has answered 507 for reroute_after and the row holds a metadata
// barrier: then it is dropped (PlacementDropped), a deficit status shows
// as bytes below copies and alert.KindCopiesDropped raises, so it cannot
// hold the barrier, or a full friend's later deletes, for good.
// Re-placement (#26) restores the copies once there is room.
//
// The pass reads without a transaction. It keeps each candidate
// friend's room in memory: computed once (quota less use less the
// metadata reserve), with what the pass's moves of every bucket take
// from it, and each bucket's unsent metadata there taken off when a row
// of that bucket looks at it, so moves of several buckets fill a friend
// once. It applies the moves in short write transactions, each under lock when lock is set. A row is applied only
// if it is still queued there and sending(place, seq) is false under the
// lock: its send may land, and the place's row must be there to record
// it, or no delete would reach the copy.
func (db *DB) Reroute(ctx context.Context, sending func(place string, seq int64) bool, lock sync.Locker) (moved, dropped int64, err error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return 0, 0, err
	}
	now := db.now()
	live, err := liveMetadata(ctx, db)
	if err != nil {
		return 0, 0, err
	}
	// The friends that could take a copy, in node ID order: reachable,
	// with room before any bucket's unsent metadata (base, in hundredths
	// of a byte) less what this pass's moves took. A friend without room
	// for one more object leaves the list.
	type target struct {
		peer        string
		quota, base int64
		taken       int64 // bytes this pass moved there
	}
	var targets []*target
	for _, p := range ps {
		if p.quota <= 0 || p.cond == ConditionRemoved || p.full() || p.gone(db, now) || p.unreachable(db, now) {
			continue
		}
		if base := freeRoom(p.quota, p.use, 0, live); base >= 100*ObjectOverhead {
			targets = append(targets, &target{peer: p.peer, quota: p.quota, base: base})
		}
	}
	unsent := map[[2]string]int64{} // bucket, place -> unsentMetadata
	room := func(t *target, bucket string) (int64, error) {
		k := [2]string{bucket, t.peer}
		meta, ok := unsent[k]
		if !ok {
			var err error
			if meta, err = unsentMetadata(ctx, db, bucket, t.peer); err != nil {
				return 0, err
			}
			unsent[k] = meta
		}
		return t.base - 100*(meta+t.taken), nil
	}
	var ops []rerouteOp
	for _, p := range ps {
		if !p.reroutable(db, now) {
			continue
		}
		gone, full := p.gone(db, now), p.fullForLong(db, now)
		if len(targets) == 0 && !gone && !full {
			continue // no room anywhere, and nothing to drop
		}
		rs, err := db.waiting(ctx, p.peer, gone || full)
		if err != nil {
			return 0, 0, err
		}
		for _, r := range rs {
			var to *target
			var toFree int64
			for _, t := range targets {
				if slices.Contains(r.holders, t.peer) {
					continue
				}
				free, err := room(t, r.bucket)
				if err != nil {
					return 0, 0, err
				}
				if free < 100*r.need {
					continue
				}
				if to == nil || byFraction(t.peer, free, t.quota, to.peer, toFree, to.quota) < 0 {
					to, toFree = t, free
				}
			}
			switch {
			case to != nil:
				to.taken += r.need
				if to.base-100*to.taken < 100*ObjectOverhead {
					targets = slices.DeleteFunc(targets, func(t *target) bool { return t == to })
				}
				ops = append(ops, rerouteOp{from: p.peer, seq: r.seq, to: to.peer})
			case gone || full && r.barrier:
				ops = append(ops, rerouteOp{from: p.peer, seq: r.seq})
			}
		}
	}
	for len(ops) > 0 {
		n := min(len(ops), 64)
		m, d, err := db.applyReroute(ctx, ops[:n], sending, lock)
		moved, dropped = moved+m, dropped+d
		if err != nil {
			return moved, dropped, err
		}
		ops = ops[n:]
	}
	return moved, dropped, nil
}

// rerouteOp moves row seq from one place to another, or drops it there
// when to is empty.
type rerouteOp struct {
	from, to string
	seq      int64
}

// waitingRow is a queued data put at a place Reroute looks at.
type waitingRow struct {
	seq     int64
	bucket  string
	need    int64    // what it counts at a place: its size and ObjectOverhead
	holders []string // the places with a row of it
	barrier bool     // it holds a metadata barrier (Held); only when asked
}

// waiting returns place's queued data puts, in sequence order, with
// whether each holds a metadata barrier when barrier is set: a queued
// metadata row of its bucket comes after it, before any later done
// write of its key (Held's rule).
func (db *DB) waiting(ctx context.Context, place string, barrier bool) ([]waitingRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.seq, p.bucket, coalesce(q.size, 0) + ?2,
		(SELECT group_concat(h.place, char(31)) FROM placement h WHERE h.seq = p.seq),
		(SELECT min(l.seq) FROM queue l WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ?1 AND p.state = 'queued' AND p.op = 'put' AND q.class = 'data' ORDER BY p.seq`,
		place, ObjectOverhead)
	if err != nil {
		return nil, err
	}
	var out []waitingRow
	var next []sql.NullInt64
	for rows.Next() {
		var r waitingRow
		var holders string
		var n sql.NullInt64
		if err := rows.Scan(&r.seq, &r.bucket, &r.need, &holders, &n); err != nil {
			rows.Close()
			return nil, err
		}
		r.holders = strings.Split(holders, "\x1f")
		out = append(out, r)
		next = append(next, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil || !barrier {
		return out, err
	}
	meta := map[string][]int64{} // bucket -> its queued metadata rows, in order
	for i, r := range out {
		ms, ok := meta[r.bucket]
		if !ok {
			ss, err := column(ctx, db, `SELECT DISTINCT p.seq FROM placement p JOIN queue q ON q.seq = p.seq
				WHERE p.bucket = ? AND p.state = 'queued' AND q.class IN ('metadata', 'snapshot') ORDER BY p.seq`, r.bucket)
			if err != nil {
				return nil, err
			}
			for _, s := range ss {
				n, _ := strconv.ParseInt(s, 10, 64)
				ms = append(ms, n)
			}
			meta[r.bucket] = ms
		}
		j, _ := slices.BinarySearch(ms, r.seq+1)
		out[i].barrier = j < len(ms) && (!next[i].Valid || ms[j] < next[i].Int64)
	}
	return out, nil
}

// applyReroute applies ops in one short write transaction, under lock
// when set, skipping rows no longer queued at their place or being sent.
func (db *DB) applyReroute(ctx context.Context, ops []rerouteOp, sending func(string, int64) bool, lock sync.Locker) (moved, dropped int64, err error) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	// Drops go in one statement per place: a full friend may drop
	// thousands of rows in a pass.
	drops := map[string][]int64{}
	for _, op := range ops {
		if sending != nil && sending(op.from, op.seq) {
			continue
		}
		if op.to == "" {
			drops[op.from] = append(drops[op.from], op.seq)
			continue
		}
		r, err := tx.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`, op.from, op.seq)
		if err != nil {
			return 0, 0, err
		}
		if n, err := r.RowsAffected(); err != nil {
			return 0, 0, err
		} else if n == 0 {
			continue // sent, refused or cancelled since the scan
		}
		if err := db.insertPlacement(ctx, tx, op.seq, op.to, false); err != nil {
			return 0, 0, err
		}
		moved++
	}
	for place, seqs := range drops {
		list, err := json.Marshal(seqs)
		if err != nil {
			return 0, 0, err
		}
		// Rows no longer queued (sent, refused or cancelled since the
		// scan) are left alone.
		r, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDropped+`'
			WHERE place = ? AND state = 'queued' AND seq IN (SELECT value FROM json_each(?))`, place, string(list))
		if err != nil {
			return 0, 0, err
		}
		n, err := r.RowsAffected()
		if err != nil {
			return 0, 0, err
		}
		dropped += n
	}
	return moved, dropped, tx.Commit()
}

// PoolSize is how many places the pool has for copies: friends with an
// address that do not refuse this node and are not gone past
// replace_after (OPS-2, EP-6).
func (db *DB) PoolSize(ctx context.Context) (int, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return 0, err
	}
	now, n := db.now(), 0
	for _, p := range ps {
		if p.cond != ConditionRemoved && !p.gone(db, now) {
			n++
		}
	}
	return n, nil
}

// DroppedBelowCopies returns, per bucket, the live objects below copies
// with a copy dropped at a full or gone friend (Reroute): data held at
// fewer places for want of room (alert.KindCopiesDropped). Writing the
// key again, or re-placement (#26), clears it.
func (db *DB) DroppedBelowCopies(ctx context.Context) (map[string]BelowStat, error) {
	return db.below(ctx, `AND EXISTS (SELECT 1 FROM placement p WHERE p.seq = q.seq AND p.state = '`+PlacementDropped+`')`)
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
	return db.below(ctx, "")
}

func (db *DB) below(ctx context.Context, and string) (map[string]BelowStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.bucket, count(*), coalesce(sum(q.size), 0) FROM queue q
		WHERE q.state = 'done' AND q.op = 'put'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		AND (SELECT count(*) FROM placement p WHERE p.seq = q.seq AND p.state IN ('queued', 'acked'))
			< coalesce((SELECT copies FROM buckets WHERE name = q.bucket), ?) `+and+`
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
