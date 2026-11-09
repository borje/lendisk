package state

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
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

// MaxQuota is the most bytes room arithmetic counts: a friendship quota,
// use or size above it counts as MaxQuota (32 PiB), so room in
// hundredths of a byte, and the products byFraction compares, never
// overflow.
const MaxQuota = 1 << 55

// capped is n within [-MaxQuota, MaxQuota].
func capped(n int64) int64 { return min(max(n, -MaxQuota), MaxQuota) }

// freeRoom is the room for data, in hundredths of a byte, at a friend of
// quota bytes holding use and with unsent more bytes to come (a bucket's
// metadata not yet sent there, and what a pass moved there), less the
// metadata reserve: the larger of MetadataReserveFactor times live
// (liveMetadata) and MetadataReservePercent of the quota. Metadata
// placement ignores the reserve: metadata may use it. Every input counts
// at most MaxQuota, and a deficit of more than MaxQuota counts as
// MaxQuota: |result| stays within 200 × MaxQuota.
func freeRoom(quota, use, unsent, live int64) int64 {
	quota = capped(quota)
	d := max(quota-capped(use)-capped(unsent), -MaxQuota)
	reserve := max(100*MetadataReserveFactor*min(capped(live), MaxQuota/MetadataReserveFactor), MetadataReservePercent*quota)
	return 100*d - reserve
}

// fits reports whether an object that counts need bytes fits in free
// hundredths of a byte.
func fits(free, need int64) bool { return free >= 100*capped(need) }

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

// poolPlace is a friend with an address, or an overflow target, as
// placement sees it.
type poolPlace struct {
	peer     string
	overflow bool  // an overflow target (overflow.go): peer is its place name
	quota    int64 // the friendship's quota (LEND-8), or the target's max_bytes (S3-7)
	use      int64 // place_use: what it holds or will hold
	cond     string
	since    time.Time // when cond began
	lastOK   time.Time // zero if never
	// failing is when its run of failing answers began (place.
	// failing_since, or since if unset): the same as since unless it
	// changed between retrying, quota and removed (#65).
	failingAt time.Time
}

func (p poolPlace) failing() bool {
	return p.cond == ConditionRetrying || p.cond == ConditionRemoved
}

// unreachable: failing now, without an ok answer for reroute_after
// (#65: a friend alternating between network errors and 507s is away
// as long as it gives no ok), or busy for reroute_after on end
// (busyForLong). A friend failing or busy for less is still counted
// reachable: a blip is not an outage.
func (p poolPlace) unreachable(db *DB, now time.Time) bool {
	// A probe's answer during a full stretch moves last_ok on while the
	// run of failures stays: one failed probe after it is a blip, not
	// reroute_after of silence (#65 review).
	return p.failing() && now.Sub(p.failingSince()) >= db.rerouteAfter() &&
		(p.lastOK.IsZero() || now.Sub(p.lastOK) >= db.rerouteAfter()) ||
		p.busyForLong(db, now)
}

// busyForLong: it has answered 503 SlowDown (busy) and nothing else
// for reroute_after. A busy answer is not a failure and moves last_ok
// on, but an honest one lasts only while another write is in flight
// there: one that lasts longer is a friend that is broken or holding
// this node's copies back, and counts as unreachable (its unsent data
// goes elsewhere, it no longer holds deletes) though never as gone,
// since it keeps answering (#92).
func (p poolPlace) busyForLong(db *DB, now time.Time) bool {
	return p.cond == ConditionBusy && now.Sub(p.since) >= db.rerouteAfter()
}

// failingSince is when its run of failing answers began.
func (p poolPlace) failingSince() time.Time {
	if p.failingAt.IsZero() {
		return p.since
	}
	return p.failingAt
}

// gone: unreachable, and silent for replace_after since its last
// answer (or its first failure, if it never answered). It is no longer
// room (REPL-12); re-placement (REPL-13, #26) moves what it held.
func (p poolPlace) gone(db *DB, now time.Time) bool {
	// Silence counts from the last answer, a probe's included (#65
	// review): a friend full for a week that answers probes, then misses
	// one, is not gone.
	from := p.lastOK
	if from.IsZero() {
		from = p.failingSince()
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

// fullForLong: it has answered 507, without an ok answer for
// reroute_after (a network error between 507s does not restart it,
// #65). A 507 can pass (the lender counts writes in flight and its
// floor, LEND-2), so only a long one lets Reroute drop copies there.
func (p poolPlace) fullForLong(db *DB, now time.Time) bool {
	return p.full() && now.Sub(p.failingSince()) >= db.rerouteAfter()
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// poolPlaces lists the pool (D6): friends with an address, then
// overflow targets, each in place name order.
func poolPlaces(ctx context.Context, tx querier) ([]poolPlace, error) {
	rows, err := tx.QueryContext(ctx, `SELECT x.place, x.overflow, x.quota,
		coalesce(u.bytes, 0) + coalesce(fr.bytes, 0),
		coalesce(c.condition, ''), coalesce(c.since, 0), c.last_ok, c.failing_since
		FROM (SELECT f.peer_key AS place, 0 AS overflow,
				min(coalesce(f.own_number, 0), coalesce(f.peer_number, 0)) AS quota
				FROM friendship f WHERE coalesce(f.address, '') != ''
			UNION ALL SELECT '`+OverflowPrefix+`' || o.name, 1, o.max_bytes FROM overflow o) x
		LEFT JOIN place_use u ON u.place = x.place LEFT JOIN place c ON c.place = x.place
		LEFT JOIN place_freed fr ON fr.place = x.place
		ORDER BY x.overflow, x.place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []poolPlace
	for rows.Next() {
		var p poolPlace
		var since int64
		var lastOK, failing sql.NullInt64
		if err := rows.Scan(&p.peer, &p.overflow, &p.quota, &p.use, &p.cond, &since, &lastOK, &failing); err != nil {
			return nil, err
		}
		p.since = time.Unix(since, 0)
		if lastOK.Valid {
			p.lastOK = time.Unix(lastOK.Int64, 0)
		}
		if failing.Valid {
			p.failingAt = time.Unix(failing.Int64, 0)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// unsentMetadata is the bytes of bucket's live metadata with no row at
// place that holds, refuses or deletes it: a friend added later receives
// them from re-placement (REPL-13, #26), and until then gets no data
// (choose, Reroute), so that its FIFO sends the bucket's metadata ahead
// of newer data (REPL-1). A change EP-11 held back is placed nowhere and
// does not count, nor does metadata the store could not supply
// (RefuseObject), which no friend is sent (#26 review).
func unsentMetadata(ctx context.Context, tx querier, bucket, place string) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(q.size + ?3), 0) FROM queue q
		WHERE q.bucket = ?1 AND q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		AND q.immutable IS NULL
		AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = ?2 AND p.seq = q.seq
			AND p.state IN ('queued', 'acked', 'refused', 'dropping'))
		AND NOT EXISTS (SELECT 1 FROM placement r WHERE r.seq = q.seq AND r.state = 'refused'
			AND r.cause IN ('store', 'oversize'))`,
		bucket, place, ObjectOverhead).Scan(&n)
	return n, err
}

// choose is D6's choice of up to n places for an object of bucket that
// counts need bytes at a place, never one in exclude: friends with free
// capacity for it (quota less use and the metadata reserve), reachable
// ones first, each by largest free fraction
// (free ÷ quota), ties by node ID. A friend gone past replace_after,
// removed, or full by its last answer is not room, nor is one whose
// replica of the bucket failed a challenge (VER-1, #27). A friend lacking
// some of the bucket's live metadata is room less that metadata, which
// the caller places there first (placeMetadata); with reachableOnly it is
// not room (its metadata first, #26), and neither is an unreachable one
// (re-routing, so rows do not move between friends that both stopped
// answering). Friends take at most maxFriends of the n (all, when
// negative), overflow targets that answer the rest by D6's step 3
// (chooseOverflow), and friends again what no target can take. Fewer
// than n means the pool lacks room: what fits is placed, and the deficit
// shows as bytes below copies (§3.4).
func (db *DB) choose(ctx context.Context, tx *sql.Tx, bucket string, need int64, n, maxFriends int, exclude []string, reachableOnly bool) ([]string, error) {
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
	unverified, err := unverifiedAt(ctx, tx, bucket)
	if err != nil {
		return nil, err
	}
	type cand struct {
		peer        string
		reachable   bool
		free, quota int64 // free in hundredths of a byte
	}
	var cs []cand
	var overflow []poolPlace
	for _, p := range ps {
		if slices.Contains(exclude, p.peer) || p.quota <= 0 || p.cond == ConditionRemoved || p.full() || p.gone(db, now) ||
			reachableOnly && p.unreachable(db, now) || unverified[p.peer] {
			continue
		}
		if p.overflow {
			// A failing target is no candidate: its queued copies would
			// hold the metadata barrier everywhere (#38 review).
			if !p.unreachable(db, now) {
				overflow = append(overflow, p)
			}
			continue
		}
		meta, err := unsentMetadata(ctx, tx, bucket, p.peer)
		if err != nil {
			return nil, err
		}
		if meta > 0 && reachableOnly {
			continue // its metadata first (REPL-13, #26)
		}
		// A friend lacking some of the bucket's metadata is room all the
		// same: placeTx queues the metadata there first (#38 review), so a
		// copy never goes to an overflow target for want of it.
		free := freeRoom(p.quota, p.use, meta, live)
		if !fits(free, need) {
			continue
		}
		cs = append(cs, cand{p.peer, !p.unreachable(db, now), free, capped(p.quota)})
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
	friends := n
	if maxFriends >= 0 {
		friends = min(n, maxFriends)
	}
	var out []string
	for _, c := range cs[:min(friends, len(cs))] {
		out = append(out, c.peer)
	}
	if len(out) < n && !reachableOnly {
		more, err := chooseOverflow(ctx, tx, bucket, need, n-len(out), overflow)
		if err != nil {
			return nil, err
		}
		out = append(out, more...)
		// With no target to take it, the copy goes to a friend after all.
		for _, c := range cs[min(friends, len(cs)):] {
			if len(out) >= n {
				break
			}
			out = append(out, c.peer)
		}
	}
	return out, nil
}

// chooseOverflow is D6's step 3: up to n of the overflow targets ps for a
// data copy of bucket that counts need bytes, by most free bytes (max_bytes
// less use, less the bucket's metadata not
// placed there yet, which goes there with it), ties by name. A target
// with no room for it gets none: placement stops there at max_bytes
// (S3-7).
func chooseOverflow(ctx context.Context, q querier, bucket string, need int64, n int, ps []poolPlace) ([]string, error) {
	type cand struct {
		place string
		free  int64
	}
	var cs []cand
	for _, p := range ps {
		meta, err := unsentMetadata(ctx, q, bucket, p.peer)
		if err != nil {
			return nil, err
		}
		free := capped(p.quota) - capped(max(p.use, 0)) - capped(meta)
		if free >= capped(need) {
			cs = append(cs, cand{p.peer, free})
		}
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if c := cmp.Compare(b.free, a.free); c != 0 {
			return c
		}
		return cmp.Compare(a.place, b.place)
	})
	var out []string
	for _, c := range cs[:min(n, len(cs))] {
		out = append(out, c.place)
	}
	return out, nil
}

// placeTx places the completed queue row seq (REPL-12), in the
// transaction that completes it:
//   - a delete first cancels every queued put of the key that no
//     sender has taken (StartPlacement), then goes to every friend
//     whose latest row of the key is still a put (REPL-2, #21): every
//     place that holds or may hold it;
//   - a metadata or snapshot put goes to every friend;
//   - a data put goes to `copies` friends by choose. A healing re-upload
//     (heal, #57) goes where the row it heals was placed, topped up to
//     `copies`.
//
// A healing re-upload is acked at once, without sending, at a place that
// acknowledged the very row it heals (the key's previous done row, a put
// of the same hash; a change EP-11 held back is passed over, since it was
// never placed, #22) and has nothing of the key queued: exactly-once
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
	// The row a healing re-upload heals: the key's latest done row that
	// no change held back (EP-11, #22; such a change was never placed). A
	// place that refused it as holding another version would refuse its
	// bytes again, so it gets no copy.
	const healed = `(SELECT max(l.seq) FROM queue l
		WHERE l.bucket = ?1 AND l.key = ?2 AND l.seq < ?3 AND l.state = 'done' AND l.immutable IS NULL)`
	var skip []string
	if heal {
		if skip, err = column(ctx, tx, `SELECT place FROM placement WHERE seq = `+healed+`
			AND state = 'refused' AND cause = '`+CauseImmutable+`'`, bucket, key, seq); err != nil {
			return err
		}
	}
	var targets []string
	switch {
	case op == OpDelete:
		// A put no sender has taken is cancelled, not followed: nothing
		// of it reached the place (REPL-2, #21), and the store no longer
		// holds its bytes. One taken may have landed, and stays ahead of
		// the delete in its FIFO.
		if _, err := tx.ExecContext(ctx, `DELETE FROM placement WHERE bucket = ? AND key = ? AND op = 'put'
			AND state = 'queued' AND started_at IS NULL AND seq < ?`, bucket, key, seq); err != nil {
			return err
		}
		targets, err = column(ctx, tx, `SELECT DISTINCT p.place FROM placement p
			WHERE p.place IN (`+poolPlacesSQL+`) AND p.bucket = ?1 AND p.key = ?2 AND p.op = 'put'
			AND p.seq = (SELECT max(x.seq) FROM placement x
				WHERE x.place = p.place AND x.bucket = p.bucket AND x.key = p.key AND x.seq < ?3)
			ORDER BY p.place`, bucket, key, seq)
	case class == "metadata" || class == "snapshot":
		// Every friend, and every overflow target holding data of the
		// bucket (REPL-12).
		targets, err = column(ctx, tx, `SELECT peer_key FROM friendship WHERE coalesce(address, '') != '' ORDER BY peer_key`)
		if err == nil {
			var held []string
			held, err = overflowHolding(ctx, tx, bucket)
			targets = append(targets, held...)
		}
		targets = slices.DeleteFunc(targets, func(p string) bool { return slices.Contains(skip, p) })
	default:
		if heal {
			targets, err = column(ctx, tx, `SELECT p.place FROM placement p
				WHERE p.place IN (`+poolPlacesSQL+`) AND p.state NOT IN ('`+PlacementDropped+`', '`+PlacementAbandoned+`')
				AND p.cause IS NOT '`+CauseImmutable+`' AND p.seq = `+healed+`
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
		// Split (REPL-12 as of v0.11.5): copies − 1 friends and one
		// overflow target, while friends cannot hold copies of it all.
		maxFriends := -1
		split, err := db.overflowSplit(ctx, tx)
		if err != nil {
			return err
		}
		if split {
			held := 0
			for _, t := range targets {
				if _, ok := OverflowName(t); !ok {
					held++
				}
			}
			maxFriends = max(copies-1-held, 0)
		}
		var more []string
		more, err = db.choose(ctx, tx, bucket, size+ObjectOverhead, copies-len(targets), maxFriends, append(slices.Clone(targets), skip...), false)
		// A place given its first data copy of the bucket gets every
		// metadata object of the bucket it lacks first, ahead of it in
		// its FIFO (REPL-12, S3-2): an overflow target, or a friend
		// added since re-placement last ran (#38 review).
		for _, place := range more {
			if err != nil {
				break
			}
			_, err = placeMetadata(ctx, tx, bucket, place, db.now().Unix())
		}
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
	return insertPlacement(ctx, tx, seq, place, heal, db.now().Unix())
}

// insertPlacement is DB.insertPlacement in x, acknowledged at now (unix
// seconds) when held.
func insertPlacement(ctx context.Context, x execer, seq int64, place string, heal bool, now int64) error {
	_, err := x.ExecContext(ctx, `WITH c AS (
		SELECT q.seq, q.bucket, q.key, q.op, q.sha256,
			?3 AND q.op = 'put'
			AND EXISTS (SELECT 1 FROM placement a
				WHERE a.place = ?2 AND a.state = 'acked' AND a.sha256 = q.sha256
				AND a.seq = (SELECT max(l.seq) FROM queue l
					WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq < q.seq AND l.state = 'done'
					AND l.immutable IS NULL))
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
		FROM c`, seq, place, heal, ObjectOverhead, now)
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

// Held reports whether row seq is a row the barrier holds back
// (REPL-1): a metadata or snapshot row, put or delete, is sent to any
// place only when no data put of its bucket before it has a queued
// placement row left, each acked at every place it is placed at. Rows
// refused for good are not queued, so a friend that refuses everything
// cannot stall the barrier (#20 review): the object is below copies,
// which status shows. A queued put that a done write of the key before
// the metadata replaced will be cancelled, not sent, so it does not hold
// it either; one replaced only after the metadata still does, as the
// metadata was written while it was the key's bytes. A data put before
// it still pending holds it too (#20 review): a completed multipart
// upload is answered before its row is done and placed (#49), so the
// tool may write the index naming it first.
//
// The mirror holds a data delete (#21 review): it is sent to any place
// only when no metadata or snapshot row of its bucket before it, put
// or delete, is still queued at a place that answers, so no place
// deletes a pack while another still holds live, or has yet to
// replace or delete, metadata naming it. A place unreachable for
// reroute_after, or full (507) for that long, does not hold deletes:
// it would stop every place's FIFO for as long as it stays away (its
// replica is behind until it returns, and its FIFO then brings it
// level).
//
// Rows a pending seed holds (REPL-8, seed.go) count in neither check,
// as an unreachable place's do not (#21): the seeded friend's replica
// of the bucket lags while the disk travels, and every other place goes
// on. The seeded friend's own FIFO keeps the order, since it is sent
// nothing of the bucket until the seed ends.
func (db *DB) Held(ctx context.Context, seq int64) (bool, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return false, err
	}
	now := db.now()
	answering := []string{}
	for _, p := range ps {
		if !p.unreachable(db, now) && !p.fullForLong(db, now) {
			answering = append(answering, p.peer)
		}
	}
	list, err := json.Marshal(answering)
	if err != nil {
		return false, err
	}
	var held bool
	err = db.QueryRowContext(ctx, `SELECT CASE
		WHEN q.class IN ('metadata', 'snapshot') THEN EXISTS (
			SELECT 1 FROM placement p JOIN queue d ON d.seq = p.seq
			WHERE p.bucket = q.bucket AND p.seq < q.seq AND p.state = 'queued' AND p.op = 'put' AND d.class = 'data'
			AND NOT `+seedHeld+`
			AND NOT EXISTS (SELECT 1 FROM queue l
				WHERE l.bucket = p.bucket AND l.key = p.key AND l.seq > p.seq AND l.seq < q.seq AND l.state = 'done'))
			OR EXISTS (SELECT 1 FROM queue u WHERE u.state = 'pending' AND u.bucket = q.bucket AND u.seq < q.seq
				AND u.op = 'put' AND u.class = 'data')
		WHEN q.op = 'delete' THEN EXISTS (
			SELECT 1 FROM placement p JOIN queue m ON m.seq = p.seq
			WHERE p.bucket = q.bucket AND p.seq < q.seq AND p.state = 'queued' AND m.class IN ('metadata', 'snapshot')
			AND p.place IN (SELECT value FROM json_each(?2)) AND NOT `+seedHeld+`)
		ELSE 0 END
		FROM queue q WHERE q.seq = ?1`, seq, string(list)).Scan(&held)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return held, err
}

// seedHeld is true for placement row p while a pending seed holds it
// (NextPlacement).
const seedHeld = `EXISTS (SELECT 1 FROM seed s WHERE s.place = p.place AND s.bucket = p.bucket AND s.done_at IS NULL)`

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
// once. A row never goes to a friend that holds the object, nor to one
// this pass already sent another copy of it to. It applies the moves in
// short write transactions, each under lock when lock is set. A row is
// applied only if it is still queued there and sending(place, seq) is
// false under the lock: its send may land, and the place's row must be
// there to record it, or no delete would reach the copy. A batch that
// fails is rolled back and retried one row at a time, so a bad row
// holds only itself (#68); the pass goes on and returns the rows'
// errors together with what the others did. A busy or locked database
// (busy) stops the pass at once: every row would wait out busy_timeout
// in turn under lock, and the rest wait for the next pass.
//
// An overflow target is never a target of a move (#38): copies go
// there only by D6's step 3, at capture or re-placement. As a source it
// is like a friend: unsent data at one failing for reroute_after goes
// to a friend that has room by now, and at one gone past replace_after
// is dropped, so it holds no barrier for good.
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
	// with room less what this pass's moves took. A friend without room
	// for one more object leaves the list.
	type target struct {
		peer       string
		quota, use int64
		taken      int64 // bytes this pass moved there
	}
	var targets []*target
	for _, p := range ps {
		if p.overflow || p.quota <= 0 || p.cond == ConditionRemoved || p.full() || p.gone(db, now) || p.unreachable(db, now) {
			continue
		}
		if fits(freeRoom(p.quota, p.use, 0, live), ObjectOverhead) {
			targets = append(targets, &target{peer: p.peer, quota: capped(p.quota), use: p.use})
		}
	}
	unsent := map[[2]string]int64{}            // bucket, place -> unsentMetadata
	unverified := map[string]map[string]bool{} // bucket -> unverifiedAt
	room := func(t *target, bucket string) (int64, error) {
		u, ok := unverified[bucket]
		if !ok {
			var err error
			if u, err = unverifiedAt(ctx, db, bucket); err != nil {
				return 0, err
			}
			unverified[bucket] = u
		}
		if u[t.peer] {
			return -1, nil // its replica of the bucket failed a challenge (#27)
		}
		k := [2]string{bucket, t.peer}
		meta, ok := unsent[k]
		if !ok {
			var err error
			if meta, err = unsentMetadata(ctx, db, bucket, t.peer); err != nil {
				return 0, err
			}
			unsent[k] = meta
		}
		if meta > 0 {
			return -1, nil // its metadata first (REPL-13, #26): no room for data
		}
		return freeRoom(t.quota, t.use, capped(t.taken), live), nil
	}
	sentTo := map[int64][]string{} // seq -> friends this pass moved a copy of it to
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
				if slices.Contains(r.holders, t.peer) || slices.Contains(sentTo[r.seq], t.peer) {
					continue
				}
				free, err := room(t, r.bucket)
				if err != nil {
					return 0, 0, err
				}
				if !fits(free, r.need) {
					continue
				}
				if to == nil || byFraction(t.peer, free, t.quota, to.peer, toFree, to.quota) < 0 {
					to, toFree = t, free
				}
			}
			switch {
			case to != nil:
				to.taken = capped(to.taken + r.need)
				if !fits(freeRoom(to.quota, to.use, to.taken, live), ObjectOverhead) {
					targets = slices.DeleteFunc(targets, func(t *target) bool { return t == to })
				}
				sentTo[r.seq] = append(sentTo[r.seq], to.peer)
				ops = append(ops, rerouteOp{from: p.peer, seq: r.seq, to: to.peer})
			case gone || full && r.barrier:
				ops = append(ops, rerouteOp{from: p.peer, seq: r.seq})
			}
		}
	}
	var errs []error
	for len(ops) > 0 && ctx.Err() == nil {
		n := min(len(ops), 64)
		m, d, err := db.applyReroute(ctx, ops[:n], sending, lock)
		if err == nil {
			moved, dropped = moved+m, dropped+d
			ops = ops[n:]
			continue
		}
		if busy(err) {
			// The whole database is held: each row would wait out
			// busy_timeout in turn. The rest wait for the next pass.
			errs = append(errs, fmt.Errorf("re-routing rows %d to %d: %w; the rest wait for the next pass",
				ops[0].seq, ops[n-1].seq, err))
			break
		}
		// One row at a time, so a bad row holds only itself (#68).
		stop := false
		for _, op := range ops[:n] {
			if ctx.Err() != nil {
				break
			}
			m, d, err := db.applyReroute(ctx, []rerouteOp{op}, sending, lock)
			if err != nil {
				errs = append(errs, fmt.Errorf("re-routing row %d from %s: %w", op.seq, op.from, err))
				if stop = busy(err); stop {
					break
				}
				continue
			}
			moved, dropped = moved+m, dropped+d
		}
		if stop {
			break
		}
		ops = ops[n:]
	}
	return moved, dropped, errors.Join(append(errs, ctx.Err())...)
}

// busy reports whether err is SQLite's SQLITE_BUSY or SQLITE_LOCKED
// (any extended code of either): the database is held, not the row bad.
func busy(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	c := se.Code() & 0xff
	return c == sqlite3.SQLITE_BUSY || c == sqlite3.SQLITE_LOCKED
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
		// A copy re-placement placed keeps what it replaces (#26).
		var replaces sql.NullString
		var replacedAt sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT replaces, replaced_at FROM placement WHERE place = ? AND seq = ?`,
			op.from, op.seq).Scan(&replaces, &replacedAt); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, 0, err
		}
		// A row a sender took may have landed: it stays, abandoned, so
		// a later delete still reaches the friend (#21 review).
		r, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementAbandoned+`'
			WHERE place = ? AND seq = ? AND state = 'queued' AND started_at IS NOT NULL`, op.from, op.seq)
		if err != nil {
			return 0, 0, err
		}
		n, err := r.RowsAffected()
		if err != nil {
			return 0, 0, err
		}
		if n == 0 {
			r, err := tx.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`, op.from, op.seq)
			if err != nil {
				return 0, 0, err
			}
			if n, err = r.RowsAffected(); err != nil {
				return 0, 0, err
			}
		}
		if n == 0 {
			continue // sent, refused or cancelled since the scan
		}
		if err := db.insertPlacement(ctx, tx, op.seq, op.to, false); err != nil {
			return 0, 0, err
		}
		if replacedAt.Valid {
			if _, err := tx.ExecContext(ctx, `UPDATE placement SET replaces = ?, replaced_at = ? WHERE place = ? AND seq = ?`,
				replaces, replacedAt, op.to, op.seq); err != nil {
				return 0, 0, err
			}
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
// replace_after, and overflow targets not gone past it (OPS-2, EP-6,
// #38).
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
// ObjectOverhead, less what deletes free; since its first quota answer,
// deletes free nothing until the next answer (place_freed, #68).
func (db *DB) PlaceUse(ctx context.Context) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT u.place, u.bytes + coalesce(fr.bytes, 0)
		FROM place_use u LEFT JOIN place_freed fr ON fr.place = u.place`)
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
// or will hold (acked or queued rows at a friend): the deficit the pool
// lacks room for, refusals, copies dropped at a friend gone past
// replace_after (REPL-4), and copies at a friend removed since (PEER-5,
// #25), until re-placement (#26). Buckets with none are absent.
func (db *DB) BelowCopies(ctx context.Context) (map[string]BelowStat, error) {
	return db.below(ctx, "")
}

func (db *DB) below(ctx context.Context, and string) (map[string]BelowStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.bucket, count(*), coalesce(sum(q.size), 0) FROM queue q
		WHERE q.state = 'done' AND q.op = 'put'
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		AND (SELECT count(*) FROM placement p WHERE p.seq = q.seq AND p.state IN ('queued', 'acked')
			AND p.place IN (SELECT peer_key FROM friendship UNION ALL `+overflowPlaces+`))
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
