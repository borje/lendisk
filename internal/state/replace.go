package state

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// This file is re-placement (REPL-13, PEER-5, PEER-6, EP-6, #26): the
// owner's node moves copies when the pool changes, never letting an
// object fall below the copies it has.
//
// A pass (Replace) looks at every live object of every bucket (the key's
// latest done write is a put whose bytes the store holds):
//   - a metadata object missing at a friend gets a row there, at its
//     own sequence, so a friend added later receives the bucket's
//     metadata ahead of any newer data; until then D6's choice gives
//     that friend no data (choose, Reroute: a friend with live metadata
//     of the bucket not placed there is no candidate);
//   - a data object held or queued at fewer than `copies` good places
//     gets the missing copies by D6's choice, excluding the places that
//     hold it, refused it or are deleting it; a copy at a place being
//     drained (a friend removed, gone past replace_after, refusing this
//     node, above its quota, or whose replica of the bucket failed a
//     challenge, VER-1, verify.go) is counted out, and its replacement
//     records which copy it replaces (placement.replaces);
//   - a copy at a drained place is marked dropping once enough copies
//     are acknowledged elsewhere: at the acknowledgement of its
//     replacement (AckPlacement), or by a pass that finds the object at
//     `copies` acknowledged good places. A dropping row is a delete in
//     its place's FIFO, at its sequence; acknowledged, it is drained
//     (the lender keeps a tomb for the window, RET-1). Until a sender
//     takes it, the removal rule is asked again: by each pass, for an
//     object it may leave short, and as it is sent (checkDrop, #104);
//     refused, it is acknowledged again and the pass places what is
//     missing;
//   - surplus copies (copies lowered, or a gone friend back while its
//     replacement waits) are cancelled while unsent, the fullest place
//     first (an overflow target before any friend, except the last
//     target copy while placement is split), then marked dropping while
//     more than `copies` are acknowledged;
//   - drain-back (#40): while the friends hold the repository with the
//     hysteresis of overflowDrain, and placement is not split, a last
//     round gives each object with an acknowledged copy at an overflow
//     target a friend copy in its place, oldest first, where a friend
//     has room after every other copy is placed; the target's copy is
//     deleted once that one is acknowledged (AckPlacement). A drain-back copy is the lowest priority in its
//     friend's FIFO and holds no metadata barrier (NextPlacement,
//     Held). A target left with no data of a bucket has its metadata
//     of the bucket deleted too (dropIdleMetadata).
//
// Every copy a pass or an acknowledgement drops or cancels goes through
// the one removal rule, mayRemove (removal.go, I1 to I3): decide's
// choices are written by removeCopy and cancelCopy, which ask it again
// in the batch's transaction.
//
// Rules a caller must keep: the bytes of a new copy come only from the
// node's store, as for any placement row (no lender forwards, PEER-11);
// a pass shares a per-bucket lock with the placer (FinishPut,
// FinishDelete), and writes in batches of at most replaceBatch objects,
// each its own short transaction.

// Re-placement's placement row states (REPL-13, #26). A dropping row is
// a copy the place holds (it was acked) that re-placement deletes
// there: in the place's FIFO as a delete of the key, at the row's
// sequence; out of place_use, but counted back in place_freed until the
// place's next quota answer, since the lender keeps the tomb counted. A
// drained row is that delete acknowledged.
const (
	PlacementDropping = "dropping"
	PlacementDrained  = "drained"
)

// replaceBatch is how many objects one re-placement transaction
// decides about.
const replaceBatch = 64

// replacePage is how many objects one read of a bucket's scan returns.
const replacePage = 2048

// ReplaceStats is what a pass did.
type ReplaceStats struct {
	Placed     int64 // copies placed, or queued again, data and metadata
	Dropping   int64 // copies marked dropping
	Cancelled  int64 // unsent surplus copies cancelled
	Below      int64 // live data objects still below copies
	BelowBytes int64 // their bytes
	// BelowSmallest is the size of the smallest of them (when Below > 0):
	// the pass records it, and the pool fingerprint asks whether a place
	// has room for it (#106).
	BelowSmallest int64
	Skipped       int64 // copies left alone as taken or being sent, which may land: a later pass looks again
}

func (s *ReplaceStats) add(o ReplaceStats) {
	s.Placed += o.Placed
	s.Dropping += o.Dropping
	s.Cancelled += o.Cancelled
	s.addBelow(o.Below, o.BelowBytes, o.BelowSmallest)
}

// below counts one object of size still below copies.
func (s *ReplaceStats) below(size int64) { s.addBelow(1, size, size) }

func (s *ReplaceStats) addBelow(objects, bytes, smallest int64) {
	if objects <= 0 {
		return
	}
	if s.Below == 0 || smallest < s.BelowSmallest {
		s.BelowSmallest = smallest
	}
	s.Below += objects
	s.BelowBytes += bytes
}

// lockBucket takes bucket's lock, which the placer and re-placement
// share so they never decide about one object at once (REPL-13), and
// returns its unlock. Take it before the transaction begins.
func (db *DB) lockBucket(bucket string) func() {
	m, _ := db.bucketLocks.LoadOrStore(bucket, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// lockBuckets takes the locks of the buckets of queue rows seqs, in name
// order, and returns their unlock.
func (db *DB) lockBuckets(ctx context.Context, seqs ...int64) (func(), error) {
	list, err := json.Marshal(seqs)
	if err != nil {
		return nil, err
	}
	names, err := column(ctx, db, `SELECT DISTINCT bucket FROM queue WHERE seq IN (SELECT value FROM json_each(?))
		ORDER BY bucket`, string(list))
	if err != nil {
		return nil, err
	}
	var unlocks []func()
	for _, b := range names {
		unlocks = append(unlocks, db.lockBucket(b))
	}
	return func() {
		for _, u := range slices.Backward(unlocks) {
			u()
		}
	}, nil
}

// SetCopies changes bucket's copies (EP-6); the daemon's next pass adds
// or removes copies to match. ErrNotFound if there is no such bucket.
func (db *DB) SetCopies(ctx context.Context, bucket string, copies int) error {
	if copies < 1 {
		return fmt.Errorf("copies %d: must be at least 1", copies)
	}
	r, err := db.ExecContext(ctx, `UPDATE buckets SET copies = ? WHERE name = ?`, copies, bucket)
	if err != nil {
		return err
	}
	if n, err := r.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNotFound
	}
	return nil
}

// PoolFingerprint describes what re-placement answers to: each friend
// with an address, its quota, whether it refuses this node, is gone
// past replace_after, counts more than its quota, or is full by its last
// answer (paused for quota, LEND-2: no room until it acknowledges again
// or the quota rises); whether it has room for the smallest object the
// last pass left below copies (#106: a quota answer replaces this
// node's count of its use there, place_use, and can give room); each
// bucket's copies; and each replica marked unverified (VER-1, #27). The
// worker runs a pass whenever it changes (#26), and Offsite counts no
// object short of places as stuck while it differs from the pool the
// last pass answered (passDue).
//
// Room is fingerprinted coarsely, as that one yes or no per place: use
// moves with every capture, and a pass per capture would be waste.
func (db *DB) PoolFingerprint(ctx context.Context) (string, error) {
	var smallest sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT short_min FROM replace_pass WHERE id = 1`).Scan(&smallest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	v, err := db.readPool(ctx, smallest.Valid)
	if err != nil {
		return "", err
	}
	return v.fingerprint(db, smallest, nil), nil
}

// poolView is the pool as PoolFingerprint reads it.
type poolView struct {
	places []poolPlace
	now    time.Time
	live   int64  // the live metadata, read when room is asked about
	rest   string // each bucket's copies and each replica marked unverified
}

// readPool reads the pool; withLive reads the live metadata too, which
// room depends on.
func (db *DB) readPool(ctx context.Context, withLive bool) (poolView, error) {
	v := poolView{now: db.now()}
	var err error
	if v.places, err = poolPlaces(ctx, db); err != nil {
		return v, err
	}
	if withLive {
		if v.live, err = liveMetadata(ctx, db); err != nil {
			return v, err
		}
	}
	var b strings.Builder
	rows, err := db.QueryContext(ctx, `SELECT name, copies FROM buckets ORDER BY name`)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var copies int
		if err := rows.Scan(&name, &copies); err != nil {
			return v, err
		}
		fmt.Fprintf(&b, "|%s %d", name, copies)
	}
	if err := rows.Err(); err != nil {
		return v, err
	}
	rows.Close()
	marks, err := column(ctx, db, `SELECT place || ' ' || bucket FROM replica_check
		WHERE unverified_at IS NOT NULL ORDER BY place, bucket`)
	if err != nil {
		return v, err
	}
	for _, m := range marks {
		fmt.Fprintf(&b, "|unverified %s", m)
	}
	v.rest = b.String()
	return v, nil
}

// fingerprint renders the pool, asking each place whether it has room
// for an object of smallest bytes (when valid): the pass's rule
// (replacer.choose), for a place that may take a copy at all. Only a
// yes is written, so a pool with no room reads as before #106. With
// after (a pass's record: v as it began, after as it ended), a place
// has room only if it had it in both: room the pass took is no room
// now, and room that came during the pass is no room it answered.
func (v poolView) fingerprint(db *DB, smallest sql.NullInt64, after *poolView) string {
	var b strings.Builder
	var roomAfter map[string]bool
	if after != nil && smallest.Valid {
		roomAfter = map[string]bool{}
		for _, p := range after.places {
			roomAfter[p.peer] = after.room(db, p, smallest.Int64+ObjectOverhead)
		}
	}
	for _, p := range v.places {
		fmt.Fprintf(&b, "%s %d %t %t %t %t", p.peer, p.quota, p.cond == ConditionRemoved, p.gone(db, v.now),
			capped(p.use) > capped(p.quota), p.full())
		if p.removing {
			b.WriteString(" removing") // #96
		}
		if smallest.Valid && v.room(db, p, smallest.Int64+ObjectOverhead) && (after == nil || roomAfter[p.peer]) {
			b.WriteString(" room") // #106
		}
		b.WriteString(";")
	}
	b.WriteString(v.rest)
	return b.String()
}

// room reports whether place p may take a copy that counts need bytes:
// a candidate (rfriend.candidate) with free room for it.
func (v poolView) room(db *DB, p poolPlace, need int64) bool {
	if p.quota <= 0 || p.cond == ConditionRemoved || p.gone(db, v.now) || p.removing || p.full() {
		return false
	}
	if p.overflow {
		return capped(p.quota)-capped(max(p.use, 0)) >= capped(need)
	}
	return fits(freeRoom(p.quota, p.use, 0, v.live), need)
}

// BelowSince is a bucket's record of live data objects below copies
// (OPS-2): since when a pass has found some, continuously, and how many
// at its last pass.
type BelowSince struct {
	Since          time.Time
	Objects, Bytes int64
}

// BelowCopiesSince returns each bucket the last pass found with live
// data objects below copies; buckets with none are absent.
func (db *DB) BelowCopiesSince(ctx context.Context) (map[string]BelowSince, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, since, objects, bytes FROM below_copies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]BelowSince{}
	for rows.Next() {
		var b string
		var since int64
		var s BelowSince
		if err := rows.Scan(&b, &since, &s.Objects, &s.Bytes); err != nil {
			return nil, err
		}
		s.Since = time.Unix(since, 0)
		out[b] = s
	}
	return out, rows.Err()
}

// ReplacingStat is a bucket's re-placement in progress: copies it
// placed that are still queued, and copies being deleted at places
// being drained (status, OPS-1).
type ReplacingStat struct {
	Queued, QueuedBytes int64
	Dropping            int64
}

// Replacing returns each bucket's re-placement in progress; buckets with
// none are absent. Only friends count: a removed friend's moved copies
// are kept there until its purge, and nothing waits to delete them.
// It reads the queued and dropping rows by their partial indexes, not
// every row (#116).
func (db *DB) Replacing(ctx context.Context) (map[string]ReplacingStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, sum(queued), sum(bytes), sum(dropping) FROM (
			SELECT p.bucket, 1 AS queued, coalesce(q.size, 0) AS bytes, 0 AS dropping
			FROM placement p INDEXED BY placement_queued JOIN queue q ON q.seq = p.seq
			WHERE p.state = 'queued' AND p.replaced_at IS NOT NULL
			AND p.place IN (SELECT peer_key FROM friendship UNION ALL `+overflowPlaces+`)
			UNION ALL
			SELECT p.bucket, 0, 0, 1 FROM placement p INDEXED BY placement_dropping JOIN queue q ON q.seq = p.seq
			WHERE p.state = 'dropping' AND p.place IN (SELECT peer_key FROM friendship UNION ALL `+overflowPlaces+`))
		GROUP BY bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ReplacingStat{}
	for rows.Next() {
		var b string
		var s ReplacingStat
		if err := rows.Scan(&b, &s.Queued, &s.QueuedBytes, &s.Dropping); err != nil {
			return nil, err
		}
		out[b] = s
	}
	return out, rows.Err()
}

// rfriend is a friend with an address as a pass sees it.
type rfriend struct {
	poolPlace
	drainAll  bool           // removed by its answer, or gone past replace_after: every copy moves
	drain     map[int64]bool // above its quota: the data copies that move
	candidate bool           // may take a data copy
	reachable bool
	taken     int64 // bytes this pass placed there
}

// replacer is one pass.
type replacer struct {
	db      *DB
	friends map[string]*rfriend
	order   []*rfriend // node ID order
	live    int64      // liveMetadata
	now     time.Time
	unsent  map[[2]string]int64 // bucket, place -> unsentMetadata
	sending func(place string, seq int64) bool
	lock    sync.Locker
	skipped int64
	// unverified are the friends whose replica of the bucket in hand
	// failed a challenge (VER-1, #27): drained of its data copies, as a
	// removed friend is, for that bucket alone.
	unverified map[string]bool
	// metaPlaces are where the bucket in hand's metadata goes: every
	// friend with an address, and each overflow target holding data of
	// it (REPL-12).
	metaPlaces []string
	// split: placement is split between copies − 1 friends and one
	// overflow target (overflowSplit, REPL-12 as of v0.11.5).
	split bool
	// moves: this round may move a copy held only at friends to a
	// target (the second round of a split bucket, dataRound).
	moves bool
	// drainBack: copies at overflow targets may move back to friends
	// (overflowDrain, never while split); a target's copy a friend's
	// replaces is then moving, and counted out.
	drainBack bool
	// drains: this round moves copies from targets to friends (the last
	// round while drainBack).
	drains bool
}

// Replace runs one re-placement pass over every bucket (REPL-13, file
// comment above). sending reports whether a place's sender holds a row
// (a send that may land), which is never cancelled; lock, when set, is
// held with the bucket's lock for each batch, as the worker's sending
// set is read under it. A batch that fails is rolled back and the pass
// goes on: the errors are returned with what the others did.
func (db *DB) Replace(ctx context.Context, sending func(place string, seq int64) bool, lock sync.Locker) (ReplaceStats, error) {
	var st ReplaceStats
	// The pool this pass answers, read before the places it decides on:
	// a change during the pass leaves the record behind it, so the next
	// pass is still due. Its room is asked about the smallest object
	// this pass leaves below copies (#106), and must last to its end, so
	// neither a pass that changed nothing nor one that took the room is
	// due again.
	pool, err := db.readPool(ctx, true)
	if err != nil {
		return st, err
	}
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return st, err
	}
	m := &replacer{db: db, friends: map[string]*rfriend{}, now: db.now(), unsent: map[[2]string]int64{},
		sending: sending, lock: lock}
	if m.split, err = db.overflowSplit(ctx, db); err != nil {
		return st, err
	}
	if m.drainBack, err = db.overflowDrain(ctx); err != nil {
		return st, err
	}
	m.drainBack = m.drainBack && !m.split
	if m.live, err = liveMetadata(ctx, db); err != nil {
		return st, err
	}
	for _, p := range ps {
		f := &rfriend{poolPlace: p, reachable: !p.unreachable(db, m.now)}
		// A target being removed is drained like a friend that ended
		// (#96): its copies are placed elsewhere and dropped there once
		// the object has `copies` acknowledged copies without them.
		f.drainAll = p.cond == ConditionRemoved || p.gone(db, m.now) || p.removing
		f.candidate = p.quota > 0 && !f.drainAll && !p.full()
		// An overflow target above max_bytes is not drained: its
		// copies move back to friends only by drain-back (#40).
		if !f.drainAll && !p.overflow {
			if f.drain, err = db.overQuota(ctx, p); err != nil {
				return st, err
			}
		}
		m.friends[p.peer] = f
		m.order = append(m.order, f)
	}
	type bucketCopies struct {
		name   string
		copies int
	}
	var bs []bucketCopies
	rows, err := db.QueryContext(ctx, `SELECT name, copies FROM buckets ORDER BY name`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var b bucketCopies
		if err := rows.Scan(&b.name, &b.copies); err != nil {
			rows.Close()
			return st, err
		}
		bs = append(bs, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	var errs []error
	for _, b := range bs {
		s, err := m.bucket(ctx, b.name, b.copies)
		st.add(s)
		if err != nil {
			errs = append(errs, fmt.Errorf("re-placing bucket %s: %w", b.name, err))
		}
		if ctx.Err() != nil {
			break
		}
	}
	st.Skipped = m.skipped
	// A pass that went through every bucket answered the pool, even
	// when a bucket's batch failed (#90), so what it left short is
	// stuck. Counting a failed bucket as answered is the sound choice:
	// not counting it would keep its short objects, and with one
	// record per pass every bucket's, waiting for as long as the error
	// repeats, and `status --wait` with them; a failure that persists
	// needs a person, which is what stuck says. The worker tries a
	// failed pass again at its next re-routing tick (Worker.replace),
	// so a passing failure is stuck for a minute, then placed. A pass
	// cut short by its context answered nothing past where it stopped:
	// the pass stays due.
	if ctx.Err() == nil {
		var smallest sql.NullInt64
		if st.Below > 0 {
			smallest = sql.NullInt64{Int64: st.BelowSmallest, Valid: true}
		}
		end, err := db.readPool(ctx, true)
		if err == nil {
			_, err = db.ExecContext(ctx, `INSERT INTO replace_pass (id, pool, done_at, short_min) VALUES (1, ?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET pool = excluded.pool, done_at = excluded.done_at, short_min = excluded.short_min`,
				pool.fingerprint(db, smallest, &end), db.now().Unix(), smallest)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("recording the re-placement pass: %w", err))
		}
	}
	return st, errors.Join(append(errs, ctx.Err())...)
}

// passDue reports whether a re-placement pass is due: the pool differs
// from the one the last pass that went through every bucket answered
// (with or without a failed batch, #90), or no pass has.
// The daemon runs one within five polls of a change (#26); until it
// has, an object short of places may yet be placed.
func (db *DB) passDue(ctx context.Context) (bool, error) {
	now, err := db.PoolFingerprint(ctx)
	if err != nil {
		return false, err
	}
	var last string
	err = db.QueryRowContext(ctx, `SELECT pool FROM replace_pass WHERE id = 1`).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return err == nil && last != now, err
}

// overQuota returns, for a friend that holds more live bytes than its
// quota (PEER-6: lowered below use), the acknowledged data copies to
// move away, oldest first, until what it still holds fits the quota;
// nil when it fits. What it holds counts its queued and acknowledged
// puts of live objects, not deleted ones it keeps as tombs, which no
// move frees.
func (db *DB) overQuota(ctx context.Context, p poolPlace) (map[int64]bool, error) {
	if capped(p.use) <= capped(p.quota) {
		return nil, nil // the count includes everything held
	}
	rows, err := db.QueryContext(ctx, `SELECT p.seq, coalesce(p.size, 0), p.state = 'acked' AND q.class = 'data'
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.state IN ('queued', 'acked') AND p.op = 'put' AND q.immutable IS NULL
		AND NOT EXISTS (SELECT 1 FROM queue l
			WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')
		ORDER BY p.seq`, p.peer)
	if err != nil {
		return nil, err
	}
	type held struct {
		seq, size int64
		movable   bool
	}
	var hs []held
	var total int64
	for rows.Next() {
		var h held
		if err := rows.Scan(&h.seq, &h.size, &h.movable); err != nil {
			rows.Close()
			return nil, err
		}
		hs = append(hs, h)
		total = capped(total + h.size)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	excess := total - capped(p.quota)
	if excess <= 0 {
		return nil, nil
	}
	out := map[int64]bool{}
	for _, h := range hs {
		if excess <= 0 {
			break
		}
		if h.movable {
			out[h.seq] = true
			excess -= h.size
		}
	}
	return out, nil
}

// unsentAt is bucket's live metadata not placed at place (cached for the
// pass; forget drops a bucket's entries once its metadata is placed).
func (m *replacer) unsentAt(ctx context.Context, bucket, place string) (int64, error) {
	k := [2]string{bucket, place}
	if n, ok := m.unsent[k]; ok {
		return n, nil
	}
	n, err := unsentMetadata(ctx, m.db, bucket, place)
	if err != nil {
		return 0, err
	}
	m.unsent[k] = n
	return n, nil
}

// prow is a placement row of an object as a pass sees it.
type prow struct {
	place, state, replaces, cause string
	taken                         bool // a sender took it once (started_at)
}

func (r prow) holds() bool { return r.state == PlacementQueued || r.state == PlacementAcked }

// storeRefused: the store could not supply the object (RefuseObject), so
// no copy of it can be sent.
func storeRefused(rows []prow) bool {
	for _, r := range rows {
		if r.state == PlacementRefused && (r.cause == CauseStore || r.cause == CauseOversize) {
			return true
		}
	}
	return false
}

// robj is a live object of a bucket.
type robj struct {
	seq, size int64
	rows      []prow
}

// Kinds of a pass's change to one row.
const (
	opPlace  = iota // a new copy: insert, or queue a dropped, abandoned or drained row again
	opDrop          // an acknowledged copy: mark it dropping
	opCancel        // an unsent surplus copy: take it out of the FIFO
)

type rop struct {
	kind     int
	place    string
	replaces string // opPlace: the drained place whose copy it replaces
}

const liveQ = `NOT EXISTS (SELECT 1 FROM queue l
	WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`

// bucket runs the pass over one bucket: metadata first, so a friend
// added later becomes a candidate for data in the same pass, then data.
func (m *replacer) bucket(ctx context.Context, bucket string, copies int) (ReplaceStats, error) {
	var st ReplaceStats
	var errs []error
	// After recover (ID-5), a bucket's placement is rebuilt from the
	// friends' lists: until every list is read and settled, a copy that
	// looks missing may be one a friend not yet listed holds, and a row
	// placed now would stand in the way of the acknowledged row its list
	// records. The bucket waits for them, except an unread list at a
	// place gone past replace_after (#71): its copies count as gone, as
	// any gone place's do, and the bucket waits again if it answers.
	unread, err := column(ctx, m.db, `SELECT place FROM relist WHERE bucket = ? AND abandoned IS NULL AND done IS NULL`, bucket)
	if err != nil {
		return st, err
	}
	for _, p := range unread {
		if f := m.friends[p]; f == nil || !f.gone(m.db, m.now) {
			return st, nil
		}
	}
	var unsettled bool
	if err := m.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM relist WHERE bucket = ?
		AND (done IS NOT NULL OR abandoned IS NOT NULL) AND settled IS NULL)`, bucket).Scan(&unsettled); err != nil || unsettled {
		return st, err
	}
	if m.unverified, err = unverifiedAt(ctx, m.db, bucket); err != nil {
		return st, err
	}
	// Metadata missing at a friend with an address, or at an overflow
	// target holding data of the bucket (REPL-12).
	m.metaPlaces = nil
	for _, f := range m.order {
		if !f.overflow {
			m.metaPlaces = append(m.metaPlaces, f.peer)
		}
	}
	held, err := overflowHolding(ctx, m.db, bucket, m.now.Unix())
	if err != nil {
		return st, err
	}
	m.metaPlaces = append(m.metaPlaces, held...)
	list, err := json.Marshal(m.metaPlaces)
	if err != nil {
		return st, err
	}
	meta, err := column(ctx, m.db, `SELECT DISTINCT q.seq FROM queue q, json_each(?2) f
		WHERE q.bucket = ?1 AND q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND q.immutable IS NULL AND `+liveQ+`
		AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = f.value AND p.seq = q.seq
			AND p.state IN ('queued', 'acked', 'refused', 'dropping'))
		AND NOT EXISTS (SELECT 1 FROM placement r WHERE r.seq = q.seq AND r.state = 'refused'
			AND r.cause IN ('store', 'oversize'))
		ORDER BY q.seq`, bucket, string(list))
	if err != nil {
		return st, err
	}
	var seqs []int64
	for _, s := range meta {
		var n int64
		fmt.Sscan(s, &n)
		seqs = append(seqs, n)
	}
	for len(seqs) > 0 && ctx.Err() == nil {
		n := min(len(seqs), replaceBatch)
		s, err := m.apply(ctx, bucket, copies, seqs[:n], true)
		if err != nil {
			errs = append(errs, fmt.Errorf("metadata rows %d to %d: %w", seqs[0], seqs[n-1], err))
		} else {
			st.add(s)
		}
		seqs = seqs[n:]
	}
	for k := range m.unsent {
		if k[0] == bucket {
			delete(m.unsent, k)
		}
	}
	// Data: read in pages, decided again in each batch's transaction. A
	// split bucket takes two rounds (#38 review): the first places
	// missing copies only, the second moves copies to a target with the
	// room the first left, so moves never take a target's room from an
	// object with fewer copies.
	// While drain-back runs, a last round moves copies from targets to
	// friends with the room every other copy left (#40).
	const (
		placeRound = iota
		moveRound
		drainRound
	)
	rounds := []int{placeRound}
	switch {
	case m.split && copies >= 2:
		rounds = append(rounds, moveRound)
	case m.drainBack:
		rounds = append(rounds, drainRound)
	}
	var below ReplaceStats // the last round's objects below copies
	for _, r := range rounds {
		m.moves, m.drains = r == moveRound, r == drainRound
		s, b, err := m.dataRound(ctx, bucket, copies)
		st.add(s)
		below = b
		if err != nil {
			errs = append(errs, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	m.moves, m.drains = false, false
	st.Below, st.BelowBytes, st.BelowSmallest = below.Below, below.BelowBytes, below.BelowSmallest
	if ctx.Err() == nil {
		s, err := m.dropIdleMetadata(ctx, bucket)
		st.add(s)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if ctx.Err() == nil {
		if err := m.recordBelow(ctx, bucket, below.Below, below.BelowBytes); err != nil {
			errs = append(errs, err)
		}
	}
	return st, errors.Join(errs...)
}

// dataRound scans bucket's live data objects and applies what decide
// asks for them, in batches; it returns what it did and the objects
// still below copies after it. A failed batch's error is returned with
// the rest done.
func (m *replacer) dataRound(ctx context.Context, bucket string, copies int) (ReplaceStats, ReplaceStats, error) {
	var st ReplaceStats
	var errs []error
	type job struct {
		seq, size int64
		short     bool // below copies even with what the scan would place
	}
	var after int64
	var work []job
	var below ReplaceStats // Below, BelowBytes and BelowSmallest alone
	for ctx.Err() == nil {
		objs, err := m.page(ctx, bucket, after)
		if err != nil {
			return st, below, errors.Join(append(errs, err)...)
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			after = o.seq
			ops, short, left, err := m.decide(ctx, bucket, o, copies, false)
			if err != nil {
				return st, below, errors.Join(append(errs, err)...)
			}
			switch {
			case len(ops) > 0 || m.dropShort(o, copies):
				work = append(work, job{o.seq, o.size, short}) // its batch counts what it leaves
			case short:
				below.below(o.size)
			}
			if len(ops) == 0 {
				m.skipped += left
			}
		}
	}
	for len(work) > 0 && ctx.Err() == nil {
		n := min(len(work), replaceBatch)
		var seqs []int64
		for _, j := range work[:n] {
			seqs = append(seqs, j.seq)
		}
		s, err := m.apply(ctx, bucket, copies, seqs, false)
		if err != nil {
			errs = append(errs, fmt.Errorf("data rows %d to %d: %w", seqs[0], seqs[n-1], err))
			// Its objects are as they were: those the scan found short
			// stay so (below_copies is recorded all the same).
			for _, j := range work[:n] {
				if j.short {
					below.below(j.size)
				}
			}
		} else {
			st.add(s)
			below.addBelow(s.Below, s.BelowBytes, s.BelowSmallest)
		}
		work = work[n:]
	}
	return st, below, errors.Join(errs...)
}

// page reads up to replacePage live data objects of bucket after
// sequence after, with their placement rows, in sequence order. Objects
// held back as changed keys (EP-11) are not placed anywhere, and are
// left out.
func (m *replacer) page(ctx context.Context, bucket string, after int64) ([]robj, error) {
	rows, err := m.db.QueryContext(ctx, `WITH s AS (SELECT q.seq, coalesce(q.size, 0) AS size FROM queue q
			WHERE +q.bucket = ?1 AND q.seq > ?2 AND q.class = 'data' AND q.op = 'put' AND q.state = 'done'
			AND q.immutable IS NULL AND `+liveQ+` ORDER BY q.seq LIMIT ?3)
		SELECT s.seq, s.size, p.place, p.state, coalesce(p.replaces, ''), coalesce(p.cause, ''), p.started_at IS NOT NULL
		FROM s LEFT JOIN placement p ON p.seq = s.seq ORDER BY s.seq`, bucket, after, replacePage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []robj
	for rows.Next() {
		var seq, size int64
		var place, state sql.NullString
		var r prow
		if err := rows.Scan(&seq, &size, &place, &state, &r.replaces, &r.cause, &r.taken); err != nil {
			return nil, err
		}
		if len(out) == 0 || out[len(out)-1].seq != seq {
			out = append(out, robj{seq: seq, size: size})
		}
		if place.Valid {
			r.place, r.state = place.String, state.String
			o := &out[len(out)-1]
			o.rows = append(o.rows, r)
		}
	}
	return out, rows.Err()
}

// reachable reports whether place is a friend that is not unreachable.
func (m *replacer) reachable(place string) bool {
	f := m.friends[place]
	return f != nil && f.reachable
}

// overflowPlace reports whether place is an overflow target in the pool.
func (m *replacer) overflowPlace(place string) bool {
	f := m.friends[place]
	return f != nil && f.overflow
}

// fullness orders places for a surplus copy to go: the smallest free
// fraction first (the fullest), ties by node ID.
func (m *replacer) fullness(a, b string) int {
	fa, fb := m.friends[a], m.friends[b]
	if fa == nil || fb == nil || fa.quota <= 0 || fb.quota <= 0 {
		return cmp.Compare(a, b)
	}
	return -byFraction(a, freeRoom(fa.quota, fa.use, fa.taken, m.live), capped(fa.quota),
		b, freeRoom(fb.quota, fb.use, fb.taken, m.live), capped(fb.quota))
}

// dropShort reports whether data object o has a dropping copy the
// removal rule may now refuse (#104): fewer than copies acknowledged
// copies at intact places, as I1 counts them (mayRemove).
func (m *replacer) dropShort(o robj, copies int) bool {
	drops, n := false, 0
	for _, r := range o.rows {
		f := m.friends[r.place]
		switch {
		case r.state == PlacementDropping:
			drops = true
		case r.state == PlacementAcked && f != nil && !f.drainAll && !m.unverified[r.place]:
			n++
		}
	}
	return drops && n < copies
}

// checkDrops asks the removal rule again about o's dropping copies
// whose delete no sender holds, when the object may be short
// (dropShort): a refused one is acknowledged again (checkDrop), in o's
// rows too, so decide counts it and places only what is still missing.
func (m *replacer) checkDrops(ctx context.Context, tx *preparedTx, o *robj, copies int) error {
	if !m.dropShort(*o, copies) {
		return nil
	}
	for i, r := range o.rows {
		if r.state != PlacementDropping || m.sending != nil && m.sending(r.place, o.seq) {
			continue
		}
		ok, err := m.db.checkDrop(ctx, tx, r.place, o.seq, m.now)
		if err != nil {
			return err
		}
		if !ok {
			o.rows[i].state = PlacementAcked
		}
	}
	return nil
}

// decide is what one data object needs (file comment above): the rows
// to change, whether it stays below copies, and how many queued rows it
// would have cancelled or drained but left alone because a sender took
// them or is sending them: such a copy may land, and once it is
// acknowledged no change to the pool announces that it must go, so the
// worker looks again at its next tick (Skipped). With commit, the room
// the new copies take is counted for the rest of the pass, and the
// sending set is consulted; without, decide only asks whether there is
// anything to do.
func (m *replacer) decide(ctx context.Context, bucket string, o robj, copies int, commit bool) ([]rop, bool, int64, error) {
	if storeRefused(o.rows) {
		return nil, false, 0, nil // the store cannot supply it: objects_not_replicated says so
	}
	var left int64
	var good, drain []prow
	var ops []rop
	at := map[string]prow{}
	replaced := map[string]bool{}
	for _, r := range o.rows {
		at[r.place] = r
		f := m.friends[r.place]
		switch {
		case m.unverified[r.place] && r.state == PlacementQueued:
			// Not sent yet to a replica that failed a challenge: it goes
			// elsewhere instead, unless its sender holds it (#27).
			if r.taken || commit && m.sending != nil && m.sending(r.place, o.seq) {
				left++ // drained once acknowledged
			} else {
				ops = append(ops, rop{kind: opCancel, place: r.place})
			}
		case f == nil || f.drainAll || f.drain[o.seq] || m.unverified[r.place]:
			if r.state == PlacementAcked {
				drain = append(drain, r)
			}
		case r.holds():
			good = append(good, r)
			if r.replaces != "" {
				replaced[r.replaces] = true
			}
		}
	}
	maxFriends := -1
	if m.split && copies >= 2 {
		// REPL-12 as of v0.11.5: copies − 1 friends and one overflow
		// target. A friend copy an overflow copy replaces is moving
		// there; with no overflow copy yet, the fullest friend's of an
		// object held only at friends moves, if a target can take it.
		var keep []prow
		friendsHeld, overflowHeld := 0, 0
		for _, r := range good {
			f := m.friends[r.place]
			switch {
			case f != nil && !f.overflow && replaced[r.place] && r.state == PlacementAcked:
				drain = append(drain, r)
				continue
			case f != nil && f.overflow:
				overflowHeld++
			default:
				friendsHeld++
			}
			keep = append(keep, r)
		}
		good = keep
		if m.moves && overflowHeld == 0 && friendsHeld >= copies {
			var acked []prow
			for _, r := range good {
				if r.state == PlacementAcked {
					acked = append(acked, r)
				}
			}
			if len(acked) == len(good) {
				ok, err := m.overflowFor(ctx, bucket, o, at)
				if err != nil {
					return nil, false, 0, err
				}
				if ok {
					slices.SortFunc(acked, func(a, b prow) int { return m.fullness(a.place, b.place) })
					drain = append(drain, acked[0])
					good = slices.DeleteFunc(good, func(r prow) bool { return r.place == acked[0].place })
					friendsHeld--
				}
			}
		}
		maxFriends = max(copies-1-friendsHeld, 0)
	} else if m.drainBack {
		// Drain-back (#40): a target's copy a friend's copy replaces is
		// moving back, and counted out; it is deleted once that one is
		// acknowledged. While drain-back is off, such a friend copy is a
		// surplus and cancelled unsent, as a split's move is when the
		// split turns off.
		var keep []prow
		for _, r := range good {
			if m.overflowPlace(r.place) && replaced[r.place] && r.state == PlacementAcked {
				drain = append(drain, r)
				continue
			}
			keep = append(keep, r)
		}
		good = keep
		if m.drains && len(ops) == 0 && len(drain) == 0 && len(good) == copies {
			var targets []prow
			for _, r := range good {
				if r.state != PlacementAcked {
					targets = nil
					break
				}
				if m.overflowPlace(r.place) {
					targets = append(targets, r)
				}
			}
			if len(targets) > 0 {
				to, err := m.choose(ctx, bucket, o, at, len(targets), -1, commit, true)
				if err != nil {
					return nil, false, 0, err
				}
				for i, p := range to {
					ops = append(ops, rop{kind: opPlace, place: p, replaces: targets[i].place})
				}
				return ops, false, left, nil
			}
		}
	}
	acked := 0
	for _, r := range good {
		if r.state == PlacementAcked {
			acked++
		}
	}
	below := false
	switch {
	case len(good) < copies:
		to, err := m.choose(ctx, bucket, o, at, copies-len(good), maxFriends, commit, false)
		if err != nil {
			return nil, false, 0, err
		}
		var pending []string // drained copies with no replacement yet
		for _, d := range drain {
			if !replaced[d.place] {
				pending = append(pending, d.place)
			}
		}
		for i, p := range to {
			op := rop{kind: opPlace, place: p}
			if i < len(pending) {
				op.replaces = pending[i]
			}
			ops = append(ops, op)
		}
		below = len(good)+len(to) < copies
	case len(good) > copies:
		surplus := len(good) - copies
		var queued, ackedRows []prow
		for _, r := range good {
			switch {
			case r.state == PlacementAcked:
				ackedRows = append(ackedRows, r)
			case !r.taken:
				queued = append(queued, r)
			}
		}
		// Which copy goes (#96): an overflow target's first (it is the
		// second choice, always: §4.11), except, while placement is
		// split, the object's last copy at a target, which goes after
		// every friend's (split keeps copies − 1 friends and one
		// target); then unreachable friends (the others answer, #26
		// review), then the fullest.
		split := m.split && copies >= 2
		targets := 0
		for _, r := range good {
			if m.overflowPlace(r.place) {
				targets++
			}
		}
		byFull := func(a, b prow) int {
			if oa, ob := m.overflowPlace(a.place), m.overflowPlace(b.place); oa != ob {
				first := !split || targets > 1 // a target's copy goes first
				if oa == first {
					return -1
				}
				return 1
			}
			ra, rb := m.reachable(a.place), m.reachable(b.place)
			if ra != rb {
				if rb {
					return -1
				}
				return 1
			}
			return m.fullness(a.place, b.place)
		}
		// next is the index in rs of the copy to go next among those ok
		// allows, or -1: the order depends on the targets' copies left.
		next := func(rs []prow, ok func(prow) bool) int {
			best := -1
			for i, r := range rs {
				if ok(r) && (best < 0 || byFull(r, rs[best]) < 0) {
					best = i
				}
			}
			return best
		}
		gone := func(r prow) {
			surplus--
			if m.overflowPlace(r.place) {
				targets--
			}
		}
		for surplus > 0 {
			i := next(queued, func(r prow) bool { return !(commit && m.sending != nil && m.sending(r.place, o.seq)) })
			if i < 0 {
				break
			}
			ops = append(ops, rop{kind: opCancel, place: queued[i].place})
			gone(queued[i])
			queued = slices.Delete(queued, i, i+1)
		}
		for surplus > 0 && acked > copies {
			i := next(ackedRows, func(prow) bool { return true })
			if i < 0 {
				break
			}
			ops = append(ops, rop{kind: opDrop, place: ackedRows[i].place})
			gone(ackedRows[i])
			acked--
			ackedRows = slices.Delete(ackedRows, i, i+1)
		}
		// What is still surplus is queued and taken or being sent, so
		// not cancelled, and keeps as many acknowledged copies.
		left += int64(surplus)
	}
	if acked >= copies {
		for _, d := range drain {
			ops = append(ops, rop{kind: opDrop, place: d.place})
		}
	}
	return ops, below, left, nil
}

// choose is D6's choice of up to n friends for a new copy of o, in
// memory (§3.4): candidates with room for it less what this pass placed
// there, never one with a row of it that holds, refused or is deleting
// it, never one being drained of it, and never one that lacks the
// bucket's live metadata; reachable ones first, by largest free fraction.
// friendsOnly leaves overflow targets out (drain-back, #40).
func (m *replacer) choose(ctx context.Context, bucket string, o robj, at map[string]prow, n, maxFriends int, commit, friendsOnly bool) ([]string, error) {
	need := o.size + ObjectOverhead
	type cand struct {
		f    *rfriend
		free int64
	}
	var cs, over []cand
	for _, f := range m.order {
		if !f.candidate || f.drain[o.seq] || m.unverified[f.peer] {
			continue
		}
		if r, ok := at[f.peer]; ok && (r.holds() || r.state == PlacementRefused || r.state == PlacementDropping) {
			continue
		}
		meta, err := m.unsentAt(ctx, bucket, f.peer)
		if err != nil {
			return nil, err
		}
		if f.overflow {
			// D6 step 3: by free bytes, its missing metadata going
			// there with the copy (write); never past max_bytes (S3-7),
			// and never to a failing target, whose queued copies would
			// hold the metadata barrier (#38 review).
			if !f.reachable || friendsOnly {
				continue
			}
			free := capped(f.quota) - capped(max(f.use, 0)) - capped(f.taken) - capped(meta)
			if free >= capped(need) {
				over = append(over, cand{f, free})
			}
			continue
		}
		if meta > 0 {
			continue
		}
		free := freeRoom(f.quota, f.use, f.taken, m.live)
		if fits(free, need) {
			cs = append(cs, cand{f, free})
		}
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if a.f.reachable != b.f.reachable {
			if a.f.reachable {
				return -1
			}
			return 1
		}
		return byFraction(a.f.peer, a.free, capped(a.f.quota), b.f.peer, b.free, capped(b.f.quota))
	})
	// Overflow targets only for the copies no friend has room for, or
	// beyond maxFriends when split; friends again for what no target
	// can take.
	slices.SortFunc(over, func(a, b cand) int {
		if c := cmp.Compare(b.free, a.free); c != 0 {
			return c
		}
		return cmp.Compare(a.f.peer, b.f.peer)
	})
	friends := n
	if maxFriends >= 0 {
		friends = min(n, maxFriends)
	}
	k := min(friends, len(cs))
	order := append(append(slices.Clone(cs[:k]), over...), cs[k:]...)
	var out []string
	for _, c := range order[:min(n, len(order))] {
		out = append(out, c.f.peer)
		if commit {
			c.f.taken = capped(c.f.taken + need)
		}
	}
	return out, nil
}

// overflowFor reports whether an overflow target that answers has room
// for a copy of o (choose's rule), without taking it.
func (m *replacer) overflowFor(ctx context.Context, bucket string, o robj, at map[string]prow) (bool, error) {
	need := o.size + ObjectOverhead
	for _, f := range m.order {
		if !f.overflow || !f.candidate || !f.reachable || f.drain[o.seq] || m.unverified[f.peer] {
			continue
		}
		if r, ok := at[f.peer]; ok && (r.holds() || r.state == PlacementRefused || r.state == PlacementDropping) {
			continue
		}
		meta, err := m.unsentAt(ctx, bucket, f.peer)
		if err != nil {
			return false, err
		}
		if capped(f.quota)-capped(max(f.use, 0))-capped(f.taken)-capped(meta) >= capped(need) {
			return true, nil
		}
	}
	return false, nil
}

// apply decides about seqs of bucket again from their rows as they now
// are, and writes the changes, in one short transaction under the
// bucket's lock (and the pass's lock, when set). meta: they are
// metadata objects, placed at every friend with an address lacking
// them. A batch that fails is rolled back, and so is what it counted
// in memory (#91): the room it took at each place, the metadata it
// marked sent and the copies it left, so the rest of the pass sees the
// room it would have used.
func (m *replacer) apply(ctx context.Context, bucket string, copies int, seqs []int64, meta bool) (st ReplaceStats, err error) {
	taken := make(map[*rfriend]int64, len(m.order))
	for _, f := range m.order {
		taken[f] = f.taken
	}
	unsent, skipped := maps.Clone(m.unsent), m.skipped
	defer func() {
		if err != nil {
			for f, n := range taken {
				f.taken = n
			}
			m.unsent, m.skipped = unsent, skipped
		}
	}()
	if m.db.beforeApply != nil {
		m.db.beforeApply(bucket, meta) // tests only
	}
	defer m.db.lockBucket(bucket)()
	if m.lock != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return st, err
	}
	defer tx.Rollback()
	ptx := prepared(tx)
	now := m.now.Unix()
	for _, seq := range seqs {
		var size int64
		var live bool
		err := ptx.QueryRowContext(ctx, `SELECT coalesce(q.size, 0), q.state = 'done' AND q.immutable IS NULL AND `+liveQ+`
			FROM queue q WHERE q.seq = ?`, seq).Scan(&size, &live)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			return st, err
		}
		if !live {
			continue // written again or deleted since the scan
		}
		o := robj{seq: seq, size: size}
		rows, err := ptx.QueryContext(ctx, `SELECT place, state, coalesce(replaces, ''), coalesce(cause, ''), started_at IS NOT NULL
			FROM placement WHERE seq = ?`, seq)
		if err != nil {
			return st, err
		}
		for rows.Next() {
			var r prow
			if err := rows.Scan(&r.place, &r.state, &r.replaces, &r.cause, &r.taken); err != nil {
				rows.Close()
				return st, err
			}
			o.rows = append(o.rows, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return st, err
		}
		var ops []rop
		short := false // counted below copies
		if meta {
			ops = m.metadataOps(o)
		} else {
			if err := m.checkDrops(ctx, ptx, &o, copies); err != nil {
				return st, err
			}
			var below bool
			var left int64
			if ops, below, left, err = m.decide(ctx, bucket, o, copies, true); err != nil {
				return st, err
			}
			m.skipped += left
			if short = below; below {
				st.below(size)
			}
		}
		for _, op := range ops {
			if f := m.friends[op.place]; f != nil && f.overflow && !meta && op.kind == opPlace {
				// The target's use as it is now, in this transaction: a
				// capture since the scan may have taken the room (#38
				// review), and max_bytes is never passed (S3-7).
				k := [2]string{bucket, op.place}
				var use int64
				if err := ptx.QueryRowContext(ctx, `SELECT coalesce((SELECT bytes FROM place_use WHERE place = ?), 0)`,
					op.place).Scan(&use); err != nil {
					return st, err
				}
				bytes, err := unsentMetadata(ctx, ptx, bucket, op.place)
				if err != nil {
					return st, err
				}
				if capped(max(use, 0))+capped(bytes)+capped(size+ObjectOverhead) > capped(f.quota) {
					if !short {
						short = true
						st.below(size)
					}
					continue
				}
				// The bucket's metadata goes to an overflow target with
				// its first data copy there, ahead of it (REPL-12).
				placed, err := placeMetadata(ctx, ptx, bucket, op.place, now)
				if err != nil {
					return st, err
				}
				st.Placed += placed
				f.taken = capped(f.taken + bytes)
				m.unsent[k] = 0
			}
			n, err := m.write(ctx, ptx, bucket, seq, op, o.rows, now)
			if err != nil {
				return st, err
			}
			if n == 0 {
				continue
			}
			switch op.kind {
			case opPlace:
				st.Placed++
			case opDrop:
				st.Dropping++
			case opCancel:
				st.Cancelled++
			}
		}
	}
	return st, tx.Commit()
}

// metadataOps places metadata object o at every friend with an address,
// and every overflow target holding data of the bucket (metaPlaces),
// whose row of it does not hold, refuse or delete it (REPL-12).
func (m *replacer) metadataOps(o robj) []rop {
	if storeRefused(o.rows) {
		return nil
	}
	var ops []rop
	for _, place := range m.metaPlaces {
		has := false
		for _, r := range o.rows {
			if r.place == place && (r.holds() || r.state == PlacementRefused || r.state == PlacementDropping) {
				has = true
			}
		}
		if !has {
			ops = append(ops, rop{kind: opPlace, place: place})
		}
	}
	return ops
}

// write applies op to row seq of bucket, returning how many rows it
// changed. A drop or a cancel goes through the removal rule (removeCopy,
// cancelCopy).
func (m *replacer) write(ctx context.Context, tx *preparedTx, bucket string, seq int64, op rop, rows []prow, now int64) (int64, error) {
	var r sql.Result
	var err error
	switch op.kind {
	case opPlace:
		// A copy placed here again is no longer one another row of the
		// object replaces: a stale pointer would count it out as moving
		// (decide, drain-back) though nothing replaces it now (#40
		// review). Only the rows' bookkeeping changes.
		if _, err := tx.ExecContext(ctx, `UPDATE placement SET replaces = NULL WHERE seq = ? AND replaces = ? AND place != ?`,
			seq, op.place, op.place); err != nil {
			return 0, err
		}
		exists := false
		for _, row := range rows {
			exists = exists || row.place == op.place
		}
		if !exists {
			if err := insertPlacement(ctx, tx, seq, op.place, false, now); err != nil {
				return 0, err
			}
			r, err = tx.ExecContext(ctx, `UPDATE placement SET replaces = ?, replaced_at = ? WHERE place = ? AND seq = ?`,
				nullStr(op.replaces), now, op.place, seq)
			break
		}
		// A copy dropped unsent, abandoned or drained there is queued
		// again: its size counted afresh, its claim kept (a taken row
		// stays taken, REPL-2).
		r, err = tx.ExecContext(ctx, `UPDATE placement SET state = 'queued', sha256 = NULL, acked_at = NULL,
			detail = NULL, cause = NULL, held = NULL, replaces = ?1, replaced_at = ?2,
			size = (SELECT coalesce(q.size, 0) + ?3 FROM queue q WHERE q.seq = placement.seq)
			WHERE place = ?4 AND seq = ?5 AND state IN ('dropped', 'abandoned', 'drained')`,
			nullStr(op.replaces), now, ObjectOverhead, op.place, seq)
	case opDrop:
		return m.db.removeCopy(ctx, tx, removal{bucket: bucket, seq: seq, place: op.place, class: "data"}, m.now)
	case opCancel:
		return m.db.cancelCopy(ctx, tx, removal{bucket: bucket, seq: seq, place: op.place, class: "data"}, false, m.now)
	}
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// dropIdleMetadata deletes bucket's metadata at each overflow target
// that holds no data of it at the pass's now (holdsLiveData, #40): a
// target holds the metadata only for the data it holds (REPL-12), so
// once drain-back has moved its data copies back to friends it is empty
// (scenario 12) and costs nothing. An acknowledged copy is marked
// dropping (a delete in the target's FIFO, deleted the
// retention-preserving way, S3-4); an unsent one is cancelled; one a
// sender holds is left for the next pass. Each goes through the removal
// rule (removeCopy, cancelCopy: I2), which holdsLiveData decides here.
func (m *replacer) dropIdleMetadata(ctx context.Context, bucket string) (ReplaceStats, error) {
	var st ReplaceStats
	var targets []string
	for _, f := range m.order {
		if f.overflow {
			targets = append(targets, f.peer)
		}
	}
	if len(targets) == 0 {
		return st, nil
	}
	defer m.db.lockBucket(bucket)()
	if m.lock != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return st, err
	}
	defer tx.Rollback()
	for _, t := range targets {
		var idle bool
		if err := tx.QueryRowContext(ctx, `SELECT NOT `+holdsLiveData("?1", "?2", "?3"), t, bucket, m.now.Unix()).Scan(&idle); err != nil {
			return st, err
		}
		if !idle {
			continue
		}
		rows, err := tx.QueryContext(ctx, `SELECT p.seq, p.state, p.started_at IS NOT NULL FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ? AND p.bucket = ? AND p.op = 'put' AND q.class IN ('metadata', 'snapshot')
			AND p.state IN ('queued', 'acked') ORDER BY p.seq`, t, bucket)
		if err != nil {
			return st, err
		}
		type row struct {
			seq   int64
			state string
			taken bool
		}
		var rs []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.seq, &r.state, &r.taken); err != nil {
				rows.Close()
				return st, err
			}
			rs = append(rs, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return st, err
		}
		for _, r := range rs {
			rm := removal{bucket: bucket, seq: r.seq, place: t, class: "metadata"}
			switch {
			case r.state == PlacementAcked:
				n, err := m.db.removeCopy(ctx, tx, rm, m.now)
				if err != nil {
					return st, err
				}
				st.Dropping += n
			case r.taken || m.sending != nil && m.sending(t, r.seq):
				m.skipped++
			default:
				n, err := m.db.cancelCopy(ctx, tx, rm, true, m.now)
				if err != nil {
					return st, err
				}
				st.Cancelled += n
			}
		}
		// A version the target's later put of its key overwrote is no
		// copy removeCopy asks about (a row of a version no longer live,
		// removal.go): it marks only the place's latest row of a key, and
		// the delete of that row takes the key off the target. Once that
		// delete is on its way or done, the older row goes with it, or it
		// would stay acknowledged and counted in the target's use, and the
		// target never empty, for good (#108).
		if _, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDrained+`'
			WHERE place = ?1 AND bucket = ?2 AND op = 'put' AND state = 'acked'
			AND seq IN (SELECT q.seq FROM queue q WHERE q.bucket = ?2 AND q.class IN ('metadata', 'snapshot'))
			AND EXISTS (SELECT 1 FROM placement l WHERE l.place = placement.place AND l.bucket = placement.bucket
				AND l.key = placement.key AND l.seq > placement.seq AND l.op = 'put'
				AND l.state IN ('`+PlacementDropping+`', '`+PlacementDrained+`'))`, t, bucket); err != nil {
			return st, err
		}
	}
	return st, tx.Commit()
}

// recordBelow keeps bucket's below-copies record: its start is kept
// while objects stay below, and the row goes when none are.
func (m *replacer) recordBelow(ctx context.Context, bucket string, objects, bytes int64) error {
	if objects == 0 {
		_, err := m.db.ExecContext(ctx, `DELETE FROM below_copies WHERE bucket = ?`, bucket)
		return err
	}
	_, err := m.db.ExecContext(ctx, `INSERT INTO below_copies (bucket, since, objects, bytes, checked_at)
		VALUES (?1, ?2, ?3, ?4, ?2)
		ON CONFLICT (bucket) DO UPDATE SET objects = excluded.objects, bytes = excluded.bytes, checked_at = excluded.checked_at`,
		bucket, m.now.Unix(), objects, bytes)
	return err
}
