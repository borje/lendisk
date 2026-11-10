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
//     (the lender keeps a tomb for the window, RET-1);
//   - surplus copies (copies lowered, or a gone friend back while its
//     replacement waits) are cancelled while unsent, the fullest place
//     first (an overflow target before any friend unless placement is
//     split), then marked dropping while more than `copies` are
//     acknowledged;
//   - drain-back (#40): while the friends hold the repository with the
//     hysteresis of overflowDrain, and placement is not split, a last
//     round gives each object with an acknowledged copy at an overflow
//     target a friend copy in its place, oldest first, where a friend
//     has room after every other copy is placed; the target's copy is
//     deleted once that one is acknowledged (AckPlacement,
//     keepReplaced). A drain-back copy is the lowest priority in its
//     friend's FIFO and holds no metadata barrier (NextPlacement,
//     Held). A target left with no data of a bucket has its metadata
//     of the bucket deleted too (dropIdleMetadata).
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
	Skipped    int64 // copies left alone as taken or being sent, which may land: a later pass looks again
}

func (s *ReplaceStats) add(o ReplaceStats) {
	s.Placed += o.Placed
	s.Dropping += o.Dropping
	s.Cancelled += o.Cancelled
	s.Below += o.Below
	s.BelowBytes += o.BelowBytes
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
// or the quota rises); each bucket's copies; and each replica marked
// unverified (VER-1, #27). The worker runs a pass whenever it changes
// (#26), and Offsite counts no object short of places as stuck while it
// differs from the pool the last pass answered (passDue).
func (db *DB) PoolFingerprint(ctx context.Context) (string, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return "", err
	}
	now := db.now()
	var b strings.Builder
	for _, p := range ps {
		fmt.Fprintf(&b, "%s %d %t %t %t %t;", p.peer, p.quota, p.cond == ConditionRemoved, p.gone(db, now),
			capped(p.use) > capped(p.quota), p.full())
	}
	rows, err := db.QueryContext(ctx, `SELECT name, copies FROM buckets ORDER BY name`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var copies int
		if err := rows.Scan(&name, &copies); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "|%s %d", name, copies)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	rows.Close()
	marks, err := column(ctx, db, `SELECT place || ' ' || bucket FROM replica_check
		WHERE unverified_at IS NOT NULL ORDER BY place, bucket`)
	if err != nil {
		return "", err
	}
	for _, m := range marks {
		fmt.Fprintf(&b, "|unverified %s", m)
	}
	return b.String(), nil
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
func (db *DB) Replacing(ctx context.Context) (map[string]ReplacingStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT p.bucket,
		count(*) FILTER (WHERE p.state = 'queued'),
		coalesce(sum(q.size) FILTER (WHERE p.state = 'queued'), 0),
		count(*) FILTER (WHERE p.state = 'dropping')
		FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE (p.state = 'dropping' OR p.state = 'queued' AND p.replaced_at IS NOT NULL)
		AND p.place IN (SELECT peer_key FROM friendship UNION ALL `+overflowPlaces+`)
		GROUP BY p.bucket`)
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
	// pass is still due.
	pool, err := db.PoolFingerprint(ctx)
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
		f.drainAll = p.cond == ConditionRemoved || p.gone(db, m.now)
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
		if _, err := db.ExecContext(ctx, `INSERT INTO replace_pass (id, pool, done_at) VALUES (1, ?, ?)
			ON CONFLICT (id) DO UPDATE SET pool = excluded.pool, done_at = excluded.done_at`,
			pool, db.now().Unix()); err != nil {
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
	// records. The bucket waits for them.
	var listing bool
	if err := m.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM relist WHERE bucket = ?
		AND (abandoned IS NULL AND done IS NULL OR settled IS NULL))`, bucket).Scan(&listing); err != nil || listing {
		return st, err
	}
	var err error
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
	held, err := overflowHolding(ctx, m.db, bucket)
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
	var below, belowBytes int64
	for _, r := range rounds {
		m.moves, m.drains = r == moveRound, r == drainRound
		s, b, bb, err := m.dataRound(ctx, bucket, copies)
		st.add(s)
		below, belowBytes = b, bb
		if err != nil {
			errs = append(errs, err)
		}
		if ctx.Err() != nil {
			break
		}
	}
	m.moves, m.drains = false, false
	st.Below, st.BelowBytes = below, belowBytes
	if ctx.Err() == nil {
		s, err := m.dropIdleMetadata(ctx, bucket)
		st.add(s)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if ctx.Err() == nil {
		if err := m.recordBelow(ctx, bucket, below, belowBytes); err != nil {
			errs = append(errs, err)
		}
	}
	return st, errors.Join(errs...)
}

// dataRound scans bucket's live data objects and applies what decide
// asks for them, in batches; it returns what it did and the objects
// still below copies after it. A failed batch's error is returned with
// the rest done.
func (m *replacer) dataRound(ctx context.Context, bucket string, copies int) (ReplaceStats, int64, int64, error) {
	var st ReplaceStats
	var errs []error
	type job struct {
		seq, size int64
		short     bool // below copies even with what the scan would place
	}
	var after int64
	var work []job
	var below, belowBytes int64
	for ctx.Err() == nil {
		objs, err := m.page(ctx, bucket, after)
		if err != nil {
			return st, below, belowBytes, errors.Join(append(errs, err)...)
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			after = o.seq
			ops, short, left, err := m.decide(ctx, bucket, o, copies, false)
			if err != nil {
				return st, below, belowBytes, errors.Join(append(errs, err)...)
			}
			switch {
			case len(ops) > 0:
				work = append(work, job{o.seq, o.size, short}) // its batch counts what it leaves
			case short:
				below++
				belowBytes += o.size
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
					below++
					belowBytes += j.size
				}
			}
		} else {
			st.add(s)
			below += s.Below
			belowBytes += s.BelowBytes
		}
		work = work[n:]
	}
	return st, below, belowBytes, errors.Join(errs...)
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
		// An overflow target first unless placement is split (it is
		// the second choice, always: §4.11), then unreachable friends
		// (the others answer, #26 review), then the fullest.
		byFull := func(a, b prow) int {
			if !(m.split && copies >= 2) {
				if oa, ob := m.overflowPlace(a.place), m.overflowPlace(b.place); oa != ob {
					if oa {
						return -1
					}
					return 1
				}
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
		slices.SortFunc(queued, byFull)
		slices.SortFunc(ackedRows, byFull)
		for _, r := range queued {
			if surplus == 0 {
				break
			}
			if commit && m.sending != nil && m.sending(r.place, o.seq) {
				continue
			}
			ops = append(ops, rop{kind: opCancel, place: r.place})
			surplus--
		}
		for _, r := range ackedRows {
			if surplus == 0 || acked <= copies {
				break
			}
			ops = append(ops, rop{kind: opDrop, place: r.place})
			surplus--
			acked--
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
			var below bool
			var left int64
			if ops, below, left, err = m.decide(ctx, bucket, o, copies, true); err != nil {
				return st, err
			}
			m.skipped += left
			if short = below; below {
				st.Below++
				st.BelowBytes += size
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
						st.Below++
						st.BelowBytes += size
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
			n, err := m.write(ctx, ptx, seq, op, o.rows, now)
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

// write applies op to row seq, returning how many rows it changed.
func (m *replacer) write(ctx context.Context, tx *preparedTx, seq int64, op rop, rows []prow, now int64) (int64, error) {
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
		r, err = tx.ExecContext(ctx, markDropping, op.place, seq)
	case opCancel:
		if r, err = tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementAbandoned+`'
			WHERE place = ? AND seq = ? AND state = 'queued' AND started_at IS NOT NULL`, op.place, seq); err != nil {
			return 0, err
		}
		if n, err := r.RowsAffected(); err != nil || n > 0 {
			return n, err
		}
		r, err = tx.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'`, op.place, seq)
	}
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// markDropping marks place ?1's acknowledged copy of row ?2 dropping,
// only while it is the place's latest row of the key: a later row there
// is ordered after the delete in its FIFO, but one acknowledged already
// must not be deleted.
const markDropping = `UPDATE placement SET state = '` + PlacementDropping + `'
	WHERE place = ?1 AND seq = ?2 AND state = 'acked' AND op = 'put'
	AND seq = (SELECT max(x.seq) FROM placement x
		WHERE x.place = placement.place AND x.bucket = placement.bucket AND x.key = placement.key)`

// dropIdleMetadata deletes bucket's metadata at each overflow target
// that holds, has queued or is deleting no copy of a live data object of
// it (#40): a
// target holds the metadata only for the data it holds (REPL-12), so
// once drain-back has moved its data copies back to friends it is empty
// (scenario 12) and costs nothing. An acknowledged copy is marked
// dropping (a delete in the target's FIFO, deleted the
// retention-preserving way, S3-4); an unsent one is cancelled; one a
// sender holds is left for the next pass.
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
		if err := tx.QueryRowContext(ctx, `SELECT NOT `+holdsLiveData("?1", "?2"), t, bucket).Scan(&idle); err != nil {
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
			var res sql.Result
			switch {
			case r.state == PlacementAcked:
				if res, err = tx.ExecContext(ctx, markDropping, t, r.seq); err != nil {
					return st, err
				}
				if n, _ := res.RowsAffected(); n > 0 {
					st.Dropping++
				}
			case r.taken || m.sending != nil && m.sending(t, r.seq):
				m.skipped++
			default:
				if res, err = tx.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'
					AND started_at IS NULL`, t, r.seq); err != nil {
					return st, err
				}
				if n, _ := res.RowsAffected(); n > 0 {
					st.Cancelled++
				}
			}
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
