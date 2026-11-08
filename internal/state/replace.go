package state

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
//     first, then marked dropping while more than `copies` are
//     acknowledged.
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
	Skipped    int64 // surplus copies left alone as being sent: a later pass looks again
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
// past replace_after, or counts more than its quota; each bucket's
// copies; and each replica marked unverified (VER-1, #27). The worker
// runs a pass whenever it changes (#26).
func (db *DB) PoolFingerprint(ctx context.Context) (string, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return "", err
	}
	now := db.now()
	var b strings.Builder
	for _, p := range ps {
		fmt.Fprintf(&b, "%s %d %t %t %t;", p.peer, p.quota, p.cond == ConditionRemoved, p.gone(db, now),
			capped(p.use) > capped(p.quota))
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
		AND p.place IN (SELECT peer_key FROM friendship)
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
}

// Replace runs one re-placement pass over every bucket (REPL-13, file
// comment above). sending reports whether a place's sender holds a row
// (a send that may land), which is never cancelled; lock, when set, is
// held with the bucket's lock for each batch, as the worker's sending
// set is read under it. A batch that fails is rolled back and the pass
// goes on: the errors are returned with what the others did.
func (db *DB) Replace(ctx context.Context, sending func(place string, seq int64) bool, lock sync.Locker) (ReplaceStats, error) {
	var st ReplaceStats
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return st, err
	}
	m := &replacer{db: db, friends: map[string]*rfriend{}, now: db.now(), unsent: map[[2]string]int64{},
		sending: sending, lock: lock}
	if m.live, err = liveMetadata(ctx, db); err != nil {
		return st, err
	}
	for _, p := range ps {
		f := &rfriend{poolPlace: p, reachable: !p.unreachable(db, m.now)}
		f.drainAll = p.cond == ConditionRemoved || p.gone(db, m.now)
		f.candidate = p.quota > 0 && !f.drainAll && !p.full()
		if !f.drainAll {
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
	return st, errors.Join(append(errs, ctx.Err())...)
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
	// Metadata missing at a friend with an address (REPL-12).
	meta, err := column(ctx, m.db, `SELECT DISTINCT q.seq FROM queue q, friendship f
		WHERE q.bucket = ?1 AND q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND q.immutable IS NULL AND `+liveQ+` AND coalesce(f.address, '') != ''
		AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = f.peer_key AND p.seq = q.seq
			AND p.state IN ('queued', 'acked', 'refused', 'dropping'))
		AND NOT EXISTS (SELECT 1 FROM placement r WHERE r.seq = q.seq AND r.state = 'refused'
			AND r.cause IN ('store', 'oversize'))
		ORDER BY q.seq`, bucket)
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
	// Data: read in pages, decided again in each batch's transaction.
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
			return st, errors.Join(append(errs, err)...)
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			after = o.seq
			ops, short, err := m.decide(ctx, bucket, o, copies, false)
			if err != nil {
				return st, errors.Join(append(errs, err)...)
			}
			switch {
			case len(ops) > 0:
				work = append(work, job{o.seq, o.size, short})
			case short:
				below++
				belowBytes += o.size
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
	st.Below, st.BelowBytes = below, belowBytes
	if ctx.Err() == nil {
		if err := m.recordBelow(ctx, bucket, below, belowBytes); err != nil {
			errs = append(errs, err)
		}
	}
	return st, errors.Join(errs...)
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
// to change, and whether it stays below copies. With commit, the room
// the new copies take is counted for the rest of the pass, and the
// sending set is consulted; without, decide only asks whether there is
// anything to do.
func (m *replacer) decide(ctx context.Context, bucket string, o robj, copies int, commit bool) ([]rop, bool, error) {
	if storeRefused(o.rows) {
		return nil, false, nil // the store cannot supply it: objects_not_replicated says so
	}
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
			if !r.taken && !(commit && m.sending != nil && m.sending(r.place, o.seq)) {
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
	acked := 0
	for _, r := range good {
		if r.state == PlacementAcked {
			acked++
		}
	}
	below := false
	switch {
	case len(good) < copies:
		to, err := m.choose(ctx, bucket, o, at, copies-len(good), commit)
		if err != nil {
			return nil, false, err
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
		// Unreachable friends first (the others answer, #26 review),
		// then the fullest.
		byFull := func(a, b prow) int {
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
				m.skipped++
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
	}
	if acked >= copies {
		for _, d := range drain {
			ops = append(ops, rop{kind: opDrop, place: d.place})
		}
	}
	return ops, below, nil
}

// choose is D6's choice of up to n friends for a new copy of o, in
// memory (§3.4): candidates with room for it less what this pass placed
// there, never one with a row of it that holds, refused or is deleting
// it, never one being drained of it, and never one that lacks the
// bucket's live metadata; reachable ones first, by largest free fraction.
func (m *replacer) choose(ctx context.Context, bucket string, o robj, at map[string]prow, n int, commit bool) ([]string, error) {
	need := o.size + ObjectOverhead
	type cand struct {
		f    *rfriend
		free int64
	}
	var cs []cand
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
	var out []string
	for _, c := range cs[:min(n, len(cs))] {
		out = append(out, c.f.peer)
		if commit {
			c.f.taken = capped(c.f.taken + need)
		}
	}
	return out, nil
}

// apply decides about seqs of bucket again from their rows as they now
// are, and writes the changes, in one short transaction under the
// bucket's lock (and the pass's lock, when set). meta: they are
// metadata objects, placed at every friend with an address lacking
// them.
func (m *replacer) apply(ctx context.Context, bucket string, copies int, seqs []int64, meta bool) (ReplaceStats, error) {
	var st ReplaceStats
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
		if meta {
			ops = m.metadataOps(o)
		} else {
			var below bool
			if ops, below, err = m.decide(ctx, bucket, o, copies, true); err != nil {
				return st, err
			}
			if below {
				st.Below++
				st.BelowBytes += size
			}
		}
		for _, op := range ops {
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

// metadataOps places metadata object o at every friend with an address
// whose row of it does not hold, refuse or delete it (REPL-12).
func (m *replacer) metadataOps(o robj) []rop {
	if storeRefused(o.rows) {
		return nil
	}
	var ops []rop
	for _, f := range m.order {
		has := false
		for _, r := range o.rows {
			if r.place == f.peer && (r.holds() || r.state == PlacementRefused || r.state == PlacementDropping) {
				has = true
			}
		}
		if !has {
			ops = append(ops, rop{kind: opPlace, place: f.peer})
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
