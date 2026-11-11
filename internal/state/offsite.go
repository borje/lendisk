package state

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

// This file is the offsite-complete watermark (REPL-4, REPL-11, #20):
// how far into a bucket's write sequence everything is held at `copies`
// places, and what is still short.
//
// The rule, per done queue row r of the bucket, against a candidate
// watermark w:
//   - a delete needs nothing: it adds no bytes to be offsite;
//   - a put is offsite while `copies` places still hold its bytes: the
//     place's latest acknowledged (or abandoned) row of the key is a put
//     of the same SHA-256. An acknowledgement a later acknowledged
//     delete or rewrite at that place ended no longer counts; an older
//     acknowledgement of the same bytes (a healing re-upload) does;
//   - a put that is not is still no obstacle when a later done write of
//     its key (a rewrite or a delete) is at or below w: as of w the
//     bucket holds that write, not r;
//   - a put still pending (a completed multipart upload answered before
//     its hash is taken, or one stranded until the next start, #49) has
//     no placement yet: no w at or above it is valid.
//
// A changed key (EP-11, #22) is such a put: placed nowhere, or refused
// by a place holding another version; it is stuck, reason immutable,
// until the key is written back or deleted.
//
// Only places in the pool hold copies: friends with an address that do
// not refuse this node and are not gone past replace_after, and overflow
// targets not gone past it (REPL-12, REPL-13, #38). An acknowledgement
// at a removed or gone friend is not counted; re-placement (#26)
// restores the copy elsewhere.
//
// w is valid when every put at or below it passes; the watermark is
// the highest valid w up to the bucket's latest done write. So the
// bucket's state as of the watermark, each key's latest write at or
// below it, is at `copies` places now. Refused, dropped, abandoned and
// queued rows are no acknowledgement: a friend refusing everything
// holds the watermark behind the objects it was to hold, never past
// them, until a later write or re-placement (#26) puts them at `copies`
// places; Offsite reports them as stuck, with the reason, so status
// says why rather than waiting silently. Local-only keys are never
// queued and so never counted.
//
// A put short of places is stuck only once re-placement has answered
// the pool as it is (passDue): after a quota rises, a friend is added
// or comes back, the copies the pass is about to place are waiting, not
// stuck (#37: `status --wait` right after a quota raise gave up on
// them). A changed key and an unmatchable pending put stay stuck: no
// pass places them.

// Write is one captured write: its sequence, key and capture time.
// Seq is 0 when there is none.
type Write struct {
	Seq        int64
	Key        string
	CapturedAt time.Time
}

// Shortfall counts objects short of `copies` places: their number and
// bytes (a pending object's bytes are not known yet), how many are still
// being written to the store (Pending), and, for stuck ones, why: a
// place refused them, a copy was dropped at a full or gone friend, a
// copy is held only by a friend removed or gone past replace_after, or
// the pool had no room for one.
type Shortfall struct {
	Objects, Bytes                   int64
	Refused, Dropped, Gone, Unplaced int64
	Unmatchable                      int64
	Pending                          int64
	// Immutable: changed keys (EP-11, #22), written with other bytes
	// than a place acknowledged and so replicated nowhere they would
	// replace them, until written back or deleted.
	Immutable int64
}

// Offsite is a bucket's offsite-complete state.
type Offsite struct {
	Copies int
	// Latest is the bucket's latest done write; 0 when it has none.
	Latest int64
	// Watermark is the highest sequence up to which the bucket is
	// offsite at Copies places (REPL-11); 0 when not even its first
	// write is.
	Watermark int64
	// Target is the sequence asked about (Latest when 0 was asked).
	Target int64
	// Snapshot is the latest snapshot key at or below the watermark
	// whose key is still live; LatestSnapshot the latest still live.
	Snapshot, LatestSnapshot Write
	// Through is the latest put at or below the watermark: the last
	// fully placed write. Next is the first write above the watermark,
	// done or pending: every write captured in a second before its
	// CapturedAt is offsite (capture times are whole seconds, and
	// sequences follow them).
	Through, Next Write
	// Waiting is what keeps the watermark below Target: objects as of
	// Target short of Copies places. Stuck is those of them, and of the
	// writes after Target the watermark would have to pass, with
	// nothing queued that could bring them to Copies: Target is not
	// reached until a later write or re-placement changes that.
	Waiting, Stuck Shortfall

	shorts []short
}

// Complete reports whether everything the bucket captured is offsite.
func (o Offsite) Complete() bool { return o.Watermark >= o.Latest }

// Reached reports whether the watermark is at or past Target.
func (o Offsite) Reached() bool { return o.Watermark >= o.Target }

// For returns o with Waiting and Stuck counted for target (Latest when
// target is 0), without reading the database again.
func (o Offsite) For(target int64) Offsite {
	o.Target = target
	if target <= 0 || target > o.Latest {
		o.Target = o.Latest
	}
	o.Waiting, o.Stuck = Shortfall{}, Shortfall{}
	if o.Reached() {
		return o
	}
	reach := o.Target
	for _, s := range o.shorts {
		if s.seq <= o.Target && (s.next == 0 || s.next > o.Target) {
			o.Waiting.add(s)
			if s.next > reach {
				reach = s.next
			}
		}
	}
	for _, s := range o.shorts {
		if s.stuck && s.seq <= reach {
			o.Stuck.add(s)
		}
	}
	return o
}

// short is a put whose bytes fewer than copies places hold, or one still
// pending.
type short struct {
	key                    string
	seq, next, size        int64 // next: the key's next done write; 0 if none
	stuck, pending         bool
	refused, dropped, gone bool
	unmatchable            bool // pending, and cannot be matched to the store (MarkUnmatchable)
	immutable              bool // a changed key (EP-11): held back at capture, or refused by a place's 412
}

// Offsite computes bucket's watermark and what keeps it below target
// (the latest done write when target is 0); For counts it for another
// target. It reads, in one read transaction unless db is already a
// Snapshot, so it is the same in `status` and the daemon. Set the DB's
// Clock and ReplaceAfter as the daemon's: they decide which friends are
// gone.
//
// The short puts come from what the daemon's fold stored
// (storedShorts, #81) when the pool and copies are those it stored them
// for, with every key changed since computed afresh; otherwise every
// put of the bucket is read (fullShorts). Both give the same rows.
func (db *DB) Offsite(ctx context.Context, bucket string, target int64) (Offsite, error) {
	return db.offsite(ctx, bucket, target, false)
}

// offsiteFull is Offsite reading every put, never the stored rows:
// what Offsite must equal (tests).
func (db *DB) offsiteFull(ctx context.Context, bucket string, target int64) (Offsite, error) {
	return db.offsite(ctx, bucket, target, true)
}

func (db *DB) offsite(ctx context.Context, bucket string, target int64, full bool) (Offsite, error) {
	if db.snap == nil {
		view, done, err := db.Snapshot(ctx)
		if err != nil {
			return Offsite{}, err
		}
		defer done()
		return view.offsite(ctx, bucket, target, full)
	}
	o := Offsite{Copies: DefaultCopies}
	if err := db.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?1), ?2),
		coalesce((SELECT seq FROM queue WHERE +bucket = ?1 AND state = 'done' ORDER BY seq DESC LIMIT 1), 0)`,
		bucket, DefaultCopies).Scan(&o.Copies, &o.Latest); err != nil {
		return o, err
	}
	holders, err := db.holders(ctx)
	if err != nil {
		return o, err
	}
	stored := false
	if !full {
		if o.shorts, stored, err = db.storedShorts(ctx, bucket, o.Copies, holders, o.Latest); err != nil {
			return o, err
		}
	}
	if !stored {
		if o.shorts, err = db.fullShorts(ctx, bucket, o.Copies, holders); err != nil {
			return o, err
		}
	}
	// A re-placement pass due may place what is short (#26): nothing
	// it could place is stuck until it has answered the pool as it is.
	due, err := db.passDue(ctx)
	if err != nil {
		return o, err
	}
	if due {
		for i, s := range o.shorts {
			if !s.pending && !s.immutable {
				o.shorts[i].stuck = false
			}
		}
	}
	o.Watermark = watermark(o.shorts, o.Latest)
	// A put superseded at or below the watermark counts for no target
	// For is asked (a target below the watermark is reached): only the
	// rest are kept, which are all storedShorts reads.
	o.shorts = slices.DeleteFunc(o.shorts, func(s short) bool { return s.next != 0 && s.next <= o.Watermark })
	if len(o.shorts) == 0 {
		o.shorts = nil // whichever way they were read
	}
	o = o.For(target)
	if o.Snapshot, err = db.write(ctx, snapshotPut+` AND bucket = ? AND seq <= ? AND `+live,
		bucket, o.Watermark); err != nil {
		return o, err
	}
	if o.LatestSnapshot, err = db.write(ctx, snapshotPut+` AND bucket = ? AND `+live, bucket); err != nil {
		return o, err
	}
	if o.Through, err = db.write(ctx, `+q.bucket = ? AND q.op = 'put' AND q.seq <= ?`, bucket, o.Watermark); err != nil {
		return o, err
	}
	var at int64
	err = db.QueryRowContext(ctx, `SELECT seq, key, created_at FROM queue WHERE seq > ? AND +bucket = ?
		ORDER BY seq LIMIT 1`, o.Watermark, bucket).Scan(&o.Next.Seq, &o.Next.Key, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return o, nil
	}
	o.Next.CapturedAt = time.Unix(at, 0)
	return o, err
}

// watermark is the highest valid w up to latest, given the short puts
// in sequence order: each short put r rules out [r, next). Scanning by
// r descending, w only falls, and an interval passed over starts above
// every later w.
func watermark(shorts []short, latest int64) int64 {
	w := latest
	for _, s := range slices.Backward(shorts) {
		if s.seq <= w && (s.next == 0 || s.next > w) {
			w = s.seq - 1
		}
	}
	return w
}

// holders are the places that count as holding copies, as JSON for
// json_each: friends with an address that do not refuse this node and
// are not gone past replace_after, and overflow targets not gone past
// it nor being removed (PoolSize's rule).
func (db *DB) holders(ctx context.Context) (string, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return "", err
	}
	now, out := db.now(), []string{}
	for _, p := range ps {
		if p.cond != ConditionRemoved && !p.gone(db, now) && !p.removing {
			out = append(out, p.peer)
		}
	}
	b, err := json.Marshal(out)
	return string(b), err
}

func (s *Shortfall) add(r short) {
	s.Objects++
	s.Bytes += r.size
	if r.pending && !r.unmatchable {
		s.Pending++
	}
	if !r.stuck {
		return
	}
	switch {
	case r.immutable:
		s.Immutable++
	case r.refused:
		s.Refused++
	case r.dropped:
		s.Dropped++
	case r.unmatchable:
		s.Unmatchable++
	case r.gone:
		s.Gone++
	default:
		s.Unplaced++
	}
}

// LatestSnapshotWrite is bucket's latest snapshot-class put at or below
// seq (any, when seq is 0), live or deleted since: the snapshot a
// `status --wait` started then waits for. Kopia's compaction deletes
// the index blobs that are its snapshot keys, so liveness cannot matter.
// Seq is 0 when there is none.
func (db *DB) LatestSnapshotWrite(ctx context.Context, bucket string, seq int64) (Write, error) {
	if seq <= 0 {
		return db.write(ctx, snapshotPut+` AND bucket = ?`, bucket)
	}
	return db.write(ctx, snapshotPut+` AND bucket = ? AND seq <= ?`, bucket, seq)
}

// LatestWrite is bucket's latest done write, put or delete; Seq is 0
// when there is none.
func (db *DB) LatestWrite(ctx context.Context, bucket string) (Write, error) {
	return db.write(ctx, `+q.bucket = ?`, bucket)
}

// live keeps queue rows q that are their key's latest done write.
const live = `NOT EXISTS (SELECT 1 FROM queue l
	WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`

// snapshotPut keeps snapshot-class puts. The class IN term, implied by
// the equality after it, lets SQLite read them from the partial index
// queue_metadata instead of every row of the bucket (#20, CI under
// -race).
const snapshotPut = `q.class IN ('metadata', 'snapshot') AND q.class = 'snapshot' AND q.op = 'put'`

// write returns the latest done queue row q matching where. A where
// that writes the bucket term as +q.bucket walks the queue backwards
// by sequence and stops at the first match, rather than reading and
// sorting every row of the bucket.
func (db *DB) write(ctx context.Context, where string, args ...any) (Write, error) {
	var w Write
	var at int64
	err := db.QueryRowContext(ctx, `SELECT q.seq, q.key, q.created_at FROM queue q
		WHERE q.state = 'done' AND `+where+` ORDER BY q.seq DESC LIMIT 1`, args...).Scan(&w.Seq, &w.Key, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return Write{}, nil
	}
	w.CapturedAt = time.Unix(at, 0)
	return w, err
}

// holds is true when place pl.place still holds the bytes of put q: its
// latest acknowledged, abandoned, dropping or drained row of the key (an
// abandoned send may have landed other bytes; a dropping or drained copy
// was moved away by re-placement, #26) is an acknowledged put of q's
// hash.
const holds = `coalesce((SELECT a.state = 'acked' AND a.op = 'put' AND qa.sha256 = q.sha256
	FROM placement a JOIN queue qa ON qa.seq = a.seq
	WHERE a.place = pl.place AND a.bucket = q.bucket AND a.key = q.key
	AND a.state IN ('acked', 'abandoned', 'dropping', 'drained')
	ORDER BY a.seq DESC LIMIT 1), 0)`

// fullShorts lists bucket's short puts, done and pending, in sequence
// order, reading every done put.
func (db *DB) fullShorts(ctx context.Context, bucket string, copies int, holders string) ([]short, error) {
	done, err := doneShorts(ctx, db, bucket, copies, holders, "")
	if err != nil {
		return nil, err
	}
	return db.withPending(ctx, bucket, done)
}

// withPending adds bucket's pending puts to done and sorts them by
// sequence: a pending put has no placement yet (#20 review), so it is
// always short, and is never stored.
func (db *DB) withPending(ctx context.Context, bucket string, done []short) ([]short, error) {
	rows, err := db.QueryContext(ctx, `SELECT seq, unmatchable IS NOT NULL FROM queue
		WHERE state = 'pending' AND bucket = ? AND op = 'put'`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		s := short{pending: true}
		if err := rows.Scan(&s.seq, &s.unmatchable); err != nil {
			return nil, err
		}
		s.stuck = s.unmatchable
		done = append(done, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(done, func(a, b short) int { return cmp.Compare(a.seq, b.seq) })
	return done, nil
}

// doneShorts lists bucket's done puts that fewer than copies places
// hold, each with its key's next done write and whether it is stuck:
// the key's latest write, with too few places holding it or queued to
// (a refusal, a dropped copy, no room). Each put costs one indexed
// look-up per place (placement_key), counted once (counted is
// materialized, so SQLite does not count again for the output); its
// next write and queued rows are read only for the short ones. keys,
// when not "", restricts the puts to the keys it selects (a subquery,
// which may name ?1, the bucket, and args after the first three).
//
// Every row it returns reads only holders, copies, and queue and
// placement rows of the put's own key: what lets the fold store the
// rows and recompute only the keys offsite_dirty marks (#81). A change
// that breaks that must widen the marks (migration 53's triggers).
func doneShorts(ctx context.Context, db querier, bucket string, copies int, holders, keys string, args ...any) ([]short, error) {
	filter := ""
	if keys != "" {
		filter = ` AND q.key IN (` + keys + `)`
	}
	rows, err := db.QueryContext(ctx, `WITH places AS MATERIALIZED (SELECT value AS place FROM json_each(?3)),
		counted AS MATERIALIZED (SELECT q.seq, q.bucket, q.key, q.sha256, coalesce(q.size, 0) AS size,
			q.immutable IS NOT NULL AS changed, (SELECT count(*) FROM places pl WHERE `+holds+`) AS held
			FROM queue q WHERE q.bucket = ?1 AND q.state = 'done' AND q.op = 'put'`+filter+`),
		puts AS (SELECT q.seq, q.key, q.size, q.changed, q.held,
			(SELECT min(l.seq) FROM queue l WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done') AS next,
			(SELECT count(*) FROM placement w JOIN places pl ON pl.place = w.place
				WHERE w.seq = q.seq AND w.state = 'queued' AND NOT `+holds+`) AS queued
			FROM counted q WHERE q.held < ?2)
		SELECT seq, key, size, coalesce(next, 0), next IS NULL AND held + queued < ?2,
			EXISTS (SELECT 1 FROM placement r WHERE r.seq = puts.seq AND r.state = 'refused'),
			EXISTS (SELECT 1 FROM placement d WHERE d.seq = puts.seq AND d.state = 'dropped'),
			EXISTS (SELECT 1 FROM placement g WHERE g.seq = puts.seq AND g.state = 'acked'
				AND g.place NOT IN (SELECT place FROM places)),
			changed OR EXISTS (SELECT 1 FROM placement i WHERE i.seq = puts.seq AND i.state = 'refused'
				AND i.cause = '`+CauseImmutable+`')
		FROM puts ORDER BY seq`, append([]any{bucket, copies, holders}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []short
	for rows.Next() {
		var s short
		if err := rows.Scan(&s.seq, &s.key, &s.size, &s.next, &s.stuck, &s.refused, &s.dropped, &s.gone, &s.immutable); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
