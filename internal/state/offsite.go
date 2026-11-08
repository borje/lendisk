package state

import (
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
// Only places in the pool hold copies: friends with an address that do
// not refuse this node and are not gone past replace_after (REPL-12,
// REPL-13). An acknowledgement at a removed or gone friend is not
// counted; re-placement (#26) restores the copy elsewhere.
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
	Pending                          int64
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
	seq, next, size        int64 // next: the key's next done write; 0 if none
	stuck, pending         bool
	refused, dropped, gone bool
}

// Offsite computes bucket's watermark and what keeps it below target
// (the latest done write when target is 0); For counts it for another
// target. It reads; nothing is stored, so it is the same in `status`
// and the daemon. Set the DB's Clock and ReplaceAfter as the daemon's:
// they decide which friends are gone.
func (db *DB) Offsite(ctx context.Context, bucket string, target int64) (Offsite, error) {
	o := Offsite{Copies: DefaultCopies}
	if err := db.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?1), ?2),
		coalesce((SELECT max(seq) FROM queue WHERE bucket = ?1 AND state = 'done'), 0)`,
		bucket, DefaultCopies).Scan(&o.Copies, &o.Latest); err != nil {
		return o, err
	}
	holders, err := db.holders(ctx)
	if err != nil {
		return o, err
	}
	if o.shorts, err = db.shorts(ctx, bucket, o.Copies, holders); err != nil {
		return o, err
	}
	// The highest valid w: each short put r rules out [r, next).
	// Scanning by r descending, w only falls, and an interval passed
	// over starts above every later w.
	o.Watermark = o.Latest
	for _, s := range slices.Backward(o.shorts) {
		if s.seq <= o.Watermark && (s.next == 0 || s.next > o.Watermark) {
			o.Watermark = s.seq - 1
		}
	}
	o = o.For(target)
	if o.Snapshot, err = db.write(ctx, `bucket = ? AND class = 'snapshot' AND op = 'put' AND seq <= ? AND `+live,
		bucket, o.Watermark); err != nil {
		return o, err
	}
	if o.LatestSnapshot, err = db.write(ctx, `bucket = ? AND class = 'snapshot' AND op = 'put' AND `+live, bucket); err != nil {
		return o, err
	}
	if o.Through, err = db.write(ctx, `bucket = ? AND op = 'put' AND seq <= ?`, bucket, o.Watermark); err != nil {
		return o, err
	}
	var at int64
	err = db.QueryRowContext(ctx, `SELECT seq, key, created_at FROM queue WHERE seq > ? AND bucket = ?
		ORDER BY seq LIMIT 1`, o.Watermark, bucket).Scan(&o.Next.Seq, &o.Next.Key, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return o, nil
	}
	o.Next.CapturedAt = time.Unix(at, 0)
	return o, err
}

// holders are the places that count as holding copies, as JSON for
// json_each: friends with an address that do not refuse this node and
// are not gone past replace_after (PoolSize's rule).
func (db *DB) holders(ctx context.Context) (string, error) {
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return "", err
	}
	now, out := db.now(), []string{}
	for _, p := range ps {
		if p.cond != ConditionRemoved && !p.gone(db, now) {
			out = append(out, p.peer)
		}
	}
	b, err := json.Marshal(out)
	return string(b), err
}

func (s *Shortfall) add(r short) {
	s.Objects++
	s.Bytes += r.size
	if r.pending {
		s.Pending++
	}
	if !r.stuck {
		return
	}
	switch {
	case r.refused:
		s.Refused++
	case r.dropped:
		s.Dropped++
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
		return db.write(ctx, `bucket = ? AND class = 'snapshot' AND op = 'put'`, bucket)
	}
	return db.write(ctx, `bucket = ? AND class = 'snapshot' AND op = 'put' AND seq <= ?`, bucket, seq)
}

// LatestWrite is bucket's latest done write, put or delete; Seq is 0
// when there is none.
func (db *DB) LatestWrite(ctx context.Context, bucket string) (Write, error) {
	return db.write(ctx, `bucket = ?`, bucket)
}

// live keeps queue rows q that are their key's latest done write.
const live = `NOT EXISTS (SELECT 1 FROM queue l
	WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done')`

// write returns the latest done queue row q matching where.
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
// latest acknowledged or abandoned row of the key (an abandoned send
// may have landed other bytes) is an acknowledged put of q's hash.
const holds = `coalesce((SELECT a.state = 'acked' AND a.op = 'put' AND qa.sha256 = q.sha256
	FROM placement a JOIN queue qa ON qa.seq = a.seq
	WHERE a.place = pl.place AND a.bucket = q.bucket AND a.key = q.key AND a.state IN ('acked', 'abandoned')
	ORDER BY a.seq DESC LIMIT 1), 0)`

// shorts lists bucket's done puts that fewer than copies places hold,
// in sequence order, each with its key's next done write and whether it
// is stuck: the key's latest write, with too few places holding it or
// queued to (a refusal, a dropped copy, no room). Each put costs one
// indexed look-up per place (placement_key).
func (db *DB) shorts(ctx context.Context, bucket string, copies int, holders string) ([]short, error) {
	rows, err := db.QueryContext(ctx, `WITH places AS (SELECT value AS place FROM json_each(?3)),
		puts AS (SELECT q.seq, coalesce(q.size, 0) AS size,
			(SELECT min(l.seq) FROM queue l WHERE l.bucket = q.bucket AND l.key = q.key AND l.seq > q.seq AND l.state = 'done') AS next,
			(SELECT count(*) FROM places pl WHERE `+holds+`) AS held,
			(SELECT count(*) FROM placement w JOIN places pl ON pl.place = w.place
				WHERE w.seq = q.seq AND w.state = 'queued' AND NOT `+holds+`) AS queued
			FROM queue q WHERE q.bucket = ?1 AND q.state = 'done' AND q.op = 'put')
		SELECT seq, size, coalesce(next, 0), next IS NULL AND held + queued < ?2,
			EXISTS (SELECT 1 FROM placement r WHERE r.seq = puts.seq AND r.state = 'refused'),
			EXISTS (SELECT 1 FROM placement d WHERE d.seq = puts.seq AND d.state = 'dropped'),
			EXISTS (SELECT 1 FROM placement g WHERE g.seq = puts.seq AND g.state = 'acked'
				AND g.place NOT IN (SELECT place FROM places)), 0
		FROM puts WHERE held < ?2
		UNION ALL
		SELECT seq, 0, 0, 0, 0, 0, 0, 1 FROM queue WHERE state = 'pending' AND bucket = ?1 AND op = 'put'
		ORDER BY 1`, bucket, copies, holders)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []short
	for rows.Next() {
		var s short
		if err := rows.Scan(&s.seq, &s.size, &s.next, &s.stuck, &s.refused, &s.dropped, &s.gone, &s.pending); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
