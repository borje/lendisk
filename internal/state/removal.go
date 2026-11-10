package state

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// This file is the one rule that decides whether a copy at a place may
// go (REPL-13, REPL-12, RET-3, #103): mayRemove. Every path that marks
// an acknowledged copy dropping (removeCopy) or takes an unsent copy out
// of its place's FIFO (cancelCopy, or a bulk cancel that asks mayRemove
// first) goes through it, and only removeCopy and drainedAck write the
// dropping and drained states:
//
//   - AckPlacement: the copy a re-placed copy replaces (placement.replaces),
//     at the replacement's acknowledgement;
//   - re-placement's decide: surplus copies, copies at drained places,
//     split moves and drain-back (opDrop, opCancel), and dropIdleMetadata;
//   - Reroute's moves and drops of unsent data;
//   - RemoveOverflow's unsent puts at the target being removed;
//   - removeFriendship's unsent rows and ForgetFormerFriend's rows
//     (exceptFriendRemoved), dropOverflow's rows of a target removed
//     with keep_data (exceptKeepData), and ForceRemoveOverflow's: each
//     live data copy asked first, and the rows forgotten (exceptForced)
//     only once none is refused or the user accepted the loss (#102).
//
// A dropping copy is asked about again before its delete is sent
// (checkDrop, #104): from StartPlacement, in the transaction that takes
// the row, at every try, and by each re-placement pass for an object
// that may have become short. A copy marked dropping passed I1 then,
// but another holder may have been lost since; a refused drop is
// acknowledged again and not sent, and the pass places what the object
// lacks. A delete a sender is sending is not checked (the pass skips
// it): it may be on its way, and a delete sent cannot be recalled. One
// whose earlier try failed is checked again before the next: a failed
// try most likely never reached the place (a target unreachable for
// days is the case this is for); one that landed with its answer lost
// leaves a copy counted that the place keeps only as a tomb, until a
// challenge of that replica finds it missing (VER-1).
//
// Rules a caller must keep: a new path that drops, drains or cancels a
// copy asks mayRemove (through removeCopy or cancelCopy where it can) in
// the transaction that writes the change, and writes nothing it refuses;
// a new exception to I1 is a named one here, with its reason. A new
// sender of a dropping row's delete takes it through StartPlacement.
//
// Not removals of a copy: a row of a version no longer live (a put a
// later write superseded, CancelPlacement; one a delete of its key
// cancels unsent, placeTx), a delete's acknowledgement, a seed's
// acknowledgements (ConfirmSeeded), and the metadata barrier (Held),
// which only holds rows back.
//
// The invariants, also in DESIGN §3.4:
//
//   - I1: after the removal, the object keeps at least `copies`
//     acknowledged copies at intact places: in the pool, not refusing
//     this node (condition removed), not gone past replace_after, not
//     an overflow target being removed, its replica of the bucket not
//     unverified (VER-1). Removing an unsent copy never lowers that
//     count; a drained copy an unsent one would replace stays until
//     another replacement is acknowledged.
//   - I2: a bucket's metadata is never removed from a place that still
//     holds, or can still undelete, data of that bucket: at an overflow
//     target, holdsLiveData (a live copy, a delete queued and not sent,
//     a held or denied tomb, a delete marker inside its window); at a
//     friend, never (every friend holds every bucket's metadata,
//     REPL-12, and keeps its deleted data as tombs for the window).
//     Unsent metadata at a target is cancelled only while it holds no
//     data of the bucket, or while it is being removed (it is sent no
//     more metadata, #96).
//   - I3: the removals that go below `copies` are named exceptions, with
//     their reason: the user lowering `copies` (I1 counts the copies
//     set now: SetCopies is the user's decision), `overflow remove
//     --keep-data` (the target's rows are forgotten with its data left
//     there, exceptKeepData), `overflow remove --force` with the loss
//     it prints confirmed (exceptForced, #102) and a friendship ended,
//     by the user or by the friend (byPeer, exceptFriendRemoved).
//     Nothing else bypasses I1.
//
// The calls that name an exception (removeFriendship, ForgetFormerFriend,
// dropOverflow, ForceRemoveOverflow), and those for unsent data rows (applyReroute,
// RemoveOverflow), always return true. They record the decision, and
// enforce nothing: removing an unsent copy never lowers the count I1
// keeps, and neither does removing a copy at a former friend or at a
// target being removed, which are not intact places.

// I3's named exceptions: a removal with one of these is the user's (or
// the friend's) decision, and mayRemove allows it without counting
// copies.
const (
	exceptFriendRemoved = "a friendship ended, by the user or by the friend"
	exceptKeepData      = "an overflow target removed with --keep-data"
	exceptForced        = "an overflow target removed with --force, its loss confirmed by the user"
)

// removal is one copy a path asks to remove: row seq of bucket at
// place, of class (the queue row's: data, metadata, snapshot; "" to read
// it), acknowledged there (marked dropping) or unsent (cancelled), or a
// named exception (I3).
type removal struct {
	bucket string
	seq    int64
	place  string
	class  string
	acked  bool
	except string
}

// mayRemove reports whether r may be removed at now, and why: the one
// rule of I1, I2 and I3 (file comment above), read in q as it is now,
// so removals decided one after another in a transaction each see the
// ones before.
func (db *DB) mayRemove(ctx context.Context, q querier, r removal, now time.Time) (bool, string, error) {
	const unsent = "unsent: no acknowledged copy goes"
	switch {
	case r.except != "":
		return true, r.except, nil
	case r.class == "data" && !r.acked:
		return true, unsent, nil
	case r.class == "" || r.bucket == "":
		err := q.QueryRowContext(ctx, `SELECT class, bucket FROM queue WHERE seq = ?`, r.seq).Scan(&r.class, &r.bucket)
		if errors.Is(err, sql.ErrNoRows) {
			return false, "no such row", nil
		} else if err != nil {
			return false, "", err
		}
	}
	_, target := OverflowName(r.place)
	if r.class != "data" {
		// I2.
		if !target {
			return false, "a friend keeps every bucket's metadata (REPL-12)", nil
		}
		var removing, holds bool
		if err := q.QueryRowContext(ctx, `SELECT coalesce((SELECT removing_at IS NOT NULL FROM overflow
			WHERE '`+OverflowPrefix+`' || name = ?1), 0), `+holdsLiveData("?1", "?2", "?3"),
			r.place, r.bucket, now.Unix()).Scan(&removing, &holds); err != nil {
			return false, "", err
		}
		switch {
		case !holds:
			return true, "the target holds no data of the bucket", nil
		case !r.acked && removing:
			return true, "a target being removed is sent no more metadata", nil
		}
		return false, "the target holds, or can undelete, data of the bucket", nil
	}
	if !r.acked {
		return true, unsent, nil
	}
	// I1.
	n, copies, err := db.intactCopies(ctx, q, r.bucket, r.seq, r.place, now)
	if err != nil {
		return false, "", err
	}
	if n < copies {
		return false, "the object would keep fewer than `copies` acknowledged copies at intact places", nil
	}
	return true, "the object keeps `copies` acknowledged copies at intact places", nil
}

// intactCopies counts row seq's acknowledged copies of bucket at intact
// places other than except at now (I1), and returns the bucket's
// copies.
func (db *DB) intactCopies(ctx context.Context, q querier, bucket string, seq int64, except string, now time.Time) (n, copies int, err error) {
	ps, err := poolPlaces(ctx, q)
	if err != nil {
		return 0, 0, err
	}
	unverified, err := unverifiedAt(ctx, q, bucket)
	if err != nil {
		return 0, 0, err
	}
	intact := map[string]bool{}
	for _, p := range ps {
		if p.cond != ConditionRemoved && !p.gone(db, now) && !p.removing && !unverified[p.peer] {
			intact[p.peer] = true
		}
	}
	acked, err := column(ctx, q, `SELECT place FROM placement WHERE seq = ? AND state = 'acked' AND place != ?`, seq, except)
	if err != nil {
		return 0, 0, err
	}
	for _, a := range acked {
		if intact[a] {
			n++
		}
	}
	err = q.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
		bucket, DefaultCopies).Scan(&copies)
	return n, copies, err
}

// querierExecer reads and writes in a transaction.
type querierExecer interface {
	querier
	execer
}

// removeCopy marks place's acknowledged copy of row seq dropping, if
// mayRemove allows it: a delete in the place's FIFO at the row's
// sequence (replace.go). It returns how many rows it marked (0 or 1).
// Only the place's latest row of the key is marked: a later row there
// is ordered after the delete in its FIFO, but one acknowledged already
// must not be deleted.
func (db *DB) removeCopy(ctx context.Context, x querierExecer, r removal, now time.Time) (int64, error) {
	r.acked = true
	ok, _, err := db.mayRemove(ctx, x, r, now)
	if err != nil || !ok {
		return 0, err
	}
	res, err := x.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDropping+`'
		WHERE place = ?1 AND seq = ?2 AND state = 'acked' AND op = 'put'
		AND seq = (SELECT max(x.seq) FROM placement x
			WHERE x.place = placement.place AND x.bucket = placement.bucket AND x.key = placement.key)`, r.place, r.seq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// cancelCopy takes place's unsent row seq out of its FIFO, if mayRemove
// allows it: abandoned when a sender took it (its send may have landed,
// so a later delete still follows it, REPL-2), else deleted; with
// untakenOnly, a taken row is left as it is. It returns how many rows
// it changed (0 or 1).
func (db *DB) cancelCopy(ctx context.Context, x querierExecer, r removal, untakenOnly bool, now time.Time) (int64, error) {
	r.acked = false
	ok, _, err := db.mayRemove(ctx, x, r, now)
	if err != nil || !ok {
		return 0, err
	}
	if !untakenOnly {
		res, err := x.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementAbandoned+`'
			WHERE place = ? AND seq = ? AND state = 'queued' AND started_at IS NOT NULL`, r.place, r.seq)
		if err != nil {
			return 0, err
		}
		if n, err := res.RowsAffected(); err != nil || n > 0 {
			return n, err
		}
	}
	res, err := x.ExecContext(ctx, `DELETE FROM placement WHERE place = ? AND seq = ? AND state = 'queued'
		AND started_at IS NULL`, r.place, r.seq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// checkDrop asks mayRemove again, at now, about place's dropping copy
// of row seq before its delete is sent (#104, file comment above). ok
// is true when the delete may go. A refused drop is acknowledged again:
// back in place_use (the trigger), and out of place_freed, where
// place_freed_dropping counted it when it was marked (or less, floored
// at 0, if a quota answer reset place_freed since: the next answer
// corrects it). A row not dropping is left as it is, and ok is false.
func (db *DB) checkDrop(ctx context.Context, x querierExecer, place string, seq int64, now time.Time) (bool, error) {
	var size int64
	err := x.QueryRowContext(ctx, `SELECT coalesce(size, 0) FROM placement WHERE place = ? AND seq = ? AND state = ?`,
		place, seq, PlacementDropping).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	ok, _, err := db.mayRemove(ctx, x, removal{seq: seq, place: place, acked: true}, now)
	if err != nil || ok {
		return ok, err
	}
	if _, err := x.ExecContext(ctx, `UPDATE placement SET state = 'acked' WHERE place = ? AND seq = ? AND state = ?`,
		place, seq, PlacementDropping); err != nil {
		return false, err
	}
	if size > 0 {
		if _, err := x.ExecContext(ctx, `UPDATE place_freed SET bytes = max(bytes - ?, 0) WHERE place = ?`,
			size, place); err != nil {
			return false, err
		}
	}
	return false, nil
}

// drainedAck records the acknowledgement at now of the delete of place's
// dropping copy of row seq: the copy is drained (the lender keeps it as
// a tomb for the window, RET-1). ok is false when the row is not
// dropping. The removal was decided when it was marked (removeCopy),
// and again as its delete was taken to be sent (checkDrop).
func drainedAck(ctx context.Context, x execer, place string, seq int64, now time.Time) (bool, error) {
	res, err := x.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDrained+`', acked_at = ?
		WHERE place = ? AND seq = ? AND state = '`+PlacementDropping+`'`, now.Unix(), place, seq)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
