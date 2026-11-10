package state

import (
	"context"
	"errors"
	"time"
)

// This file is changing and removing overflow targets (S3-1, S3-7,
// #96).
//
// Removal never leaves an object short of copies (REPL-13), in this
// order:
//  1. RemoveOverflow marks the target removing: from then on it is no
//     candidate for a copy, at capture (choose) or in re-placement, it
//     is sent no more metadata (overflowHolding), and its copies stop
//     counting in the pool (PoolSize, the offsite holders); its unsent
//     puts that no sender took leave its FIFO.
//  2. Re-placement drains it like a friend that ended (replace.go,
//     drainAll): each object it holds a copy of gets one at a friend or
//     another target by D6's choice, and keeps `copies` acknowledged
//     copies throughout; the target's copy is dropped only once the
//     object has them without it.
//  3. A dropped copy is deleted there the retention-preserving way, as
//     every delete at a target: a delete marker at a versioned target,
//     a client-side tomb sent after --retention at one without
//     (S3-4). With keep_data, nothing more is deleted there: a delete
//     or drop is acknowledged unsent (HoldOverflowDelete) and the held
//     deletes are forgotten, so the target keeps what it holds. One the
//     worker claimed to send is left to it, which asks TombToSend before
//     each send, so at most the delete on its way goes (#102).
//  4. FinishOverflowRemovals drops the row, and every per-place row of
//     the target, once nothing depends on it: nothing in its FIFO, no
//     live data copy acknowledged there, and no held delete (without
//     keep_data, no live metadata either, which re-placement drops once
//     the target holds no data of the bucket, after the window of the
//     deletes it holds there, dropIdleMetadata). It returns what the
//     target still holds that nothing deletes (LeftBehind): puts whose
//     answer was lost, which may have landed (#102).
//
// ForceRemoveOverflow (`overflow remove --force`, #102) ends a removal
// its sender cannot finish, the credential revoked or gone: it drops
// the row at once, deleting nothing more there, but only while no live
// copy there is one its object needs (I1), unless the user accepted
// that loss (I3, exceptForced).
//
// A caller must keep: the daemon (its worker) runs steps 2 to 4; a
// removing target keeps its sender, and so needs its credential, until
// its row goes; a forced removal is the way out without it.

// RemoveOverflow begins removing overflow target name at now (step 1
// above); keepData leaves its copies there. Asked again, it keeps the
// first time and turns keepData on, never off. ErrNotFound if there is
// no such target.
func (db *DB) RemoveOverflow(ctx context.Context, name string, keepData bool, now time.Time) error {
	defer db.forgetSplit()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE overflow SET removing_at = coalesce(removing_at, ?),
		keep_data = CASE WHEN ? THEN 1 ELSE keep_data END WHERE name = ?`, now.Unix(), keepData, name)
	if err != nil {
		return err
	}
	if n, err := r.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNotFound
	}
	place := OverflowPlace(name)
	// Its unsent puts no sender took: the target never had them, and
	// re-placement places the copies elsewhere. The removal rule allows
	// each (I1: unsent; I2: a target being removed is sent no more
	// metadata).
	rows, err := tx.QueryContext(ctx, `SELECT p.seq, p.bucket, q.class FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.state = 'queued' AND p.op = 'put' AND p.started_at IS NULL ORDER BY p.seq`, place)
	if err != nil {
		return err
	}
	var rs []removal
	for rows.Next() {
		r := removal{place: place}
		if err := rows.Scan(&r.seq, &r.bucket, &r.class); err != nil {
			rows.Close()
			return err
		}
		rs = append(rs, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range rs {
		if ok, _, err := db.mayRemove(ctx, tx, r, now); err != nil {
			return err
		} else if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE placement SET state = '`+PlacementDropped+`'
			WHERE place = ? AND seq = ? AND state = 'queued' AND started_at IS NULL`, place, r.seq); err != nil {
			return err
		}
	}
	if keepData {
		// The deletes held there are never sent: what they hold stays.
		// A tomb the worker claimed is left to it (#102): it may be
		// sending it now. It asks again before each send (TombToSend),
		// which forgets it, and FinishOverflowRemovals forgets one given
		// back or whose claim went stale.
		if err := forgetKeptTombs(ctx, tx, now, place); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// keptTarget is the SQL condition that place (an expression) is an
// overflow target being removed with keep_data, which is sent no more
// deletes (#96).
func keptTarget(place string) string {
	return `EXISTS (SELECT 1 FROM overflow k WHERE '` + OverflowPrefix + `' || k.name = ` + place + `
		AND k.removing_at IS NOT NULL AND coalesce(k.keep_data, 0) = 1)`
}

// forgetKeptTombs forgets the held deletes no one holds a live claim on
// at places (all targets removed with keep_data when none are given) at
// now (#102): they are never sent.
func forgetKeptTombs(ctx context.Context, x execer, now time.Time, places ...string) error {
	q := `DELETE FROM overflow_tomb WHERE coalesce(claimed_at, 0) <= ? AND ` + keptTarget("overflow_tomb.place")
	args := []any{now.Add(-tombClaim).Unix()}
	for _, p := range places {
		q += ` AND place = ?`
		args = append(args, p)
	}
	_, err := x.ExecContext(ctx, q, args...)
	return err
}

// TombToSend reports whether the held delete of row seq at place, which
// the caller claimed (DueOverflowTombs), is still to be sent, asked just
// before it is sent (#102): not when the tomb is gone, nor at a target
// removed with keep_data since the claim, whose tomb it forgets here. A
// delete already on its way when keep_data was set cannot be recalled;
// this stops the rest of the claimed batch.
func (db *DB) TombToSend(ctx context.Context, place string, seq int64) (bool, error) {
	if _, err := db.ExecContext(ctx, `DELETE FROM overflow_tomb WHERE place = ? AND seq = ? AND `+
		keptTarget("overflow_tomb.place"), place, seq); err != nil {
		return false, err
	}
	var ok bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM overflow_tomb WHERE place = ? AND seq = ?)`,
		place, seq).Scan(&ok)
	return ok, err
}

// forgetSplit makes the next capture ask overflowSplit again: the pool
// changed.
func (db *DB) forgetSplit() {
	db.splitMu.Lock()
	db.splitAt = time.Time{}
	db.splitMu.Unlock()
}

// UpdateOverflow writes o's endpoint, region, max_bytes and full cycle, and what a
// new check found (lifecycle, version_removal), to the target of its
// name (#96), in one transaction with the versioning that check read
// at now, recorded as RecordOverflowVersioning records it ("": no
// check, versioning unchanged). It returns the versioning change, if
// any. ErrNotFound if there is no such target.
func (db *DB) UpdateOverflow(ctx context.Context, o Overflow, versioning string, now time.Time) (*VersioningChange, error) {
	defer db.forgetSplit()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var cycle any
	if o.FullCycle > 0 {
		cycle = int64(o.FullCycle / time.Second)
	}
	r, err := tx.ExecContext(ctx, `UPDATE overflow SET endpoint = ?, region = ?, max_bytes = ?, lifecycle = ?,
		version_removal = ?, full_cycle = ? WHERE name = ?`, o.Endpoint, o.Region, o.MaxBytes, o.Lifecycle, o.VersionRemoval,
		cycle, o.Name)
	if err != nil {
		return nil, err
	}
	if n, err := r.RowsAffected(); err != nil {
		return nil, err
	} else if n == 0 {
		return nil, ErrNotFound
	}
	var ch *VersioningChange
	if versioning != "" {
		if ch, err = recordVersioning(ctx, tx, o.Name, versioning, now); err != nil {
			return nil, err
		}
	}
	return ch, tx.Commit()
}

// HeldSample returns up to n of the objects place acknowledged and
// still holds as their keys' latest writes, spread over the write order
// (#96: `overflow set --endpoint` asks the new endpoint for them).
func (db *DB) HeldSample(ctx context.Context, place string, n int) ([]SampleObject, error) {
	total, _, err := db.Sampleable(ctx, place)
	if err != nil || total == 0 || n <= 0 {
		return nil, err
	}
	var out []SampleObject
	seen := map[int64]bool{}
	for i := range n {
		at := int64(0)
		if n > 1 {
			at = int64(i) * (total - 1) / int64(n-1)
		}
		if seen[at] {
			continue
		}
		seen[at] = true
		o, err := db.SampleableAt(ctx, place, at)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

// removable is the SQL condition that the overflow target of table
// alias o, being removed, is left with nothing that depends on it (step
// 4 above).
const removable = `o.removing_at IS NOT NULL
	AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = '` + OverflowPrefix + `' || o.name
		AND p.state IN ('queued', 'dropping'))
	AND NOT EXISTS (SELECT 1 FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = '` + OverflowPrefix + `' || o.name AND p.state = 'acked' AND p.op = 'put'
		AND (q.class = 'data' OR coalesce(o.keep_data, 0) = 0) AND ` + liveQ + `)
	AND NOT EXISTS (SELECT 1 FROM overflow_tomb t WHERE t.place = '` + OverflowPrefix + `' || o.name)`

// LeftBehind is what a target lendisk drops still holds, as far as
// lendisk knows, out of its hands from then on (#102): bytes the
// provider keeps billing until the user deletes them there.
type LeftBehind struct {
	// Copies are the copies lendisk put there and never deleted:
	// acknowledged, or dropping with the delete not sent (a forced
	// removal). At a target removed with keep_data, a copy whose delete
	// was acknowledged unsent is not counted.
	Copies, CopyBytes int64
	// Deletes are deletes held or refused there and never sent (a
	// forced removal): what they hold stays.
	Deletes, DeleteBytes int64
	// Abandoned are puts whose answer was lost: they may have landed,
	// and nothing deletes them there.
	Abandoned, AbandonedBytes int64
}

// Empty reports whether nothing is known to be left there.
func (l LeftBehind) Empty() bool { return l.Copies == 0 && l.Deletes == 0 && l.Abandoned == 0 }

// RemovedTarget is an overflow target whose row went, with what it
// still holds.
type RemovedTarget struct {
	Name string
	Left LeftBehind
}

// latestOfKey is the SQL condition that placement row p is the latest
// row of its key that may have reached its place: nothing sent there
// after it (a row dropped or refused unsent, or queued and never taken,
// was not) superseded or deleted it.
const latestOfKey = `NOT EXISTS (SELECT 1 FROM placement x WHERE x.place = p.place AND x.bucket = p.bucket
	AND x.key = p.key AND x.seq > p.seq AND x.state NOT IN ('dropped', 'refused')
	AND NOT (x.state = 'queued' AND x.started_at IS NULL))`

// leftAt reads what target place holds that nothing will delete once
// its rows go (LeftBehind).
func leftAt(ctx context.Context, q querier, place string) (LeftBehind, error) {
	var l LeftBehind
	err := q.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE p.state IN ('acked', 'dropping')),
		coalesce(sum(p.size) FILTER (WHERE p.state IN ('acked', 'dropping')), 0),
		count(*) FILTER (WHERE p.state = 'abandoned'), coalesce(sum(p.size) FILTER (WHERE p.state = 'abandoned'), 0)
		FROM placement p WHERE p.place = ? AND p.op = 'put' AND `+latestOfKey, place).Scan(
		&l.Copies, &l.CopyBytes, &l.Abandoned, &l.AbandonedBytes)
	if err != nil {
		return l, err
	}
	err = q.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(size), 0) FROM overflow_tomb WHERE place = ?`, place).Scan(
		&l.Deletes, &l.DeleteBytes)
	return l, err
}

// OverflowLeftAt reads what target name would hold that nothing deletes
// if its row went now (LeftBehind): `overflow remove` names the puts
// abandoned there that may have landed (#102).
func (db *DB) OverflowLeftAt(ctx context.Context, name string) (LeftBehind, error) {
	return leftAt(ctx, db, OverflowPlace(name))
}

// FinishOverflowRemovals drops each target being removed that nothing
// depends on any more (step 4 above), with every row of it as a place,
// and returns them with what each still holds (LeftBehind): the puts
// abandoned there that may have landed, and with keep_data its copies.
// First it forgets the held deletes no one claims at targets removed
// with keep_data (#102).
func (db *DB) FinishOverflowRemovals(ctx context.Context) ([]RemovedTarget, error) {
	if err := forgetKeptTombs(ctx, db, db.now()); err != nil {
		return nil, err
	}
	names, err := column(ctx, db, `SELECT o.name FROM overflow o WHERE `+removable+` ORDER BY o.name`)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	defer db.forgetSplit()
	var done []RemovedTarget
	for _, name := range names {
		r, ok, err := db.dropOverflow(ctx, name)
		if err != nil {
			return done, err
		}
		if ok {
			done = append(done, r)
		}
	}
	return done, nil
}

// dropOverflow deletes target name and its rows as a place, in one
// transaction, if it is still removable.
func (db *DB) dropOverflow(ctx context.Context, name string) (RemovedTarget, bool, error) {
	r := RemovedTarget{Name: name}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return r, false, err
	}
	defer tx.Rollback()
	var ok bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM overflow o WHERE o.name = ? AND `+removable+`)`,
		name).Scan(&ok); err != nil || !ok {
		return r, false, err
	}
	place := OverflowPlace(name)
	// Its rows left are of objects no longer live (removable), and,
	// removed with --keep-data, its acknowledged live metadata, which
	// stays there out of lendisk's hands. The call names the exception
	// (I3, removal.go) and always returns true: a target being removed
	// is no intact place, so forgetting its rows never lowers the count
	// I1 keeps. It records the decision, and enforces nothing.
	var keep bool
	if err := tx.QueryRowContext(ctx, `SELECT coalesce(keep_data, 0) FROM overflow WHERE name = ?`, name).Scan(&keep); err != nil {
		return r, false, err
	}
	if keep {
		if _, _, err := db.mayRemove(ctx, tx, removal{place: place, except: exceptKeepData}, db.now()); err != nil {
			return r, false, err
		}
	}
	if r.Left, err = leftAt(ctx, tx, place); err != nil {
		return r, false, err
	}
	if err := dropTargetRows(ctx, tx, name); err != nil {
		return r, false, err
	}
	return r, true, tx.Commit()
}

// dropTargetRows deletes target name's row and every row of it as a
// place.
func dropTargetRows(ctx context.Context, x execer, name string) error {
	place := OverflowPlace(name)
	for _, q := range []string{
		`DELETE FROM placement WHERE place = ?`,
		`DELETE FROM place_use WHERE place = ?`,
		`DELETE FROM place_freed WHERE place = ?`,
		`DELETE FROM place WHERE place = ?`,
		`DELETE FROM replica_check WHERE place = ?`,
		`DELETE FROM verify_request WHERE place = ?`,
		`DELETE FROM overflow_sample WHERE place = ?`,
		`DELETE FROM transfer_part WHERE place = ?`,
		`DELETE FROM transfer WHERE place = ?`,
		`DELETE FROM listed WHERE place = ?`,
		`DELETE FROM relist WHERE place = ?`,
		`DELETE FROM overflow_tomb WHERE place = ?`,
	} {
		if _, err := x.ExecContext(ctx, q, place); err != nil {
			return err
		}
	}
	for _, q := range []string{`DELETE FROM overflow_versioning WHERE name = ?`, `DELETE FROM overflow WHERE name = ?`} {
		if _, err := x.ExecContext(ctx, q, name); err != nil {
			return err
		}
	}
	return nil
}

// ErrWouldLose is ForceRemoveOverflow's refusal: the target holds
// copies objects still need, and the loss was not accepted.
var ErrWouldLose = errors.New("the target holds copies objects still need")

// Loss is what a forced removal takes from the objects (#102): the live
// objects whose copy at the target is one of their `copies` (I1 refuses
// its removal), with the bytes of those copies. Unprotected of them have
// no acknowledged copy at any other intact place: none offsite once the
// target goes. Keys names up to lossKeys of them, as bucket/key.
type Loss struct {
	Objects, Bytes int64
	Unprotected    int64
	Keys           []string
}

// lossKeys is how many of a Loss's objects it names.
const lossKeys = 10

// ForceRemoveOverflow drops target name now, with every row of it, as
// FinishOverflowRemovals does once nothing depends on it (#102): for a
// removal stuck where its sender cannot go, its credential revoked or
// gone. Nothing more is deleted there, and what it still holds is
// returned (LeftBehind). It marks the target removing first, if it is
// not. Each live data copy there is asked of the removal rule (I1); when
// any is refused, the target holds copies objects still need, and it
// returns their Loss and ErrWouldLose, changing nothing, unless
// acceptLoss: the user saw that loss and accepted it, a named exception
// (I3, exceptForced). ErrNotFound if there is no such target.
func (db *DB) ForceRemoveOverflow(ctx context.Context, name string, acceptLoss bool, now time.Time) (Loss, LeftBehind, error) {
	var loss Loss
	var left LeftBehind
	defer db.forgetSplit()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return loss, left, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `UPDATE overflow SET removing_at = coalesce(removing_at, ?) WHERE name = ?`, now.Unix(), name)
	if err != nil {
		return loss, left, err
	}
	if n, err := r.RowsAffected(); err != nil {
		return loss, left, err
	} else if n == 0 {
		return loss, left, ErrNotFound
	}
	place := OverflowPlace(name)
	type copyAt struct {
		removal
		key  string
		size int64
	}
	rows, err := tx.QueryContext(ctx, `SELECT p.seq, p.bucket, p.key, coalesce(p.size, 0) FROM placement p
		JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.op = 'put' AND p.state IN ('acked', 'dropping') AND q.class = 'data' AND `+liveQ+`
		ORDER BY p.bucket, p.key`, place)
	if err != nil {
		return loss, left, err
	}
	var cs []copyAt
	for rows.Next() {
		c := copyAt{removal: removal{place: place, class: "data", acked: true}}
		if err := rows.Scan(&c.seq, &c.bucket, &c.key, &c.size); err != nil {
			rows.Close()
			return loss, left, err
		}
		cs = append(cs, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return loss, left, err
	}
	for _, c := range cs {
		ok, _, err := db.mayRemove(ctx, tx, c.removal, now)
		if err != nil {
			return loss, left, err
		}
		if ok {
			continue
		}
		loss.Objects++
		loss.Bytes += c.size
		n, _, err := db.intactCopies(ctx, tx, c.bucket, c.seq, place, now)
		if err != nil {
			return loss, left, err
		}
		if n == 0 {
			loss.Unprotected++
		}
		if len(loss.Keys) < lossKeys {
			loss.Keys = append(loss.Keys, c.bucket+"/"+c.key)
		}
	}
	if loss.Objects > 0 && !acceptLoss {
		return loss, left, ErrWouldLose
	}
	// I3: the user forgets the target, accepting the loss above if any.
	// The call records the decision, and enforces nothing.
	if _, _, err := db.mayRemove(ctx, tx, removal{place: place, except: exceptForced}, now); err != nil {
		return loss, left, err
	}
	if left, err = leftAt(ctx, tx, place); err != nil {
		return loss, left, err
	}
	if err := dropTargetRows(ctx, tx, name); err != nil {
		return loss, left, err
	}
	return loss, left, tx.Commit()
}
