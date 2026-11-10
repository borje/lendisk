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
//     deletes are forgotten, so the target keeps what it holds.
//  4. FinishOverflowRemovals drops the row, and every per-place row of
//     the target, once nothing depends on it: nothing in its FIFO, no
//     live data copy acknowledged there, and no held delete (without
//     keep_data, no live metadata either, which re-placement drops once
//     the target holds no data of the bucket, after the window of the
//     deletes it holds there, dropIdleMetadata).
//
// A caller must keep: the daemon (its worker) runs steps 2 to 4; a
// removing target keeps its sender, and so needs its credential, until
// its row goes.

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
		if _, err := tx.ExecContext(ctx, `DELETE FROM overflow_tomb WHERE place = ?`, place); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// forgetSplit makes the next capture ask overflowSplit again: the pool
// changed.
func (db *DB) forgetSplit() {
	db.splitMu.Lock()
	db.splitAt = time.Time{}
	db.splitMu.Unlock()
}

// UpdateOverflow writes o's endpoint, region and max_bytes, and what a
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
	r, err := tx.ExecContext(ctx, `UPDATE overflow SET endpoint = ?, region = ?, max_bytes = ?, lifecycle = ?,
		version_removal = ? WHERE name = ?`, o.Endpoint, o.Region, o.MaxBytes, o.Lifecycle, o.VersionRemoval, o.Name)
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

// FinishOverflowRemovals drops each target being removed that nothing
// depends on any more (step 4 above), with every row of it as a place,
// and returns their names.
func (db *DB) FinishOverflowRemovals(ctx context.Context) ([]string, error) {
	names, err := column(ctx, db, `SELECT o.name FROM overflow o WHERE `+removable+` ORDER BY o.name`)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	defer db.forgetSplit()
	var done []string
	for _, name := range names {
		ok, err := db.dropOverflow(ctx, name)
		if err != nil {
			return done, err
		}
		if ok {
			done = append(done, name)
		}
	}
	return done, nil
}

// dropOverflow deletes target name and its rows as a place, in one
// transaction, if it is still removable.
func (db *DB) dropOverflow(ctx context.Context, name string) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var ok bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM overflow o WHERE o.name = ? AND `+removable+`)`,
		name).Scan(&ok); err != nil || !ok {
		return false, err
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
		return false, err
	}
	if keep {
		if _, _, err := db.mayRemove(ctx, tx, removal{place: place, except: exceptKeepData}, db.now()); err != nil {
			return false, err
		}
	}
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
	} {
		if _, err := tx.ExecContext(ctx, q, place); err != nil {
			return false, err
		}
	}
	for _, q := range []string{`DELETE FROM overflow_versioning WHERE name = ?`, `DELETE FROM overflow WHERE name = ?`} {
		if _, err := tx.ExecContext(ctx, q, name); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}
