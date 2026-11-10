package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// Recovery (ID-5, #29): placement and the hash index are rebuilt from
// each place's list of what it holds of each bucket (NET-7 list), one
// page at a time. The lists still to read are the relist table, so a
// place that is offline at recovery is listed when it returns, and a
// list cut short resumes after its last recorded key.
//
// What each place listed is kept per place (table listed), and which
// version of a key counts is decided from all of them (table
// recovered_key), again whenever another place's list arrives:
//
//   - the version most places list counts (keys named by their hash are
//     checked against the name before they get here, so their places
//     all agree);
//   - a key one place lists is a single source and counts, except a
//     metadata or snapshot key that another place, listed to its end,
//     lacks: REPL-12 puts metadata at every friend, so that is a
//     conflict;
//   - places that tie are a conflict: no version counts, so no place
//     counts as holding it and the read-through refuses it (KeyConflict)
//     until the user picks a place (ResolveKey) or a later list breaks
//     the tie;
//   - except a tie on a key the bucket's profile names rewritable (EP-7,
//     #74), where a place lists the versions it replaced by rewrites
//     (its tombstones, kept for the retention window): if exactly one
//     of the tied versions is one no other place replaced, it is the
//     newest and counts (VerdictNewest), and the user may still pick
//     another.
//
// Only the version that counts has a queue row and acked placement rows,
// at the places that list that hash.

// Verdicts on a recovered key.
const (
	VerdictHeld     = "held"
	VerdictSingle   = "single"
	VerdictConflict = "conflict"
	// VerdictNewest: the places tie on a rewritable key, and one version
	// is the only one no other place replaced by a rewrite (#74).
	VerdictNewest = "newest"
)

// Listed is one entry of a place's list of a bucket: a key it holds,
// with the size and hash it stored (REPL-10), the class the bucket's
// profile gives the key (EP-7), and whether it is tombstoned there.
type Listed struct {
	Key        string
	Size       int64
	SHA256     string
	Class      string
	Tombstoned bool
	// HashNamed: the key is named by its hash, and the caller checked
	// that the hash is the name, so this place's copy is the object.
	HashNamed bool
	// Rewritable: the bucket's profile names the key rewritable (EP-7).
	Rewritable bool
	// Replaced are the hashes of the versions of the key this place
	// replaced by rewrites and still keeps as tombstones (#74).
	Replaced []string
}

// ListedCount is what a page of a list held: live keys recorded, and
// tombstoned keys (not live, only counted).
type ListedCount struct {
	Listed, Tombstoned int
}

// Add sums n into c.
func (c *ListedCount) Add(n ListedCount) {
	c.Listed += n.Listed
	c.Tombstoned += n.Tombstoned
}

// Relist is a list still to read: place's copy of bucket.
type Relist struct {
	Place, Bucket string
	After         string    // the next page starts after this key
	Seen          int64     // entries read so far
	Pages         int64     // pages read so far
	TriedAt       time.Time // the last failed attempt; zero if none
	Error         string    // its failure
	Failures      int64     // failed attempts in a row
}

// ErrNotConflict means ResolveKey was asked to settle a key that is not
// in conflict: nothing to resolve.
var ErrNotConflict = errors.New("not a conflict; nothing to resolve")

// RecoveredKey is recovery's verdict on a key: which version counts
// (SHA256, empty for a conflict), why (Reason), and what each place
// listed (Places: place → hash).
type RecoveredKey struct {
	Bucket, Key string
	Verdict     string
	Reason      string
	SHA256      string
	Places      map[string]string
	// DecidedBy is, for VerdictNewest (NewestKeys), the place whose
	// replaced versions named the others' version older (#74).
	DecidedBy string
}

// AddRelists records that every place's copy of every bucket is to be
// listed. A list already pending keeps its progress and failures.
func (db *DB) AddRelists(ctx context.Context, places, buckets []string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range places {
		for _, b := range buckets {
			if _, err := tx.ExecContext(ctx, `INSERT INTO relist (place, bucket) VALUES (?, ?) ON CONFLICT DO NOTHING`, p, b); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// Relists returns the lists still to read, by place and bucket.
func (db *DB) Relists(ctx context.Context) ([]Relist, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, bucket, after, coalesce(seen, 0), coalesce(pages, 0), tried_at,
		coalesce(error, ''), failures
		FROM relist WHERE abandoned IS NULL AND done IS NULL ORDER BY place, bucket`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Relist
	for rows.Next() {
		var r Relist
		var tried sql.NullInt64
		if err := rows.Scan(&r.Place, &r.Bucket, &r.After, &r.Seen, &r.Pages, &tried, &r.Error, &r.Failures); err != nil {
			return nil, err
		}
		if tried.Valid {
			r.TriedAt = time.Unix(tried.Int64, 0)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RelistFailed records a failed attempt to list place's copy of bucket.
func (db *DB) RelistFailed(ctx context.Context, place, bucket, msg string, now time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE relist SET tried_at = ?, error = ?, failures = failures + 1
		WHERE place = ? AND bucket = ? AND done IS NULL AND abandoned IS NULL`, now.Unix(), msg, place, bucket)
	return err
}

// AbandonRelist gives up on place's list of bucket (it listed more than
// a list may hold): the list is no longer read, and SettleFinished, which
// the caller runs next, drops what it recorded and decides the keys it
// listed again without it. The place does not count as listed to its
// end.
func (db *DB) AbandonRelist(ctx context.Context, place, bucket string) error {
	_, err := db.ExecContext(ctx, `UPDATE relist SET abandoned = 1, settled = NULL WHERE place = ? AND bucket = ?`,
		place, bucket)
	return err
}

// Unsettled counts the lists that ended (read to their end or
// abandoned) whose keys SettleFinished has not yet decided again.
func (db *DB) Unsettled(ctx context.Context) (int, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM relist
		WHERE (done IS NOT NULL OR abandoned IS NOT NULL) AND settled IS NULL`).Scan(&n)
	return n, err
}

// SettleBatch is how many keys one write transaction decides again
// after a list ends, at most; SettleBudget is how long it runs, at most
// (it ends after the key that passes it). Other writers (the gateway's
// captures) wait one batch at most: a fraction of a second, on a slow
// machine too, where 2,000 keys would take seconds.
var (
	SettleBatch  = 2000
	SettleBudget = 250 * time.Millisecond
)

// SettlePause is the pause between batches. SQLite's busy handler polls
// a waiting writer at most every 100 ms, so a pause at least that long
// lets every waiting writer in before the next batch.
var SettlePause = 100 * time.Millisecond

// pause waits SettlePause, or until ctx ends.
func pause(ctx context.Context) error {
	t := time.NewTimer(SettlePause)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// SettleFinished decides again the keys that lists ended since affect,
// in write transactions of SettleBatch keys or SettleBudget, and marks
// each list settled: an abandoned list's keys, without its entries; a
// finished list's metadata keys one place alone lists (the
// lone-metadata rule counts the finished list now). It resumes where a crash stopped it, so the
// lister calls it at the start of every round.
func (db *DB) SettleFinished(ctx context.Context) error {
	type ended struct {
		place, bucket string
		abandoned     bool
	}
	rows, err := db.QueryContext(ctx, `SELECT place, bucket, abandoned IS NOT NULL FROM relist
		WHERE (done IS NOT NULL OR abandoned IS NOT NULL) AND settled IS NULL ORDER BY place, bucket`)
	if err != nil {
		return err
	}
	var es []ended
	for rows.Next() {
		var e ended
		if err := rows.Scan(&e.place, &e.bucket, &e.abandoned); err != nil {
			rows.Close()
			return err
		}
		es = append(es, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, e := range es {
		if e.abandoned {
			for {
				n, err := db.dropBatch(ctx, e.place, e.bucket)
				if err != nil {
					return err
				}
				if n == 0 {
					break
				}
				if err := pause(ctx); err != nil {
					return err
				}
			}
		} else {
			keys, err := column(ctx, db, `SELECT key FROM listed WHERE bucket = ? AND class IN ('metadata', 'snapshot') AND named = 0
				GROUP BY key HAVING count(*) = 1 ORDER BY key`, e.bucket)
			if err != nil {
				return err
			}
			for len(keys) > 0 {
				n, err := db.settleKeys(ctx, e.bucket, keys[:min(len(keys), SettleBatch)])
				if err != nil {
					return err
				}
				keys = keys[n:]
				if err := pause(ctx); err != nil {
					return err
				}
			}
		}
		if _, err := db.ExecContext(ctx, `UPDATE relist SET settled = 1 WHERE place = ? AND bucket = ?`, e.place, e.bucket); err != nil {
			return err
		}
	}
	return nil
}

// dropBatch drops up to SettleBatch of an abandoned place's entries for
// bucket, stopping once SettleBudget has passed, and decides their keys
// again, in one transaction; n is how many.
func (db *DB) dropBatch(ctx context.Context, place, bucket string) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	keys, err := column(ctx, tx, `SELECT key FROM listed WHERE place = ? AND bucket = ? ORDER BY key LIMIT ?`,
		place, bucket, SettleBatch)
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	done, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return 0, err
	}
	p := prepared(tx)
	start := time.Now()
	n := 0
	for _, k := range keys {
		if _, err := p.ExecContext(ctx, `DELETE FROM listed WHERE place = ? AND bucket = ? AND key = ?`, place, bucket, k); err != nil {
			return 0, err
		}
		if err := db.settleKey(ctx, p, bucket, k, done); err != nil {
			return 0, err
		}
		n++
		if time.Since(start) >= SettleBudget {
			break
		}
	}
	return n, tx.Commit()
}

// settleKeys decides keys of bucket again in one transaction, stopping
// once SettleBudget has passed; n is how many of the first keys it
// decided.
func (db *DB) settleKeys(ctx context.Context, bucket string, keys []string) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	done, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return 0, err
	}
	p := prepared(tx)
	start := time.Now()
	n := 0
	for _, k := range keys {
		if err := db.settleKey(ctx, p, bucket, k, done); err != nil {
			return 0, err
		}
		n++
		if time.Since(start) >= SettleBudget {
			break
		}
	}
	return n, tx.Commit()
}

// RecordListed records a page of place's list of bucket, in one
// transaction: each live key's size and hash at place, and the verdict
// on each key it names, made again with every place's entries. seen is
// how many entries the page carried, rejected ones included, which
// counts toward the list's cap. The list's resume point moves to next,
// or, when last, the list is done: SettleFinished, which the caller runs
// next, decides again the keys its end affects (a place listed to its
// end that lacks a metadata key makes it a conflict).
func (db *DB) RecordListed(ctx context.Context, place, bucket string, objs []Listed, seen int, next string, last bool) (ListedCount, error) {
	var c ListedCount
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return c, err
	}
	defer tx.Rollback()
	p := prepared(tx)
	var keys []string
	for _, o := range objs {
		if o.Tombstoned {
			c.Tombstoned++
			continue
		}
		if _, err := p.ExecContext(ctx, `INSERT INTO listed (bucket, key, place, size, sha256, class, named, rewritable, replaced)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (bucket, key, place) DO UPDATE SET size = excluded.size, sha256 = excluded.sha256, class = excluded.class,
				named = excluded.named, rewritable = excluded.rewritable, replaced = excluded.replaced`,
			bucket, o.Key, place, o.Size, o.SHA256, o.Class, o.HashNamed, nullTrue(o.Rewritable),
			nullString(strings.Join(o.Replaced, " "))); err != nil {
			return c, err
		}
		keys = append(keys, o.Key)
		c.Listed++
	}
	var done any
	if last {
		done = 1
	}
	if _, err := tx.ExecContext(ctx, `UPDATE relist SET after = ?, seen = coalesce(seen, 0) + ?, pages = coalesce(pages, 0) + 1,
		tried_at = NULL, error = NULL, failures = 0, done = ?, settled = NULL
		WHERE place = ? AND bucket = ?`, next, seen, done, place, bucket); err != nil {
		return c, err
	}
	listed, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return c, err
	}
	for _, k := range keys {
		if err := db.settleKey(ctx, p, bucket, k, listed); err != nil {
			return c, err
		}
	}
	return c, tx.Commit()
}

// listedPlaces are the friends whose list of bucket was read to its end:
// it had a list to read, and that list is done, not abandoned. A friend
// added since recovery has none, so it does not count.
func listedPlaces(ctx context.Context, tx *sql.Tx, bucket string) (map[string]bool, error) {
	ps, err := column(ctx, tx, `SELECT place FROM relist
		WHERE bucket = ? AND done IS NOT NULL AND abandoned IS NULL
		AND place IN (SELECT peer_key FROM friendship)`, bucket)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, p := range ps {
		out[p] = true
	}
	return out, nil
}

type listedRow struct {
	place, sha, class string
	size              int64
	named, rewritable bool
	replaced          []string
}

// preparedTx runs statements in a transaction, preparing each distinct
// one once: settling runs the same dozen statements for every key of a
// batch, and preparing one costs several times what running it does
// (#29: a 2,000-key batch spent most of its time compiling SQL, which
// the race detector makes seconds). The statements close with the
// transaction.
type preparedTx struct {
	tx    *sql.Tx
	stmts map[string]*sql.Stmt
}

func prepared(tx *sql.Tx) *preparedTx {
	return &preparedTx{tx: tx, stmts: map[string]*sql.Stmt{}}
}

func (p *preparedTx) stmt(ctx context.Context, q string) (*sql.Stmt, error) {
	if s, ok := p.stmts[q]; ok {
		return s, nil
	}
	s, err := p.tx.PrepareContext(ctx, q)
	if err != nil {
		return nil, err
	}
	p.stmts[q] = s
	return s, nil
}

func (p *preparedTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	s, err := p.stmt(ctx, q)
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

func (p *preparedTx) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	s, err := p.stmt(ctx, q)
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...)
}

func (p *preparedTx) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	s, err := p.stmt(ctx, q)
	if err != nil {
		return p.tx.QueryRowContext(ctx, q, args...) // fails again, with the error
	}
	return s.QueryRowContext(ctx, args...)
}

// settleKey decides which version of key counts from what every place
// listed, and makes the queue and placement rows say so; done are the
// places listed to their end.
func (db *DB) settleKey(ctx context.Context, tx *preparedTx, bucket, key string, done map[string]bool) error {
	ls, err := listedRows(ctx, tx, bucket, key)
	if err != nil {
		return err
	}
	if onlyTargetsList(ls) {
		// #96 review: a metadata or snapshot key no friend lists is not
		// live on an overflow target's word. Friends hold every live
		// metadata key (REPL-12); a target may still hold one the node
		// pruned, its delete held as a client-side tomb in the lost
		// lendisk.db (S3-4), and a revived restic index would let later
		// backups dedupe against packs pruned at the friends. A friend's
		// list that names it decides it again.
		ls = nil
	}
	var own sql.NullInt64
	var picked sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT seq, picked FROM recovered_key WHERE bucket = ? AND key = ?`, bucket, key).Scan(&own, &picked)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	verdict, reason, winner := decide(ls, picked.String, done)
	gone := len(ls) == 0

	// The node wrote the key itself since recovery: that version is its
	// own; the lists only add the places that hold it.
	var latest int64
	var op string
	var latestSHA sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT seq, op, sha256 FROM queue
		WHERE bucket = ? AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, bucket, key).Scan(&latest, &op, &latestSHA)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return err
	case err == nil && (!own.Valid || latest != own.Int64):
		if op == OpPut {
			for _, r := range ls {
				if r.sha == latestSHA.String {
					if err := ackListed(ctx, tx, latest, bucket, key, r, db.now()); err != nil {
						return err
					}
				}
			}
		}
		verdict, reason, winner = VerdictHeld, "written on this node since recovery", latestSHA.String
		return saveVerdict(ctx, tx, bucket, key, verdict, reason, winner, own, picked)
	}

	if gone {
		// No place lists it any more (its only lister was abandoned), and
		// the node did not write it: recovery knows nothing of it.
		if own.Valid {
			for _, q := range []string{`DELETE FROM placement WHERE seq = ?`, `DELETE FROM queue WHERE seq = ?`} {
				if _, err := tx.ExecContext(ctx, q, own.Int64); err != nil {
					return err
				}
			}
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM recovered_key WHERE bucket = ? AND key = ?`, bucket, key)
		return err
	}
	if own.Valid {
		var ownSHA sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT sha256 FROM queue WHERE seq = ?`, own.Int64).Scan(&ownSHA); err != nil &&
			!errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if winner == "" || ownSHA.String != winner {
			for _, q := range []string{`DELETE FROM placement WHERE seq = ?`, `DELETE FROM queue WHERE seq = ?`} {
				if _, err := tx.ExecContext(ctx, q, own.Int64); err != nil {
					return err
				}
			}
			own = sql.NullInt64{}
		}
	}
	if winner != "" {
		var w listedRow
		for _, r := range ls {
			if r.sha == winner {
				w = r
				break
			}
		}
		if !own.Valid {
			res, err := tx.ExecContext(ctx, `INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at, rewritable)
				VALUES (?, ?, 'put', ?, 'done', ?, ?, ?, ?)`, bucket, key, w.class, w.size, winner, db.now().Unix(), nullTrue(w.rewritable))
			if err != nil {
				return err
			}
			id, err := res.LastInsertId()
			if err != nil {
				return err
			}
			own = sql.NullInt64{Int64: id, Valid: true}
		}
		holders := []any{own.Int64}
		marks := []string{}
		for _, r := range ls {
			if r.sha == winner {
				holders = append(holders, r.place)
				marks = append(marks, "?")
				if err := ackListed(ctx, tx, own.Int64, bucket, key, r, db.now()); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM placement WHERE seq = ? AND place NOT IN (`+strings.Join(marks, ",")+`)`,
			holders...); err != nil {
			return err
		}
	}
	return saveVerdict(ctx, tx, bucket, key, verdict, reason, winner, own, picked)
}

func ackListed(ctx context.Context, tx *preparedTx, seq int64, bucket, key string, r listedRow, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
		VALUES (?, ?, ?, ?, 'put', 'acked', ?, ?, ?) ON CONFLICT (place, seq) DO NOTHING`,
		seq, r.place, bucket, key, r.sha, now.Unix(), r.size+ObjectOverhead)
	return err
}

func saveVerdict(ctx context.Context, tx *preparedTx, bucket, key, verdict, reason, sha string, seq sql.NullInt64, picked sql.NullString) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO recovered_key (bucket, key, verdict, reason, sha256, seq, picked)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket, key) DO UPDATE SET verdict = excluded.verdict, reason = excluded.reason,
			sha256 = excluded.sha256, seq = excluded.seq, picked = excluded.picked`,
		bucket, key, verdict, nullString(reason), nullString(sha), seq, picked)
	return err
}

// onlyTargetsList reports whether ls is a metadata or snapshot key's
// listing by overflow targets alone (#96 review).
func onlyTargetsList(ls []listedRow) bool {
	if len(ls) == 0 {
		return false
	}
	for _, r := range ls {
		if _, target := OverflowName(r.place); !target || r.class != "metadata" && r.class != "snapshot" {
			return false
		}
	}
	return true
}

// decide is the rule: the user's pick, else the hash most places list,
// a tie being a conflict; one place alone is a single source, unless it
// is a metadata key another place listed to its end lacks.
func decide(ls []listedRow, picked string, done map[string]bool) (verdict, reason, winner string) {
	if len(ls) == 0 {
		return VerdictConflict, "no place lists it any more", ""
	}
	count := map[string]int{}
	for _, r := range ls {
		count[r.sha]++
		if picked != "" && r.sha == picked {
			winner = picked
		}
	}
	if winner != "" {
		return VerdictHeld, "picked by the user", winner
	}
	if len(ls) == 1 {
		r := ls[0]
		// A key named by its hash is that object whoever lists it: a
		// friend that lacks it does not make it doubtful.
		if (r.class == "metadata" || r.class == "snapshot") && !r.named {
			var lacking []string
			for p := range done {
				if p != r.place {
					lacking = append(lacking, p)
				}
			}
			if len(lacking) > 0 {
				sort.Strings(lacking)
				return VerdictConflict, fmt.Sprintf("only %s lists this metadata key; %s listed this bucket to its end without it, "+
					"though every friend should hold metadata", r.place, strings.Join(lacking, ", ")), ""
			}
		}
		return VerdictSingle, "one place lists it", r.sha
	}
	best, n, tie := "", 0, false
	for h, c := range count {
		switch {
		case c > n:
			best, n, tie = h, c, false
		case c == n:
			tie = true
		}
	}
	if tie {
		if h, by, ok := newest(ls); ok {
			return VerdictNewest, fmt.Sprintf("the places that list it disagree, %d to %d; %s replaced the others' version "+
				"there by a rewrite, so its version is the newest", n, n, by), h
		}
		return VerdictConflict, fmt.Sprintf("the places that list it disagree, %d to %d", n, n), ""
	}
	if len(count) == 1 {
		return VerdictHeld, "every place that lists it agrees", best
	}
	return VerdictHeld, fmt.Sprintf("%d places agree; the others list another version", n), best
}

// listedRows is what every place listed of key.
func listedRows(ctx context.Context, q querier, bucket, key string) ([]listedRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT place, sha256, class, size, named, coalesce(rewritable, 0), coalesce(replaced, '')
		FROM listed WHERE bucket = ? AND key = ? ORDER BY place`, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ls []listedRow
	for rows.Next() {
		var r listedRow
		var replaced string
		if err := rows.Scan(&r.place, &r.sha, &r.class, &r.size, &r.named, &r.rewritable, &replaced); err != nil {
			return nil, err
		}
		r.replaced = strings.Fields(replaced)
		ls = append(ls, r)
	}
	return ls, rows.Err()
}

// NewestKeys returns every recovered key decided as the newest of the
// versions its friends disagreed on (VerdictNewest, #74) that this node
// has not written since, with the place whose replaced versions decided
// it: the user may still pick another (ResolveKey).
func (db *DB) NewestKeys(ctx context.Context) ([]RecoveredKey, error) {
	ks, err := db.recoveredKeys(ctx, byNewest)
	if err != nil {
		return nil, err
	}
	for i, k := range ks {
		ls, err := listedRows(ctx, db, k.Bucket, k.Key)
		if err != nil {
			return nil, err
		}
		if _, by, ok := newest(ls); ok {
			ks[i].DecidedBy = by
		}
	}
	return ks, nil
}

// newest finds, for a rewritable key (#74), the one listed version that
// no place listing another version replaced by a rewrite: by is a place
// that replaced another's version, and lists it. ok is false for a key
// that is not rewritable, or when no version, or more than one, is
// left.
func newest(ls []listedRow) (winner, by string, ok bool) {
	replaced := map[string]bool{} // a version some place replaced, listing another
	var rewritable bool
	for _, r := range ls {
		rewritable = rewritable || r.rewritable
		for _, h := range r.replaced {
			if h != r.sha {
				replaced[h] = true
			}
		}
	}
	if !rewritable {
		return "", "", false
	}
	left := map[string]bool{}
	for _, r := range ls {
		if !replaced[r.sha] {
			left[r.sha] = true
		}
	}
	if len(left) != 1 {
		return "", "", false
	}
	for h := range left {
		winner = h
	}
	for _, r := range ls {
		if r.sha != winner {
			continue
		}
		for _, h := range r.replaced {
			if h != winner && slices.ContainsFunc(ls, func(o listedRow) bool { return o.sha == h }) {
				return winner, r.place, true
			}
		}
	}
	// The newest version's places replaced none of the others directly;
	// another place's chain did (it replaced them and was replaced too).
	for _, r := range ls {
		if r.sha == winner {
			return winner, r.place, true
		}
	}
	return "", "", false
}

// RecoveredKeys returns recovery's verdict on every key of bucket ("":
// every bucket), with what each place listed.
func (db *DB) RecoveredKeys(ctx context.Context, bucket string) ([]RecoveredKey, error) {
	return db.recoveredKeys(ctx, `(? = '' OR k.bucket = ?)`, bucket, bucket)
}

// Conflicts returns every recovered key in conflict.
func (db *DB) Conflicts(ctx context.Context) ([]RecoveredKey, error) {
	return db.recoveredKeys(ctx, inConflict)
}

// inConflict selects recovered keys k in conflict that this node has
// not written since recovery: a write of its own settles the key.
const inConflict = `k.verdict = '` + VerdictConflict + `' AND NOT EXISTS (SELECT 1 FROM queue q
	WHERE q.bucket = k.bucket AND q.key = k.key AND q.state = 'done' AND (k.seq IS NULL OR q.seq > k.seq))`

// byNewest selects recovered keys k decided as the newest of tied
// versions (VerdictNewest) that this node has not written since
// recovery: the user may still pick another (ResolveKey).
const byNewest = `k.verdict = '` + VerdictNewest + `' AND NOT EXISTS (SELECT 1 FROM queue q
	WHERE q.bucket = k.bucket AND q.key = k.key AND q.state = 'done' AND (k.seq IS NULL OR q.seq > k.seq))`

// KeyConflict reports whether key of bucket is a recovered key in
// conflict, and the verdict: the read-through (#30) must refuse it,
// saying why, until the user picks a place (ResolveKey).
func (db *DB) KeyConflict(ctx context.Context, bucket, key string) (RecoveredKey, bool, error) {
	ks, err := db.recoveredKeys(ctx, `k.bucket = ? AND k.key = ? AND `+inConflict, bucket, key)
	if err != nil || len(ks) == 0 {
		return RecoveredKey{}, false, err
	}
	return ks[0], true, nil
}

// SingleSources counts, per bucket, the recovered keys only one place
// lists.
func (db *DB) SingleSources(ctx context.Context) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT bucket, count(*) FROM recovered_key WHERE verdict = ? GROUP BY bucket`, VerdictSingle)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var b string
		var n int64
		if err := rows.Scan(&b, &n); err != nil {
			return nil, err
		}
		out[b] = n
	}
	return out, rows.Err()
}

func (db *DB) recoveredKeys(ctx context.Context, where string, args ...any) ([]RecoveredKey, error) {
	rows, err := db.QueryContext(ctx, `SELECT k.bucket, k.key, k.verdict, coalesce(k.reason, ''), coalesce(k.sha256, ''),
		coalesce(l.place, ''), coalesce(l.sha256, '')
		FROM recovered_key k LEFT JOIN listed l ON l.bucket = k.bucket AND l.key = k.key
		WHERE `+where+` ORDER BY k.bucket, k.key, l.place`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecoveredKey
	for rows.Next() {
		var k RecoveredKey
		var place, sha string
		if err := rows.Scan(&k.Bucket, &k.Key, &k.Verdict, &k.Reason, &k.SHA256, &place, &sha); err != nil {
			return nil, err
		}
		if n := len(out); n == 0 || out[n-1].Bucket != k.Bucket || out[n-1].Key != k.Key {
			k.Places = map[string]string{}
			out = append(out, k)
		}
		if place != "" {
			out[len(out)-1].Places[place] = sha
		}
	}
	return out, rows.Err()
}

// ResolveKey settles a recovered key in conflict, or one decided as the
// newest of tied versions (#74), with the version place listed: the
// user's pick, which stands until the key is written again.
// ErrNotFound if recovery knows no such key or place does not list it;
// ErrNotConflict if the key is not in conflict (held, a single source,
// or written on this node since recovery).
func (db *DB) ResolveKey(ctx context.Context, bucket, key, place string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var known, conflict bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM recovered_key WHERE bucket = ?1 AND key = ?2),
		EXISTS (SELECT 1 FROM recovered_key k WHERE k.bucket = ?1 AND k.key = ?2 AND (`+inConflict+` OR `+byNewest+`))`,
		bucket, key).Scan(&known, &conflict)
	switch {
	case err != nil:
		return err
	case !known:
		return ErrNotFound
	case !conflict:
		return ErrNotConflict
	}
	var sha string
	err = tx.QueryRowContext(ctx, `SELECT sha256 FROM listed WHERE bucket = ? AND key = ? AND place = ?`,
		bucket, key, place).Scan(&sha)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recovered_key SET picked = ? WHERE bucket = ? AND key = ?`, sha, bucket, key); err != nil {
		return err
	}
	done, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return err
	}
	if err := db.settleKey(ctx, prepared(tx), bucket, key, done); err != nil {
		return err
	}
	return tx.Commit()
}

// HeldAt counts, per place, the keys of bucket it is recorded holding.
func (db *DB) HeldAt(ctx context.Context, bucket string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, count(*) FROM placement WHERE bucket = ? AND state = 'acked' GROUP BY place`, bucket)
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
