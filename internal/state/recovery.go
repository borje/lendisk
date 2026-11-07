package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
//     the tie.
//
// Only the version that counts has a queue row and acked placement rows,
// at the places that list that hash.

// Verdicts on a recovered key.
const (
	VerdictHeld     = "held"
	VerdictSingle   = "single"
	VerdictConflict = "conflict"
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
		FROM relist WHERE abandoned IS NULL ORDER BY place, bucket`)
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
		WHERE place = ? AND bucket = ?`, now.Unix(), msg, place, bucket)
	return err
}

// AbandonRelist gives up on place's list of bucket (it listed more than
// a list may hold): what it recorded is dropped, the list is no longer
// read, and the bucket's keys are decided again without it. The place
// does not count as listed to its end.
func (db *DB) AbandonRelist(ctx context.Context, place, bucket string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{`UPDATE relist SET abandoned = 1 WHERE place = ? AND bucket = ?`,
		`DELETE FROM listed WHERE place = ? AND bucket = ?`} {
		if _, err := tx.ExecContext(ctx, q, place, bucket); err != nil {
			return err
		}
	}
	if err := db.settleBucket(ctx, tx, bucket); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordListed records a page of place's list of bucket, in one
// transaction: each live key's size and hash at place, and the verdict
// on each key it names, made again with every place's entries. seen is
// how many entries the page carried, rejected ones included, which
// counts toward the list's cap. The list's resume point moves to next,
// or, when last, the list is dropped from relist and every key of the
// bucket is decided again, since a place listed to its end that lacks a
// metadata key makes it a conflict.
func (db *DB) RecordListed(ctx context.Context, place, bucket string, objs []Listed, seen int, next string, last bool) (ListedCount, error) {
	var c ListedCount
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return c, err
	}
	defer tx.Rollback()
	var keys []string
	for _, o := range objs {
		if o.Tombstoned {
			c.Tombstoned++
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO listed (bucket, key, place, size, sha256, class, named) VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (bucket, key, place) DO UPDATE SET size = excluded.size, sha256 = excluded.sha256, class = excluded.class,
				named = excluded.named`,
			bucket, o.Key, place, o.Size, o.SHA256, o.Class, o.HashNamed); err != nil {
			return c, err
		}
		keys = append(keys, o.Key)
		c.Listed++
	}
	if last {
		if _, err := tx.ExecContext(ctx, `DELETE FROM relist WHERE place = ? AND bucket = ?`, place, bucket); err != nil {
			return c, err
		}
		if err := db.settleBucket(ctx, tx, bucket); err != nil {
			return c, err
		}
		return c, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE relist SET after = ?, seen = coalesce(seen, 0) + ?, pages = coalesce(pages, 0) + 1,
		tried_at = NULL, error = NULL, failures = 0
		WHERE place = ? AND bucket = ?`, next, seen, place, bucket); err != nil {
		return c, err
	}
	done, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return c, err
	}
	for _, k := range keys {
		if err := db.settleKey(ctx, tx, bucket, k, done); err != nil {
			return c, err
		}
	}
	return c, tx.Commit()
}

// listedPlaces are the friends whose list of bucket was read to its end:
// none is pending, and none was abandoned.
func listedPlaces(ctx context.Context, tx *sql.Tx, bucket string) (map[string]bool, error) {
	ps, err := column(ctx, tx, `SELECT peer_key FROM friendship
		WHERE peer_key NOT IN (SELECT place FROM relist WHERE bucket = ?)`, bucket)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, p := range ps {
		out[p] = true
	}
	return out, nil
}

func (db *DB) settleBucket(ctx context.Context, tx *sql.Tx, bucket string) error {
	done, err := listedPlaces(ctx, tx, bucket)
	if err != nil {
		return err
	}
	keys, err := column(ctx, tx, `SELECT DISTINCT key FROM listed WHERE bucket = ?
		UNION SELECT key FROM recovered_key WHERE bucket = ?`, bucket, bucket)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := db.settleKey(ctx, tx, bucket, k, done); err != nil {
			return err
		}
	}
	return nil
}

type listedRow struct {
	place, sha, class string
	size              int64
	named             bool
}

// settleKey decides which version of key counts from what every place
// listed, and makes the queue and placement rows say so; done are the
// places listed to their end.
func (db *DB) settleKey(ctx context.Context, tx *sql.Tx, bucket, key string, done map[string]bool) error {
	rows, err := tx.QueryContext(ctx, `SELECT place, sha256, class, size, named FROM listed WHERE bucket = ? AND key = ? ORDER BY place`,
		bucket, key)
	if err != nil {
		return err
	}
	var ls []listedRow
	for rows.Next() {
		var r listedRow
		if err := rows.Scan(&r.place, &r.sha, &r.class, &r.size, &r.named); err != nil {
			rows.Close()
			return err
		}
		ls = append(ls, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
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
			res, err := tx.ExecContext(ctx, `INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
				VALUES (?, ?, 'put', ?, 'done', ?, ?, ?)`, bucket, key, w.class, w.size, winner, db.now().Unix())
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

func ackListed(ctx context.Context, tx *sql.Tx, seq int64, bucket, key string, r listedRow, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
		VALUES (?, ?, ?, ?, 'put', 'acked', ?, ?, ?) ON CONFLICT (place, seq) DO NOTHING`,
		seq, r.place, bucket, key, r.sha, now.Unix(), r.size+ObjectOverhead)
	return err
}

func saveVerdict(ctx context.Context, tx *sql.Tx, bucket, key, verdict, reason, sha string, seq sql.NullInt64, picked sql.NullString) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO recovered_key (bucket, key, verdict, reason, sha256, seq, picked)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket, key) DO UPDATE SET verdict = excluded.verdict, reason = excluded.reason,
			sha256 = excluded.sha256, seq = excluded.seq, picked = excluded.picked`,
		bucket, key, verdict, nullString(reason), nullString(sha), seq, picked)
	return err
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
		return VerdictConflict, fmt.Sprintf("the places that list it disagree, %d to %d", n, n), ""
	}
	if len(count) == 1 {
		return VerdictHeld, "every place that lists it agrees", best
	}
	return VerdictHeld, fmt.Sprintf("%d places agree; the others list another version", n), best
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

// ResolveKey settles a recovered key in conflict with the version place
// listed: the user's pick, which stands until the key is written again.
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
		EXISTS (SELECT 1 FROM recovered_key k WHERE k.bucket = ?1 AND k.key = ?2 AND `+inConflict+`)`,
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
	if err := db.settleKey(ctx, tx, bucket, key, done); err != nil {
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
