package state

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

// This file is overflow targets (S3-1, S3-7, REPL-12, #38): external S3
// buckets in the pool that hold only the data copies no friend has room
// for. A target is a place like a friend (D6), named OverflowPlace(name)
// in placement, place, place_use and every other per-place table, with
// max_bytes as its capacity. Its credential is never here (SEC-3): the
// row names the environment variables that hold it.
//
// Placement (pool.go, replace.go) treats a target as a place in these
// ways only: it is chosen for a data copy only when fewer than `copies`
// friends, reachable or not, have room for it (D6 step 3), by free bytes
// (max_bytes less use, less the bucket's metadata it lacks); a target
// chosen for a bucket's first data copy there is given every live
// metadata object of the bucket in the same transaction, ahead of the
// data in its FIFO; it then receives each later metadata put and every
// delete of a key it holds; it is never a target or a source of
// re-routing; it counts towards `copies`, the pool size and the
// offsite holders like a friend; and its copies move back to friends
// by drain-back (REPL-13, #40: overflowDrain here, replace.go), with
// its metadata once it holds no data of a bucket.

// OverflowPrefix begins every overflow target's place name.
const OverflowPrefix = "overflow:"

// OverflowPlace is the place name of the overflow target name.
func OverflowPlace(name string) string { return OverflowPrefix + name }

// OverflowName is the target's name of place, ok false when place is no
// overflow target's (a friend's node ID).
func OverflowName(place string) (string, bool) {
	return strings.CutPrefix(place, OverflowPrefix)
}

// overflowPlaces is every overflow target's place name, as SQL.
const overflowPlaces = `SELECT '` + OverflowPrefix + `' || name FROM overflow`

// poolPlacesSQL is every place copies may be held at: friends with an
// address and overflow targets, as SQL.
const poolPlacesSQL = `SELECT peer_key FROM friendship WHERE coalesce(address, '') != '' UNION ALL ` + overflowPlaces

// What overflow add found at a target (S3-4, S3-5).
const (
	VersioningEnabled   = "enabled"
	VersioningSuspended = "suspended"
	VersioningOff       = "off"

	LifecyclePresent     = "present"
	LifecycleCreated     = "created"
	LifecycleAbsent      = "absent"
	LifecycleUnsupported = "unsupported"

	VersionRemovalAllowed = "allowed"
	VersionRemovalDenied  = "denied"
	VersionRemovalUnknown = "unknown"
)

// Overflow is an overflow target's row.
type Overflow struct {
	Name     string
	Endpoint string // URL: scheme and host
	Region   string // "" lets the client ask the provider
	Bucket   string
	Prefix   string // keys go under <Prefix>/<sealed name>/
	MaxBytes int64  // the room it adds to the pool (S3-7)
	// EnvPrefix names the credential's environment variables:
	// EnvPrefix + "ACCESS_KEY" and EnvPrefix + "SECRET_KEY" (SEC-3).
	EnvPrefix string
	Retention time.Duration // the window its lifecycle rule keeps versions for (S3-4)
	// PricePerGB is the price per GB (10^9 bytes) and month for the cost
	// estimate (S3-7); 0 when not given.
	PricePerGB     float64
	Versioning     string // Versioning…
	Lifecycle      string // Lifecycle…
	VersionRemoval string // VersionRemoval…
	AddedAt        time.Time
}

// Place is the target's place name.
func (o Overflow) Place() string { return OverflowPlace(o.Name) }

const overflowCols = `name, endpoint, region, bucket, prefix, max_bytes, env_prefix, retention,
	coalesce(price_per_gb, 0), versioning, lifecycle, version_removal, added_at`

func scanOverflow(s interface{ Scan(...any) error }) (Overflow, error) {
	var o Overflow
	var retention, added int64
	err := s.Scan(&o.Name, &o.Endpoint, &o.Region, &o.Bucket, &o.Prefix, &o.MaxBytes, &o.EnvPrefix, &retention,
		&o.PricePerGB, &o.Versioning, &o.Lifecycle, &o.VersionRemoval, &added)
	o.Retention = time.Duration(retention) * time.Second
	o.AddedAt = time.Unix(added, 0)
	return o, err
}

// AddOverflow inserts an overflow target; ErrExists if one has its name.
func (db *DB) AddOverflow(ctx context.Context, o Overflow) error {
	defer func() {
		db.splitMu.Lock()
		db.splitAt = time.Time{} // the pool changed: ask again
		db.splitMu.Unlock()
	}()
	var price any
	if o.PricePerGB > 0 {
		price = o.PricePerGB
	}
	r, err := db.ExecContext(ctx, `INSERT INTO overflow (name, endpoint, region, bucket, prefix, max_bytes, env_prefix,
		retention, price_per_gb, versioning, lifecycle, version_removal, added_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name) DO NOTHING`,
		o.Name, o.Endpoint, o.Region, o.Bucket, o.Prefix, o.MaxBytes, o.EnvPrefix, int64(o.Retention/time.Second),
		price, o.Versioning, o.Lifecycle, o.VersionRemoval, o.AddedAt.Unix())
	if err != nil {
		return err
	}
	if n, err := r.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrExists
	}
	return nil
}

// Overflows lists the overflow targets by name.
func (db *DB) Overflows(ctx context.Context) ([]Overflow, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+overflowCols+` FROM overflow ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Overflow
	for rows.Next() {
		o, err := scanOverflow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// GetOverflow reads one overflow target; ErrNotFound if there is none.
func (db *DB) GetOverflow(ctx context.Context, name string) (Overflow, error) {
	o, err := scanOverflow(db.QueryRowContext(ctx, `SELECT `+overflowCols+` FROM overflow WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return o, ErrNotFound
	}
	return o, err
}

// OverflowHold is what an overflow target holds or has queued (S3-7):
// the live data copies for which it is one of the `copies`, and
// everything it holds or will hold as the place counts it (use: objects
// at their size plus ObjectOverhead, metadata included).
type OverflowHold struct {
	DataObjects, DataBytes int64
	Use                    int64
}

// OverflowHolds returns each overflow target's hold; a target holding
// nothing is present with zeros.
func (db *DB) OverflowHolds(ctx context.Context) (map[string]OverflowHold, error) {
	rows, err := db.QueryContext(ctx, `SELECT o.name, coalesce(u.bytes, 0),
		(SELECT count(*) FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = '`+OverflowPrefix+`' || o.name AND p.state IN ('queued', 'acked') AND p.op = 'put'
			AND q.class = 'data' AND `+liveQ+`),
		(SELECT coalesce(sum(q.size), 0) FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = '`+OverflowPrefix+`' || o.name AND p.state IN ('queued', 'acked') AND p.op = 'put'
			AND q.class = 'data' AND `+liveQ+`)
		FROM overflow o LEFT JOIN place_use u ON u.place = '`+OverflowPrefix+`' || o.name ORDER BY o.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]OverflowHold{}
	for rows.Next() {
		var name string
		var h OverflowHold
		if err := rows.Scan(&name, &h.Use, &h.DataObjects, &h.DataBytes); err != nil {
			return nil, err
		}
		h.Use = max(h.Use, 0)
		out[name] = h
	}
	return out, rows.Err()
}

// splitCacheTTL is how long overflowSplit's answer is kept: it reads the
// whole repository, and a capture asks it each time.
const splitCacheTTL = 30 * time.Second

// FriendsHold reports whether friends with quotas hold copies copies of
// a repository of data bytes plus metadata bytes (DESIGN §4): with D the
// data and m the metadata, held at every friend, Σ min(D, quota − m)
// ≥ copies × D, each term floored at 0. capacity.Plan.Split applies
// the same rule, for status.
func FriendsHold(data, metadata int64, copies int, quotas []int64) bool {
	if data <= 0 {
		return true
	}
	var room float64
	for _, q := range quotas {
		room += min(float64(data), max(float64(q)-float64(metadata), 0))
	}
	return room >= float64(max(copies, 1))*float64(data)
}

// overflowSplit reports whether placement is split (REPL-12 as of
// v0.11.5, #38): there is an overflow target, and the node's friends
// cannot hold `copies` copies of its repository (FriendsHold, the
// capacity plan's rule). Then each data object goes to copies − 1
// friends and one overflow target. The answer is kept splitTTL, and
// only while the pool is as it was (splitKey: friends, their numbers,
// overflow targets, buckets' copies, #38 review), whichever process
// changed it; the repository's growth is what the TTL bounds.
func (db *DB) overflowSplit(ctx context.Context, q querier) (bool, error) {
	ttl := cmp.Or(db.splitTTL, splitCacheTTL)
	var key string
	if err := q.QueryRowContext(ctx, splitKey).Scan(&key); err != nil {
		return false, err
	}
	db.splitMu.Lock()
	defer db.splitMu.Unlock()
	if ttl > 0 && !db.splitAt.IsZero() && time.Since(db.splitAt) < ttl && key == db.splitKey {
		return db.splitVal, nil
	}
	var targets int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM overflow`).Scan(&targets); err != nil {
		return false, err
	}
	split := false
	if targets > 0 {
		u, err := repositoryUse(ctx, q)
		if err != nil {
			return false, err
		}
		ps, err := poolPlaces(ctx, q)
		if err != nil {
			return false, err
		}
		var quotas []int64
		for _, p := range ps {
			if !p.overflow {
				quotas = append(quotas, p.quota)
			}
		}
		split = !FriendsHold(u.Data, u.Metadata, cmp.Or(u.Copies, DefaultCopies), quotas)
		// Every look at the repository is a sample for drain-back's
		// hysteresis (overflowDrain), where q can write: a capture's
		// transaction, or the database.
		if x, ok := q.(execer); ok {
			if err := recordPeak(ctx, x, u, db.now()); err != nil {
				return false, err
			}
		}
	}
	db.splitAt, db.splitVal, db.splitKey = time.Now(), split, key
	return split, nil
}

// DrainWindow is how long drain-back remembers the repository's largest
// size (overflowDrain, #40).
const DrainWindow = 30 * 24 * time.Hour

// overflowDrain reports whether copies at overflow targets may move back
// to friends (REPL-13, §4.11, #40): there is an overflow target, and
// the node's friends hold `copies` copies (FriendsHold, with their
// quotas now) of the largest the repository has been in the last
// DrainWindow, each of its data bytes, metadata bytes and copies taken
// at its largest that day. That is the hysteresis against the split
// (overflowSplit), which turns on as soon as they cannot hold the
// repository as it is now: a prune shrinking the repository, and the
// growth that follows it, never move copies back and forth, since
// drain-back waits until the friends hold what the repository was
// before the prune, or until the window has passed. A friend added or a
// quota raised counts at once (scenario 12). It records the repository
// as it is now first.
func (db *DB) overflowDrain(ctx context.Context) (bool, error) {
	var targets int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM overflow`).Scan(&targets); err != nil || targets == 0 {
		return false, err
	}
	u, err := repositoryUse(ctx, db)
	if err != nil {
		return false, err
	}
	now := db.now()
	if err := recordPeak(ctx, db, u, now); err != nil {
		return false, err
	}
	var peak RepositoryUse
	if err := db.QueryRowContext(ctx, `SELECT coalesce(max(data), 0), coalesce(max(metadata), 0), coalesce(max(copies), 0)
		FROM repository_peak WHERE day > ? AND day <= ?`, peakDay(now.Add(-DrainWindow)), peakDay(now)).Scan(&peak.Data, &peak.Metadata, &peak.Copies); err != nil {
		return false, err
	}
	ps, err := poolPlaces(ctx, db)
	if err != nil {
		return false, err
	}
	var quotas []int64
	for _, p := range ps {
		if !p.overflow {
			quotas = append(quotas, p.quota)
		}
	}
	return FriendsHold(peak.Data, peak.Metadata, cmp.Or(peak.Copies, DefaultCopies), quotas), nil
}

// peakDay is t's day in repository_peak: days since the Unix epoch.
func peakDay(t time.Time) int64 { return t.Unix() / 86400 }

// peakKept is how many of the newest days recordPeak keeps whatever
// the clock says: a clock run forward deletes no history by them.
const peakKept = 31

// recordPeak keeps u as a sample of the repository at now in
// repository_peak (its day's largest of each). A day after now's, left
// by a clock that ran ahead, is folded into now's day, so it ages out
// with the window rather than holding drain-back back until the clock
// reaches it. Days past DrainWindow are deleted, except the newest
// peakKept, so a clock that jumps ahead and back keeps the history.
func recordPeak(ctx context.Context, x execer, u RepositoryUse, now time.Time) error {
	today := peakDay(now)
	if _, err := x.ExecContext(ctx, `INSERT INTO repository_peak (day, data, metadata, copies)
		SELECT ?1, max(data), max(metadata), max(copies) FROM repository_peak WHERE day > ?1 HAVING count(*) > 0
		ON CONFLICT (day) DO UPDATE SET data = max(data, excluded.data), metadata = max(metadata, excluded.metadata),
			copies = max(copies, excluded.copies)`, today); err != nil {
		return err
	}
	if _, err := x.ExecContext(ctx, `DELETE FROM repository_peak WHERE day > ?`, today); err != nil {
		return err
	}
	if _, err := x.ExecContext(ctx, `INSERT INTO repository_peak (day, data, metadata, copies) VALUES (?, ?, ?, ?)
		ON CONFLICT (day) DO UPDATE SET data = max(data, excluded.data), metadata = max(metadata, excluded.metadata),
			copies = max(copies, excluded.copies)`, today, u.Data, u.Metadata, u.Copies); err != nil {
		return err
	}
	_, err := x.ExecContext(ctx, `DELETE FROM repository_peak WHERE day <= ?
		AND day NOT IN (SELECT day FROM repository_peak ORDER BY day DESC LIMIT ?)`, peakDay(now.Add(-DrainWindow)), peakKept)
	return err
}

// splitKey is what overflowSplit's answer depends on besides the
// repository: one cheap read of the friendships, the overflow targets
// and the buckets' copies.
const splitKey = `SELECT coalesce((SELECT group_concat(peer_key || ':' || coalesce(address, '') || ':' ||
		coalesce(own_number, 0) || ':' || coalesce(peer_number, 0), ',') FROM friendship), '') || '|' ||
	coalesce((SELECT group_concat(name || ':' || max_bytes, ',') FROM overflow), '') || '|' ||
	coalesce((SELECT group_concat(name || ':' || copies, ',') FROM buckets), '')`

// overflowHolding lists the overflow targets that hold, have queued or
// are deleting a copy of a live data object of bucket, in place order
// (holdsLiveData).
func overflowHolding(ctx context.Context, q querier, bucket string) ([]string, error) {
	return column(ctx, q, `SELECT p FROM (SELECT '`+OverflowPrefix+`' || name AS p FROM overflow)
		WHERE `+holdsLiveData("p", "?1")+` ORDER BY p`, bucket)
}

// holdsLiveData is the SQL condition that the overflow target place
// holds, has queued or is deleting a copy of a live data object (the
// key's latest done write) of bucket: then the bucket's metadata goes
// there (overflowHolding, REPL-12), and otherwise it is dropped there
// (dropIdleMetadata, #40). One condition for both, so the two never
// place and drop the same metadata in turn (#40 review). place and
// bucket are SQL expressions.
func holdsLiveData(place, bucket string) string {
	return `EXISTS (SELECT 1 FROM placement h JOIN queue hq ON hq.seq = h.seq
		WHERE h.place = ` + place + ` AND h.bucket = ` + bucket + ` AND h.op = 'put' AND hq.class = 'data'
		AND h.state IN ('queued', 'acked', 'dropping')
		AND NOT EXISTS (SELECT 1 FROM queue l WHERE l.bucket = hq.bucket AND l.key = hq.key AND l.seq > hq.seq AND l.state = 'done'))`
}

// placeMetadata puts every live metadata object of bucket that place
// lacks (unsentMetadata's rows) in place's FIFO, at its own sequence, so
// an overflow target given its first data copy of the bucket gets the
// bucket's metadata ahead of it (REPL-12, S3-2). It returns how many
// rows it placed.
func placeMetadata(ctx context.Context, tx interface {
	querier
	execer
}, bucket, place string, now int64) (int64, error) {
	seqs, err := column(ctx, tx, `SELECT q.seq FROM queue q
		WHERE q.bucket = ?1 AND q.class IN ('metadata', 'snapshot') AND q.op = 'put' AND q.state = 'done'
		AND `+liveQ+` AND q.immutable IS NULL
		AND NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = ?2 AND p.seq = q.seq
			AND p.state IN ('queued', 'acked', 'refused', 'dropping'))
		AND NOT EXISTS (SELECT 1 FROM placement r WHERE r.seq = q.seq AND r.state = 'refused'
			AND r.cause IN ('store', 'oversize'))
		ORDER BY q.seq`, bucket, place)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, s := range seqs {
		seq, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return n, err
		}
		// A row of it there that no longer holds (dropped, abandoned,
		// drained) is queued again; otherwise a new row.
		r, err := tx.ExecContext(ctx, `UPDATE placement SET state = 'queued', sha256 = NULL, acked_at = NULL,
			detail = NULL, cause = NULL, held = NULL,
			size = (SELECT coalesce(q.size, 0) + ?3 FROM queue q WHERE q.seq = placement.seq)
			WHERE place = ?1 AND seq = ?2 AND state IN ('dropped', 'abandoned', 'drained')`, place, seq, ObjectOverhead)
		if err != nil {
			return n, err
		}
		if k, err := r.RowsAffected(); err != nil {
			return n, err
		} else if k > 0 {
			n++
			continue
		}
		if err := insertPlacement(ctx, tx, seq, place, false, now); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
