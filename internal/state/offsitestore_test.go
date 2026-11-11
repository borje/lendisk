package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sameOffsite compares bucket's Offsite, from what the fold stored and
// the keys marked since, with the full computation, at the latest write
// and at target. stored reports whether the read used the stored rows
// (the pool and copies are the basis's); diff is "" when they agree.
func sameOffsite(db *DB, bucket string, target int64) (stored bool, diff string, err error) {
	ctx := context.Background()
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		return false, "", err
	}
	defer done()
	if stored, err = basisMatches(view, bucket); err != nil {
		return false, "", err
	}
	got, err := view.Offsite(ctx, bucket, 0)
	if err != nil {
		return stored, "", err
	}
	want, err := view.offsiteFull(ctx, bucket, 0)
	if err != nil {
		return stored, "", err
	}
	// For recounts from the same rows (TestForRecountsLikeAFreshRead).
	for _, at := range []int64{0, target} {
		if got, want := got.For(at), want.For(at); !reflect.DeepEqual(got, want) {
			return stored, fmt.Sprintf("bucket %s at %d: stored %+v, computed %+v", bucket, at, got, want), nil
		}
	}
	return stored, "", nil
}

// storedNow reports whether a read of bucket now would use the stored
// rows: the basis is the pool's and copies as they are.
func storedNow(db *DB, bucket string) (bool, error) {
	view, done, err := db.Snapshot(context.Background())
	if err != nil {
		return false, err
	}
	defer done()
	return basisMatches(view, bucket)
}

// basisMatches reports whether bucket's basis is the pool and copies as
// view sees them.
func basisMatches(view *DB, bucket string) (bool, error) {
	ctx := context.Background()
	var copies int
	if err := view.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
		bucket, DefaultCopies).Scan(&copies); err != nil {
		return false, err
	}
	holders, err := view.holders(ctx)
	if err != nil {
		return false, err
	}
	var bh string
	var bc int
	err = view.QueryRowContext(ctx, `SELECT holders, copies FROM offsite_basis WHERE bucket = ?`, bucket).Scan(&bh, &bc)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil && bh == holders && bc == copies, err
}

func (p *pool) fold() {
	p.t.Helper()
	if err := p.db.FoldOffsite(context.Background()); err != nil {
		p.t.Fatal(err)
	}
}

// REPL-11 (#81): the stored watermark, what the daemon's fold stored
// with the keys changed since computed afresh, equals the one computed
// from every row, after every step of random puts, deletes, rewrites,
// acknowledgements (in any order, of old versions too), refusals,
// re-routes, pending multipart rows settling or marked unmatchable,
// friends added, removed and gone, copies changed, and folds at random
// points. The removal model (TestRemovalModel) checks the same after
// each of its steps, through the rest of the real writers.
func TestStoredWatermarkEqualsTheComputedOne(t *testing.T) {
	var withDropped atomic.Int64
	t.Cleanup(func() {
		if withDropped.Load() == 0 && !t.Failed() {
			t.Error("no step had a copy dropped below copies: the test does not compare DroppedBelowCopies (#118)")
		}
	})
	for seed := uint64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			withDropped.Add(int64(storedWatermarkRun(t, seed, 400)))
		})
	}
}

// storedWatermarkRun runs seed's steps and returns how many of them
// left copies dropped below copies.
func storedWatermarkRun(t *testing.T, seed uint64, steps int) (withDropped int) {
	p := newPool(t)
	p.db.ReplaceAfter = 7 * 24 * time.Hour
	ctx := context.Background()
	r := rand.New(rand.NewPCG(seed, 81))
	buckets := []string{"laptop", "photos"}
	p.bucket("laptop", 2)
	p.bucket("photos", 1)
	friends := []string{}
	addFriend := func() {
		f := fmt.Sprintf("F%d", len(friends))
		p.friend(f, 1000)
		friends = append(friends, f)
	}
	for range 3 {
		addFriend()
	}
	var log []string
	did := func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) }
	key := func() string { return fmt.Sprintf("k%d", r.IntN(12)) }
	bucket := func() string { return buckets[r.IntN(len(buckets))] }
	class := func() string { return []string{"data", "data", "metadata", "snapshot"}[r.IntN(4)] }
	// rows lists rows of placement in a state, for a random pick.
	rows := func(state string) [][2]any {
		rs, err := p.db.Query(`SELECT place, seq FROM placement WHERE state = ?`, state)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out [][2]any
		for rs.Next() {
			var place string
			var seq int64
			if err := rs.Scan(&place, &seq); err != nil {
				t.Fatal(err)
			}
			out = append(out, [2]any{place, seq})
		}
		return out
	}
	pick := func(state string) (string, int64, bool) {
		rs := rows(state)
		if len(rs) == 0 {
			return "", 0, false
		}
		x := rs[r.IntN(len(rs))]
		return x[0].(string), x[1].(int64), true
	}
	var pending []int64
	var stale *fold
	var staleBucket string
	usedStored, usedStoredBelow := 0, 0
	// Between a fold's claim and its batches, the read is still the
	// computed one.
	p.db.betweenFoldBatches = func() {
		for _, b := range buckets {
			if _, diff, err := sameOffsite(p.db, b, 0); err != nil || diff != "" {
				t.Errorf("mid-fold: %s %v", diff, err)
			}
		}
		if _, diff, err := sameStatusReads(p.db, p.now); err != nil || diff != "" {
			t.Errorf("mid-fold: %s %v", diff, err)
		}
	}
	for step := 1; step <= steps; step++ {
		switch n := r.IntN(100); {
		case n < 18:
			b, k := bucket(), key()
			did("put %s/%s: %d", b, k, p.put(b, k, class(), 0.01))
		case n < 24:
			b, k := bucket(), key()
			did("rewrite %s/%s: %d", b, k, p.rewrite(b, k, class(), 0.01))
		case n < 30:
			b, k := bucket(), key()
			did("delete %s/%s: %d", b, k, p.del(b, k, class()))
		case n < 34:
			// A completed multipart upload, answered before its hash.
			b, k := bucket(), key()
			seq := p.pending(b, k, "data")
			pending = append(pending, seq)
			did("pending %s/%s: %d", b, k, seq)
		case n < 38 && len(pending) > 0:
			i := r.IntN(len(pending))
			seq := pending[i]
			pending = append(pending[:i], pending[i+1:]...)
			if r.IntN(4) == 0 {
				if err := p.db.MarkUnmatchable(ctx, seq, "lost"); err != nil {
					t.Fatal(err)
				}
				pending = append(pending, seq) // still pending
				did("unmatchable %d", seq)
				break
			}
			if _, err := p.db.FinishPut(ctx, seq, 1000, fmt.Sprintf("hp%d", seq)); err != nil {
				t.Fatal(err)
			}
			did("settle %d", seq)
		case n < 62:
			if place, seq, ok := pick(PlacementQueued); ok {
				p.ackRow(place, seq)
				did("ack %s %d", place, seq)
			}
		case n < 66:
			if place, seq, ok := pick(PlacementQueued); ok {
				if err := p.db.RefusePlacement(ctx, place, seq, "BadDigest"); err != nil {
					t.Fatal(err)
				}
				did("refuse %s %d", place, seq)
			}
		case n < 68:
			if place, seq, ok := pick(PlacementQueued); ok {
				if _, err := p.db.StartPlacement(ctx, place, seq, p.now); err != nil {
					t.Fatal(err)
				}
				did("start %s %d", place, seq)
			}
		case n < 70:
			if place, seq, ok := pick(PlacementQueued); ok {
				if err := p.db.CancelPlacement(ctx, place, seq); err != nil {
					t.Fatal(err)
				}
				did("cancel %s %d", place, seq)
			}
		case n < 74:
			moved, dropped, err := p.db.Reroute(ctx, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			did("reroute: %d moved, %d dropped", moved, dropped)
		case n < 77:
			st, err := p.db.Replace(ctx, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			did("replace: %+v", st)
		case n < 80:
			// A friend fails from now on: past reroute_after its queue
			// moves, past replace_after it is gone and holds nothing.
			f := friends[r.IntN(len(friends))]
			if err := p.db.SetPlaceCondition(ctx, f, ConditionRetrying, "down", p.now); err != nil {
				t.Fatal(err)
			}
			did("%s fails", f)
		case n < 82:
			f := friends[r.IntN(len(friends))]
			if err := p.db.ProbeAnswered(ctx, f, p.now); err != nil {
				t.Fatal(err)
			}
			did("%s answers", f)
		case n < 86:
			d := time.Duration(1+r.IntN(4*24)) * time.Hour
			p.now = p.now.Add(d)
			did("time +%s", d)
		case n < 87:
			addFriend()
			did("friend %s added", friends[len(friends)-1])
		case n < 88:
			f := friends[r.IntN(len(friends))]
			if err := p.db.SetPlaceCondition(ctx, f, ConditionRemoved, "refuses", p.now); err != nil {
				t.Fatal(err)
			}
			did("%s refuses this node", f)
		case n < 89:
			b := bucket()
			c := 1 + r.IntN(3)
			if err := p.db.SetCopies(ctx, b, c); err != nil {
				t.Fatal(err)
			}
			did("%s copies %d", b, c)
		case n < 95:
			p.fold()
			did("fold")
		default:
			// A fold whose read and write other steps come between.
			if stale == nil {
				b := bucket()
				f, err := p.db.readFold(ctx, b)
				if err != nil {
					t.Fatal(err)
				}
				stale, staleBucket = &f, b
				did("fold %s read", b)
				break
			}
			if err := p.db.writeFold(ctx, staleBucket, *stale); err != nil {
				t.Fatal(err)
			}
			stale = nil
			did("fold %s written", staleBucket)
		}
		for _, b := range buckets {
			var latest int64
			if err := p.db.QueryRow(`SELECT coalesce(max(seq), 0) FROM queue WHERE bucket = ?`, b).Scan(&latest); err != nil {
				t.Fatal(err)
			}
			stored, diff, err := sameOffsite(p.db, b, r.Int64N(latest+1))
			if err != nil {
				t.Fatal(err)
			}
			if stored {
				usedStored++
			}
			if diff != "" {
				from := max(0, len(log)-30)
				t.Fatalf("seed %d step %d: %s\nlast steps:\n%v", seed, step, diff, log[from:])
			}
		}
		// Status's other reads (#116), below copies among them from the
		// rows the same folds stored.
		n, diff, err := sameStatusReads(p.db, p.now)
		if err != nil {
			t.Fatal(err)
		}
		usedStoredBelow += n
		if diff != "" {
			from := max(0, len(log)-30)
			t.Fatalf("seed %d step %d: %s\nlast steps:\n%v", seed, step, diff, log[from:])
		}
		if d, err := p.db.DroppedBelowCopies(ctx); err != nil {
			t.Fatal(err)
		} else if len(d) > 0 {
			withDropped++
		}
	}
	if usedStored < steps/2 || usedStoredBelow < steps/2 {
		t.Fatalf("the stored rows were read in %d and %d of %d reads: the test compares the full computation with itself",
			usedStored, usedStoredBelow, 2*steps)
	}
	t.Logf("%d of %d reads used the stored rows, %d the stored below rows; %d steps had copies dropped below copies",
		usedStored, 2*steps, usedStoredBelow, withDropped)
	return withDropped
}

// REPL-11 (#81): a fold that read before a key changed clears only the
// marks it read: the change keeps its mark, so the next read computes
// that key afresh rather than trusting what the fold stored.
func TestAFoldKeepsTheMarksOfChangesAfterItsRead(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	for _, f := range []string{"B", "C"} {
		p.friend(f, 100)
	}
	d := p.put("laptop", "data/1", "data", 1)
	p.fold()
	p.ackRow("B", d)
	f, err := p.db.readFold(ctx, "laptop")
	if err != nil {
		t.Fatal(err)
	}
	if len(f.rows) != 1 || f.rows[0].seq != d {
		t.Fatalf("the fold read %+v, want data/1 short", f.rows)
	}
	// Between the fold's read and its write, the second place
	// acknowledges: what the fold read is stale.
	p.ackRow("C", d)
	if err := p.db.writeFold(ctx, "laptop", f); err != nil {
		t.Fatal(err)
	}
	if w := p.watermark("laptop"); w != d {
		t.Fatalf("data/1 at both places after a fold that read it at one: watermark %d, want %d", w, d)
	}
	if stored, diff, err := sameOffsite(p.db, "laptop", 0); err != nil || diff != "" || !stored {
		t.Fatalf("stored %v: %s %v", stored, diff, err)
	}
	p.fold()
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM offsite_dirty`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("after the next fold, %d marks (%v)", n, err)
	}
	if w := p.watermark("laptop"); w != d {
		t.Fatalf("after the next fold: watermark %d, want %d", w, d)
	}
}

// REPL-11 (#81): a full fold (the first after an upgrade, or after the
// pool or copies changed) stores a row per short put, and every
// deleted or rewritten object stays short: it writes in batches of
// SettleBudget, so a writer beside it waits about one batch (three
// budgets at most; 0.93 s for 200,000 objects in one transaction),
// never the whole bucket; and between batches the watermark read is still the
// computed one, never more.
func TestAFullFoldKeepsWritersMoving(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	p := newPool(t)
	ctx := context.Background()
	const objects = settleKeys / 2
	p.largeBucket(objects)
	// At copies 3 every data object, held at two places, is short. The
	// random test (copies changed) covers a full fold over stored rows.
	if err := p.db.SetCopies(ctx, "laptop", 3); err != nil {
		t.Fatal(err)
	}
	batches := 0
	var longest time.Duration
	p.db.foldTook = func(d time.Duration) { longest = max(longest, d) }
	p.db.betweenFoldBatches = func() {
		batches++
		// Mid-fold, no read trusts the half-written rows: the basis
		// matches no pool, so it does the full read.
		if stored, err := storedNow(p.db, "laptop"); err != nil || stored {
			t.Errorf("between batches %d, a read used the stored rows (%v)", batches, err)
		}
	}
	var worst atomic.Int64
	var writeErr error
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			start := time.Now()
			if err := p.db.SetNode(ctx, "probe", fmt.Sprint(i)); err != nil {
				writeErr = err
				return
			}
			if d := int64(time.Since(start)); d > worst.Load() {
				worst.Store(d)
			}
		}
	}()
	start := time.Now()
	p.fold()
	took := time.Since(start)
	close(stop)
	wg.Wait()
	if writeErr != nil {
		t.Fatalf("a write beside the fold failed: %v", writeErr)
	}
	t.Logf("a full fold of %d objects took %v in %d batches, the longest %v; the longest write beside it waited %v",
		objects, took, batches, longest, time.Duration(worst.Load()))
	// A batch ends after the row that passes the budget.
	if longest > 500*time.Millisecond {
		t.Fatalf("a fold's write transaction held the lock %v, over twice SettleBudget (0.25 s)", longest)
	}
	if w := time.Duration(worst.Load()); w > 750*time.Millisecond {
		t.Fatalf("a write waited %v while the fold took %v", w, took)
	}
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM offsite_short WHERE bucket = 'laptop'`).Scan(&n); err != nil || n < objects {
		t.Fatalf("%d rows stored (%v), want every data object", n, err)
	}
	if stored, diff, err := sameOffsite(p.db, "laptop", 0); err != nil || diff != "" || !stored {
		t.Fatalf("after the fold: stored %v, %s %v", stored, diff, err)
	}
}

// REPL-11 (#81): with the stored rows folded, a friend gone past
// replace_after with nothing written (only the clock moved) no longer
// holds copies: the pool differs from the basis, so the read computes
// every put rather than trusting rows stored for the pool before.
func TestAFriendGoneByTheClockAloneLowersTheStoredWatermark(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.db.ReplaceAfter = 7 * 24 * time.Hour
	p.bucket("laptop", 2)
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 100)
	}
	d := p.put("laptop", "data/1", "data", 1)
	p.ackAll(d)
	p.fold()
	if w := p.watermark("laptop"); w != d {
		t.Fatalf("watermark %d, want %d", w, d)
	}
	gone := p.places(d)[0]
	if err := p.db.SetPlaceCondition(ctx, gone, ConditionRetrying, "down", p.now); err != nil {
		t.Fatal(err)
	}
	p.fold()
	p.now = p.now.Add(8 * 24 * time.Hour)
	if w := p.watermark("laptop"); w != d-1 {
		t.Fatalf("%s gone past replace_after: watermark %d, want %d", gone, w, d-1)
	}
	if _, diff, err := sameOffsite(p.db, "laptop", 0); err != nil || diff != "" {
		t.Fatalf("%s %v", diff, err)
	}
}

// REPL-11 (#81): on a bucket of a million objects, once folded, the
// watermark is read in well under a second (the full read it replaces
// takes some 8 s on the development machine), and equals the full
// read. Seeding a million rows takes a while and some 300 MB of disk,
// so the test runs only with LENDISK_BIG_OFFSITE set to the number of
// objects:
//
//	LENDISK_BIG_OFFSITE=1000000 go test -run TestStoredOffsiteOfAMillionObjectBucketIsFast -v ./internal/state
func TestStoredOffsiteOfAMillionObjectBucketIsFast(t *testing.T) {
	objects, _ := strconv.Atoi(os.Getenv("LENDISK_BIG_OFFSITE"))
	if objects <= 0 {
		t.Skip("LENDISK_BIG_OFFSITE is not set")
	}
	p := newPool(t)
	ctx := context.Background()
	start := time.Now()
	p.ackAll(p.largeBucket(objects))
	t.Logf("seeded %d objects in %v", objects, time.Since(start))
	start = time.Now()
	full, err := p.db.offsiteFull(ctx, "laptop", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the full read took %v", time.Since(start))
	start = time.Now()
	p.fold()
	t.Logf("the first fold, of every key, took %v", time.Since(start))
	took := time.Duration(1 << 62)
	var o Offsite
	for range 3 {
		start = time.Now()
		o = p.offsite("laptop", 0)
		took = min(took, time.Since(start))
	}
	t.Logf("the read from the stored rows took %v", took)
	if !reflect.DeepEqual(o, full) || !o.Complete() {
		t.Fatalf("stored %+v, full %+v", o, full)
	}
	if took > 250*time.Millisecond {
		t.Fatalf("the read from the stored rows took %v, want well under a second", took)
	}
}

// REPL-11 (#81 review): the offsite read of a quiet bucket, written
// before a busy one's many rows, finds its latest, next and last fully
// placed writes by its own rows (queue_bucket_seq), not by walking the
// busy bucket's: it takes less than one plain scan of the queue.
func TestAQuietBucketsOffsiteDoesNotWalkABusyOnesRows(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 100)
	p.friend("C", 100)
	p.bucket("quiet", 2)
	p.ackAll(p.put("quiet", "data/1", "data", 1))
	p.bucket("busy", 2)
	if _, err := p.db.ExecContext(ctx, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
		INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
		SELECT 'busy', 'data/' || i, 'put', 'data', 'done', 1000, 'h' || i, 0 FROM n`, settleKeys/4); err != nil {
		t.Fatal(err)
	}
	p.fold()
	took, yard := time.Duration(1<<62), time.Duration(1<<62)
	for range 3 {
		start := time.Now()
		var n int
		if err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM queue WHERE +state = 'done'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		yard = min(yard, time.Since(start))
		start = time.Now()
		o := p.offsite("quiet", 0)
		took = min(took, time.Since(start))
		if !o.Complete() || o.Through.Seq != o.Latest {
			t.Fatalf("the quiet bucket: %+v", o)
		}
	}
	t.Logf("the quiet bucket's offsite read took %v; a scan of the queue's %d rows %v", took, settleKeys/4+1, yard)
	if took > yard {
		t.Fatalf("the quiet bucket's offsite read took %v, more than a scan of the queue (%v)", took, yard)
	}
}

// REPL-11 (#81 review): a bucket whose fold fails does not keep the
// buckets after it from being folded; the error names it.
func TestAFailingBucketDoesNotStopTheFoldOfTheOthers(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 100)
	p.friend("C", 100)
	p.bucket("a", 2)
	p.bucket("b", 2)
	p.fold()
	d := p.put("a", "data/1", "data", 1)
	p.put("b", "data/1", "data", 1)
	// A stored row under another key holds data/1's sequence: storing
	// data/1's row fails.
	mustExec(t, p.db, `INSERT INTO offsite_short (bucket, key, seq, size, next, stuck, refused, dropped, gone, immutable)
		VALUES ('a', 'other', ?, 0, 0, 0, 0, 0, 0, 0)`, d)
	err := p.db.FoldOffsite(ctx)
	if err == nil || !strings.Contains(err.Error(), "bucket a") {
		t.Fatalf("fold: %v, want bucket a's failure", err)
	}
	var marks int
	if err := p.db.QueryRow(`SELECT count(*) FROM offsite_dirty WHERE bucket = 'b'`).Scan(&marks); err != nil || marks != 0 {
		t.Fatalf("bucket b: %d marks left (%v)", marks, err)
	}
}

// REPL-11 (#81): a change marks its key with a newer mark whatever
// conflict policy the writing statement names: an upsert and OR IGNORE
// and OR REPLACE writes of a key already marked succeed and renew the
// mark, so a fold that read before them cannot clear it.
func TestAMarkIsRenewedWhateverTheWritersConflictPolicy(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	d := p.put("laptop", "data/1", "data", 1)
	mark := func() int64 {
		t.Helper()
		var id int64
		if err := p.db.QueryRow(`SELECT id FROM offsite_dirty WHERE bucket = 'laptop' AND key = 'data/1'`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	for _, w := range []struct{ name, sql string }{
		{"an OR IGNORE update", `UPDATE OR IGNORE placement SET started_at = 1 WHERE seq = ?`},
		{"an upsert", `INSERT INTO queue (seq, bucket, key, op, class, state, created_at)
			VALUES (?, 'laptop', 'data/1', 'put', 'data', 'done', 0) ON CONFLICT (seq) DO UPDATE SET local = 2`},
		{"an OR IGNORE insert", `INSERT OR IGNORE INTO placement (seq, place, bucket, key, op, state)
			VALUES (?, 'D', 'laptop', 'data/1', 'put', 'queued')`},
		{"an OR REPLACE update", `UPDATE OR REPLACE queue SET local = 1 WHERE seq = ?`},
	} {
		before := mark()
		if _, err := p.db.Exec(w.sql, d); err != nil {
			t.Fatalf("%s of a marked key: %v", w.name, err)
		}
		if after := mark(); after <= before {
			t.Fatalf("%s left the mark at %d (was %d)", w.name, after, before)
		}
	}
	// A row moved to another key marks both.
	before := mark()
	mustExec(t, p.db, `UPDATE queue SET key = 'data/2' WHERE seq = ?`, d)
	var renewed, moved int
	if err := p.db.QueryRow(`SELECT count(*) FILTER (WHERE key = 'data/1' AND id > ?), count(*) FILTER (WHERE key = 'data/2')
		FROM offsite_dirty WHERE bucket = 'laptop'`, before).Scan(&renewed, &moved); err != nil || renewed != 1 || moved != 1 {
		t.Fatalf("a row moved from data/1 to data/2: old key renewed %d, new key marked %d (%v)", renewed, moved, err)
	}
}
