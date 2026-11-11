package state

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
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
	var copies int
	if err := view.QueryRowContext(ctx, `SELECT coalesce((SELECT copies FROM buckets WHERE name = ?), ?)`,
		bucket, DefaultCopies).Scan(&copies); err != nil {
		return false, "", err
	}
	holders, err := view.holders(ctx)
	if err != nil {
		return false, "", err
	}
	got, err := view.Offsite(ctx, bucket, 0)
	if err != nil {
		return stored, "", err
	}
	if _, stored, err = view.storedShorts(ctx, bucket, copies, holders, got.Latest); err != nil {
		return false, "", err
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
	for seed := uint64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			storedWatermarkRun(t, seed, 600)
		})
	}
}

func storedWatermarkRun(t *testing.T, seed uint64, steps int) {
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
	usedStored := 0
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
	}
	if usedStored < steps/2 {
		t.Fatalf("the stored rows were read in %d of %d reads: the test compares the full computation with itself", usedStored, 2*steps)
	}
	t.Logf("%d of %d reads used the stored rows", usedStored, 2*steps)
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
