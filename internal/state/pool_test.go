package state

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
)

// unit scales the specification's gigabytes down: one unit counts as
// exactly unit bytes at a place, its object overhead included.
const unit = int64(1 << 20)

// pool is a node's lendisk.db with friends of given quotas, a clock, and
// helpers to capture objects of a class and size.
type pool struct {
	t   *testing.T
	db  *DB
	now time.Time
	n   int
}

func newPool(t *testing.T) *pool {
	t.Helper()
	p := &pool{t: t, db: openTest(t), now: time.Unix(1_800_000_000, 0)}
	p.db.Clock = func() time.Time { return p.now }
	return p
}

// friend adds a friend with quota units of room each way.
func (p *pool) friend(peer string, quota float64) {
	p.t.Helper()
	n := int64(quota * float64(unit))
	if err := p.db.AddFriendship(context.Background(), Friendship{Peer: peer, Address: "tc" + peer,
		OwnNumber: n, PeerNumber: n, CreatedAt: p.now}); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pool) bucket(name string, copies int) {
	p.t.Helper()
	if err := p.db.AddBucket(context.Background(), Bucket{Name: name, Profile: "restic", Copies: copies,
		AccessKey: "AK" + name, SecretKey: "s", CreatedAt: p.now}); err != nil {
		p.t.Fatal(err)
	}
}

// put captures a completed put of units (counted at a place with its
// overhead) and returns its sequence.
func (p *pool) put(bucket, key, class string, units float64) int64 {
	p.t.Helper()
	ctx := context.Background()
	seqs, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: bucket, Key: key, Op: OpPut, Class: class}})
	if err != nil {
		p.t.Fatal(err)
	}
	size := int64(units*float64(unit)) - ObjectOverhead
	if _, err := p.db.FinishPut(ctx, seqs[0], size, "h-"+key); err != nil {
		p.t.Fatal(err)
	}
	return seqs[0]
}

func (p *pool) del(bucket, key, class string) int64 {
	p.t.Helper()
	ctx := context.Background()
	seqs, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: bucket, Key: key, Op: OpDelete, Class: class}})
	if err != nil {
		p.t.Fatal(err)
	}
	if err := p.db.FinishDelete(ctx, seqs...); err != nil {
		p.t.Fatal(err)
	}
	return seqs[0]
}

// places lists where row seq is placed, in states queued or acked.
func (p *pool) places(seq int64) []string {
	p.t.Helper()
	rows, err := p.db.Query(`SELECT place FROM placement WHERE seq = ? AND state IN ('queued', 'acked') ORDER BY place`, seq)
	if err != nil {
		p.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}

func (p *pool) wantPlaces(what string, seq int64, want ...string) {
	p.t.Helper()
	if got := p.places(seq); !slices.Equal(got, want) {
		p.t.Fatalf("%s placed at %v, want %v", what, got, want)
	}
}

// condition sets place's last answer: cond since since, and its last
// acknowledgement at lastOK (none if zero).
func (p *pool) condition(place, cond string, since, lastOK time.Time) {
	p.t.Helper()
	var ok any
	if !lastOK.IsZero() {
		ok = lastOK.Unix()
	}
	if _, err := p.db.Exec(`INSERT OR REPLACE INTO place (place, condition, detail, since, last_ok) VALUES (?, ?, 'x', ?, ?)`,
		place, cond, since.Unix(), ok); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pool) ack(place string, seq int64) {
	p.t.Helper()
	if err := p.db.AckPlacement(context.Background(), place, seq, "h", p.now); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pool) held(seq int64) bool {
	p.t.Helper()
	h, err := p.db.Held(context.Background(), seq)
	if err != nil {
		p.t.Fatal(err)
	}
	return h
}

// REPL-12, D6 (#19): placement is decided in the transaction that
// completes the capture. A data object goes to `copies` distinct
// friends by largest free fraction; a metadata object to every friend.
func TestDataGoesToCopiesFriendsByLargestFreeFraction(t *testing.T) {
	p := newPool(t)
	p.friend("B", 100)
	p.friend("C", 50)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	// All empty: equal fractions, ties broken by node ID.
	d1 := p.put("laptop", "data/1", "data", 5)
	p.wantPlaces("data/1", d1, "B", "C")
	// B 95%, C 90%, D 100% free.
	d2 := p.put("laptop", "data/2", "data", 5)
	p.wantPlaces("data/2", d2, "B", "D")
	// B 90%, C 90%, D 50%.
	d3 := p.put("laptop", "data/3", "data", 1)
	p.wantPlaces("data/3", d3, "B", "C")
	m := p.put("laptop", "index/1", "metadata", 1)
	p.wantPlaces("index/1", m, "B", "C", "D")
	s := p.put("laptop", "snapshots/1", "snapshot", 1)
	p.wantPlaces("snapshots/1", s, "B", "C", "D")
	// A bucket with no row (a test's, or one removed) has the default.
	x := p.put("other", "data/x", "data", 1)
	if got := p.places(x); len(got) != DefaultCopies {
		t.Fatalf("default copies: placed at %v", got)
	}
}

// D6 step 1, LEND-2 (#19): free capacity is the quota less what is
// stored (or queued) there, less the bucket's metadata not yet sent
// there; a place whose last answer was 507 counts as full.
func TestFreeCapacityCountsStoredAndUnsentMetadata(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 1)
	d1 := p.put("laptop", "data/1", "data", 3)
	p.wantPlaces("data/1", d1, "B")
	// What is stored at B counts: C now has the larger free fraction.
	d2 := p.put("laptop", "data/2", "data", 1)
	p.wantPlaces("data/2", d2, "C")
	// B 7/10, C 9/10. Metadata goes everywhere and counts: B 5, C 7.
	p.put("laptop", "index/1", "metadata", 2)
	// D joins with 6: it has none of the bucket's 2 units of metadata
	// yet (re-placement sends them, #26), so 4/6 free, not 6/6, which
	// is less than C's 7/10.
	p.friend("D", 6)
	d3 := p.put("laptop", "data/3", "data", 1)
	p.wantPlaces("data/3", d3, "C")
	// Free: B 5/10, C 6/10, D 4/6. Once C and D answered 507 (the
	// borrower's count was short), they are full.
	p.condition("C", ConditionQuota, p.now, time.Time{})
	p.condition("D", ConditionQuota, p.now, time.Time{})
	d4 := p.put("laptop", "data/4", "data", 1)
	p.wantPlaces("data/4", d4, "B")
	// A deletion frees what it held.
	p.condition("C", ConditionOK, p.now, time.Time{})
	p.condition("D", ConditionOK, p.now, time.Time{})
	p.ack("B", d1)
	p.del("laptop", "data/1", "data")
	use, err := p.db.PlaceUse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// B: data/1, index/1 and data/4, less data/1; C: data/2, index/1,
	// data/3; D: nothing yet (its metadata is not placed).
	if use["B"] != 3*unit || use["C"] != 4*unit || use["D"] != 0 {
		t.Fatalf("use after the delete %v (units of %d)", use, unit)
	}
}

// REPL-12 (#19): reachable friends first; an unreachable friend with
// room is still chosen when the reachable ones are not enough, until it
// has been unreachable past replace_after.
func TestUnreachableFriendChosenOnlyWhenNeededUntilReplaceAfter(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 100) // the most room by far
	p.bucket("two", 2)
	p.bucket("three", 3)
	d := p.put("three", "data/0", "data", 3) // B and C 70% free, D 97%
	p.wantPlaces("data/0", d, "B", "C", "D")
	// D failing for ten minutes is still reachable: within reroute_after.
	p.condition("D", ConditionRetrying, p.now.Add(-10*time.Minute), p.now.Add(-time.Hour))
	d = p.put("two", "data/a", "data", 1)
	p.wantPlaces("data/a", d, "B", "D")
	// Failing for two hours it is not: B and C first.
	p.condition("D", ConditionRetrying, p.now.Add(-2*time.Hour), time.Time{})
	d = p.put("two", "data/b", "data", 1)
	p.wantPlaces("data/b", d, "B", "C")
	// Three copies need D: its copy waits for it.
	d = p.put("three", "data/c", "data", 1)
	p.wantPlaces("data/c", d, "B", "C", "D")
	// Unreachable past replace_after (7 days from its last answer): no
	// longer room. What fits is placed.
	p.now = p.now.Add(7 * 24 * time.Hour)
	d = p.put("three", "data/d", "data", 1)
	p.wantPlaces("data/d", d, "B", "C")
	// It answers again: it is room again.
	p.condition("D", ConditionOK, p.now, time.Time{})
	d = p.put("three", "data/e", "data", 1)
	p.wantPlaces("data/e", d, "B", "C", "D")
	// A friend that refuses this node (removed) is never chosen.
	p.condition("D", ConditionRemoved, p.now, time.Time{})
	d = p.put("three", "data/f", "data", 1)
	p.wantPlaces("data/f", d, "B", "C")
}

// REPL-1, §3.4 (#19): unsent data rows at a friend unreachable longer
// than reroute_after move to another friend with room; with none they
// wait; past replace_after they are dropped as a deficit, so they never
// hold the metadata barrier for good.
func TestRerouteMovesUnsentDataFromAnUnreachableFriend(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 2)
	p.bucket("laptop", 2)
	p.put("laptop", "index/0", "metadata", 0.5) // everywhere: D is 75% free
	d1 := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d1, "B", "C")
	d2 := p.put("laptop", "data/2", "data", 1)
	p.wantPlaces("data/2", d2, "B", "C")             // B and C 85%
	m := p.put("laptop", "index/1", "metadata", 0.5) // D: 1 unit free
	p.ack("B", d1)
	p.ack("B", d2)
	p.ack("C", d1) // C acknowledged data/1 before it went away
	reroute := func() (int64, int64) {
		t.Helper()
		moved, dropped, err := p.db.Reroute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return moved, dropped
	}
	// Failing for half an hour: nothing moves.
	p.condition("C", ConditionRetrying, p.now.Add(-30*time.Minute), p.now.Add(-time.Hour))
	if moved, dropped := reroute(); moved != 0 || dropped != 0 {
		t.Fatalf("moved %d dropped %d within reroute_after", moved, dropped)
	}
	if !p.held(m) {
		t.Fatal("index/1 not held while data/2 waits at C")
	}
	// Past reroute_after: data/2 moves to D, the only friend with room
	// that does not hold it. The acknowledged data/1 and the metadata stay.
	p.now = p.now.Add(time.Hour)
	if moved, _ := reroute(); moved != 1 {
		t.Fatalf("moved %d", moved)
	}
	p.wantPlaces("data/1", d1, "B", "C")
	p.wantPlaces("data/2", d2, "B", "D")
	p.wantPlaces("index/1", m, "B", "C", "D")
	p.ack("D", d2)
	if p.held(m) {
		t.Fatal("index/1 held after data/2 was acknowledged where it is now placed")
	}
	// With no friend with room the row waits at C.
	d3 := p.put("laptop", "data/3", "data", 3) // B and C: D is full
	p.wantPlaces("data/3", d3, "B", "C")
	p.ack("B", d3)
	if moved, dropped := reroute(); moved != 0 || dropped != 0 {
		t.Fatalf("moved %d dropped %d with no friend with room", moved, dropped)
	}
	p.wantPlaces("data/3", d3, "B", "C")
	// Past replace_after there is nowhere to wait for: the copy is
	// dropped, the object is below copies, and the barrier passes.
	m2 := p.put("laptop", "index/2", "metadata", 0.1)
	if !p.held(m2) {
		t.Fatal("index/2 not held")
	}
	p.now = p.now.Add(7 * 24 * time.Hour)
	if _, dropped := reroute(); dropped != 1 {
		t.Fatalf("dropped %d past replace_after", dropped)
	}
	p.wantPlaces("data/3", d3, "B")
	if p.held(m2) {
		t.Fatal("index/2 still held by a copy dropped as a deficit")
	}
	below, err := p.db.BelowCopies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b := below["laptop"]; b.Objects < 1 || b.Bytes < 3*unit-ObjectOverhead {
		t.Fatalf("below copies %+v", b)
	}
}

// REPL-1 (#19, #20 review): a metadata row waits until every earlier
// data object of its bucket has no queued placement left. A refusal is
// not queued: a hostile friend refusing everything cannot stall the
// barrier. A superseded queued put will be cancelled, so it does not
// hold it either.
func TestMetadataBarrier(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 2)
	p.bucket("phone", 2)
	d1 := p.put("laptop", "data/1", "data", 1)
	other := p.put("phone", "data/9", "data", 1)
	m := p.put("laptop", "index/1", "metadata", 0.1)
	later := p.put("laptop", "data/2", "data", 1)
	if !p.held(m) {
		t.Fatal("not held with data/1 queued at B and C")
	}
	if p.held(d1) || p.held(later) {
		t.Fatal("a data row is held")
	}
	p.ack("B", d1)
	if !p.held(m) {
		t.Fatal("not held with data/1 queued at C")
	}
	// C refuses data/1 for good (BadDigest from a hostile friend).
	if err := p.db.RefusePlacement(ctx, "C", d1, "BadDigest"); err != nil {
		t.Fatal(err)
	}
	if p.held(m) {
		t.Fatal("a refusal holds the barrier")
	}
	_ = other // another bucket's queued data does not hold laptop's metadata
	// A snapshot is metadata too, and a delete of metadata is held
	// like a put.
	s := p.put("laptop", "snapshots/1", "snapshot", 0.1)
	if !p.held(s) {
		t.Fatal("snapshot not held with data/2 queued")
	}
	dm := p.del("laptop", "index/1", "metadata")
	if !p.held(dm) {
		t.Fatal("metadata delete not held with data/2 queued")
	}
	// data/2 is deleted: its queued rows will be cancelled.
	p.del("laptop", "data/2", "data")
	if p.held(s) {
		t.Fatal("a superseded put holds the barrier")
	}
}

// REPL-1 AC, second half (#19): with too little room for `copies`, what
// fits is placed, the deficit shows, and every place with room still
// gets its data and metadata: the barrier waits only for real rows.
func TestTooLittleRoomPlacesWhatFits(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 1)
	p.bucket("laptop", 2)
	d := p.put("laptop", "data/1", "data", 3)
	p.wantPlaces("data/1", d, "B")
	m := p.put("laptop", "index/1", "metadata", 0.5)
	p.wantPlaces("index/1", m, "B", "C")
	p.ack("B", d)
	if p.held(m) {
		t.Fatal("metadata held by a copy that has nowhere to go")
	}
	below, err := p.db.BelowCopies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// data/1 is at 1 of 2 places; index/1 at both.
	if b := below["laptop"]; b.Objects != 1 || b.Bytes != 3*unit-ObjectOverhead {
		t.Fatalf("below copies %+v", b)
	}
}

// REPL-2 (#19): a delete goes to every place the key was placed at, and
// nowhere else.
func TestDeleteGoesWhereTheKeyIsPlaced(t *testing.T) {
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d, "B", "C")
	del := p.del("laptop", "data/1", "data")
	p.wantPlaces("delete data/1", del, "B", "C")
	never := p.del("laptop", "data/never", "data")
	p.wantPlaces("delete of a key never placed", never)
	m := p.put("laptop", "index/1", "metadata", 1)
	p.wantPlaces("index/1", m, "B", "C", "D")
	del = p.del("laptop", "index/1", "metadata")
	p.wantPlaces("delete index/1", del, "B", "C", "D")
}

// REPL-12 AC (#19): friends B, C and D at quotas 100, 55 and 55 GB
// hold a 100 GB restic bucket at two copies: fully placed, no data
// object twice at one friend, every metadata object at all three, no
// friend above its quota. Scaled: a GB is a unit; restic writes packs,
// then an index, then a snapshot, in several backup runs.
func TestREPL12PoolAcceptance(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 100)
	p.friend("C", 55)
	p.friend("D", 55)
	p.bucket("restic", 2)
	var data, meta []int64
	p.put("restic", "config", "metadata", 0.01)
	p.put("restic", "keys/k", "metadata", 0.01)
	pack := 0
	for run := range 4 {
		for range 24 + run/3*3 { // 24, 24, 24, 27: 99 packs
			pack++
			data = append(data, p.put("restic", fmt.Sprintf("data/%02x/pack%d", pack, pack), "data", 0.99))
		}
		meta = append(meta, p.put("restic", fmt.Sprintf("index/%d", run), "metadata", 0.2))
		meta = append(meta, p.put("restic", fmt.Sprintf("snapshots/%d", run), "snapshot", 0.01))
	}
	if pack != 99 {
		t.Fatalf("%d packs", pack)
	}
	for _, s := range data {
		if got := p.places(s); len(got) != 2 {
			t.Fatalf("data row %d at %v, want two friends", s, got)
		}
	}
	for _, s := range meta {
		p.wantPlaces("metadata", s, "B", "C", "D")
	}
	// No object twice at one friend.
	var dup int
	p.db.QueryRow(`SELECT count(*) FROM (SELECT place, key FROM placement GROUP BY place, key HAVING count(*) > 1)`).Scan(&dup)
	if dup != 0 {
		t.Fatalf("%d keys placed twice at one friend", dup)
	}
	use, err := p.db.PlaceUse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for peer, q := range map[string]int64{"B": 100, "C": 55, "D": 55} {
		if use[peer] > q*unit {
			t.Fatalf("%s holds %d, over its quota of %d", peer, use[peer], q*unit)
		}
	}
	below, err := p.db.BelowCopies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b := below["restic"]; b.Objects != 0 {
		t.Fatalf("not fully placed: %+v below copies", b)
	}
}

// #57, #19: a healing re-upload of a data object goes where the row it
// heals was placed: acked where that row was acknowledged, queued where
// it was refused, and nowhere else.
func TestHealingReuploadGoesWhereTheHealedRowWasPlaced(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 2)
	d := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("data/1", d, "B", "C")
	if err := p.db.AckPlacement(ctx, "B", d, "h-data/1", p.now); err != nil {
		t.Fatal(err)
	}
	if err := p.db.RefusePlacement(ctx, "C", d, "BadDigest"); err != nil {
		t.Fatal(err)
	}
	h := p.put("laptop", "data/1", "data", 1)
	p.wantPlaces("the heal", h, "B", "C")
	if nx, ok, _ := p.db.NextPlacement(ctx, "C"); !ok || nx.Seq != h {
		t.Fatalf("C's head %+v: the heal is not queued there", nx)
	}
	use, _ := p.db.PlaceUse(ctx)
	if use["B"] != unit || use["C"] != unit {
		t.Fatalf("use %v: B holds the object once, C will", use)
	}
}

// D6 (#19): the borrower's count follows the rows: a refused or
// cancelled put no longer counts.
func TestPlaceUseFollowsTheRows(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.bucket("laptop", 1)
	use := func() int64 {
		t.Helper()
		u, err := p.db.PlaceUse(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return u["B"]
	}
	d1 := p.put("laptop", "data/1", "data", 2)
	d2 := p.put("laptop", "data/2", "data", 3)
	if use() != 5*unit {
		t.Fatalf("use %d", use())
	}
	if err := p.db.RefusePlacement(ctx, "B", d1, "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if err := p.db.CancelPlacement(ctx, "B", d2); err != nil {
		t.Fatal(err)
	}
	if use() != 0 {
		t.Fatalf("use %d after a refusal and a cancel", use())
	}
}
