package state

import (
	"context"
	"testing"
	"time"
)

// ackRow acknowledges row seq at place as the worker does: with the
// capture's hash for a put, none for a delete.
func (p *pool) ackRow(place string, seq int64) {
	p.t.Helper()
	var op, sha string
	if err := p.db.QueryRow(`SELECT op, coalesce(sha256, '') FROM queue WHERE seq = ?`, seq).Scan(&op, &sha); err != nil {
		p.t.Fatal(err)
	}
	if op != OpPut {
		sha = ""
	}
	if err := p.db.AckPlacement(context.Background(), place, seq, sha, p.now); err != nil {
		p.t.Fatal(err)
	}
}

// ackAll acknowledges every queued row of seq.
func (p *pool) ackAll(seq int64) {
	p.t.Helper()
	for _, place := range p.places(seq) {
		p.ackRow(place, seq)
	}
}

func (p *pool) offsite(bucket string, target int64) Offsite {
	p.t.Helper()
	o, err := p.db.Offsite(context.Background(), bucket, target)
	if err != nil {
		p.t.Fatal(err)
	}
	return o
}

func (p *pool) watermark(bucket string) int64 {
	p.t.Helper()
	return p.offsite(bucket, 0).Watermark
}

// REPL-11 (#20): the watermark is the highest sequence up to which
// every object has `copies` acknowledged placements, and it advances as
// the acknowledgements arrive, in any order.
func TestWatermarkAdvancesAsAcknowledgementsArrive(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 100)
	}
	if o := p.offsite("laptop", 0); o.Watermark != 0 || o.Latest != 0 || !o.Complete() {
		t.Fatalf("an empty bucket: %+v", o)
	}
	d1 := p.put("laptop", "data/1", "data", 1)
	d2 := p.put("laptop", "data/2", "data", 1)
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
	if w := p.watermark("laptop"); w != d1-1 {
		t.Fatalf("nothing acknowledged: watermark %d, want %d", w, d1-1)
	}
	// d2's copies arrive first: the watermark waits for d1.
	p.ackAll(d2)
	if w := p.watermark("laptop"); w != d1-1 {
		t.Fatalf("d2 acknowledged before d1: watermark %d", w)
	}
	// One of d1's two copies is not enough.
	p.ackRow(p.places(d1)[0], d1)
	if w := p.watermark("laptop"); w != d1-1 {
		t.Fatalf("d1 at one place: watermark %d", w)
	}
	p.ackRow(p.places(d1)[1], d1)
	if w := p.watermark("laptop"); w != idx-1 {
		t.Fatalf("d1 and d2 at two places: watermark %d, want %d", w, idx-1)
	}
	// Metadata goes to every friend, but `copies` acknowledgements are
	// enough for the watermark.
	p.ackRow("B", idx)
	p.ackRow("C", idx)
	p.ackRow("B", snap)
	p.ackRow("D", snap)
	o := p.offsite("laptop", snap)
	if o.Watermark != snap || !o.Complete() || !o.Reached() || o.Latest != snap {
		t.Fatalf("everything at two places: %+v", o)
	}
	_ = d2
}

// REPL-11 (#20), the row types: a delete needs no acknowledgement; a put
// replaced by a later write of its key counts once that write is within
// the watermark (the state of the bucket as of the watermark is what is
// offsite); an acknowledgement counts while the place still holds the
// bytes, so a delete acknowledged there ends it.
func TestWatermarkFollowsRewritesAndDeletes(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	cfg := p.put("laptop", "config", "metadata", 0.01)
	// The config is rewritten before any copy left: its first version
	// is never sent, and the rewrite stands for it.
	cfg2 := p.rewrite("laptop", "config", "metadata", 0.01)
	if w := p.watermark("laptop"); w != cfg-1 {
		t.Fatalf("neither version sent: watermark %d", w)
	}
	p.ackAll(cfg2)
	if w := p.watermark("laptop"); w != cfg2 {
		t.Fatalf("the rewrite at two places stands for the first version: watermark %d, want %d", w, cfg2)
	}
	// A pack deleted before it was sent: the delete's own row counts as
	// nothing to wait for, and the put is replaced within the watermark.
	gone := p.put("laptop", "data/gone", "data", 1)
	del := p.del("laptop", "data/gone", "data")
	if w := p.watermark("laptop"); w != del {
		t.Fatalf("a put deleted unsent: watermark %d, want %d", w, del)
	}
	// A pack acknowledged at both, then deleted, with the delete
	// acknowledged at one place: the place that deleted it no longer
	// holds it, so as of any point before the delete it is at one place.
	// Past the delete it no longer matters.
	pack := p.put("laptop", "data/pack", "data", 1)
	p.ackAll(pack)
	snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
	p.ackAll(snap)
	if w := p.watermark("laptop"); w != snap {
		t.Fatalf("pack and snapshot acknowledged: watermark %d", w)
	}
	repack := p.put("laptop", "data/repack", "data", 1) // the prune's new pack, not yet sent
	idx := p.put("laptop", "index/2", "metadata", 0.01)
	p.ackAll(idx)
	pd := p.del("laptop", "data/pack", "data")
	p.ackRow(p.places(pd)[0], pd)
	if w := p.watermark("laptop"); w != pack-1 {
		t.Fatalf("the pack deleted at one place while its repack is unsent: watermark %d, want %d", w, pack-1)
	}
	p.ackAll(repack)
	if w := p.watermark("laptop"); w != pd {
		t.Fatalf("the repack at two places: watermark %d, want %d", w, pd)
	}
	_ = gone
}

// REPL-11 (#20, from the #57 review): one friend refusing everything
// (BadDigest for bytes that match, 412) never makes the watermark claim
// what is not offsite: a refused copy is no acknowledgement. Metadata,
// placed at every friend, still reaches copies at the others; a data
// object one of whose two places refused stays short, with nothing
// queued that could complete it, which Offsite reports as stuck with
// the reason, and the watermark waits only behind those objects, until
// re-placement (#26) or a write of the key places it elsewhere.
func TestAFriendRefusingEverythingLeavesTheWatermarkHonest(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.bucket("laptop", 2)
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 100)
	}
	var seqs []int64
	for _, k := range []string{"data/1", "data/2", "data/3", "data/4"} {
		seqs = append(seqs, p.put("laptop", k, "data", 1))
	}
	idx := p.put("laptop", "index/1", "metadata", 0.01)
	snap := p.put("laptop", "snapshots/1", "snapshot", 0.01)
	seqs = append(seqs, idx, snap)
	var refusedAtD []int64
	for _, s := range seqs {
		for _, place := range p.places(s) {
			if place == "D" {
				if err := p.db.RefusePlacement(ctx, "D", s, "BadDigest"); err != nil {
					t.Fatal(err)
				}
				if s != idx && s != snap {
					refusedAtD = append(refusedAtD, s)
				}
				continue
			}
			p.ackRow(place, s)
		}
	}
	if len(refusedAtD) == 0 {
		t.Fatal("no data object was placed at D")
	}
	o := p.offsite("laptop", snap)
	if o.Reached() || o.Watermark != refusedAtD[0]-1 {
		t.Fatalf("with D refusing everything: %+v, want the watermark before seq %d", o, refusedAtD[0])
	}
	if o.Waiting.Objects != int64(len(refusedAtD)) || o.Stuck.Objects != int64(len(refusedAtD)) ||
		o.Stuck.Refused != int64(len(refusedAtD)) || o.Stuck.Bytes != int64(len(refusedAtD))*(unit-ObjectOverhead) {
		t.Fatalf("waiting %+v, stuck %+v, want the %d objects D refused", o.Waiting, o.Stuck, len(refusedAtD))
	}
	// The watermark moves on once the short objects no longer matter:
	// here a prune deletes them, and the next snapshot is offsite.
	for _, s := range refusedAtD {
		var key string
		p.db.QueryRow(`SELECT key FROM queue WHERE seq = ?`, s).Scan(&key)
		p.del("laptop", key, "data")
	}
	snap2 := p.put("laptop", "snapshots/2", "snapshot", 0.01)
	for _, place := range p.places(snap2) {
		if place != "D" {
			p.ackRow(place, snap2)
		}
	}
	if o := p.offsite("laptop", snap2); !o.Reached() || o.Stuck.Objects != 0 {
		t.Fatalf("once the refused keys were deleted: %+v", o)
	}
}

// REPL-11 (#20): a copy dropped at a full or gone friend, and an object
// the pool had no room for, are short of copies with nothing queued to
// complete them: stuck, each with its reason. A copy still queued at an
// unreachable friend is waiting, not stuck: re-routing may move it.
func TestDroppedAndUnplacedCopiesAreStuckQueuedOnesWait(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	lonely := p.put("laptop", "data/lonely", "data", 1) // one friend for copies 2
	p.ackAll(lonely)
	p.friend("C", 100)
	waits := p.put("laptop", "data/waits", "data", 1)
	p.ackRow("B", waits)
	dropped := p.put("laptop", "data/dropped", "data", 1)
	p.ackRow("B", dropped)
	if _, err := p.db.Exec(`UPDATE placement SET state = 'dropped' WHERE seq = ? AND place = 'C'`, dropped); err != nil {
		t.Fatal(err)
	}
	o := p.offsite("laptop", dropped)
	if o.Watermark != lonely-1 || o.Waiting.Objects != 3 || o.Stuck.Objects != 2 || o.Stuck.Dropped != 1 ||
		o.Stuck.Unplaced != 1 || o.Stuck.Refused != 0 {
		t.Fatalf("offsite %+v", o)
	}
}

// §3.4 REPL-11 (#20): the watermark of a large bucket, offsiteObjects data
// objects (20000, fewer under -race) at two of three friends, a tenth deleted, and 1200 metadata
// objects everywhere, takes well under passLimit, as status --wait reads
// it every 2 s; one object still queued holds it.
func TestOffsiteOfALargeBucketIsFast(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 1<<20)
	}
	p.bucket("laptop", 2)
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
		INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
			SELECT 'laptop', 'data/' || i, 'put', 'data', 'done', 1000, 'h' || i, ?1 FROM n`,
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1200)
		INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
			SELECT 'laptop', 'index/' || i, 'put', 'metadata', 'done', 100, 'm' || i, ?1 FROM n`,
		`INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			SELECT q.seq, f.peer_key, q.bucket, q.key, 'put', 'acked', q.sha256, ?1, 100
			FROM queue q, friendship f WHERE q.class = 'metadata' OR f.peer_key != (CASE q.seq % 3 WHEN 0 THEN 'B' WHEN 1 THEN 'C' ELSE 'D' END)`,
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2 / 10)
		INSERT INTO queue (bucket, key, op, class, state, created_at)
			SELECT 'laptop', 'data/' || i, 'delete', 'data', 'done', ?1 FROM n`,
		`INSERT INTO placement (seq, place, bucket, key, op, state, acked_at, size)
			SELECT q.seq, f.peer_key, q.bucket, q.key, 'delete', 'acked', ?1, 0
			FROM queue q, friendship f WHERE q.op = 'delete'`,
	} {
		if _, err := tx.ExecContext(ctx, q, p.now.Unix(), offsiteObjects); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	last := p.put("laptop", "data/last", "data", 1)
	start := time.Now()
	o := p.offsite("laptop", 0)
	took := time.Since(start)
	t.Logf("Offsite over %d rows took %v", o.Latest, took)
	if took > passLimit {
		t.Fatalf("Offsite took %v, over %v", took, passLimit)
	}
	if o.Watermark != last-1 || o.Waiting.Objects != 1 {
		t.Fatalf("offsite %+v, want the watermark just before %d", o, last)
	}
	p.ackAll(last)
	if o := p.offsite("laptop", 0); !o.Complete() {
		t.Fatalf("offsite %+v", o)
	}
}

// REPL-4, REPL-11 (#20): Offsite names the latest snapshot key at or
// below the watermark, still live, and the latest one written; and the
// last write at or below it, with its capture time.
func TestOffsiteNamesTheLatestSnapshotWithinTheWatermark(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	s1 := p.put("laptop", "snapshots/aaaa", "snapshot", 0.01)
	p.ackAll(s1)
	at := p.now
	p.now = p.now.Add(3600e9)
	d := p.put("laptop", "data/x", "data", 1)
	s2 := p.put("laptop", "snapshots/bbbb", "snapshot", 0.01)
	o := p.offsite("laptop", s2)
	if o.Snapshot.Key != "snapshots/aaaa" || o.Snapshot.Seq != s1 || !o.Snapshot.CapturedAt.Equal(at) ||
		o.LatestSnapshot.Key != "snapshots/bbbb" || o.Through.Seq != s1 || !o.Through.CapturedAt.Equal(at) {
		t.Fatalf("offsite %+v", o)
	}
	p.ackAll(d)
	p.ackAll(s2)
	if o := p.offsite("laptop", s2); o.Snapshot.Key != "snapshots/bbbb" || !o.Reached() {
		t.Fatalf("after the second is acknowledged: %+v", o)
	}
	// A forgotten snapshot is not named.
	f := p.del("laptop", "snapshots/bbbb", "snapshot")
	p.ackAll(f)
	if o := p.offsite("laptop", 0); o.Snapshot.Key != "snapshots/aaaa" || o.LatestSnapshot.Key != "snapshots/aaaa" {
		t.Fatalf("after forgetting the second: %+v", o)
	}
	// The latest snapshot write, live or not (Kopia's index blobs are
	// deleted by compaction), is the target status --wait uses.
	if s, err := p.db.LatestSnapshotWrite(context.Background(), "laptop", 0); err != nil || s.Seq != s2 {
		t.Fatalf("latest snapshot write %+v %v, want seq %d", s, err, s2)
	}
}
