package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ackedAt is place's acknowledged rows: key → hash.
func ackedAt(t *testing.T, db *DB, place, bucket string) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT key, sha256 FROM placement WHERE place = ? AND bucket = ? AND state = 'acked'`, place, bucket)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, h string
		if err := rows.Scan(&k, &h); err != nil {
			t.Fatal(err)
		}
		out[k] = h
	}
	return out
}

// list records place's whole list of bucket in one page.
func list(t *testing.T, db *DB, place, bucket string, objs ...Listed) {
	t.Helper()
	if _, err := db.RecordListed(context.Background(), place, bucket, objs, len(objs), "", true); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
}

func verdict(t *testing.T, db *DB, bucket, key string) RecoveredKey {
	t.Helper()
	ks, err := db.RecoveredKeys(context.Background(), bucket)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range ks {
		if k.Key == key {
			return k
		}
	}
	t.Fatalf("no verdict for %s/%s in %+v", bucket, key, ks)
	return RecoveredKey{}
}

func recoveringWith(t *testing.T, places ...string) *DB {
	t.Helper()
	db := openTest(t)
	for _, p := range places {
		friend(t, db, p)
	}
	if err := db.AddRelists(context.Background(), places, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	return db
}

// ID-5, REPL-12 (#29): a recovered node rebuilds placement and the hash
// index from each place's list, a page at a time; a list repeated after
// a crash changes nothing; the place's use counts what it holds; a
// tombstoned key is no live object; what a place acknowledges is what
// the read-through asks it for (#30).
func TestListsRebuildPlacement(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	b := []Listed{
		{Key: "data/1", Size: 10, SHA256: "h1", Class: "data"},
		{Key: "index/1", Size: 5, SHA256: "h2", Class: "metadata"},
	}
	got, err := db.RecordListed(ctx, "B", "laptop", b[:1], 1, "data/1", false)
	if err != nil || got.Listed != 1 {
		t.Fatalf("first page: %+v %v", got, err)
	}
	if rs, _ := db.Relists(ctx); rs[0].After != "data/1" || rs[0].Seen != 1 {
		t.Fatalf("resume point not kept: %+v", rs)
	}
	if _, err = db.RecordListed(ctx, "B", "laptop", b[1:], 1, "", true); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	list(t, db, "B", "laptop", b...) // repeated, as after a crash
	got, err = db.RecordListed(ctx, "C", "laptop", []Listed{b[0], b[1],
		{Key: "data/gone", Size: 3, SHA256: "h3", Class: "data", Tombstoned: true}}, 3, "", true)
	if err != nil || got.Listed != 2 || got.Tombstoned != 1 {
		t.Fatalf("C's list: %+v %v", got, err)
	}
	if rs, _ := db.Relists(ctx); len(rs) != 0 {
		t.Fatalf("finished lists still pending: %+v", rs)
	}
	for _, p := range []string{"B", "C"} {
		if a := ackedAt(t, db, p, "laptop"); len(a) != 2 || a["data/1"] != "h1" || a["index/1"] != "h2" {
			t.Fatalf("%s holds %v", p, a)
		}
	}
	if es, _ := db.QueueEntries(ctx, "laptop"); len(es) != 2 {
		t.Fatalf("queue %+v", es)
	}
	use, _ := db.PlaceUse(ctx)
	if use["B"] != 15+2*ObjectOverhead || use["C"] != 15+2*ObjectOverhead {
		t.Fatalf("use %v", use)
	}
	if h, ok, _ := db.AckedHash(ctx, "C", "laptop", "data/1"); !ok || h != "h1" {
		t.Fatalf("C's hash %q %v", h, ok)
	}
	if held, _ := db.HeldAt(ctx, "laptop"); held["B"] != 2 || held["C"] != 2 {
		t.Fatalf("held %v", held)
	}
}

// #29 review (user decision 2026-11-07): for a key not named by its
// hash, the hash most places list wins, whatever order the places are
// listed in: a stale friend that sorts first loses 2 to 1, and only the
// agreeing places count as holding it.
func TestStaleKeyLosesTwoToOne(t *testing.T) {
	db := recoveringWith(t, "A0", "C", "D")
	list(t, db, "A0", "laptop", Listed{Key: "doc.txt", Size: 7, SHA256: "stale", Class: "data"})
	list(t, db, "C", "laptop", Listed{Key: "doc.txt", Size: 9, SHA256: "fresh", Class: "data"})
	list(t, db, "D", "laptop", Listed{Key: "doc.txt", Size: 9, SHA256: "fresh", Class: "data"})
	if v := verdict(t, db, "laptop", "doc.txt"); v.Verdict != VerdictHeld || v.SHA256 != "fresh" {
		t.Fatalf("verdict %+v", v)
	}
	for p, want := range map[string]int{"A0": 0, "C": 1, "D": 1} {
		if a := ackedAt(t, db, p, "laptop"); len(a) != want || want == 1 && a["doc.txt"] != "fresh" {
			t.Fatalf("%s holds %v", p, a)
		}
	}
	es, _ := db.QueueEntries(context.Background(), "laptop")
	if len(es) != 1 || es[0].SHA256 != "fresh" || es[0].Size != 9 {
		t.Fatalf("queue %+v", es)
	}
}

// #29 review: two places listing different hashes, one each, is a
// conflict: no place counts as holding the key, so the read-through has
// nothing to fetch and KeyConflict says why, until the user picks a
// place; a later list that makes a majority settles it too.
func TestTieIsConflictUntilResolved(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop", Listed{Key: "config", Size: 155, SHA256: "hb", Class: "metadata"})
	list(t, db, "C", "laptop", Listed{Key: "config", Size: 155, SHA256: "hc", Class: "metadata"})
	v := verdict(t, db, "laptop", "config")
	if v.Verdict != VerdictConflict || v.Places["B"] != "hb" || v.Places["C"] != "hc" {
		t.Fatalf("verdict %+v", v)
	}
	if c, ok, err := db.KeyConflict(ctx, "laptop", "config"); err != nil || !ok || c.Verdict != VerdictConflict {
		t.Fatalf("KeyConflict %+v %v %v", c, ok, err)
	}
	for _, p := range []string{"B", "C"} {
		if _, ok, _ := db.AckedHash(ctx, p, "laptop", "config"); ok {
			t.Fatalf("%s counts as holding a conflicted key", p)
		}
	}
	if es, _ := db.QueueEntries(ctx, "laptop"); len(es) != 0 {
		t.Fatalf("a conflicted key has a row: %+v", es)
	}
	if err := db.ResolveKey(ctx, "laptop", "config", "D"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("picking a place that does not list it: %v", err)
	}
	if err := db.ResolveKey(ctx, "laptop", "nope", "B"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("picking for an unknown key: %v", err)
	}
	if err := db.ResolveKey(ctx, "laptop", "config", "C"); err != nil {
		t.Fatal(err)
	}
	if v := verdict(t, db, "laptop", "config"); v.Verdict != VerdictHeld || v.SHA256 != "hc" {
		t.Fatalf("after resolving %+v", v)
	}
	if _, ok, _ := db.KeyConflict(ctx, "laptop", "config"); ok {
		t.Fatal("still a conflict after resolving")
	}
	if a := ackedAt(t, db, "C", "laptop"); a["config"] != "hc" {
		t.Fatalf("C holds %v", a)
	}
	if a := ackedAt(t, db, "B", "laptop"); len(a) != 0 {
		t.Fatalf("B holds %v", a)
	}

	// Another tie settled by a third place's list.
	db = recoveringWith(t, "B", "C", "D")
	list(t, db, "B", "laptop", Listed{Key: "x", Size: 1, SHA256: "h1", Class: "data"})
	list(t, db, "C", "laptop", Listed{Key: "x", Size: 1, SHA256: "h2", Class: "data"})
	if v := verdict(t, db, "laptop", "x"); v.Verdict != VerdictConflict {
		t.Fatalf("%+v", v)
	}
	list(t, db, "D", "laptop", Listed{Key: "x", Size: 1, SHA256: "h2", Class: "data"})
	if v := verdict(t, db, "laptop", "x"); v.Verdict != VerdictHeld || v.SHA256 != "h2" {
		t.Fatalf("after the third list %+v", v)
	}
	if a := ackedAt(t, db, "C", "laptop"); a["x"] != "h2" {
		t.Fatalf("C holds %v", a)
	}
}

// #29 review: a key one place lists is accepted, as a single source;
// a metadata key one place lists while another place that was listed
// lacks it is not (REPL-12 puts metadata at every friend): it is a
// conflict until the user picks or another place lists it.
func TestSingleSourceAndLoneMetadata(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop",
		Listed{Key: "data/1", Size: 1, SHA256: "h1", Class: "data"},
		Listed{Key: "index/9", Size: 1, SHA256: "h9", Class: "metadata"})
	// C not listed yet: both are single sources, accepted.
	for _, k := range []string{"data/1", "index/9"} {
		if v := verdict(t, db, "laptop", k); v.Verdict != VerdictSingle {
			t.Fatalf("%s: %+v", k, v)
		}
	}
	if a := ackedAt(t, db, "B", "laptop"); len(a) != 2 {
		t.Fatalf("B holds %v", a)
	}
	list(t, db, "C", "laptop") // C holds nothing of it
	if v := verdict(t, db, "laptop", "data/1"); v.Verdict != VerdictSingle {
		t.Fatalf("data at one place: %+v", v)
	}
	v := verdict(t, db, "laptop", "index/9")
	if v.Verdict != VerdictConflict || v.Reason == "" {
		t.Fatalf("lone metadata: %+v", v)
	}
	if a := ackedAt(t, db, "B", "laptop"); len(a) != 1 || a["data/1"] != "h1" {
		t.Fatalf("B holds %v", a)
	}
	if err := db.ResolveKey(ctx, "laptop", "index/9", "B"); err != nil {
		t.Fatal(err)
	}
	if a := ackedAt(t, db, "B", "laptop"); a["index/9"] != "h9" {
		t.Fatalf("after accepting B's: %v", a)
	}
}

// #78: in a bucket whose profile reads the union of its metadata
// (Kopia), a metadata or snapshot key one friend lists and another,
// listed to its end, lacks is a single source, not a conflict: the
// other has not received it yet, or already deleted it as superseded.
// Each friend's lone keys count, and are held where they are listed,
// whichever list ends first.
func TestLoneMetadataOfAUnionProfileIsASingleSource(t *testing.T) {
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop",
		Listed{Key: "kopia.repository", Size: 1, SHA256: "hr", Class: "metadata", Union: true, Rewritable: true},
		Listed{Key: "xn0_new", Size: 1, SHA256: "hn", Class: "snapshot", Union: true},
		Listed{Key: "xw2", Size: 1, SHA256: "hw2", Class: "metadata", Union: true})
	list(t, db, "C", "laptop",
		Listed{Key: "kopia.repository", Size: 1, SHA256: "hr", Class: "metadata", Union: true, Rewritable: true},
		Listed{Key: "xw1", Size: 1, SHA256: "hw1", Class: "metadata", Union: true})
	for key, place := range map[string]string{"xn0_new": "B", "xw2": "B", "xw1": "C"} {
		if v := verdict(t, db, "laptop", key); v.Verdict != VerdictSingle {
			t.Errorf("%s, listed by %s alone: %+v", key, place, v)
		}
		if a := ackedAt(t, db, place, "laptop"); a[key] == "" {
			t.Errorf("%s not held at %s: %v", key, place, a)
		}
	}
	if v := verdict(t, db, "laptop", "kopia.repository"); v.Verdict != VerdictHeld {
		t.Errorf("kopia.repository at both: %+v", v)
	}
	if cs, err := db.Conflicts(context.Background()); err != nil || len(cs) != 0 {
		t.Fatalf("conflicts %+v %v", cs, err)
	}
}

// #29 review: a key the node wrote itself after recovery is its own;
// lists only add the places that hold that version.
func TestLocalWriteAfterRecoveryWins(t *testing.T) {
	db := recoveringWith(t, "B")
	seq := capture(t, db, "laptop", "doc", OpPut) // hash "h-doc"
	db.Exec(`DELETE FROM placement WHERE seq = ?`, seq)
	list(t, db, "B", "laptop", Listed{Key: "doc", Size: 3, SHA256: "h-doc", Class: "data"})
	if a := ackedAt(t, db, "B", "laptop"); a["doc"] != "h-doc" {
		t.Fatalf("B holds %v", a)
	}
	if es, _ := db.QueueEntries(context.Background(), "laptop"); len(es) != 1 || es[0].Seq != seq {
		t.Fatalf("queue %+v", es)
	}
}

// ID-5 (#29): an offline place's list stays pending, with its failure
// and a backoff; abandoning it ends it.
func TestFailedListStaysPending(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	now := time.Unix(1_800_000_000, 0)
	friend(t, db, "B")
	if err := db.AddRelists(ctx, []string{"B"}, []string{"laptop", "desktop"}); err != nil {
		t.Fatal(err)
	}
	db.RelistFailed(ctx, "B", "laptop", "offline", now)
	if err := db.RelistFailed(ctx, "B", "laptop", "offline again", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	rs, err := db.Relists(ctx)
	if err != nil || len(rs) != 2 {
		t.Fatalf("%+v %v", rs, err)
	}
	for _, r := range rs {
		if r.Bucket == "laptop" && (r.Failures != 2 || r.Error != "offline again" || !r.TriedAt.Equal(now.Add(time.Minute))) {
			t.Fatalf("failed list %+v", r)
		}
		if r.Bucket == "desktop" && (r.Failures != 0 || r.Error != "") {
			t.Fatalf("untried list %+v", r)
		}
	}
	if err := db.AddRelists(ctx, []string{"B"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	for _, r := range mustRelists(t, db) {
		if r.Bucket == "laptop" && r.Failures != 2 {
			t.Fatalf("re-adding reset the failure: %+v", r)
		}
	}
	if err := db.AbandonRelist(ctx, "B", "laptop"); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	if rs := mustRelists(t, db); len(rs) != 1 || rs[0].Bucket != "desktop" {
		t.Fatalf("after abandoning %+v", rs)
	}
}

func settle(t *testing.T, db *DB) {
	t.Helper()
	if err := db.SettleFinished(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Unsettled(context.Background()); err != nil || n != 0 {
		t.Fatalf("unsettled %d %v", n, err)
	}
}

func mustRelists(t *testing.T, db *DB) []Relist {
	t.Helper()
	rs, err := db.Relists(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

// ID-5, REPL-2, RET-1 (#21): a key every place keeps only as a tomb
// (deleted there, kept for the retention window) is not recovered: no
// verdict, no source for the read-through, not in the bucket's
// listing, nothing acknowledged at any place, and no metadata conflict
// from a place that lacks it live. A key one place still holds live is
// recovered from that place alone.
func TestKeyTombstonedAtEveryPlaceIsNotRecovered(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	gone := []Listed{
		{Key: "data/ab/gone", Size: 3, SHA256: "h3", Class: "data", Tombstoned: true},
		{Key: "index/old", Size: 2, SHA256: "h4", Class: "metadata", Tombstoned: true},
		{Key: "snapshots/old", Size: 1, SHA256: "h5", Class: "snapshot", Tombstoned: true},
	}
	live := Listed{Key: "index/new", Size: 5, SHA256: "h6", Class: "metadata"}
	// data/cd/half: deleted at B, whose delete has not reached C yet.
	half := Listed{Key: "data/cd/half", Size: 4, SHA256: "h7", Class: "data"}
	halfGone := half
	halfGone.Tombstoned = true
	list(t, db, "B", "laptop", append([]Listed{live, halfGone}, gone...)...)
	list(t, db, "C", "laptop", append([]Listed{live, half}, gone...)...)
	ks, err := db.RecoveredKeys(ctx, "laptop")
	if err != nil || len(ks) != 2 || ks[0].Key != "data/cd/half" || ks[0].Verdict != VerdictSingle ||
		ks[1].Key != "index/new" || ks[1].Verdict != VerdictHeld {
		t.Fatalf("recovered keys %+v %v", ks, err)
	}
	if src, ok, err := db.Source(ctx, "laptop", "data/cd/half"); err != nil || !ok || len(src.Places) != 1 || src.Places[0].Place != "C" {
		t.Fatalf("source of data/cd/half: %+v %v %v", src, ok, err)
	}
	for _, g := range gone {
		if _, ok, err := db.Source(ctx, "laptop", g.Key); err != nil || ok {
			t.Fatalf("source of tombstoned %s: %v %v", g.Key, ok, err)
		}
	}
	lk, err := db.LiveKeys(ctx, "laptop", "", "", 100)
	if err != nil || len(lk) != 2 || lk[0].Key != "data/cd/half" || lk[1].Key != "index/new" {
		t.Fatalf("live keys %+v %v", lk, err)
	}
	if a := ackedAt(t, db, "B", "laptop"); len(a) != 1 || a["index/new"] != "h6" {
		t.Fatalf("B holds %v", a)
	}
	if a := ackedAt(t, db, "C", "laptop"); len(a) != 2 || a["index/new"] != "h6" || a["data/cd/half"] != "h7" {
		t.Fatalf("C holds %v", a)
	}
}

// #96 review: a metadata or snapshot key only an overflow target lists
// is not live on its word: a target without versioning may still hold
// an index the node pruned, its delete held in the lost lendisk.db, and
// a revived index would let restic dedupe against packs pruned at the
// friends. Its data packs count as a friend's. Once a friend lists the
// key, it counts, held at both.
func TestMetadataOnlyATargetListsIsNotLive(t *testing.T) {
	db := recoveringWith(t, "B")
	target := OverflowPlace("spill")
	if err := db.AddRelists(context.Background(), []string{target}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	list(t, db, target, "laptop",
		Listed{Key: "data/1", Size: 10, SHA256: "h1", Class: "data", HashNamed: true},
		Listed{Key: "index/old", Size: 5, SHA256: "h2", Class: "metadata", HashNamed: true},
		Listed{Key: "snapshots/old", Size: 5, SHA256: "h3", Class: "snapshot", HashNamed: true},
		Listed{Key: "index/new", Size: 5, SHA256: "h4", Class: "metadata", HashNamed: true})
	if got := ackedAt(t, db, target, "laptop"); len(got) != 1 || got["data/1"] != "h1" {
		t.Fatalf("held at the target %v, want its pack only", got)
	}
	ks, err := db.RecoveredKeys(context.Background(), "laptop")
	if err != nil || len(ks) != 1 || ks[0].Key != "data/1" || ks[0].Verdict != VerdictSingle {
		t.Fatalf("recovered %+v %v", ks, err)
	}
	list(t, db, "B", "laptop", Listed{Key: "index/new", Size: 5, SHA256: "h4", Class: "metadata", HashNamed: true})
	if k := verdict(t, db, "laptop", "index/new"); k.SHA256 != "h4" || k.Places[target] != "h4" || k.Places["B"] != "h4" {
		t.Fatalf("index/new %+v", k)
	}
	if got := ackedAt(t, db, target, "laptop"); got["index/new"] != "h4" || got["index/old"] != "" {
		t.Fatalf("held at the target %v", got)
	}
}

// queueOf is bucket's done rows of key: sequence → hash.
func queueOf(t *testing.T, db *DB, bucket, key string) map[int64]string {
	t.Helper()
	rows, err := db.Query(`SELECT seq, coalesce(sha256, '') FROM queue WHERE bucket = ? AND key = ? AND state = 'done'`, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var s int64
		var h string
		if err := rows.Scan(&s, &h); err != nil {
			t.Fatal(err)
		}
		out[s] = h
	}
	return out
}

// ID-5, REPL-13 (#71): a place's list that arrives while this node is
// writing the same key (captured, not yet done) never supersedes that
// write, in a generic bucket, whose keys are not named by their hash:
// the listed version gets no queue row while the write is in flight, so
// the write, once done, is the key's latest and is replicated, and the
// lists only add the places that hold it. A list that would change the
// version an earlier list made count waits the same way. A write that
// fails leaves the listed version counting, decided again once it ends.
func TestAListedKeyNeverSupersedesALocalWriteInFlight(t *testing.T) {
	ctx := context.Background()
	const key = "docs/report.odt"
	begin := func(db *DB) int64 {
		t.Helper()
		seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: key, Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		return seqs[0]
	}
	recovering := func(t *testing.T) *DB {
		db := recoveringWith(t, "B", "C")
		if err := db.AddBucket(ctx, Bucket{Name: "laptop", Profile: "generic", Copies: 2, AccessKey: "AK", SecretKey: "s",
			CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	old := Listed{Key: key, Size: 5, SHA256: "h-old", Class: "data"}

	t.Run("written", func(t *testing.T) {
		db := recovering(t)
		w := begin(db)
		list(t, db, "B", "laptop", old)
		if q := queueOf(t, db, "laptop", key); len(q) != 0 {
			t.Fatalf("the listed version has a queue row while the write is in flight: %v", q)
		}
		if v := verdict(t, db, "laptop", key); v.Verdict != VerdictWriting {
			t.Fatalf("verdict %+v", v)
		}
		if _, err := db.FinishPut(ctx, w, 7, "h-new"); err != nil {
			t.Fatal(err)
		}
		if sup, err := db.Superseded(ctx, w); err != nil || sup {
			t.Fatalf("the local write is superseded: %v %v", sup, err)
		}
		settle(t, db)
		list(t, db, "C", "laptop", old)
		if q := queueOf(t, db, "laptop", key); len(q) != 1 || q[w] != "h-new" {
			t.Fatalf("queue %v", q)
		}
		if v := verdict(t, db, "laptop", key); v.Verdict != VerdictHeld || v.SHA256 != "h-new" {
			t.Fatalf("verdict %+v", v)
		}
		for _, p := range []string{"B", "C"} {
			var st string
			if err := db.QueryRow(`SELECT state FROM placement WHERE place = ? AND seq = ?`, p, w).Scan(&st); err != nil || st != "queued" {
				t.Fatalf("the write at %s is %q %v, want queued", p, st, err)
			}
		}
	})

	t.Run("failed", func(t *testing.T) {
		db := recovering(t)
		w := begin(db)
		list(t, db, "B", "laptop", old)
		if err := db.DropCaptures(ctx, w); err != nil {
			t.Fatal(err)
		}
		if n, err := db.Unsettled(ctx); err != nil || n != 1 {
			t.Fatalf("unsettled %d %v: the key is not decided again", n, err)
		}
		settle(t, db)
		if v := verdict(t, db, "laptop", key); v.Verdict != VerdictSingle || v.SHA256 != "h-old" {
			t.Fatalf("verdict %+v", v)
		}
		if a := ackedAt(t, db, "B", "laptop"); a[key] != "h-old" {
			t.Fatalf("B holds %v", a)
		}
	})

	t.Run("decided before", func(t *testing.T) {
		db := recovering(t)
		list(t, db, "B", "laptop", old)
		q := queueOf(t, db, "laptop", key)
		if len(q) != 1 {
			t.Fatalf("queue %v", q)
		}
		w := begin(db)
		// C's list ties with B's: decided now, the recovered row would
		// go and a new one come after the write.
		list(t, db, "C", "laptop", Listed{Key: key, Size: 6, SHA256: "h-other", Class: "data"})
		if got := queueOf(t, db, "laptop", key); len(got) != 1 {
			t.Fatalf("queue changed under the write: %v, was %v", got, q)
		}
		if _, err := db.FinishPut(ctx, w, 7, "h-new"); err != nil {
			t.Fatal(err)
		}
		if sup, err := db.Superseded(ctx, w); err != nil || sup {
			t.Fatalf("the local write is superseded: %v %v", sup, err)
		}
		settle(t, db)
		if v := verdict(t, db, "laptop", key); v.Verdict != VerdictHeld || v.SHA256 != "h-new" {
			t.Fatalf("verdict %+v", v)
		}
		if cs, _ := db.Conflicts(ctx); len(cs) != 0 {
			t.Fatalf("conflicts %+v", cs)
		}
	})
}
