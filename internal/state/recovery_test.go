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
	if rs := mustRelists(t, db); len(rs) != 1 || rs[0].Bucket != "desktop" {
		t.Fatalf("after abandoning %+v", rs)
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
