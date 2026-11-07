package state

import (
	"context"
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

// ID-5, REPL-12 (#29): a recovered node rebuilds placement and the hash
// index from each place's list, a page at a time: the first list of a
// key makes its done row, a later one of the same hash adds that place,
// and one of another hash is not counted as held. A list repeated after
// a crash changes nothing; the place's use counts what it holds; a
// tombstoned key is no live object.
func TestListsRebuildPlacement(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	now := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return now }
	friend(t, db, "B")
	friend(t, db, "C")
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	if rs, err := db.Relists(ctx); err != nil || len(rs) != 2 {
		t.Fatalf("relists %+v %v", rs, err)
	}
	b := []Listed{
		{Key: "data/1", Size: 10, SHA256: "h1", Class: "data"},
		{Key: "index/1", Size: 5, SHA256: "h2", Class: "metadata"},
	}
	got, err := db.RecordListed(ctx, "B", "laptop", b[:1], "data/1", false)
	if err != nil || got.Held != 1 {
		t.Fatalf("first page: %+v %v", got, err)
	}
	if rs, _ := db.Relists(ctx); rs[0].After != "data/1" {
		t.Fatalf("resume point not kept: %+v", rs)
	}
	if got, err = db.RecordListed(ctx, "B", "laptop", b[1:], "", true); err != nil || got.Held != 1 {
		t.Fatalf("last page: %+v %v", got, err)
	}
	// Repeated, as after a crash: nothing doubles.
	if _, err := db.RecordListed(ctx, "B", "laptop", b, "", true); err != nil {
		t.Fatal(err)
	}
	c := []Listed{
		{Key: "data/1", Size: 10, SHA256: "h1", Class: "data"},
		{Key: "index/1", Size: 5, SHA256: "other", Class: "metadata"},
		{Key: "data/gone", Size: 3, SHA256: "h3", Class: "data", Tombstoned: true},
	}
	got, err = db.RecordListed(ctx, "C", "laptop", c, "", true)
	if err != nil || got.Held != 1 || got.Differ != 1 || got.Tombstoned != 1 {
		t.Fatalf("C's list: %+v %v", got, err)
	}
	if rs, _ := db.Relists(ctx); len(rs) != 0 {
		t.Fatalf("finished lists still pending: %+v", rs)
	}
	if a := ackedAt(t, db, "B", "laptop"); len(a) != 2 || a["data/1"] != "h1" || a["index/1"] != "h2" {
		t.Fatalf("B holds %v", a)
	}
	if a := ackedAt(t, db, "C", "laptop"); len(a) != 1 || a["data/1"] != "h1" {
		t.Fatalf("C holds %v", a)
	}
	es, err := db.QueueEntries(ctx, "laptop")
	if err != nil || len(es) != 2 {
		t.Fatalf("queue %+v %v", es, err)
	}
	for _, e := range es {
		if e.State != StateDone || e.Op != OpPut || e.SHA256 == "" {
			t.Fatalf("recovered row %+v", e)
		}
	}
	use, _ := db.PlaceUse(ctx)
	if use["B"] != 15+2*ObjectOverhead || use["C"] != 10+ObjectOverhead {
		t.Fatalf("use %v", use)
	}
	// The object C holds another version of is below copies 2; the
	// other is at both.
	below, _ := db.BelowCopies(ctx)
	if below["laptop"].Objects != 1 {
		t.Fatalf("below copies %+v", below)
	}
	// What a place acknowledges is what the read-through asks it for (#30).
	if h, ok, _ := db.AckedHash(ctx, "C", "laptop", "data/1"); !ok || h != "h1" {
		t.Fatalf("C's hash %q %v", h, ok)
	}
}

// ID-5 (#29): an offline place's list stays pending, with its failure
// and a backoff; the others' objects count as held where they were
// found until it returns.
func TestFailedListStaysPending(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	now := time.Unix(1_800_000_000, 0)
	friend(t, db, "B")
	if err := db.AddRelists(ctx, []string{"B"}, []string{"laptop", "desktop"}); err != nil {
		t.Fatal(err)
	}
	if err := db.RelistFailed(ctx, "B", "laptop", "offline", now); err != nil {
		t.Fatal(err)
	}
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
	// Adding again keeps what is known.
	if err := db.AddRelists(ctx, []string{"B"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	if rs, _ := db.Relists(ctx); rs[len(rs)-1].Failures != 2 && rs[0].Failures != 2 {
		t.Fatalf("re-adding reset the failure: %+v", rs)
	}
}
