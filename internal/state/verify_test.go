package state

import (
	"context"
	"slices"
	"testing"
	"time"
)

// heldAt returns the sequences of bucket's data rows acked at place.
func (p *pool) heldAt(place, bucket string) []int64 {
	p.t.Helper()
	rows, err := p.db.Query(`SELECT seq FROM placement WHERE place = ? AND bucket = ? AND state = 'acked' ORDER BY seq`,
		place, bucket)
	if err != nil {
		p.t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var s int64
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}

// VER-1, REPL-13 (#27): a failed challenge marks one friend's replica of
// one bucket unverified, and the next pass moves every data copy of that
// bucket it holds to other friends, as for a removed friend: the old
// copy stays until the new one is acknowledged, then it is deleted
// there. Its copies of another bucket stay, and new data of the bucket
// goes elsewhere; metadata still goes to every friend (REPL-12).
func TestAFailedVerificationMovesTheReplicasCopies(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 10)
	}
	p.bucket("laptop", 2)
	p.bucket("photos", 2)
	var laptop, photos []int64
	for i := range 6 {
		laptop = append(laptop, p.put("laptop", "data/"+string(rune('a'+i)), "data", 0.1))
		photos = append(photos, p.put("photos", "data/"+string(rune('a'+i)), "data", 0.1))
	}
	for _, s := range append(slices.Clone(laptop), photos...) {
		p.ackAll(s)
	}
	atB := p.heldAt("B", "laptop")
	photosAtB := p.heldAt("B", "photos")
	if len(atB) == 0 || len(photosAtB) == 0 {
		t.Fatalf("B holds nothing: laptop %v photos %v", atB, photosAtB)
	}
	before, err := p.db.PoolFingerprint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.db.RecordUnverified(ctx, "B", "laptop", "data/a: the friend does not hold it", p.now); err != nil {
		t.Fatal(err)
	}
	if after, err := p.db.PoolFingerprint(ctx); err != nil || after == before {
		t.Fatalf("the fingerprint did not change (%v): re-placement would not run", err)
	}
	p.replace()
	for _, s := range atB {
		got := p.places(s)
		if len(got) != 3 || !slices.Contains(got, "B") {
			t.Fatalf("row %d: placed at %v, want B's copy kept until the new one is acknowledged", s, got)
		}
		_, replaces := p.rowOf(otherThan(got, "B", p.heldBy(s)), s)
		if replaces != "B" {
			t.Fatalf("row %d: the new copy replaces %q", s, replaces)
		}
		p.ackAll(s)
		p.wantState("B", s, PlacementDropping)
	}
	for _, s := range photosAtB {
		p.wantState("B", s, PlacementAcked)
	}
	// New data of the bucket is not placed at B; metadata is.
	d := p.put("laptop", "data/new", "data", 0.1)
	if got := p.places(d); slices.Contains(got, "B") || len(got) != 2 {
		t.Fatalf("new data placed at %v", got)
	}
	m := p.put("laptop", "index/1", "metadata", 0.1)
	if got := p.places(m); !slices.Contains(got, "B") {
		t.Fatalf("metadata placed at %v", got)
	}
	if st := p.replace(); st.Placed != 0 {
		t.Fatalf("a second pass: %+v", st)
	}
}

// heldBy is the places whose row of seq was acked before the pass.
func (p *pool) heldBy(seq int64) []string {
	p.t.Helper()
	rows, err := p.db.Query(`SELECT place FROM placement WHERE seq = ? AND state = 'acked'`, seq)
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

// otherThan is the place in got that is neither skip nor in held.
func otherThan(got []string, skip string, held []string) string {
	for _, g := range got {
		if g != skip && !slices.Contains(held, g) {
			return g
		}
	}
	return ""
}

// VER-1 (#27): copies of the bucket queued at an unverified friend and
// not yet taken by its sender are cancelled and placed elsewhere.
func TestUnsentCopiesAtAnUnverifiedFriendGoElsewhere(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	for _, f := range []string{"B", "C", "D"} {
		p.friend(f, 10)
	}
	p.bucket("laptop", 2)
	var atB int64
	for i := 0; atB == 0 && i < 10; i++ {
		s := p.put("laptop", "data/"+string(rune('a'+i)), "data", 0.1)
		if slices.Contains(p.places(s), "B") {
			atB = s
		}
	}
	if atB == 0 {
		t.Fatal("nothing placed at B")
	}
	if err := p.db.RecordUnverified(ctx, "B", "laptop", "x", p.now); err != nil {
		t.Fatal(err)
	}
	p.replace()
	if got := p.places(atB); slices.Contains(got, "B") || len(got) != 2 {
		t.Fatalf("placed at %v after the pass", got)
	}
}

// VER-1 (#27): each friend's replica of each bucket is due for its
// challenge once a day from its last answer, an hour after a transient
// error (not counted as a failure), and not at all while unverified.
// Only replicas holding acknowledged objects are due.
func TestReplicasAreDueDaily(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 1)
	due := func() []ReplicaKey {
		t.Helper()
		ks, err := p.db.DueReplicas(ctx, p.now, 24*time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return ks
	}
	if ks := due(); len(ks) != 0 {
		t.Fatalf("due with nothing acked: %v", ks)
	}
	s := p.put("laptop", "data/1", "data", 0.1)
	p.ackAll(s)
	// Not while copies of the bucket still wait to be sent there.
	q := p.put("laptop", "data/2", "data", 0.1)
	if ks := due(); len(ks) != 0 {
		t.Fatalf("due while replicating: %v", ks)
	}
	p.ackAll(q)
	want := []ReplicaKey{{Place: "B", Bucket: "laptop"}}
	if ks := due(); !slices.Equal(ks, want) {
		t.Fatalf("due %v", ks)
	}
	if err := p.db.RecordVerified(ctx, "B", "laptop", false, p.now); err != nil {
		t.Fatal(err)
	}
	if ks := due(); len(ks) != 0 {
		t.Fatalf("due just after a pass: %v", ks)
	}
	p.now = p.now.Add(24 * time.Hour)
	if ks := due(); !slices.Equal(ks, want) {
		t.Fatalf("not due a day later: %v", ks)
	}
	if err := p.db.RecordCheckError(ctx, "B", "laptop", "network down", p.now); err != nil {
		t.Fatal(err)
	}
	if ks := due(); len(ks) != 0 {
		t.Fatalf("due just after an error: %v", ks)
	}
	p.now = p.now.Add(time.Hour)
	if ks := due(); !slices.Equal(ks, want) {
		t.Fatalf("not due an hour after an error: %v", ks)
	}
	cs, err := p.db.ReplicaChecks(ctx)
	if err != nil || len(cs) != 1 || cs[0].Error != "network down" || !cs[0].UnverifiedAt.IsZero() || cs[0].VerifiedAt.IsZero() {
		t.Fatalf("checks %+v %v", cs, err)
	}
	if err := p.db.RecordUnverified(ctx, "B", "laptop", "lost data/1", p.now); err != nil {
		t.Fatal(err)
	}
	p.now = p.now.Add(48 * time.Hour)
	if ks := due(); len(ks) != 0 {
		t.Fatalf("an unverified replica is due: %v", ks)
	}
	// A passing full check marks it verified again; a ranged one does not.
	if err := p.db.RecordVerified(ctx, "B", "laptop", false, p.now); err != nil {
		t.Fatal(err)
	}
	if cs, _ := p.db.ReplicaChecks(ctx); cs[0].UnverifiedAt.IsZero() {
		t.Fatal("a ranged check cleared the mark")
	}
	if err := p.db.RecordVerified(ctx, "B", "laptop", true, p.now); err != nil {
		t.Fatal(err)
	}
	if cs, _ := p.db.ReplicaChecks(ctx); !cs[0].UnverifiedAt.IsZero() || cs[0].FullAt != p.now || cs[0].Error != "" {
		t.Fatalf("after a full check: %+v", cs[0])
	}
}

// VER-1 (#27): the objects a challenge may name at a place are those it
// acknowledged whose latest write is that very row, so the local store
// holds the same bytes: not one written again or deleted since, nor an
// empty one, nor one of another bucket or place.
func TestChallengeableObjectsAreTheLiveAcknowledgedOnes(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 1)
	p.bucket("photos", 1)
	keep := p.put("laptop", "data/keep", "data", 0.1)
	old := p.put("laptop", "data/again", "data", 0.1)
	gone := p.put("laptop", "data/gone", "data", 0.1)
	meta := p.put("laptop", "index/1", "metadata", 0.1)
	other := p.put("photos", "data/x", "data", 0.1)
	unacked := p.put("laptop", "data/unacked", "data", 0.1)
	for _, s := range []int64{keep, old, gone, meta, other} {
		p.ackAll(s)
	}
	_ = unacked
	again := p.rewrite("laptop", "data/again", "data", 0.1) // other bytes, not yet sent
	p.del("laptop", "data/gone", "data")
	n, err := p.db.ChallengeableCount(ctx, "B", "laptop")
	if err != nil || n != 2 {
		t.Fatalf("count %d %v", n, err)
	}
	var keys []string
	for i := range n {
		c, err := p.db.ChallengeableAt(ctx, "B", "laptop", i)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, c.Key)
		if c.Size <= 0 || c.SHA256 != "h-"+c.Key {
			t.Fatalf("object %+v", c)
		}
	}
	if !slices.Equal(keys, []string{"data/keep", "index/1"}) {
		t.Fatalf("challengeable %v", keys)
	}
	page, err := p.db.ChallengeablePage(ctx, "B", "laptop", 0, 1)
	if err != nil || len(page) != 1 || page[0].Key != "data/keep" {
		t.Fatalf("page %+v %v", page, err)
	}
	page, err = p.db.ChallengeablePage(ctx, "B", "laptop", page[0].Seq, 10)
	if err != nil || len(page) != 1 || page[0].Key != "index/1" {
		t.Fatalf("page %+v %v", page, err)
	}
	_ = again
}

// VER-3 (#27): `lendisk verify` asks the daemon for a check through
// lendisk.db and reads the outcome there.
func TestVerifyRequestsRoundTrip(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	id, err := p.db.RequestVerify(ctx, "B", "laptop", true, p.now)
	if err != nil {
		t.Fatal(err)
	}
	rs, err := p.db.PendingVerifies(ctx)
	if err != nil || len(rs) != 1 || rs[0].ID != id || !rs[0].Full || rs[0].Place != "B" || rs[0].Bucket != "laptop" {
		t.Fatalf("pending %+v %v", rs, err)
	}
	if err := p.db.FinishVerify(ctx, id, VerifyOutcome{Outcome: VerifyFailed, Checked: 3, Skipped: 1, Detail: "lost"}, p.now); err != nil {
		t.Fatal(err)
	}
	if rs, _ := p.db.PendingVerifies(ctx); len(rs) != 0 {
		t.Fatalf("still pending: %+v", rs)
	}
	r, done, err := p.db.VerifyResult(ctx, id)
	if err != nil || !done || r.Outcome != VerifyFailed || r.Checked != 3 || r.Skipped != 1 || r.Detail != "lost" {
		t.Fatalf("result %+v %v %v", r, done, err)
	}
}

// VER-1 (#27 review): a replica's verification record ends with the
// friendship or the bucket, so a friend befriended again, or a bucket
// added again by the same name, starts with no unverified mark.
func TestReplicaRecordsEndWithTheFriendshipAndTheBucket(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.friend("C", 10)
	p.bucket("laptop", 1)
	for _, f := range []string{"B", "C"} {
		if err := p.db.RecordUnverified(ctx, f, "laptop", "lost", p.now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.db.RemoveFriendship(ctx, "B", false, p.now); err != nil {
		t.Fatal(err)
	}
	p.friend("B", 10)
	if err := p.db.DeleteBucket(ctx, "laptop"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM replica_check`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d records left (%v)", n, err)
	}
}

// VER-1, SEC-7 (#27 review): a replica whose FIFO never empties (a
// friend at quota, a slow uplink) is challenged anyway once it is more
// than a day overdue, so it is not starved for good: a day after its
// last answer it waits for quiet, two days after it no longer does. One
// never answered is overdue a day after its first acknowledgement.
func TestAStarvedReplicaIsChallengedOnceOverdue(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 1)
	due := func() bool {
		t.Helper()
		ks, err := p.db.DueReplicas(ctx, p.now, 24*time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		return len(ks) == 1
	}
	a := p.put("laptop", "data/a", "data", 0.1)
	p.ackAll(a)
	p.put("laptop", "data/b", "data", 0.1) // stays queued
	if due() {
		t.Fatal("due while replicating, just acknowledged")
	}
	p.now = p.now.Add(24*time.Hour + time.Minute)
	if !due() {
		t.Fatal("never answered, a day after the first acknowledgement: not due")
	}
	if err := p.db.RecordVerified(ctx, "B", "laptop", false, p.now); err != nil {
		t.Fatal(err)
	}
	p.now = p.now.Add(36 * time.Hour)
	if due() {
		t.Fatal("due half a day overdue while replicating")
	}
	p.now = p.now.Add(12*time.Hour + time.Minute)
	if !due() {
		t.Fatal("not due more than a day overdue")
	}
}

// VER-1 (#27 review): an object is still challengeable at a place while
// the place's row of it is acked and it is its key's latest done write;
// not once deleted, written again, or dropped there.
func TestStillChallengeable(t *testing.T) {
	p := newPool(t)
	ctx := context.Background()
	p.friend("B", 10)
	p.bucket("laptop", 1)
	still := func(seq int64) bool {
		t.Helper()
		ok, err := p.db.StillChallengeable(ctx, "B", seq)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	a := p.put("laptop", "data/a", "data", 0.1)
	if still(a) {
		t.Fatal("challengeable before it was acknowledged")
	}
	p.ackAll(a)
	b := p.put("laptop", "data/b", "data", 0.1)
	c := p.put("laptop", "data/c", "data", 0.1)
	p.ackAll(b)
	p.ackAll(c)
	if !still(a) || !still(b) || !still(c) {
		t.Fatal("an acknowledged live object is not challengeable")
	}
	p.del("laptop", "data/a", "data")
	p.rewrite("laptop", "data/b", "data", 0.1)
	if _, err := p.db.Exec(`UPDATE placement SET state = 'dropping' WHERE seq = ?`, c); err != nil {
		t.Fatal(err)
	}
	if still(a) || still(b) || still(c) {
		t.Fatalf("deleted %v, written again %v, dropping %v", still(a), still(b), still(c))
	}
}
