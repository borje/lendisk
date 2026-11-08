package state

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func capture(t *testing.T, db *DB, bucket, key, op string) int64 {
	t.Helper()
	ctx := context.Background()
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: bucket, Key: key, Op: op, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if op == OpPut {
		if _, err := db.FinishPut(ctx, seqs[0], int64(len(key)), "h-"+key); err != nil {
			t.Fatal(err)
		}
	} else if err := db.FinishDelete(ctx, seqs[0]); err != nil {
		t.Fatal(err)
	}
	return seqs[0]
}

func friend(t *testing.T, db *DB, peer string) {
	t.Helper()
	if err := db.AddFriendship(context.Background(), Friendship{Peer: peer, Address: "tc" + peer,
		OwnNumber: 1 << 40, PeerNumber: 1 << 40, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

// REPL-1, REPL-3 (#18): a completed capture is placed, in the same
// transaction, at every friend; each place's FIFO is the write
// sequence; a pending or dropped capture is placed nowhere.
func TestDoneCapturesArePlacedAtEveryFriendInWriteOrder(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	friend(t, db, "C")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	pending, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/p", Op: OpPut, Class: "data"}})
	s2 := capture(t, db, "laptop", "index/1", OpPut)
	for _, f := range []string{"B", "C"} {
		db.StartPlacement(ctx, f, s1, time.Now()) // taken, so the delete follows it (#21)
	}
	s3 := capture(t, db, "laptop", "data/1", OpDelete)
	for _, place := range []string{"B", "C"} {
		var got []int64
		for {
			p, ok, err := db.NextPlacement(ctx, place)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			got = append(got, p.Seq)
			if err := db.AckPlacement(ctx, place, p.Seq, p.SHA256, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		if len(got) != 3 || got[0] != s1 || got[1] != s2 || got[2] != s3 {
			t.Fatalf("%s FIFO %v, want %d %d %d (pending %v placed nowhere)", place, got, s1, s2, s3, pending)
		}
	}
	// The index of acknowledged hashes (REPL-10).
	h, ok, err := db.AckedHash(ctx, "B", "laptop", "index/1")
	if err != nil || !ok || h != "h-index/1" {
		t.Fatalf("acked hash %q %v %v", h, ok, err)
	}
	// Acknowledging twice changes nothing and does not fail (a retry
	// after a crash between the send and the record).
	if err := db.AckPlacement(ctx, "B", s1, "h-data/1", time.Now()); err != nil {
		t.Fatal(err)
	}
}

// The FIFO head carries what the worker sends: bucket, key, op, size
// and the capture's hash.
func TestNextPlacementCarriesTheQueueRow(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s := capture(t, db, "laptop", "data/ab", OpPut)
	p, ok, err := db.NextPlacement(ctx, "B")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want := Placement{Seq: s, Place: "B", Bucket: "laptop", Key: "data/ab", Op: OpPut, Size: 7, SHA256: "h-data/ab", State: PlacementQueued}
	if p.Seq != want.Seq || p.Place != want.Place || p.Bucket != want.Bucket || p.Key != want.Key || p.Op != want.Op ||
		p.Size != want.Size || p.SHA256 != want.SHA256 || p.State != want.State || p.CreatedAt.IsZero() {
		t.Fatalf("head %+v", p)
	}
}

// A put that a later write or delete of the same key replaced is
// superseded: the store no longer holds its bytes, so it is cancelled
// rather than sent.
func TestSupersededPutIsCancelled(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	if sup, err := db.Superseded(ctx, s1); err != nil || sup {
		t.Fatal(sup, err)
	}
	// A later write still in flight does not supersede, but is a later
	// capture.
	pending, _ := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpPut, Class: "data"}})
	if sup, _ := db.Superseded(ctx, s1); sup {
		t.Fatal("superseded by a pending write")
	}
	if later, err := db.LaterCapture(ctx, s1); err != nil || !later {
		t.Fatal(later, err)
	}
	db.DropCaptures(ctx, pending...)
	if ok, err := db.StartPlacement(ctx, "B", s1, time.Now()); err != nil || !ok {
		t.Fatal(ok, err) // taken, so the delete follows it (#21)
	}
	s2 := capture(t, db, "laptop", "data/1", OpDelete)
	if sup, err := db.Superseded(ctx, s1); err != nil || !sup {
		t.Fatal(sup, err)
	}
	if err := db.CancelPlacement(ctx, "B", s1); err != nil {
		t.Fatal(err)
	}
	p, ok, _ := db.NextPlacement(ctx, "B")
	if !ok || p.Seq != s2 {
		t.Fatalf("head after cancel %+v", p)
	}
}

// EP-11, REPL-10: a refusal the place will repeat (immutability, a
// rejected hash) leaves the FIFO and stays visible.
func TestRefusedPlacementLeavesTheFIFO(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	s2 := capture(t, db, "laptop", "data/2", OpPut)
	if err := db.RefusePlacement(ctx, "B", s1, "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if p, _, _ := db.NextPlacement(ctx, "B"); p.Seq != s2 {
		t.Fatalf("head %+v", p)
	}
	st, err := db.PlaceStats(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st["B"].Refused != 1 || st["B"].Queued != 1 {
		t.Fatalf("stats %+v", st["B"])
	}
}

// REPL-3 (#50): an object the store cannot supply as captured leaves
// every place's FIFO at once and is counted against its bucket, not
// against any place.
func TestRefusedObjectCountsAgainstTheBucket(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	friend(t, db, "C")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	s2 := capture(t, db, "laptop", "data/2", OpPut)
	capture(t, db, "phone", "data/1", OpPut)
	if err := db.AckPlacement(ctx, "C", s1, "h-data/1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if refused, err := db.RefuseObject(ctx, s1, CauseStore, "gone"); err != nil || !refused {
		t.Fatal(refused, err)
	}
	// Another place's sender comes second: nothing left to refuse (#57).
	if refused, err := db.RefuseObject(ctx, s1, CauseStore, "gone"); err != nil || refused {
		t.Fatal(refused, err)
	}
	for _, place := range []string{"B", "C"} {
		if p, _, _ := db.NextPlacement(ctx, place); p.Seq != s2 {
			t.Fatalf("%s head %+v", place, p)
		}
	}
	st, err := db.PlaceStats(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if st["B"].Refused != 0 || st["C"].Refused != 0 {
		t.Fatalf("stats %+v", st)
	}
	r, err := db.RefusedObjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r["laptop"] != 1 || r["phone"] != 0 {
		t.Fatalf("refused objects %v", r)
	}
	// C's acknowledgement stands: it holds the object.
	if h, ok, _ := db.AckedHash(ctx, "C", "laptop", "data/1"); !ok || h != "h-data/1" {
		t.Fatalf("C's hash %q %v", h, ok)
	}
}

// `status` (#18): per place, the queued objects and bytes and the lag,
// the age of the oldest queued row; and the place's condition.
func TestPlaceStatsAndCondition(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	capture(t, db, "laptop", "data/22", OpPut)
	if _, err := db.StartPlacement(ctx, "B", s1, time.Now()); err != nil {
		t.Fatal(err) // taken, so the delete follows it (#21)
	}
	capture(t, db, "laptop", "data/1", OpDelete)
	later := time.Now().Add(90 * time.Second)
	st, err := db.PlaceStats(ctx, later)
	if err != nil {
		t.Fatal(err)
	}
	b := st["B"]
	if b.Queued != 3 || b.QueuedBytes != int64(len("data/1")+len("data/22")) || b.Lag < 89*time.Second {
		t.Fatalf("stats %+v", b)
	}
	now := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(ctx, "B", ConditionQuota, "QuotaExceeded", now); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "B", ConditionQuota, "QuotaExceeded again", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	cs, err := db.PlaceConditions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := cs["B"]
	if c.Condition != ConditionQuota || c.Detail != "QuotaExceeded again" || !c.Since.Equal(now) {
		t.Fatalf("condition %+v", c)
	}
	if err := db.SetPlaceCondition(ctx, "B", ConditionOK, "", now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	cs, _ = db.PlaceConditions(ctx)
	if c := cs["B"]; c.Condition != ConditionOK || !c.LastOK.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("condition %+v", c)
	}
}

// REPL-1, REPL-3 (#57, #61): an identical re-upload of an object whose
// last done row was refused somewhere, by a place or because the store
// could not supply it, is queued again where the refusal was, and acked
// at once where the place already holds the hash (EP-3); once it is
// done, the refusals of the older row no longer count. An ordinary
// retry of an object refused nowhere is still dropped (EP-3).
func TestIdenticalReuploadHealsARefusedObject(t *testing.T) {
	ctx := context.Background()
	for _, refuse := range []struct {
		name string
		fn   func(db *DB, seq int64) error
	}{
		{"place", func(db *DB, seq int64) error { return db.RefusePlacement(ctx, "B", seq, "BadDigest") }},
		{"store", func(db *DB, seq int64) error {
			_, err := db.RefuseObject(ctx, seq, CauseStore, "the store holds 0 bytes")
			return err
		}},
	} {
		t.Run(refuse.name, func(t *testing.T) {
			db := openTest(t)
			friend(t, db, "B")
			friend(t, db, "C")
			put := func() (int64, bool) {
				t.Helper()
				s, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpPut, Class: "data"}})
				if err != nil {
					t.Fatal(err)
				}
				dup, err := db.FinishPut(ctx, s[0], 6, "h-data/1")
				if err != nil {
					t.Fatal(err)
				}
				return s[0], dup
			}
			s1, _ := put()
			// Queued or acknowledged, an identical re-upload is a retry.
			if _, dup := put(); !dup {
				t.Fatal("a retry of a queued object was queued again")
			}
			if err := db.AckPlacement(ctx, "C", s1, "h-data/1", time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, dup := put(); !dup {
				t.Fatal("a retry of an acknowledged object was queued again")
			}
			if err := refuse.fn(db, s1); err != nil {
				t.Fatal(err)
			}
			if st, _ := db.PlaceStats(ctx, time.Now()); st["B"].Refused+mustRefused(t, db)["laptop"] != 1 {
				t.Fatalf("refusal not counted: %+v %v", st, mustRefused(t, db))
			}
			s2, dup := put()
			if dup {
				t.Fatal("the re-upload of a refused object was dropped")
			}
			if p, ok, _ := db.NextPlacement(ctx, "B"); !ok || p.Seq != s2 {
				t.Fatalf("B head %+v %v", p, ok)
			}
			// C holds the hash already: nothing is sent to it again.
			if p, ok, _ := db.NextPlacement(ctx, "C"); ok {
				t.Fatalf("C was queued %+v", p)
			}
			if h, ok, _ := db.AckedHash(ctx, "C", "laptop", "data/1"); !ok || h != "h-data/1" {
				t.Fatalf("C's hash %q %v", h, ok)
			}
			st, _ := db.PlaceStats(ctx, time.Now())
			if st["B"].Refused != 0 || mustRefused(t, db)["laptop"] != 0 {
				t.Fatalf("a healed refusal still counts: %+v %v", st, mustRefused(t, db))
			}
			// The new row is refused nowhere: the next identical upload is a retry.
			if _, dup := put(); !dup {
				t.Fatal("a retry after healing was queued again")
			}
		})
	}
}

// EP-3, REPL-1 (#61): a healing re-upload is sent only where the place
// does not hold the hash: where the older row is still queued (it may be
// in flight, and a row of another hash before it could still land), the
// re-upload is queued; where the place acknowledged the hash, it is
// acked at placement.
func TestHealingReuploadIsSentOnlyWhereTheHashIsNotHeld(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	for _, f := range []string{"B", "C", "D"} {
		friend(t, db, f)
	}
	// Three copies: the object is placed at every friend.
	if err := db.AddBucket(ctx, Bucket{Name: "laptop", Profile: "restic", Copies: 3, AccessKey: "AK", SecretKey: "s"}); err != nil {
		t.Fatal(err)
	}
	s1 := capture(t, db, "laptop", "data/1", OpPut)
	if err := db.RefusePlacement(ctx, "B", s1, "BadDigest"); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, "C", s1, "h-data/1", time.Now()); err != nil {
		t.Fatal(err)
	}
	// D has not been sent s1 yet.
	s2 := capture(t, db, "laptop", "data/1", OpPut)
	for _, tc := range []struct {
		place  string
		queued bool
	}{{"B", true}, {"C", false}, {"D", true}} {
		var state string
		if err := db.QueryRowContext(ctx, `SELECT state FROM placement WHERE place = ? AND seq = ?`, tc.place, s2).
			Scan(&state); err != nil {
			t.Fatalf("%s: %v", tc.place, err)
		}
		if want := map[bool]string{true: PlacementQueued, false: PlacementAcked}[tc.queued]; state != want {
			t.Fatalf("%s: %s, want %s", tc.place, state, want)
		}
	}
	var at sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT acked_at FROM placement WHERE place = 'C' AND seq = ?`, s2).Scan(&at); err != nil || !at.Valid {
		t.Fatalf("C's row has no acknowledgement time: %v %v", at, err)
	}
}

// EP-3, REPL-10 (#61): a healing re-upload is acked at placement only
// where the place acknowledged the very row it heals. An earlier
// acknowledgement of the same hash proves nothing: a row of other bytes
// sent since may have landed with its answer lost, and was then
// cancelled as superseded, leaving no trace.
func TestHealingDoesNotTrustAnOlderAcknowledgement(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "P")
	friend(t, db, "Q")
	put := func(hash string) int64 {
		t.Helper()
		s, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if dup, err := db.FinishPut(ctx, s[0], 1, hash); err != nil || dup {
			t.Fatalf("put %s: dup %v, %v", hash, dup, err)
		}
		return s[0]
	}
	ack := func(place string, seq int64, hash string) {
		t.Helper()
		if err := db.AckPlacement(ctx, place, seq, hash, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	s1 := put("H")
	ack("P", s1, "H")
	ack("Q", s1, "H")
	s2 := put("X")
	ack("Q", s2, "X") // P stored X, but its answer was lost: s2 stays queued there
	s3 := put("H")
	if err := db.CancelPlacement(ctx, "P", s2); err != nil { // superseded by s3
		t.Fatal(err)
	}
	if ok, err := db.RefuseObject(ctx, s3, CauseStore, "the store cannot supply it"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	s4 := put("H") // heals s3
	for _, place := range []string{"P", "Q"} {
		if p, ok, _ := db.NextPlacement(ctx, place); !ok || p.Seq != s4 {
			t.Fatalf("%s head %+v %v: the heal must be sent; P may hold X", place, p, ok)
		}
	}
}

// REPL-1 (#61): a row refused only for being larger than one PUT
// carries keeps that cause and counts against its bucket until
// multipart replication (#23) re-queues it; so is one refused before
// the cause was told apart (cause store, but larger than one PUT). One a
// later write replaced is not re-queued, nor is one refused for another
// store cause.
func TestOversizeRefusalsCanBeRequeued(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	friend(t, db, "C")
	const putLimit = 10 // capture's sizes are the key's length
	big := capture(t, db, "laptop", "data/big", OpPut)
	oldBig := capture(t, db, "laptop", "data/old-big", OpPut)
	gone := capture(t, db, "laptop", "data/gone", OpPut)
	replaced := capture(t, db, "laptop", "data/replaced", OpPut)
	for _, r := range []struct {
		seq   int64
		cause string
	}{{big, CauseOversize}, {oldBig, CauseStore}, {gone, CauseStore}, {replaced, CauseOversize}} {
		if ok, err := db.RefuseObject(ctx, r.seq, r.cause, "refused"); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if r := mustRefused(t, db); r["laptop"] != 4 {
		t.Fatalf("refused %v", r)
	}
	later := capture(t, db, "laptop", "data/replaced", OpPut)
	n, err := db.RequeueOversize(ctx, putLimit)
	if err != nil || n != 4 {
		t.Fatalf("requeued %d %v, want big and old-big at B and C", n, err)
	}
	for _, place := range []string{"B", "C"} {
		for _, want := range []int64{big, oldBig, later} {
			p, ok, _ := db.NextPlacement(ctx, place)
			if !ok || p.Seq != want {
				t.Fatalf("%s head %+v %v, want %d", place, p, ok, want)
			}
			if err := db.CancelPlacement(ctx, place, want); err != nil {
				t.Fatal(err)
			}
		}
	}
	if r := mustRefused(t, db); r["laptop"] != 1 {
		t.Fatalf("refused %v, want only data/gone", r)
	}
}

func mustRefused(t *testing.T, db *DB) map[string]int64 {
	t.Helper()
	r, err := db.RefusedObjects(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// #57, #61: a place's condition goes back to ok, without counting as an
// acknowledgement, when its FIFO empties while it waited on this node's
// store; a condition that is the place's own answer (retrying, quota,
// removed) stands until the place answers again.
func TestPlaceConditionResets(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0)
	for _, tc := range []struct {
		cond  string
		reset bool
	}{{ConditionStore, true}, {ConditionRetrying, false}, {ConditionQuota, false}, {ConditionRemoved, false}} {
		t.Run(tc.cond, func(t *testing.T) {
			db := openTest(t)
			if err := db.SetPlaceCondition(ctx, "B", ConditionOK, "", now); err != nil {
				t.Fatal(err)
			}
			if err := db.SetPlaceCondition(ctx, "B", tc.cond, "why", now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := db.ResetPlaceCondition(ctx, "B", now.Add(2*time.Hour)); err != nil {
				t.Fatal(err)
			}
			c, _ := db.PlaceConditions(ctx)
			b := c["B"]
			if !b.LastOK.Equal(now) {
				t.Fatalf("last ok moved: %+v", b)
			}
			if tc.reset && (b.Condition != ConditionOK || b.Detail != "" || !b.Since.Equal(now.Add(2*time.Hour))) {
				t.Fatalf("not reset: %+v", b)
			}
			if !tc.reset && (b.Condition != tc.cond || b.Detail != "why" || !b.Since.Equal(now.Add(time.Hour))) {
				t.Fatalf("the place's own answer was hidden: %+v", b)
			}
			// A place never contacted gets no condition.
			if err := db.ResetPlaceCondition(ctx, "C", now); err != nil {
				t.Fatal(err)
			}
			if c, _ := db.PlaceConditions(ctx); len(c) != 1 {
				t.Fatalf("conditions %+v", c)
			}
		})
	}
}
