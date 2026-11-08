package state

import (
	"context"
	"testing"
)

// capture captures a completed put of key with hash sha, rewritable or
// not, and returns its sequence and whether it was dropped as an
// identical re-upload.
func (p *pool) capture(bucket, key, class, sha string, rewritable bool) (int64, bool) {
	p.t.Helper()
	ctx := context.Background()
	seqs, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: bucket, Key: key, Op: OpPut, Class: class, Rewritable: rewritable}})
	if err != nil {
		p.t.Fatal(err)
	}
	dup, err := p.db.FinishPut(ctx, seqs[0], 100, sha)
	if err != nil {
		p.t.Fatal(err)
	}
	return seqs[0], dup
}

// rows counts row seq's placement rows in any state.
func (p *pool) rows(seq int64) int {
	p.t.Helper()
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM placement WHERE seq = ?`, seq).Scan(&n); err != nil {
		p.t.Fatal(err)
	}
	return n
}

// queuedRows counts the rows waiting to be sent anywhere.
func (p *pool) queuedRows() int {
	p.t.Helper()
	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM placement WHERE state = 'queued'`).Scan(&n); err != nil {
		p.t.Fatal(err)
	}
	return n
}

func (p *pool) changed() []ChangedKey {
	p.t.Helper()
	cs, err := p.db.ChangedKeys(context.Background())
	if err != nil {
		p.t.Fatal(err)
	}
	return cs
}

// EP-11 (#22): a put to a key a place acknowledged with another hash,
// the key not rewritable, is done (the local write stands) but placed
// nowhere, and ChangedKeys names the bucket, the key, the acknowledged
// hash and the new one. The callback hears of it once committed.
func TestAChangedAcknowledgedKeyGetsNoPlacement(t *testing.T) {
	p := newPool(t)
	p.bucket("files", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	var heard []ChangedKey
	p.db.KeyChanged = func(c ChangedKey) { heard = append(heard, c) }
	first, _ := p.capture("files", "doc", "data", "aaa", false)
	p.ackAll(first)
	second, dup := p.capture("files", "doc", "data", "bbb", false)
	if dup {
		t.Fatal("a changed key was dropped as a re-upload")
	}
	if n := p.rows(second); n != 0 {
		t.Fatalf("the changed key has %d placement rows", n)
	}
	var state, sha string
	if err := p.db.QueryRow(`SELECT state, sha256 FROM queue WHERE seq = ?`, second).Scan(&state, &sha); err != nil ||
		state != StateDone || sha != "bbb" {
		t.Fatalf("the local write: %s %s %v", state, sha, err)
	}
	cs := p.changed()
	if len(cs) != 1 || cs[0].Bucket != "files" || cs[0].Key != "doc" || cs[0].Seq != second ||
		cs[0].Acknowledged != "aaa" || cs[0].SHA256 != "bbb" || cs[0].Places["B"] != "aaa" || cs[0].Places["C"] != "aaa" {
		t.Fatalf("changed keys %+v", cs)
	}
	if len(heard) != 1 || heard[0].Key != "doc" || heard[0].Acknowledged != "aaa" || heard[0].SHA256 != "bbb" {
		t.Fatalf("callback heard %+v", heard)
	}
	// It is below copies, and stuck as immutable: nothing will bring
	// it offsite as things stand.
	o := p.offsite("files", 0)
	if o.Watermark != second-1 || o.Stuck.Objects != 1 || o.Stuck.Immutable != 1 || o.Stuck.Unplaced != 0 {
		t.Fatalf("offsite %+v", o)
	}
	if b, _ := p.db.BelowCopies(context.Background()); b["files"].Objects != 1 {
		t.Fatalf("below copies %+v", b)
	}
	// Writing the same changed bytes again is a retry of that write:
	// dropped, no transfer, still one changed key.
	if _, dup := p.capture("files", "doc", "data", "bbb", false); !dup || p.queuedRows() != 0 || len(p.changed()) != 1 {
		t.Fatalf("the changed bytes again: dup %v, queued %d, changed %+v", dup, p.queuedRows(), p.changed())
	}
}

// EP-11 (#22, decision): acknowledged at one place and only queued at
// another, the old version is still acknowledged, so the change is
// held back. The old version's queued row stays queued; the worker
// cancels it as superseded (the store no longer holds its bytes), so
// the place that never had the key gets neither version, and the key
// is stuck.
func TestAChangeOfAKeyAcknowledgedAtOnePlaceIsHeldBack(t *testing.T) {
	p := newPool(t)
	p.bucket("files", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	first, _ := p.capture("files", "doc", "data", "aaa", false)
	p.ackRow("B", first)
	second, _ := p.capture("files", "doc", "data", "bbb", false)
	if n := p.rows(second); n != 0 {
		t.Fatalf("the changed key has %d placement rows", n)
	}
	if sup, err := p.db.Superseded(context.Background(), first); err != nil || !sup {
		t.Fatalf("the old version's queued row at C is superseded: %v %v", sup, err)
	}
	cs := p.changed()
	if len(cs) != 1 || len(cs[0].Places) != 1 || cs[0].Places["B"] != "aaa" {
		t.Fatalf("changed keys %+v", cs)
	}
	// Queued only, nowhere acknowledged: no rule to break; the new
	// version replaces the old in the FIFOs as before.
	q1, _ := p.capture("files", "other", "data", "ccc", false)
	q2, _ := p.capture("files", "other", "data", "ddd", false)
	if p.rows(q2) != 2 || len(p.changed()) != 1 {
		t.Fatalf("a change of a key acknowledged nowhere: rows %d (%d), changed %+v", p.rows(q2), p.rows(q1), p.changed())
	}
}

// EP-11 (#22): identical re-uploads are silent: no alert, and nothing
// queued, also after a refusal heals at the places that acknowledged
// the bytes (#57, #61).
func TestIdenticalReuploadIsSilent(t *testing.T) {
	p := newPool(t)
	p.bucket("files", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	first, _ := p.capture("files", "doc", "data", "aaa", false)
	p.ackAll(first)
	if _, dup := p.capture("files", "doc", "data", "aaa", false); !dup || p.queuedRows() != 0 || len(p.changed()) != 0 {
		t.Fatalf("an identical re-upload: dup %v, queued %d, changed %+v", dup, p.queuedRows(), p.changed())
	}
	// Refused at C (a BadDigest for bytes that match): the re-upload
	// heals it, acked at once at B, sent to C only.
	other, _ := p.capture("files", "pack", "data", "ppp", false)
	p.ackRow("B", other)
	if err := p.db.RefusePlacement(context.Background(), "C", other, "BadDigest"); err != nil {
		t.Fatal(err)
	}
	heal, dup := p.capture("files", "pack", "data", "ppp", false)
	if dup || len(p.changed()) != 0 {
		t.Fatalf("healing: dup %v, changed %+v", dup, p.changed())
	}
	var queued []string
	rows, _ := p.db.Query(`SELECT place FROM placement WHERE seq = ? AND state = 'queued'`, heal)
	for rows.Next() {
		var s string
		rows.Scan(&s)
		queued = append(queued, s)
	}
	rows.Close()
	if len(queued) != 1 || queued[0] != "C" {
		t.Fatalf("the healing re-upload is queued at %v, want C only", queued)
	}
}

// EP-7, EP-11 (#22, #74): a rewritable key is exempt: a changed
// version of restic's config or Kopia's kopia.maintenance is placed
// like a new write, and no changed key is reported.
func TestARewritableKeyIsPlacedAsARewrite(t *testing.T) {
	p := newPool(t)
	p.bucket("vault", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	for _, key := range []string{"config", "kopia.maintenance"} {
		first, _ := p.capture("vault", key, "metadata", "v1-"+key, true)
		p.ackAll(first)
		second, dup := p.capture("vault", key, "metadata", "v2-"+key, true)
		if dup || p.rows(second) != 2 {
			t.Fatalf("%s rewritten: dup %v, rows %d", key, dup, p.rows(second))
		}
		p.ackAll(second)
	}
	if cs := p.changed(); len(cs) != 0 {
		t.Fatalf("changed keys %+v", cs)
	}
	if o := p.offsite("vault", 0); !o.Complete() {
		t.Fatalf("offsite %+v", o)
	}
}

// EP-11 (#22, decision): the condition clears when the key's latest
// write no longer changes what a place acknowledged: written back to
// the acknowledged bytes (acked at once where they are held, so no
// transfer), or deleted. A write after the delete is a new object,
// placed as usual; each lender keeps the old version as a tombstone.
func TestAChangedKeyClearsWhenRevertedOrDeleted(t *testing.T) {
	p := newPool(t)
	p.bucket("files", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	first, _ := p.capture("files", "doc", "data", "aaa", false)
	p.ackAll(first)
	p.capture("files", "doc", "data", "bbb", false)
	back, dup := p.capture("files", "doc", "data", "aaa", false)
	if dup {
		t.Fatal("the write back was dropped: the latest write would still be the changed one")
	}
	if cs := p.changed(); len(cs) != 0 {
		t.Fatalf("after writing the acknowledged bytes back: %+v", cs)
	}
	if p.queuedRows() != 0 || len(p.places(back)) != 2 {
		t.Fatalf("the write back: queued %d, places %v", p.queuedRows(), p.places(back))
	}
	if o := p.offsite("files", 0); !o.Complete() {
		t.Fatalf("offsite after the write back %+v", o)
	}

	p.capture("files", "doc", "data", "bbb", false)
	if len(p.changed()) != 1 {
		t.Fatal("changed again: not reported")
	}
	del := p.del("files", "doc", "data")
	if cs := p.changed(); len(cs) != 0 {
		t.Fatalf("after the delete: %+v", cs)
	}
	p.ackAll(del)
	fresh, _ := p.capture("files", "doc", "data", "bbb", false)
	if p.rows(fresh) != 2 || len(p.changed()) != 0 {
		t.Fatalf("a write after the delete: rows %d, changed %+v", p.rows(fresh), p.changed())
	}
	// Even before the delete reaches the places: the delete is ahead of
	// the new write in each FIFO.
	p.ackAll(fresh)
	p.del("files", "doc", "data")
	again, _ := p.capture("files", "doc", "data", "ccc", false)
	if p.rows(again) != 2 || len(p.changed()) != 0 {
		t.Fatalf("a write behind an unsent delete: rows %d, changed %+v", p.rows(again), p.changed())
	}
}

// EP-11, LEND-4 (#22): a lender's 412 to a key that is not rewritable
// is the same condition: ChangedKeys names it, with the hash the place
// said it holds and the place that said so (its word, not an
// acknowledgement). It still counts as the place's refusal
// (RefusedAtPlaces, so place_refused names the friend, which may be
// broken or hostile), not against the bucket (RefusedObjects). The same
// bytes again do not heal it (the place would refuse them again):
// dropped as a retry.
func TestALendersImmutabilityRefusalIsAChangedKey(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.bucket("files", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	seq, _ := p.capture("files", "doc", "data", "bbb", false)
	p.ackRow("C", seq)
	if err := p.db.RefuseChanged(ctx, "B", seq, "aaa", "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	cs := p.changed()
	if len(cs) != 1 || cs[0].Key != "doc" || cs[0].SHA256 != "bbb" || cs[0].Acknowledged != "aaa" ||
		len(cs[0].Places) != 1 || cs[0].Places["B"] != "aaa" || len(cs[0].ReportedBy) != 1 || cs[0].ReportedBy[0] != "B" {
		t.Fatalf("changed keys %+v", cs)
	}
	if r, _ := p.db.RefusedAtPlaces(ctx); r["B"] != 1 {
		t.Fatalf("place_refused does not count it: %v", r)
	}
	if r, _ := p.db.RefusedObjects(ctx); r["files"] != 0 {
		t.Fatalf("counted against the bucket: %v", r)
	}
	if st, _ := p.db.PlaceStats(ctx, p.now); st["B"].Refused != 1 {
		t.Fatalf("place stats %+v", st["B"])
	}
	if o := p.offsite("files", 0); o.Stuck.Immutable != 1 || o.Stuck.Refused != 0 {
		t.Fatalf("offsite %+v", o)
	}
	if _, dup := p.capture("files", "doc", "data", "bbb", false); !dup || p.queuedRows() != 0 {
		t.Fatalf("the same bytes again: dup %v, queued %d", dup, p.queuedRows())
	}
	// Unknown hash: the place answered 412 but the worker could not
	// learn what it holds.
	other, _ := p.capture("files", "other", "data", "xxx", false)
	if err := p.db.RefuseChanged(ctx, "B", other, "", "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if cs := p.changed(); len(cs) != 2 || cs[1].Key != "other" || cs[1].Acknowledged != "" {
		t.Fatalf("changed keys %+v", cs)
	}
}

// EP-11, ID-5 (#22, decision): after `recover`, the placement rebuilt
// from the friends' lists is acknowledged with their hashes, so the
// node's first write of such a key with other bytes is a changed key,
// as it would be on the old node; the friends would refuse it (LEND-4)
// in any case. The same bytes are a silent re-upload.
func TestAChangedWriteOfARecoveredKeyIsHeldBack(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	for _, place := range []string{"B", "C"} {
		list(t, db, place, "laptop",
			Listed{Key: "data/1", Size: 5, SHA256: "aaa", Class: "data"},
			Listed{Key: "data/2", Size: 5, SHA256: "ppp", Class: "data"})
	}
	put := func(key, sha string) (int64, bool) {
		seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: key, Op: OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		dup, err := db.FinishPut(ctx, seqs[0], 5, sha)
		if err != nil {
			t.Fatal(err)
		}
		return seqs[0], dup
	}
	if _, dup := put("data/2", "ppp"); !dup {
		t.Fatal("the recovered bytes again were not a silent re-upload")
	}
	put("data/1", "bbb")
	cs, err := db.ChangedKeys(ctx)
	if err != nil || len(cs) != 1 || cs[0].Key != "data/1" || cs[0].Acknowledged != "aaa" || len(cs[0].Places) != 2 ||
		len(cs[0].ReportedBy) != 0 {
		t.Fatalf("changed keys %+v %v", cs, err)
	}
}

// EP-11, LEND-4, RET-1 (#22 review): after a delete a place
// acknowledged, a write of the key with other bytes is a new object on
// the borrower's side, but the lender keeps the deleted version in a
// tomb and refuses other bytes over it unless they are marked as
// replacing it. ReplacedHash names that version for any key: the
// place's last acknowledged put before its acknowledged delete, or the
// hash it reported holding when it refused a put (412) before that
// delete. With no acknowledged delete since, or nothing before it,
// there is nothing to name.
func TestReplacedHashNamesTheVersionADeleteTombed(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.bucket("files", 1)
	p.friend("B", 100)
	replaced := func(key string, seq int64) (string, bool) {
		t.Helper()
		h, ok, err := p.db.ReplacedHash(ctx, "B", "files", key, seq)
		if err != nil {
			t.Fatal(err)
		}
		return h, ok
	}
	first, _ := p.capture("files", "doc", "data", "aaa", false)
	if h, ok := replaced("doc", first); ok {
		t.Fatalf("a first write names %q", h)
	}
	p.ackAll(first)
	del := p.del("files", "doc", "data")
	second, _ := p.capture("files", "doc", "data", "bbb", false)
	if p.rows(second) != 1 || len(p.changed()) != 0 {
		t.Fatalf("a write after the delete: rows %d, changed %+v", p.rows(second), p.changed())
	}
	if h, ok := replaced("doc", second); ok {
		t.Fatalf("named %q while the delete is unacknowledged", h)
	}
	p.ackAll(del)
	if h, ok := replaced("doc", second); !ok || h != "aaa" {
		t.Fatalf("after the acknowledged delete: %q %v", h, ok)
	}

	// The place refused a put as holding other bytes, then took the
	// delete: its tomb holds what it reported.
	r, _ := p.capture("files", "theirs", "data", "ours", false)
	if err := p.db.RefuseChanged(ctx, "B", r, "theirs-hash", "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	d2 := p.del("files", "theirs", "data")
	p.ackAll(d2)
	again, _ := p.capture("files", "theirs", "data", "ours", false)
	if h, ok := replaced("theirs", again); !ok || h != "theirs-hash" {
		t.Fatalf("after a refused put and a delete: %q %v", h, ok)
	}
	if cs := p.changed(); len(cs) != 0 {
		t.Fatalf("changed keys %+v", cs)
	}
}
