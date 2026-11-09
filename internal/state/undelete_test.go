package state

import (
	"context"
	"slices"
	"testing"
	"time"
)

// confirmed is an undelete request of bucket previewed with keys, as
// the user confirmed it.
func (p *pool) confirmed(bucket string, keys ...string) int64 {
	p.t.Helper()
	ctx := context.Background()
	id, err := p.db.RequestUndelete(ctx, bucket, time.Time{}, false, p.now)
	if err != nil {
		p.t.Fatal(err)
	}
	if err := p.db.PreviewUndelete(ctx, id, "{}", keys, p.now); err != nil {
		p.t.Fatal(err)
	}
	if err := p.db.ConfirmUndelete(ctx, id, p.now); err != nil {
		p.t.Fatal(err)
	}
	return id
}

func (p *pool) restored(place, bucket string, objs ...Restored) RestoreResult {
	p.t.Helper()
	r, err := p.db.RecordRestored(context.Background(), place, bucket, p.request, objs)
	if err != nil {
		p.t.Fatal(err)
	}
	return r
}

func (p *pool) sourcePlaces(bucket, key string) (string, []string) {
	p.t.Helper()
	s, ok, err := p.db.Source(context.Background(), bucket, key)
	if err != nil {
		p.t.Fatal(err)
	}
	if !ok {
		return "", nil
	}
	var out []string
	for _, pl := range s.Places {
		out = append(out, pl.Place)
	}
	slices.Sort(out)
	return s.SHA256, out
}

// RET-3 (#32): what a friend's undelete restored joins the index as a
// done put of that version, acknowledged at that friend, so the
// read-through fetches it and the watermark counts it; a second friend
// restoring the same version adds its acknowledgement. The local store
// is not refilled, so the rows are not held locally (queue.local) and
// the store reconciler does not take their absence for a delete
// (EP-14). A key whose latest write is this node's own other version
// keeps it (EP-11: the local store is the user's), and a key being
// written now is left to that write.
func TestRestoredObjectsJoinTheIndex(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	size := unit - ObjectOverhead
	obj := func(key, class string) Restored {
		return Restored{Key: key, Size: size, SHA256: "h-" + key, Class: class}
	}
	// The attacker's prune: a pack and a snapshot, replicated, deleted.
	for _, k := range []struct{ key, class string }{{"data/1", "data"}, {"snapshots/s", "snapshot"}} {
		p.ackAll(p.put("laptop", k.key, k.class, 1))
		p.ackAll(p.del("laptop", k.key, k.class))
	}
	// A pack this node never saw: a recovered node's, every copy
	// tombstoned at recovery, which the confirmed preview listed.
	p.request = p.confirmed("laptop", "data/1", "snapshots/s", "data/4")
	r := p.restored("B", "laptop", obj("data/1", "data"), obj("snapshots/s", "snapshot"), obj("data/4", "data"))
	if r.Recorded != 3 || r.Acked != 0 || len(r.Kept) != 0 || len(r.Busy) != 0 {
		t.Fatalf("restored at B: %+v", r)
	}
	if h, ps := p.sourcePlaces("laptop", "data/1"); h != "h-data/1" || !slices.Equal(ps, []string{"B"}) {
		t.Fatalf("data/1 fetched as %q from %v", h, ps)
	}
	if o := p.offsite("laptop", 0); o.Complete() {
		t.Fatalf("restored at one place of two, offsite: %+v", o)
	}
	r = p.restored("C", "laptop", obj("data/1", "data"), obj("snapshots/s", "snapshot"), obj("data/4", "data"))
	if r.Recorded != 0 || r.Acked != 3 {
		t.Fatalf("restored at C: %+v", r)
	}
	if _, ps := p.sourcePlaces("laptop", "snapshots/s"); !slices.Equal(ps, []string{"B", "C"}) {
		t.Fatalf("snapshots/s held at %v", ps)
	}
	if o := p.offsite("laptop", 0); !o.Complete() || o.Snapshot.Key != "snapshots/s" {
		t.Fatalf("restored at both places, offsite: %+v", o)
	}
	// Not held locally: the store lacks them until read through.
	if got := indexed(t, p.db, "laptop"); len(got) != 3 || got["data/1"] || got["data/4"] || got["snapshots/s"] {
		t.Fatalf("indexed %v", got)
	}
	// Restoring again changes nothing.
	if r := p.restored("C", "laptop", obj("data/1", "data")); r.Recorded != 0 || r.Acked != 1 {
		t.Fatalf("again: %+v", r)
	}

	// This node wrote another version since: its own wins (EP-11).
	p.ackAll(p.put("laptop", "config", "metadata", 1))
	p.ackAll(p.del("laptop", "config", "metadata"))
	ours := p.rewrite("laptop", "config", "metadata", 1)
	r = p.restored("B", "laptop", obj("config", "metadata"))
	if r.Recorded != 0 || len(r.Kept) != 1 || r.Kept[0] != (KeptVersion{Key: "config", SHA256: "h-config", Ours: "h2-config"}) {
		t.Fatalf("restored over this node's version: %+v", r)
	}
	if h, _ := p.sourcePlaces("laptop", "config"); h != "h2-config" {
		t.Fatalf("config counts as %q", h)
	}
	var latest int64
	if err := p.db.QueryRowContext(ctx, `SELECT max(seq) FROM queue WHERE key = 'config'`).Scan(&latest); err != nil || latest != ours {
		t.Fatalf("config's latest row %d, ours %d (%v)", latest, ours, err)
	}

	// The same version still waiting to be sent to B: acknowledged now.
	five := p.put("laptop", "data/5", "data", 1)
	if r := p.restored("B", "laptop", obj("data/5", "data")); r.Acked != 1 || r.Recorded != 0 {
		t.Fatalf("restored a queued version: %+v", r)
	}
	var state string
	if err := p.db.QueryRowContext(ctx, `SELECT state FROM placement WHERE seq = ? AND place = 'B'`, five).Scan(&state); err != nil ||
		state != PlacementAcked {
		t.Fatalf("B's row of data/5: %q %v", state, err)
	}

	// A write of the key in flight: left to it.
	if _, err := p.db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/6", Op: OpPut, Class: "data"}}); err != nil {
		t.Fatal(err)
	}
	if r := p.restored("B", "laptop", obj("data/6", "data")); len(r.Busy) != 1 || r.Busy[0] != "data/6" || r.Recorded != 0 {
		t.Fatalf("restored a key being written: %+v", r)
	}
}

// RET-3 (#32 review): a friend cannot add keys this node never had. A
// restored key is taken only if this node's latest write of it is a
// delete, or, with no history of it here (a recovered node), if the
// preview the user confirmed listed it; any other is reported unknown
// and recorded nowhere.
func TestAFriendCannotAddKeysByUndelete(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.request = p.confirmed("laptop", "data/listed")
	obj := func(key string) Restored { return Restored{Key: key, Size: 1, SHA256: "h-" + key, Class: "data"} }
	r := p.restored("B", "laptop", obj("data/listed"), obj("data/made-up"))
	if r.Recorded != 1 || len(r.Unknown) != 1 || r.Unknown[0] != "data/made-up" {
		t.Fatalf("restored %+v", r)
	}
	if h, _ := p.sourcePlaces("laptop", "data/made-up"); h != "" {
		t.Fatalf("a made-up key counts as %q", h)
	}
	// Another request's preview does not vouch for it either.
	p.request = p.confirmed("laptop")
	if r := p.restored("B", "laptop", obj("data/made-up")); len(r.Unknown) != 1 {
		t.Fatalf("under another request: %+v", r)
	}
}

// RET-3 (#32 review): a key a friend lists live whose delete it
// acknowledged here was revived by an undelete whose answer was lost: it
// is adopted as restored at that friend, as is a key with no history
// here that the confirmed preview listed. A delete not yet acknowledged
// there, a key this node holds in another version, and a made-up key
// are left alone.
func TestOrphansOfALostAnswerAreAdopted(t *testing.T) {
	p := newPool(t)
	p.bucket("laptop", 2)
	p.friend("B", 100)
	p.friend("C", 100)
	obj := func(key string) Restored { return Restored{Key: key, Size: 1, SHA256: "h-" + key, Class: "data"} }
	p.ackAll(p.put("laptop", "data/orphan", "data", 1))
	p.ackAll(p.del("laptop", "data/orphan", "data"))
	p.ackAll(p.put("laptop", "data/pending", "data", 1))
	del := p.del("laptop", "data/pending", "data") // queued at B and C, not sent
	p.request = p.confirmed("laptop", "data/recovered")
	a, err := p.db.AdoptRestored(context.Background(), "B", "laptop", p.request,
		[]Restored{obj("data/orphan"), obj("data/pending"), obj("data/recovered"), obj("data/made-up")})
	if err != nil {
		t.Fatal(err)
	}
	if a != 2 {
		t.Fatalf("adopted %d", a)
	}
	if _, ps := p.sourcePlaces("laptop", "data/orphan"); !slices.Equal(ps, []string{"B"}) {
		t.Fatalf("the orphan held at %v", ps)
	}
	if _, ps := p.sourcePlaces("laptop", "data/recovered"); !slices.Equal(ps, []string{"B"}) {
		t.Fatalf("the recovered key held at %v", ps)
	}
	for _, k := range []string{"data/pending", "data/made-up"} {
		if h, _ := p.sourcePlaces("laptop", k); h != "" {
			t.Fatalf("%s adopted as %q", k, h)
		}
	}
	if got := p.places(del); !slices.Equal(got, []string{"B", "C"}) {
		t.Fatalf("the pending delete is placed at %v", got)
	}
}
