package state

import (
	"context"
	"slices"
	"testing"
)

func (p *pool) restored(place, bucket string, objs ...Restored) RestoreResult {
	p.t.Helper()
	r, err := p.db.RecordRestored(context.Background(), place, bucket, objs)
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
	// tombstoned at recovery.
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
