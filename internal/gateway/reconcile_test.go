package gateway

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// drop writes a file into bucket's directory behind the daemon's back.
func (h *harness) drop(bucket, key string, body []byte) {
	h.t.Helper()
	p := filepath.Join(h.store, bucket, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// remove deletes key's file from bucket's directory behind the daemon's
// back.
func (h *harness) remove(bucket, key string) {
	h.t.Helper()
	if err := os.Remove(filepath.Join(h.store, bucket, filepath.FromSlash(key))); err != nil {
		h.t.Fatal(err)
	}
}

// reconciler is a store reconciler on the running gateway's decorator,
// with settings s.
func (h *harness) reconciler(s ReconcileSettings) *reconciler {
	h.t.Helper()
	d, ok := h.gw.running.Load().Next.(*Decorator)
	if !ok {
		h.t.Fatal("no decorator")
	}
	return &reconciler{d: d, storeDir: h.store, ReconcileSettings: s, log: quiet, now: time.Now}
}

func (h *harness) pass(r *reconciler) {
	h.t.Helper()
	if err := r.pass(context.Background(), nil); err != nil {
		h.t.Fatal(err)
	}
}

// reconciled returns bucket's reconciled rows as key → op.
func (h *harness) reconciled(bucket string) map[string]string {
	h.t.Helper()
	rows, err := h.db.Query(`SELECT key, op FROM queue WHERE bucket = ? AND reconciled = 1 AND state = 'done'`, bucket)
	if err != nil {
		h.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, op string
		rows.Scan(&k, &op)
		out[k] = op
	}
	return out
}

func (h *harness) putAll(bucket string, keys ...string) {
	h.t.Helper()
	c := h.client(bucket)
	for _, k := range keys {
		if _, err := c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(k),
			Body: bytes.NewReader([]byte("bytes of " + k))}); err != nil {
			h.t.Fatal(err)
		}
	}
}

func dataKeys(n int) []string {
	var ks []string
	for i := range n {
		ks = append(ks, fmt.Sprintf("data/%02d", i))
	}
	return ks
}

// EP-14 (#8): files dropped into a bucket's directory while the daemon
// was stopped are hashed and enqueued as writes in their profile's
// class, rewritable keys marked so; local-only keys and lendisk's and
// versitygw's housekeeping are not; lent buckets are never looked at.
// Each is counted once, however many passes follow.
func TestDroppedFilesEnqueuedAsWritesInTheirClass(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	pack := bytes.Repeat([]byte("dropped pack "), 10_000)
	h.drop("laptop", "data/ab/pack", pack)
	h.drop("laptop", "config", []byte("config"))
	h.drop("laptop", "snapshots/s1", []byte("snap"))
	h.drop("laptop", "locks/l1", []byte("lock"))
	h.drop("lent-0123456789abcdef-sealed", "data/x", []byte("a friend's"))
	h.serve(false)
	r := h.reconciler(ReconcileSettings{})
	h.pass(r)
	h.pass(r)

	got := map[string]state.Entry{}
	for _, e := range h.entries("laptop") {
		got[e.Key] = e
	}
	if len(got) != 3 {
		t.Fatalf("queued %v; want data/ab/pack, config and snapshots/s1", got)
	}
	if e := got["data/ab/pack"]; e.Op != state.OpPut || e.Class != "data" || e.SHA256 != sum(pack) ||
		e.Size != int64(len(pack)) || e.State != state.StateDone || e.Rewritable {
		t.Fatalf("data/ab/pack %+v", e)
	}
	if e := got["config"]; e.Class != "metadata" || !e.Rewritable {
		t.Fatalf("config %+v; want metadata, rewritable", e)
	}
	if e := got["snapshots/s1"]; e.Class != "snapshot" {
		t.Fatalf("snapshots/s1 %+v", e)
	}
	if len(h.entries("lent-0123456789abcdef-sealed")) != 0 {
		t.Fatal("a lent bucket was reconciled")
	}
	st, err := h.db.ReconcileState(context.Background(), "laptop")
	if err != nil || st.Writes != 3 || st.Deletes != 0 || st.PassedAt.IsZero() {
		t.Fatalf("reconcile state %+v %v", st, err)
	}
	// Placed like any capture: queued at the bucket's places.
	if _, q := h.queueRows(); q != 0 {
		// No friend yet: nothing to place. Placement itself is FinishPut's.
		t.Fatalf("%d placements with no friend", q)
	}
	if _, err := os.Stat(filepath.Join(h.store, HousekeepingDir, StoreMarker)); err != nil {
		t.Fatalf("no store marker after the first pass: %v", err)
	}
}

// EP-14 (#8): an object removed from disk behind the daemon's back is
// enqueued as a delete, placed where the key was, and counted. A key
// the node never held locally (recovered, not read yet) is no delete;
// once the read-through fetched it, it is held, and its loss is one.
func TestMissingObjectEnqueuedAsDeleteOnlyIfHeldLocally(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {"data/r1": []byte("r1"), "data/r2": []byte("r2")}})
	h.fetch = &fakeFetch{held: map[string][]byte{"B data/r1": []byte("r1")}}
	h.serve(false)
	h.putAll("laptop", dataKeys(20)...)
	h.remove("laptop", "data/03")
	r := h.reconciler(ReconcileSettings{})
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 1 || got["data/03"] != state.OpDelete {
		t.Fatalf("reconciled %v; want data/03's delete only", got)
	}
	k, _ := h.db.KeyNow(context.Background(), "laptop", "data/03")
	if k.Indexed() {
		t.Fatal("data/03 still indexed after its reconciled delete")
	}

	// Read r1 through, then lose it: a delete now.
	if b, err := getBody(t, h.client("laptop"), "data/r1", ""); err != nil || string(b) != "r1" {
		t.Fatalf("read-through %q %v", b, err)
	}
	h.remove("laptop", "data/r1")
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 2 || got["data/r1"] != state.OpDelete || got["data/r2"] != "" {
		t.Fatalf("reconciled %v; want data/r1's delete, never data/r2's", got)
	}
	st, _ := h.db.ReconcileState(context.Background(), "laptop")
	if st.Deletes != 2 || st.Writes != 0 || st.Held != 0 {
		t.Fatalf("state %+v", st)
	}
}

// ID-5, ID-6, EP-14 (#8): the safety case. A recovered node's store is
// empty by design and fills only through the read-through; a pass, as
// many as run, deletes nothing and holds nothing, so no friend is told
// to delete the user's backup.
func TestRecoveredNodeDeletesNothingItNeverHeld(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	objs := map[string][]byte{}
	for _, k := range dataKeys(50) {
		objs[k] = []byte("bytes of " + k)
	}
	objs["config"] = []byte("config")
	h.recovered(map[string]map[string][]byte{"B": objs, "C": objs})
	held := map[string][]byte{}
	for k, b := range objs {
		held["B "+k] = b
	}
	h.fetch = &fakeFetch{held: held}
	h.serve(false)
	var alerted atomic.Int32
	r := h.reconciler(ReconcileSettings{Alerted: func() { alerted.Add(1) }})
	rows, _ := h.queueRows()
	h.pass(r)
	// restic restore reads a few objects through.
	for _, k := range []string{"config", "data/07"} {
		if _, err := getBody(t, h.client("laptop"), k, ""); err != nil {
			t.Fatal(err)
		}
	}
	h.pass(r)
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a recovered node reconciled %v", got)
	}
	if n, _ := h.queueRows(); n != rows {
		t.Fatalf("queue rows %d, had %d", n, rows)
	}
	st, _ := h.db.ReconcileState(context.Background(), "laptop")
	if st.Held != 0 || alerted.Load() != 0 {
		t.Fatalf("held %d, alerted %d", st.Held, alerted.Load())
	}
	for _, k := range []string{"config", "data/07"} {
		if k, _ := h.db.KeyNow(context.Background(), "laptop", k); !k.Local {
			t.Fatalf("%v read through but not held", k)
		}
	}
}

// EP-14 (#8): a pass that finds more than the delete limit's share of a
// bucket's locally held objects missing (an emptied or unmounted bucket
// directory) enqueues no delete, holds them with an alert, and goes on
// holding until the user allows at least as many; a missing bucket
// directory deletes nothing either.
func TestMassDeleteHeldUntilAllowed(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic", "work": "restic"})
	h.serve(false)
	keys := dataKeys(20)
	h.putAll("laptop", keys...)
	h.putAll("work", "data/w1")
	var alerted atomic.Int32
	r := h.reconciler(ReconcileSettings{Alerted: func() { alerted.Add(1) }})
	h.pass(r)
	for _, k := range keys[:3] { // 15% > 10%
		h.remove("laptop", k)
	}
	if err := os.RemoveAll(filepath.Join(h.store, "work")); err != nil {
		t.Fatal(err)
	}
	h.pass(r)
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("reconciled %v past the limit", got)
	}
	if got := h.reconciled("work"); len(got) != 0 {
		t.Fatalf("reconciled %v with the bucket directory gone", got)
	}
	st, _ := h.db.ReconcileState(context.Background(), "laptop")
	if st.Held != 3 || st.HeldOf != 20 || st.HeldAt.IsZero() || alerted.Load() == 0 {
		t.Fatalf("state %+v, alerted %d", st, alerted.Load())
	}
	if _, err := h.db.AllowDeletes(context.Background(), "laptop", 3); err != nil {
		t.Fatal(err)
	}
	// A pass of the allowed buckets only, as the loop's poll runs.
	if err := r.pass(context.Background(), []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	if got := h.reconciled("laptop"); len(got) != 3 {
		t.Fatalf("reconciled %v after the allowance", got)
	}
	st, _ = h.db.ReconcileState(context.Background(), "laptop")
	if st.Held != 0 || st.Allowed != 0 || st.Deletes != 3 {
		t.Fatalf("state after the allowance %+v", st)
	}
	// A limit of 100 never holds.
	for _, k := range keys[3:] {
		h.remove("laptop", k)
	}
	h.pass(h.reconciler(ReconcileSettings{DeleteLimit: 100}))
	if got := h.reconciled("laptop"); len(got) != 20 {
		t.Fatalf("reconciled %d with a limit of 100", len(got))
	}
}

// EP-14 (#8), after Syncthing's folder marker: once a pass has made the
// store's marker, a store without it (unmounted, or another directory
// in its place) is not reconciled at all, and the alert says so; the
// marker back, passes resume.
func TestMissingStoreMarkerStopsReconciling(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.putAll("laptop", dataKeys(20)...)
	var alerted atomic.Int32
	r := h.reconciler(ReconcileSettings{Alerted: func() { alerted.Add(1) }})
	h.pass(r)
	marker := filepath.Join(h.store, HousekeepingDir, StoreMarker)
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	h.remove("laptop", "data/01")
	h.drop("laptop", "data/new", []byte("new"))
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("reconciled %v without the marker", got)
	}
	if v, _, _ := h.db.GetNode(context.Background(), MarkerMissingNode); v != marker || alerted.Load() != 1 {
		t.Fatalf("marker missing %q, alerted %d", v, alerted.Load())
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the pass made the marker again")
	}
	if err := os.WriteFile(marker, nil, 0o600); err != nil { // the user's touch
		t.Fatal(err)
	}
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 2 {
		t.Fatalf("reconciled %v with the marker back", got)
	}
	if v, _, _ := h.db.GetNode(context.Background(), MarkerMissingNode); v != "" {
		t.Fatalf("marker still missing: %q", v)
	}
}

// EP-14, EP-3 (#8): a key with a capture in flight (a pending row) is
// the capture's: its object is not enqueued as a write while versitygw
// writes it, nor its absence as a delete.
func TestKeysWithACaptureInFlightAreLeftAlone(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.putAll("laptop", dataKeys(20)...)
	ctx := context.Background()
	if _, err := h.db.BeginCaptures(ctx, []state.Capture{
		{Bucket: "laptop", Key: "data/inflight", Op: state.OpPut, Class: "data", Token: "t"},
		{Bucket: "laptop", Key: "data/05", Op: state.OpDelete, Class: "data"},
	}); err != nil {
		t.Fatal(err)
	}
	h.drop("laptop", "data/inflight", []byte("being written"))
	h.remove("laptop", "data/05")
	h.pass(h.reconciler(ReconcileSettings{}))
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("reconciled %v under captures in flight", got)
	}
}

// EP-14 (#8): a pass pages through the store's listing and the index,
// a page of each at a time, never the whole bucket: with pages of 3 it
// finds every write and delete among keys on both sides of each page's
// edge, and asks the store for no more than 3 keys at once.
func TestPassPagesThroughListingAndIndex(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	keys := dataKeys(30)
	h.putAll("laptop", keys...)
	h.recovered(map[string]map[string][]byte{"B": {"data/05x": []byte("r"), "data/29x": []byte("r"), "index/zz": []byte("r")}})
	r := h.reconciler(ReconcileSettings{Page: 3, DeleteLimit: 100})
	var most atomic.Int32
	r.listed = func(n int32) {
		if n > most.Load() {
			most.Store(n)
		}
	}
	h.pass(r)
	for _, k := range []string{"data/00", "data/10", "data/11", "data/29"} {
		h.remove("laptop", k)
	}
	for _, k := range []string{"data/055", "data/115", "data/295", "keys/k"} {
		h.drop("laptop", k, []byte(k))
	}
	h.pass(r)
	got := h.reconciled("laptop")
	want := map[string]string{"data/00": "delete", "data/10": "delete", "data/11": "delete", "data/29": "delete",
		"data/055": "put", "data/115": "put", "data/295": "put", "keys/k": "put"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reconciled %v\nwant %v", got, want)
	}
	if most.Load() == 0 || most.Load() > 3 {
		t.Fatalf("asked the store for %d keys at once", most.Load())
	}
}
