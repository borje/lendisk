package gateway

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// deletes counts bucket's reconciled deletes.
func (h *harness) deletes(bucket string) int {
	n := 0
	for _, op := range h.reconciled(bucket) {
		if op == state.OpDelete {
			n++
		}
	}
	return n
}

// EP-14 (#8 review): the second walk is capped at what the first found
// within the limit. Objects removed between the walks (the first may
// spend hours hashing dropped files) are held, not deleted: 1 of 20
// missing at the first walk, all 20 at the second, gives one delete and
// 19 held with the alert.
func TestDeletesBetweenTheWalksAreHeld(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	keys := dataKeys(20)
	h.putAll("laptop", keys...)
	h.remove("laptop", keys[0])
	var alerted atomic.Int32
	r := h.reconciler(ReconcileSettings{Alerted: func() { alerted.Add(1) }})
	var calls atomic.Int32
	r.listed = func(int32) {
		if calls.Add(1) == 2 { // the second walk's first page
			for _, k := range keys[1:] {
				h.remove("laptop", k)
			}
		}
	}
	h.pass(r)
	if n := h.deletes("laptop"); n > 1 {
		t.Fatalf("%d deletes; the first walk found 1 missing", n)
	}
	st, _ := h.db.ReconcileState(context.Background(), "laptop")
	if st.Held < 19 || alerted.Load() == 0 {
		t.Fatalf("state %+v, alerted %d; want the rest held", st, alerted.Load())
	}
}

// EP-14 (#8 review): an allowance of N never deletes more than N, even
// when more go missing between the walks of the allowed pass.
func TestAllowanceNeverDeletesMoreThanAllowed(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	keys := dataKeys(20)
	h.putAll("laptop", keys...)
	for _, k := range keys[:3] {
		h.remove("laptop", k)
	}
	r := h.reconciler(ReconcileSettings{})
	h.pass(r)
	if st, _ := h.db.ReconcileState(context.Background(), "laptop"); st.Held != 3 {
		t.Fatalf("held %d", st.Held)
	}
	if _, err := h.db.AllowDeletes(context.Background(), "laptop", 3); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	r.listed = func(int32) {
		if calls.Add(1) == 2 {
			for _, k := range keys[3:8] {
				h.remove("laptop", k)
			}
		}
	}
	h.pass(r)
	if n := h.deletes("laptop"); n != 3 {
		t.Fatalf("%d deletes under an allowance of 3", n)
	}
	if st, _ := h.db.ReconcileState(context.Background(), "laptop"); st.Held != 5 || st.Allowed != 0 {
		t.Fatalf("state %+v; want the 5 others held, the allowance used", st)
	}
}

// EP-14 (#8 review): an allowance is for the hold it was given for. A
// pass that does not finish (the bucket's directory gone) drops it, and
// so does a later hold of another count; neither deletes under it.
func TestAllowanceIsTiedToItsHold(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	keys := dataKeys(20)
	h.putAll("laptop", keys...)
	for _, k := range keys[:3] {
		h.remove("laptop", k)
	}
	r := h.reconciler(ReconcileSettings{})
	h.pass(r)
	ctx := context.Background()
	if _, err := h.db.AllowDeletes(ctx, "laptop", 3); err != nil {
		t.Fatal(err)
	}
	// The directory goes away and comes back: the pass did not finish.
	dir := filepath.Join(h.store, "laptop")
	if err := os.Rename(dir, dir+".away"); err != nil {
		t.Fatal(err)
	}
	h.pass(r)
	if err := os.Rename(dir+".away", dir); err != nil {
		t.Fatal(err)
	}
	if st, _ := h.db.ReconcileState(ctx, "laptop"); st.Allowed != 0 {
		t.Fatalf("allowance kept over an unfinished pass: %+v", st)
	}
	h.pass(r)
	if n := h.deletes("laptop"); n != 0 {
		t.Fatalf("%d deletes after the allowance was dropped", n)
	}
	// Allowed for the hold of 3; then a pass holds another count.
	if _, err := h.db.AllowDeletes(ctx, "laptop", 3); err != nil {
		t.Fatal(err)
	}
	if err := h.db.FinishReconcile(ctx, "laptop", time.Now(), 4, 20, 0); err != nil {
		t.Fatal(err)
	}
	h.pass(r)
	if n := h.deletes("laptop"); n != 0 {
		t.Fatalf("%d deletes under an allowance given for another hold", n)
	}
}

// EP-14 (#8 review): something still at a missing key's path (a
// directory where the object was, or an object whose attributes were
// lost) is no delete, though the store's HEAD says NoSuchKey.
func TestSomethingAtThePathIsNoDelete(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.putAll("laptop", dataKeys(20)...)
	h.remove("laptop", "data/04")
	if err := os.MkdirAll(filepath.Join(h.store, "laptop", "data", "04", "inner"), 0o700); err != nil {
		t.Fatal(err)
	}
	h.pass(h.reconciler(ReconcileSettings{DeleteLimit: 100}))
	if got := h.reconciled("laptop"); got["data/04"] == state.OpDelete {
		t.Fatalf("data/04 deleted with a directory at its path: %v", got)
	}
}

// EP-14 (#8 review): the limit counts the reconciler's own deletes since
// the last allowance or over the last 7 days, whichever is later, so a
// store losing 1 object in 40 each pass is not emptied over several
// passes: four passes delete (10%), the fifth holds. Once the 7 days
// are over the old deletes no longer count; an allowance starts a new
// window too.
func TestDeleteLimitIsCumulativeAcrossPasses(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	var now atomic.Int64
	now.Store(time.Now().Unix())
	clock := func() time.Time { return time.Unix(now.Load(), 0) }
	h.db.Clock = clock
	h.serve(false)
	keys := dataKeys(40)
	h.putAll("laptop", keys...)
	r := h.reconciler(ReconcileSettings{})
	r.now = clock
	ctx := context.Background()
	lose := func(k string) {
		h.remove("laptop", k)
		h.pass(r)
		now.Add(3600)
	}
	want := func(deletes int, held int64) {
		t.Helper()
		st, _ := h.db.ReconcileState(ctx, "laptop")
		if n := h.deletes("laptop"); n != deletes || st.Held != held {
			t.Fatalf("%d deletes, %d held; want %d and %d", n, st.Held, deletes, held)
		}
	}
	for _, k := range keys[:5] {
		lose(k)
	}
	want(4, 1)
	now.Add(8 * 24 * 3600)
	h.pass(r)
	want(5, 0)
	for _, k := range keys[5:8] {
		lose(k)
	}
	want(7, 1)
	if _, err := h.db.AllowDeletes(ctx, "laptop", 1); err != nil {
		t.Fatal(err)
	}
	h.pass(r)
	want(8, 0)
	lose(keys[8])
	want(9, 0)
}
