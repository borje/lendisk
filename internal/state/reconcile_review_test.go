package state

import (
	"context"
	"testing"
	"time"
)

// EP-14 (#8 review): an allowance records the hold it was given for and
// is valid only while that hold stands; DropAllowance clears it. The
// reconciler's deletes count since the window's start: the last used
// allowance's sequence, or since a time, whichever is later.
func TestAllowanceTiedAndDeleteWindow(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	now := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return now }
	if err := db.FinishReconcile(ctx, "laptop", now, 3, 20, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AllowDeletes(ctx, "laptop", 5); err != nil {
		t.Fatal(err)
	}
	st, _ := db.ReconcileState(ctx, "laptop")
	if !st.AllowanceValid() {
		t.Fatalf("allowance for the standing hold not valid: %+v", st)
	}
	db.FinishReconcile(ctx, "laptop", now.Add(time.Hour), 4, 20, 0)
	if st, _ := db.ReconcileState(ctx, "laptop"); st.AllowanceValid() {
		t.Fatalf("allowance valid for another hold: %+v", st)
	}
	if err := db.DropAllowance(ctx, "laptop"); err != nil {
		t.Fatal(err)
	}
	if st, _ := db.ReconcileState(ctx, "laptop"); st.Allowed != 0 {
		t.Fatalf("allowance not dropped: %+v", st)
	}

	del := func() {
		seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "k", Op: OpDelete, Class: "data", Reconciled: true}})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishDelete(ctx, seqs...); err != nil {
			t.Fatal(err)
		}
	}
	del()
	now = now.Add(time.Hour)
	del()
	if n, err := db.RecentDeletes(ctx, "laptop", now.Add(-30*time.Minute)); err != nil || n != 1 {
		t.Fatalf("deletes in the last half hour %d %v", n, err)
	}
	if n, _ := db.RecentDeletes(ctx, "laptop", now.Add(-2*time.Hour)); n != 2 {
		t.Fatalf("deletes in two hours %d", n)
	}
	if err := db.StartDeleteWindow(ctx, "laptop"); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.RecentDeletes(ctx, "laptop", now.Add(-2*time.Hour)); n != 0 {
		t.Fatalf("deletes since the new window %d", n)
	}
}
