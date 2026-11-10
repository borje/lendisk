package verify

import (
	"errors"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// S3-4 (#97): a target's versioning is read again daily with the
// verification pass. A suspension switches the target to client-side
// tombs and alerts; a read that gets no answer decides nothing and is
// tried again after DefaultRetry, not at every pass; versioning enabled
// again switches it back to the provider's retention without an alert.
func TestATargetsVersioningIsReadAgainDailyAndASuspensionAlerts(t *testing.T) {
	n, pr := targetNode(t, 1, 100)
	ctx := t.Context()
	target := func() state.Overflow {
		t.Helper()
		o, err := n.db.GetOverflow(ctx, "g")
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	n.v.pass(ctx)
	if r := pr.reads(); r != 0 {
		t.Fatalf("read %d times within a day of overflow add's read", r)
	}
	n.advance(24 * time.Hour)
	n.v.pass(ctx)
	n.v.pass(ctx)
	if r := pr.reads(); r != 1 {
		t.Fatalf("read %d times on the day it was due, want once", r)
	}
	if o := target(); !o.VersioningCheckedAt.Equal(n.clock()) || o.ClientSide() || n.alerts.Load() != 0 {
		t.Fatalf("after an unchanged read: %+v, alerts %d", o, n.alerts.Load())
	}

	pr.setVersioning(state.VersioningSuspended, nil)
	n.advance(24 * time.Hour)
	n.v.pass(ctx)
	if o := target(); o.Versioning != state.VersioningSuspended || !o.ClientSide() || n.alerts.Load() != 1 {
		t.Fatalf("after a suspension: %+v, alerts %d", o, n.alerts.Load())
	}

	pr.setVersioning("", errors.New("dial tcp: connection refused"))
	checked := target().VersioningCheckedAt
	n.advance(24 * time.Hour)
	n.v.pass(ctx)
	n.v.pass(ctx)
	if r, o := pr.reads(), target(); r != 3 || o.Versioning != state.VersioningSuspended || !o.VersioningCheckedAt.Equal(checked) {
		t.Fatalf("a read with no answer: %d reads, %+v", r, o)
	}
	n.advance(DefaultRetry)
	pr.setVersioning(state.VersioningEnabled, nil)
	n.v.pass(ctx)
	if r, o := pr.reads(), target(); r != 4 || o.ClientSide() || n.alerts.Load() != 1 {
		t.Fatalf("versioning enabled again: %d reads, %+v, alerts %d", r, o, n.alerts.Load())
	}
}
