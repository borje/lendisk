package replica

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

// drainQueued counts place's queued drain-back copies (#40).
func (fx *fixture) drainQueued(place string) int {
	fx.t.Helper()
	var n int
	if err := fx.db.QueryRow(`SELECT count(*) FROM placement WHERE place = ? AND state = 'queued'
		AND replaces GLOB 'overflow:*'`, place).Scan(&n); err != nil {
		fx.t.Fatal(err)
	}
	return n
}

// befriend adds friend name, whose place the fixture already has, while
// the worker runs.
func (fx *fixture) befriend(name string) {
	fx.t.Helper()
	if err := fx.db.AddFriendship(fx.t.Context(), state.Friendship{Peer: name, Address: "tc" + name,
		OwnNumber: 1 << 40, PeerNumber: 1 << 40, CreatedAt: time.Now()}); err != nil {
		fx.t.Fatal(err)
	}
}

// REPL-13, REPL-7 (#40): copies held at an overflow target move to a
// friend that joins, with the lowest priority and only inside the
// replication schedule: outside it nothing is sent to the friend and
// nothing is deleted at the target; once it opens, a newer object goes
// to the friend first, then the moved copies, each deleted at the
// target once the friend has it.
func TestDrainBackSendsOnlyInsideTheScheduleAndAfterNewerCopies(t *testing.T) {
	fx := newFixture(t) // no friend yet: the copies go to the target
	target := fx.withTarget("b2", state.VersioningEnabled)
	fx.places["B"] = newPlace("B")
	fx.bucket("laptop", 1)
	s, err := pace.ParseSchedule("01:00-07:00")
	if err != nil {
		t.Fatal(err)
	}
	_, clock := fx.paced(pace.Settings{Schedule: s})
	day := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	clock.set(day.Add(2 * time.Hour)) // inside the window
	keys := []string{"data/0", "data/1", "data/2"}
	for _, k := range keys {
		fx.write("laptop", k, "pack "+k)
	}
	fx.run()
	fx.drained("overflow:b2")
	for _, k := range keys {
		if !target.holds("laptop", k) {
			t.Fatalf("%s not at the target", k)
		}
	}

	clock.set(day.Add(12 * time.Hour)) // outside
	fx.befriend("B")
	fx.settle("drain-back copies queued at B", func() bool { return fx.drainQueued("B") == len(keys) })
	fx.write("laptop", "data/new", "a newer pack")
	time.Sleep(200 * time.Millisecond)
	if got := fx.placeLog("B"); len(got) != 0 {
		t.Fatalf("sent to B outside the schedule: %v", got)
	}
	for _, k := range keys {
		if !target.holds("laptop", k) {
			t.Fatalf("%s deleted at the target outside the schedule", k)
		}
	}

	clock.set(day.Add(25 * time.Hour)) // 01:00 the next day
	fx.drained("B")
	fx.settle("the target emptied", func() bool {
		_, n, _, _ := target.snapshot()
		return n == 0
	})
	got := fx.placeLog("B")
	if len(got) != len(keys)+1 || got[0] != "put laptop/data/new" {
		t.Fatalf("B received %v, want data/new first, then the copies from the target", got)
	}
	holds, err := fx.db.OverflowHolds(t.Context())
	if err != nil || holds["b2"].DataObjects != 0 || holds["b2"].Use != 0 {
		t.Fatalf("the target holds %+v (%v), want nothing", holds["b2"], err)
	}
}

// S3-5 (#40): a provider's 403 AccessDenied to a delete is
// ErrDeleteDenied, which the worker keeps going past; the error never
// shows the credential.
func TestAnOverflowDeleteRefusedForPermissionIsDeleteDenied(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code>`+
				`<Message>Access Denied for GKLEAKEDACCESS</Message></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	o := state.Overflow{Name: "b2", Endpoint: srv.URL, Region: "us-east-1", Bucket: "offsite", Prefix: "lendisk"}
	c, err := NewOverflow(o, overflow.Credential{AccessKey: "GKLEAKEDACCESS", SecretKey: "LEAKEDSECRET"},
		func(b string) string { return b }, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Delete(t.Context(), "laptop", "data/1")
	if !errors.Is(err, ErrDeleteDenied) || strings.Contains(err.Error(), "LEAKED") {
		t.Fatalf("delete refused for permission: %v", err)
	}
}

// S3-5 (#40): with a credential that may not delete, the copies drained
// back to a friend stay at the target: its FIFO goes on, they are
// counted in its use and as denied deletes for status, and the delete
// is tried again later.
func TestDrainedCopiesTheTargetMayNotDeleteStayAndAreCounted(t *testing.T) {
	fx := newFixture(t)
	target := fx.withTarget("b2", state.VersioningEnabled)
	fx.places["B"] = newPlace("B")
	fx.bucket("laptop", 1)
	target.fail = func(op, key string) error {
		if op == "delete" {
			return fmt.Errorf("deleting %s: %w: 403 AccessDenied", key, ErrDeleteDenied)
		}
		return nil
	}
	keys := []string{"data/0", "data/1"}
	for _, k := range keys {
		fx.write("laptop", k, "pack "+k)
	}
	fx.run()
	fx.drained("overflow:b2")
	fx.befriend("B")
	fx.drained("B")
	fx.settle("the denied deletes recorded", func() bool {
		h, err := fx.db.OverflowTombHolds(t.Context())
		return err == nil && h["b2"].Denied == int64(len(keys))
	})
	fx.drained("overflow:b2")
	for _, k := range keys {
		if !target.holds("laptop", k) || !fx.holds("B", "laptop", k) {
			t.Fatalf("%s: at the target %t, at B %t; want both", k, target.holds("laptop", k), fx.holds("B", "laptop", k))
		}
	}
	h, err := fx.db.OverflowTombHolds(t.Context())
	if err != nil || h["b2"].Tombs != 0 || h["b2"].DeniedBytes <= 0 {
		t.Fatalf("tombs %+v (%v)", h["b2"], err)
	}
	holds, err := fx.db.OverflowHolds(t.Context())
	if err != nil || holds["b2"].DataObjects != 0 || holds["b2"].Use != h["b2"].DeniedBytes {
		t.Fatalf("the target holds %+v (%v), want no live copy and its use the kept copies' %d bytes", holds["b2"], err,
			h["b2"].DeniedBytes)
	}
	if c := fx.condition("overflow:b2"); c.Condition != state.ConditionOK {
		t.Fatalf("the target's condition %+v", c)
	}
}
