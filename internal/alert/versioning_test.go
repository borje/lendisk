package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// S3-4 (#97): a target whose versioning was found suspended after it
// had it raises overflow_versioning, saying its deletes are held as
// client-side tombstones from now on; it stays raised for
// VersioningAlertFor, or until versioning is enabled again. Versioning
// enabled later raises nothing.
func TestASuspendedVersioningIsAlertedForAWeek(t *testing.T) {
	db := openDB(t)
	now := time.Unix(1_800_000_000, 0)
	for _, o := range []state.Overflow{
		{Name: "b2", Endpoint: "https://s3.example", Bucket: "x", MaxBytes: 10 << 20, EnvPrefix: "P_",
			Versioning: state.VersioningEnabled, AddedAt: now},
		{Name: "garage", Endpoint: "https://g.example", Bucket: "y", MaxBytes: 10 << 20, EnvPrefix: "G_",
			Versioning: state.VersioningOff, AddedAt: now},
	} {
		if err := db.AddOverflow(t.Context(), o); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{DB: db, Log: quiet, Now: func() time.Time { return now }}
	evaluate := func() map[string]state.Alert {
		t.Helper()
		if err := s.evaluate(t.Context()); err != nil {
			t.Fatal(err)
		}
		return active(t, db)
	}
	now = now.Add(24 * time.Hour)
	for name, v := range map[string]string{"b2": state.VersioningSuspended, "garage": state.VersioningEnabled} {
		if _, err := db.RecordOverflowVersioning(t.Context(), name, v, now); err != nil {
			t.Fatal(err)
		}
	}
	as := evaluate()
	a, ok := as[KindOverflowVersioning+":b2"]
	if !ok || !strings.Contains(a.Message, "suspended") || !strings.Contains(a.Message, "client-side") {
		t.Fatalf("alerts after a suspension %v", as)
	}
	if _, ok := as[KindOverflowVersioning+":garage"]; ok {
		t.Fatalf("versioning enabled was alerted: %v", as)
	}
	now = now.Add(VersioningAlertFor - time.Second)
	if _, ok := evaluate()[KindOverflowVersioning+":b2"]; !ok {
		t.Fatal("cleared within the week")
	}
	now = now.Add(time.Second)
	if _, ok := evaluate()[KindOverflowVersioning+":b2"]; ok {
		t.Fatal("still raised after the week")
	}
	// Suspended again after it was enabled again: raised again, and
	// cleared at once when enabled once more.
	for _, v := range []string{state.VersioningEnabled, state.VersioningSuspended} {
		now = now.Add(24 * time.Hour)
		if _, err := db.RecordOverflowVersioning(t.Context(), "b2", v, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := evaluate()[KindOverflowVersioning+":b2"]; !ok {
		t.Fatal("a second suspension not alerted")
	}
	now = now.Add(time.Hour)
	if _, err := db.RecordOverflowVersioning(t.Context(), "b2", state.VersioningEnabled, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := evaluate()[KindOverflowVersioning+":b2"]; ok {
		t.Fatal("still raised with versioning enabled again")
	}
}
