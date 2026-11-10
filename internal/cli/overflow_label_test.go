package cli

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// S3-4 (#39): status labels a target without versioning "retention:
// client-side", with the deletes it holds; a versioned target's
// retention is the provider's, and has no such label.
func TestStatusLabelsAnUnversionedTargetClientSide(t *testing.T) {
	parallel(t, 0)
	off := overflowLine(StatusCapacityOverflow{Name: "garage", MaxBytes: 1 << 30, Versioning: "off", Retention: "client-side",
		Tombstones: 3, TombstoneBytes: 3 << 20})
	if !strings.Contains(off, "retention: client-side") || strings.Contains(off, "final") ||
		!strings.Contains(off, "3 deletes held") {
		t.Fatalf("unversioned: %s", off)
	}
	on := overflowLine(StatusCapacityOverflow{Name: "b2", MaxBytes: 1 << 30, Versioning: "enabled", Retention: "provider"})
	if strings.Contains(on, "client-side") || strings.Contains(on, "held") {
		t.Fatalf("versioned: %s", on)
	}
}

// S3-4 (#97): status shows a target's versioning as last read, when
// that was, and when it changed from what: a suspended target is
// client-side from then on.
func TestStatusShowsAVersioningChange(t *testing.T) {
	parallel(t, 0)
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	added := time.Unix(1_800_000_000, 0)
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "b2", Endpoint: "https://s3.example", Bucket: "offsite",
		MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, Versioning: state.VersioningEnabled, AddedAt: added}); err != nil {
		t.Fatal(err)
	}
	got, err := overflowStatus(t.Context(), db)
	if err != nil || len(got) != 1 || got[0].VersioningChecked != "" || got[0].VersioningChanged != "" {
		t.Fatalf("before a read: %+v %v", got, err)
	}
	at := added.Add(24 * time.Hour)
	if _, err := db.RecordOverflowVersioning(t.Context(), "b2", state.VersioningSuspended, at); err != nil {
		t.Fatal(err)
	}
	got, err = overflowStatus(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	o, when := got[0], at.UTC().Format(time.RFC3339)
	if o.Versioning != "suspended" || o.Retention != "client-side" || o.VersioningChecked != when ||
		o.VersioningChanged != when || o.VersioningBefore != "enabled" {
		t.Fatalf("after a suspension: %+v", o)
	}
	if l := overflowLine(o); !strings.Contains(l, "versioning suspended since "+when+" (was enabled) (retention: client-side") {
		t.Fatalf("status line: %s", l)
	}
}
