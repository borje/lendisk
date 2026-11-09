package cli

import (
	"strings"
	"testing"
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
