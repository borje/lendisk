package cli

import (
	"strings"
	"testing"
)

// S3-4, #38 review: until lendisk keeps tombstones for an unversioned
// target (#39), status says deletes there are final rather than claiming
// client-side retention; a versioned target has no such label.
func TestStatusLabelsAnUnversionedTargetHonestly(t *testing.T) {
	parallel(t, 0)
	off := overflowLine(StatusCapacityOverflow{Name: "garage", MaxBytes: 1 << 30, Versioning: "off"})
	if !strings.Contains(off, "retention: none, deletes there are final") || strings.Contains(off, "client-side") {
		t.Fatalf("unversioned: %s", off)
	}
	on := overflowLine(StatusCapacityOverflow{Name: "b2", MaxBytes: 1 << 30, Versioning: "enabled"})
	if strings.Contains(on, "retention:") {
		t.Fatalf("versioned: %s", on)
	}
}
