package recovery

import (
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/state"
)

// ID-5, EP-7 (#74): two friends hold different versions of Kopia's
// kopia.maintenance (one was offline for the last maintenance run), and
// the friend with the newer one lists the older among the versions its
// rewrites replaced: recovery counts the newer one, held there, with no
// conflict for the user to resolve. A list entry with malformed or too
// many replaced versions is rejected like any other malformed entry.
func TestRecoveryCountsTheNewestRewrittenVersion(t *testing.T) {
	db, l := recovering(t, "kopia", "B", "C")
	v1, v2, v3 := sum("schedule one"), sum("schedule two"), sum("schedule three")
	tooMany := make([]string, gateway.MaxReplaced+1)
	for i := range tooMany {
		tooMany[i] = v1
	}
	l.Page = served(map[string][]control.ListObject{
		"B": {{Key: "kopia.maintenance", Size: 12, SHA256: v3, Replaced: []string{v2, v1}},
			{Key: "kopia.repository", Size: 3, SHA256: v1, Replaced: []string{"nothex"}}},
		"C": {{Key: "kopia.maintenance", Size: 12, SHA256: v1},
			{Key: "kopia.repository", Size: 3, SHA256: v2, Replaced: tooMany}},
	})
	rs, err := l.Round(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.Err != nil || r.Listed != 1 || r.Rejected != 1 {
			t.Fatalf("result %+v", r)
		}
	}
	if got := heldKeys(t, db, "B"); got["kopia.maintenance"] != v3 {
		t.Fatalf("B holds %v", got)
	}
	if got := heldKeys(t, db, "C"); got["kopia.maintenance"] != "" {
		t.Fatalf("C, with the replaced version, holds %v", got)
	}
	if cs, _ := db.Conflicts(t.Context()); len(cs) != 0 {
		t.Fatalf("conflicts %+v", cs)
	}
	ks, err := db.RecoveredKeys(t.Context(), "laptop")
	if err != nil || len(ks) != 1 || ks[0].Verdict != state.VerdictNewest || !strings.Contains(ks[0].Reason, "B replaced") {
		t.Fatalf("verdicts %+v %v", ks, err)
	}
}
