package gateway

import (
	"slices"
	"strings"
	"testing"
)

// SEC-9, LEND-4 (#74): the rewrite mark is user metadata on an ordinary
// PutObject, x-amz-meta-lendisk-replaces, so the gate admits a
// borrower's marked put as the PutObject it is, and the Surface entry
// says so; Lending, not the gate, decides whether the mark holds.
func TestGateAdmitsTheRewriteMark(t *testing.T) {
	r := startRouter(t, true)
	friend := newIdentity(t).NodeID().String()
	h := signedAs(friend)
	h["X-Amz-Meta-Lendisk-Sha256"] = strings.Repeat("ab", 32)
	h["X-Amz-Meta-Lendisk-Replaces"] = strings.Repeat("cd", 32)
	if a := r.send(t, "PUT", "/sbucket/kopia.maintenance", h); !slices.Equal(a.actions, []string{"s3_PutObject"}) {
		t.Fatalf("a marked rewrite from a borrower: %d %v %s", a.status, a.actions, a.body)
	}
	for _, op := range Surface {
		if op.Action == "s3_PutObject" && !strings.Contains(op.Why, "x-amz-meta-lendisk-replaces") {
			t.Errorf("the PutObject entry does not name the rewrite mark: %q", op.Why)
		}
	}
}
