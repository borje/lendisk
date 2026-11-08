//go:build admitall

package cli

import (
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/transport"
)

// SEC-11 AC: in the admitall build the tunnel admits every key, and an
// identity with no relationship still has every request of the
// hostile-peer harness refused by the middleware (every S3 operation
// versitygw routes, signed with the stranger's own derivable secret,
// and every control call), on a node with zero peers and on one with a
// friend. Run with `go test -tags admitall ./internal/cli`.
func TestMiddlewareAloneRefusesStranger(t *testing.T) {
	serving(t, 0)
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	stranger := strangerID(t)
	addr := a.status().Node.Address
	aKey, err := transport.KeyOf(addr)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := stranger.S3Secret(aKey)
	if err != nil {
		t.Fatal(err)
	}
	h := attacker(t, "a stranger", stranger, addr, stranger.NodeID().String(), secret)
	sealed := stranger.SealedName("laptop")
	check := func(when string) {
		t.Helper()
		h.label = "a stranger, " + when
		attacks := append(s3Attacks(sealed, "data/ab/x", nil), controlAttacks(sealed, sealed)...)
		for _, at := range attacks {
			r := h.send(at)
			if r.err != nil {
				t.Fatalf("%s: %s %s: the connection was not admitted: %v", when, at.method, at.target, r.err)
			}
			if r.status != 403 {
				t.Errorf("%s: %s (%s %s): %v, want 403", when, at.name, at.method, at.target, r)
			}
		}
	}
	check("zero peers")
	if code, _, e := b.run("\n", "", "peer", "add", a.invite("--quota", "1G")); code != 0 {
		t.Fatal(e)
	}
	check("one peer")
	if log := stop(); !strings.Contains(log, "TEST BUILD (admitall)") {
		t.Fatal("the admitall build did not say so")
	}
}
