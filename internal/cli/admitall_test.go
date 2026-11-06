//go:build admitall

package cli

import (
	"net/http"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
)

// SEC-11 AC: in the admitall build the tunnel admits every key, and an
// identity with no relationship still has every request refused by the
// middleware, on a node with zero peers and on one with a friend.
// Run with `go test -tags admitall ./internal/cli`.
func TestMiddlewareAloneRefusesStranger(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	p, _ := identity.NewPhrase()
	strangerKey, _ := identity.FromPhrase(p)
	addr := a.status().Node.Address

	check := func(when string) {
		c := tunnelClient(t, strangerKey, addr)
		for _, r := range [][2]string{
			{"GET", control.VersionPath},
			{"POST", control.RedeemPath},
			{"POST", control.Prefix + "challenge"},
			{"GET", control.Prefix + "manifest"},
			{"POST", control.Prefix + "quota"},
			{"GET", control.Prefix + "list"},
			{"GET", "/"},
			{"PUT", "/sbucket/key"},
		} {
			resp, err := c.Do(t.Context(), r[0], r[1], strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("%s: %s %s: the connection was not admitted: %v", when, r[0], r[1], err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s: %s %s from a stranger: %d, want 403", when, r[0], r[1], resp.StatusCode)
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
