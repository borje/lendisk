package invite

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

const addr = "tcomVzZXJ2ZXJfcHVibGljWCDp-_example"

func TestRoundTrip(t *testing.T) {
	inv, err := New(bytes.NewReader(bytes.Repeat([]byte{7}, 32)), addr, 80<<30, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s := inv.String()
	if !strings.HasPrefix(s, "li1.") {
		t.Fatalf("invite %q", s)
	}
	got, err := Parse("  " + s + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if got != inv {
		t.Fatalf("round trip: %+v != %+v", got, inv)
	}
}

// PEER-1, SEC-14: the string holds the address, the number, the
// retention and the secret, and nothing more.
func TestCarriesNothingMore(t *testing.T) {
	inv, _ := New(bytes.NewReader(bytes.Repeat([]byte{1}, 32)), addr, 1<<30, time.Hour)
	parts := strings.Split(inv.String(), ".")
	want := []string{"li1", addr, "1073741824", "3600", "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"}
	if strings.Join(parts, "|") != strings.Join(want, "|") {
		t.Fatalf("fields %q, want %q", parts, want)
	}
}

func TestParseRejects(t *testing.T) {
	good, _ := New(bytes.NewReader(bytes.Repeat([]byte{7}, 32)), addr, 1<<30, time.Hour)
	for _, s := range []string{
		"",
		"li2" + strings.TrimPrefix(good.String(), "li1"),
		strings.Replace(good.String(), ".1073741824.", ".0.", 1),
		strings.Replace(good.String(), ".1073741824.", ".-5.", 1),
		strings.Replace(good.String(), ".3600.", ".0.", 1),
		strings.Replace(good.String(), ".3600.", ".x.", 1),
		strings.Replace(good.String(), addr, "notanaddress", 1),
		good.String()[:len(good.String())-2],
		good.String() + ".extra",
	} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted", s)
		}
	}
}

// The invite key is derived from the secret alone, so both sides
// compute it, and it is not the inviting node's identity.
func TestKeyDerivedFromSecret(t *testing.T) {
	a, _ := New(bytes.NewReader(bytes.Repeat([]byte{7}, 32)), addr, 1<<30, time.Hour)
	b, _ := New(bytes.NewReader(bytes.Repeat([]byte{8}, 32)), addr, 1<<30, time.Hour)
	ka, err := a.Key()
	if err != nil {
		t.Fatal(err)
	}
	ka2, _ := a.Key()
	kb, _ := b.Key()
	if ka.NodeID() != ka2.NodeID() || ka.NodeID() == kb.NodeID() {
		t.Fatal("invite key not a function of the secret")
	}
	want, _ := identity.FromSecret(a.Secret[:], KeyInfo)
	if ka.NodeID() != want.NodeID() {
		t.Fatal("key derivation changed")
	}
	// Pins the derivation: changing it breaks every invite in flight.
	const pinned = "kophznvykebyycdpfz2e34sfsjtxd7szwswpnqihu3uwb55ekuuc4rgy"
	if got := ka.NodeID().String(); got != pinned {
		t.Fatalf("pinned invite key changed: %s", got)
	}
}
