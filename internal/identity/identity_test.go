package identity

import (
	"crypto/ecdh"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A fixed BIP-39 test vector phrase (all-zero entropy, 24 words).
const vector = "abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon art"

func TestSamePhraseSameID(t *testing.T) {
	// ID-1 AC: two nodes initialised from the same phrase have the same ID.
	p, err := NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Fields(p)); n != 24 {
		t.Fatalf("phrase has %d words", n)
	}
	a, err := FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	b, err := FromPhrase("  " + strings.ToUpper(p) + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if a.NodeID() != b.NodeID() {
		t.Fatal("same phrase gave different IDs")
	}
	q, _ := NewPhrase()
	c, _ := FromPhrase(q)
	if a.NodeID() == c.NodeID() {
		t.Fatal("different phrases gave the same ID")
	}
}

func TestDerivationIsStable(t *testing.T) {
	// Pins the derivation: if this changes, every existing node's ID changes.
	id, err := FromPhrase(vector)
	if err != nil {
		t.Fatal(err)
	}
	const want = "wl7pwugdmzdxneeo5ytgnmjvxqzvvd6ubidjb7wvs7ubn5vcgqu3yj6v"
	if got := id.NodeID().String(); got != want {
		t.Fatalf("node ID for test vector changed: got %s want %s", got, want)
	}
}

// A key from a secret is deterministic, depends on the info string, is
// clamped, and refuses a secret too short to be one.
func TestFromSecretIsDeterministicAndSeparated(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	a, err := FromSecret(secret, "info-a")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := FromSecret(secret, "info-a")
	c, _ := FromSecret(secret, "info-b")
	if a.NodeID() != b.NodeID() || a.NodeID() == c.NodeID() {
		t.Fatal("derivation not deterministic or not separated by info")
	}
	if k := a.PrivateKey().Bytes(); k[0]&7 != 0 || k[31]&128 != 0 || k[31]&64 == 0 {
		t.Fatalf("key not clamped: %x", k)
	}
	if _, err := FromSecret([]byte("short"), "info-a"); err == nil {
		t.Fatal("a 5-byte secret was accepted")
	}
}

func TestKeyIsClamped(t *testing.T) {
	id, err := FromPhrase(vector)
	if err != nil {
		t.Fatal(err)
	}
	k := id.PrivateKey().Bytes()
	if k[0]&7 != 0 || k[31]&128 != 0 || k[31]&64 == 0 {
		t.Fatalf("key not clamped: %x", k)
	}
	// The public key is the X25519 base-point multiple of the private key.
	priv, _ := ecdh.X25519().NewPrivateKey(k)
	if string(priv.PublicKey().Bytes()) != string(id.PrivateKey().PublicKey().Bytes()) {
		t.Fatal("public key mismatch")
	}
}

func TestBadPhrases(t *testing.T) {
	words := strings.Fields(vector)
	cases := map[string]string{
		"short":    strings.Join(words[:12], " "),
		"checksum": strings.Join(append(words[:23:23], "abandon"), " "),
		"unknown":  strings.Join(append(words[:23:23], "zzzz"), " "),
	}
	for name, p := range cases {
		if _, err := FromPhrase(p); err == nil {
			t.Errorf("%s: accepted invalid phrase", name)
		}
	}
}

func TestSaveLoad(t *testing.T) {
	dir := t.TempDir()
	id, _ := FromPhrase(vector)
	if err := id.Save(dir); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dir, KeyFileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %04o, want 0600", fi.Mode().Perm())
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID() != id.NodeID() {
		t.Fatal("loaded key differs")
	}
	if err := id.Save(dir); err == nil {
		t.Fatal("Save overwrote an existing key")
	}
	os.Chmod(filepath.Join(dir, KeyFileName), 0o644)
	if _, err := Load(dir); err == nil {
		t.Fatal("Load accepted a world-readable key")
	}
	if _, err := Load(t.TempDir()); !errors.Is(err, ErrNoIdentity) {
		t.Fatalf("Load on empty dir: %v", err)
	}
}

func TestNodeIDRoundTrip(t *testing.T) {
	id, _ := FromPhrase(vector)
	s := id.NodeID().String()
	n, err := ParseNodeID(s)
	if err != nil {
		t.Fatal(err)
	}
	if n != id.NodeID() {
		t.Fatal("round trip changed the ID")
	}
	if _, err := ParseNodeID(strings.ToUpper(s[:20]) + "-" + s[20:]); err != nil {
		t.Fatalf("case/dash-insensitive parse failed: %v", err)
	}
	if id.NodeID().Short() != s[:16] {
		t.Fatal("Short is not the first 16 characters")
	}
}

func TestMistypedNodeIDRejected(t *testing.T) {
	id, _ := FromPhrase(vector)
	s := id.NodeID().String()
	// Every single-character substitution must be rejected.
	alphabet := "abcdefghijklmnopqrstuvwxyz234567"
	for i := range s {
		for _, c := range alphabet {
			if byte(c) == s[i] {
				continue
			}
			typo := s[:i] + string(c) + s[i+1:]
			if _, err := ParseNodeID(typo); err == nil {
				t.Fatalf("accepted typo at %d: %s", i, typo)
			}
		}
	}
	// Adjacent transposition.
	b := []byte(s)
	for i := 0; i+1 < len(b); i++ {
		if b[i] == b[i+1] {
			continue
		}
		b[i], b[i+1] = b[i+1], b[i]
		if _, err := ParseNodeID(string(b)); err == nil {
			t.Fatalf("accepted transposition at %d", i)
		}
		b[i], b[i+1] = b[i+1], b[i]
	}
	for _, bad := range []string{"", s[:55], s + "a", "1" + s[1:]} {
		if _, err := ParseNodeID(bad); !errors.Is(err, ErrBadNodeID) {
			t.Errorf("ParseNodeID(%q) = %v", bad, err)
		}
	}
}
