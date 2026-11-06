package identity

import (
	"regexp"
	"strings"
	"testing"
)

// dialVector is the test phrase's dial key for the all-zero node ID.
const dialVector = "3gzxeblrqbx5l37n5fiaamohptvjt2zindazckha5hdtgfvmf5eteio3"

var sealedForm = regexp.MustCompile(`^s[a-z2-7]{26}$`)

// SEC-16 AC (naming half): the sealed name is a valid S3 bucket name that
// shows nothing of the real one, is the same every time (so at every
// lender), and is recomputed from the phrase and the bucket name alone.
func TestSealedNameIsStableAndRecomputableFromPhrase(t *testing.T) {
	a, err := FromPhrase(vector)
	if err != nil {
		t.Fatal(err)
	}
	s := a.SealedName("laptop")
	if !sealedForm.MatchString(s) || !IsSealedName(s) {
		t.Fatalf("sealed name %q is not s<26> in lowercase base32", s)
	}
	if strings.Contains(s, "laptop") {
		t.Fatalf("sealed name %q shows the bucket name", s)
	}
	again, _ := FromPhrase(strings.ToUpper(vector))
	if again.SealedName("laptop") != s {
		t.Fatal("the same phrase and name gave another sealed name")
	}
	if a.SealedName("laptop2") == s {
		t.Fatal("two names gave the same sealed name")
	}
	p, _ := NewPhrase()
	other, _ := FromPhrase(p)
	if other.SealedName("laptop") == s {
		t.Fatal("two identities gave the same sealed name for one bucket name")
	}
	// A fixed vector, so a change to the derivation is caught: every
	// lender would lose track of every bucket.
	if s != "sxhngjco2iixhedb6rnikvgwmlo" {
		t.Logf("sealed name of laptop under the test phrase: %s", s)
		t.Fatal("the sealed-name derivation changed")
	}
}

func TestIsSealedNameRejectsOtherNames(t *testing.T) {
	for _, n := range []string{"laptop", "lent-aaaaaaaaaaaaaaaa-s" + strings.Repeat("a", 26),
		"s" + strings.Repeat("a", 25), "s" + strings.Repeat("a", 27), "s" + strings.Repeat("A", 26),
		"s" + strings.Repeat("1", 26), "t" + strings.Repeat("a", 26), ""} {
		if IsSealedName(n) {
			t.Errorf("%q accepted as a sealed name", n)
		}
	}
}

// NET-7a: either side of a pair computes the same S3 secret from its own
// key and the other's public key; another pair's differs.
func TestPairSecretIsSymmetric(t *testing.T) {
	p1, _ := NewPhrase()
	p2, _ := NewPhrase()
	p3, _ := NewPhrase()
	a, _ := FromPhrase(p1)
	b, _ := FromPhrase(p2)
	c, _ := FromPhrase(p3)
	ab, err := a.S3Secret(b.NodeID())
	if err != nil {
		t.Fatal(err)
	}
	ba, err := b.S3Secret(a.NodeID())
	if err != nil {
		t.Fatal(err)
	}
	if ab != ba {
		t.Fatal("the two sides of a pair derived different secrets")
	}
	if len(ab) != 64 || strings.Trim(ab, "0123456789abcdef") != "" {
		t.Fatalf("secret %q is not 64 hex characters", ab)
	}
	ac, _ := a.S3Secret(c.NodeID())
	if ac == ab {
		t.Fatal("two pairs share a secret")
	}
	if _, err := a.S3Secret(NodeID{}); err == nil {
		t.Fatal("a key agreement with the all-zero key was accepted")
	}
}

// ID-1 (#69): a node dials each friend with its own key, derived from
// the identity key and the friend's node ID, so the phrase and the
// friend list recover it; it is not the identity key, is the same on
// every derivation, differs between friends and between identities,
// and is fixed by a vector.
func TestDialKeyPerFriendIsDerivedAndDistinct(t *testing.T) {
	a, err := FromPhrase(vector)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := NewPhrase()
	other, _ := FromPhrase(p)
	var friend NodeID // the all-zero ID, for the vector
	d := a.DialKeyFor(friend)
	if d.NodeID() == a.NodeID() {
		t.Fatal("the dial key is the identity key")
	}
	again, _ := FromPhrase(vector)
	if again.DialKeyFor(friend).NodeID() != d.NodeID() {
		t.Fatal("the same phrase gave another dial key")
	}
	if a.DialKeyFor(other.NodeID()).NodeID() == d.NodeID() {
		t.Fatal("two friends share a dial key")
	}
	if other.DialKeyFor(friend).NodeID() == d.NodeID() {
		t.Fatal("two identities gave the same dial key")
	}
	if k := d.PrivateKey().Bytes(); k[0]&7 != 0 || k[31]&128 != 0 || k[31]&64 == 0 {
		t.Fatal("dial key not clamped")
	}
	// A change to the derivation would leave every friend admitting a
	// key this node no longer dials with.
	if got := d.NodeID().String(); got != dialVector {
		t.Fatalf("dial key of the test phrase is %s; the derivation changed", got)
	}
}

// ID-4: the manifest key is derived from the phrase alone, with its own
// info string: the same for two machines restored from one phrase, and
// unlike K_name or another identity's.
func TestManifestKeyIsItsOwnDerivation(t *testing.T) {
	a, _ := FromPhrase(vector)
	again, _ := FromPhrase(vector)
	k := a.ManifestKey()
	if len(k) != 32 || string(k) != string(again.ManifestKey()) {
		t.Fatalf("manifest key %x is not 32 bytes, stable across restores", k)
	}
	p, _ := NewPhrase()
	other, _ := FromPhrase(p)
	if string(other.ManifestKey()) == string(k) {
		t.Fatal("two identities share a manifest key")
	}
	if string(a.nameKey()) == string(k) {
		t.Fatal("the manifest key is K_name")
	}
}
