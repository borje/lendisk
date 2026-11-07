package identity

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Derivations from the identity key that nothing stores (DESIGN §3, §3.7).

const (
	// nameInfo fixes K_name; changing it renames every bucket at every
	// lender.
	nameInfo = "lendisk-bucket-name-v1"
	// s3Info fixes the per-pair S3 secret (NET-7a).
	s3Info = "lendisk-s3-v1"
	// dialInfo fixes the dial keys, before the friend's 32 key bytes;
	// changing it makes every friend refuse this node's dials until they
	// learn the new key.
	dialInfo = "lendisk-dial-v1"
	// sealedLen is the characters of the HMAC kept: 130 bits.
	sealedLen = 26
)

// SealedName is the name a bucket has on the wire to every lender and,
// after `lent-<id16>-`, in its store (SEC-16): "s" and the first 26
// characters, lowercase base32, of HMAC-SHA256(K_name, bucket), where
// K_name = HKDF-SHA256(private key, info="lendisk-bucket-name-v1"). It
// depends on the phrase and the name alone.
func (id *Identity) SealedName(bucket string) string {
	k, err := hkdf.Key(sha256.New, id.priv.Bytes(), nil, nameInfo, 32)
	if err != nil {
		panic(err) // only for a length HKDF cannot produce
	}
	m := hmac.New(sha256.New, k)
	m.Write([]byte(bucket))
	return "s" + b32.EncodeToString(m.Sum(nil))[:sealedLen]
}

// DialKeyFor is the key this node dials friend with (ID-1, #69):
// HKDF-SHA256(private key, info="lendisk-dial-v1" || friend's 32 key
// bytes), clamped for X25519. The identity key serves; each friend gets
// a key of its own to be dialled with, because a relay delivers a key's
// packets to the last of its connections to write, so one key held by
// two of a node's tailcat engines at one relay (its server and a
// client, or two clients) lets one take the other's packets (DESIGN
// §3.7). It depends on the phrase and the friend's ID alone, so
// recovery re-derives it from the friend list.
func (id *Identity) DialKeyFor(friend NodeID) *Identity {
	d, err := FromSecret(id.priv.Bytes(), dialInfo+string(friend[:]))
	if err != nil {
		panic(err) // a 32-byte secret always derives
	}
	return d
}

// IsSealedName reports whether s has the form of a sealed name.
func IsSealedName(s string) bool {
	if len(s) != 1+sealedLen || s[0] != 's' {
		return false
	}
	return strings.Trim(s[1:], "abcdefghijklmnopqrstuvwxyz234567") == ""
}

// S3Secret is the S3 secret key of the pair this node forms with peer
// (NET-7a): HKDF-SHA256(X25519(own key, peer key), info="lendisk-s3-v1"),
// in lowercase hex. Both sides compute the same value; the access key
// is the borrowing side's node ID. It is computed on demand and must
// never be written anywhere.
func (id *Identity) S3Secret(peer NodeID) (string, error) {
	pub, err := ecdh.X25519().NewPublicKey(peer[:])
	if err != nil {
		return "", err
	}
	shared, err := id.priv.ECDH(pub)
	if err != nil {
		return "", fmt.Errorf("key agreement with %s: %w", peer, err)
	}
	k, err := hkdf.Key(sha256.New, shared, nil, s3Info, 32)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(k), nil
}
