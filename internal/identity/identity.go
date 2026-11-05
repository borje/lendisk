// Package identity holds the node's one cryptographic identity: an X25519
// key derived from a 24-word BIP-39 recovery phrase (ID-1, ID-2), stored as
// a plain file in the config directory (ID-3) and shown as a node ID.
package identity

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-smith/go-bip39"
)

// KeyFileName is the private key's file name inside the config directory.
const KeyFileName = "identity.key"

// hkdfInfo fixes the derivation; changing it changes every node ID.
const hkdfInfo = "lendisk-identity-v1"

// Identity is the node's X25519 key pair.
type Identity struct {
	priv *ecdh.PrivateKey
}

// NewPhrase returns a fresh 24-word phrase (256 bits of entropy).
func NewPhrase() (string, error) {
	entropy, err := bip39.NewEntropy(256)
	if err != nil {
		return "", err
	}
	return bip39.NewMnemonic(entropy)
}

// NormalizePhrase lowercases and collapses whitespace so a phrase typed
// back with stray spaces or capitals still derives the same key.
func NormalizePhrase(phrase string) string {
	return strings.Join(strings.Fields(strings.ToLower(phrase)), " ")
}

// FromPhrase derives the identity: HKDF-SHA256 over the BIP-39 seed (empty
// passphrase) with a fixed info string, clamped for X25519 (DESIGN §3.1).
// The same phrase always gives the same key.
func FromPhrase(phrase string) (*Identity, error) {
	phrase = NormalizePhrase(phrase)
	if n := len(strings.Fields(phrase)); n != 24 {
		return nil, fmt.Errorf("recovery phrase has %d words, want 24", n)
	}
	if !bip39.IsMnemonicValid(phrase) {
		return nil, errors.New("recovery phrase is not valid: a word is misspelt or out of order")
	}
	seed := bip39.NewSeed(phrase, "")
	k, err := hkdf.Key(sha256.New, seed, nil, hkdfInfo, 32)
	if err != nil {
		return nil, err
	}
	clamp(k)
	return fromPrivateBytes(k)
}

func clamp(k []byte) {
	k[0] &= 248
	k[31] &= 127
	k[31] |= 64
}

func fromPrivateBytes(k []byte) (*Identity, error) {
	priv, err := ecdh.X25519().NewPrivateKey(k)
	if err != nil {
		return nil, err
	}
	return &Identity{priv: priv}, nil
}

// PublicKey is the 32-byte X25519 public key.
func (id *Identity) PublicKey() [32]byte {
	var pk [32]byte
	copy(pk[:], id.priv.PublicKey().Bytes())
	return pk
}

// NodeID is the public key in its display form.
func (id *Identity) NodeID() NodeID { return NodeID(id.PublicKey()) }

// PrivateKey exposes the raw key for components that need it (the tunnel,
// key derivations). Callers must not log or persist it.
func (id *Identity) PrivateKey() *ecdh.PrivateKey { return id.priv }

// Save writes the private key to dir/identity.key at mode 0600. It refuses
// to overwrite an existing key: replacing it would change the node's ID.
func (id *Identity) Save(dir string) error {
	path := filepath.Join(dir, KeyFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%s already exists; this node already has an identity", path)
		}
		return err
	}
	data := base64.StdEncoding.EncodeToString(id.priv.Bytes()) + "\n"
	if _, err := f.WriteString(data); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ErrNoIdentity means the config directory holds no key yet.
var ErrNoIdentity = errors.New("no identity: run `lendisk init` first")

// Load reads dir/identity.key. It refuses a key file that others can read.
func Load(dir string) (*Identity, error) {
	path := filepath.Join(dir, KeyFileName)
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoIdentity
		}
		return nil, err
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("%s has mode %04o; it must be readable by its owner alone (chmod 600)", path, perm)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	k, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(k) != 32 {
		return nil, fmt.Errorf("%s is not a valid identity key", path)
	}
	return fromPrivateBytes(k)
}
