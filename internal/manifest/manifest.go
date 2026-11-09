// Package manifest owns the node manifest (ID-4): what a node needs to
// be rebuilt from its phrase and one lender's address (ID-5), sealed
// under a key derived from its identity, pushed to every friend, kept
// by each friend outside lendisk.db, and fetched back with nothing but
// the identity (DESIGN §3.1).
//
// Rules a caller must keep:
//
//   - The manifest holds friends, overflow targets, buckets with their
//     copies and sealed names, and settings. Never placement (it is
//     rebuilt by listing the replicas, REPL-12) and never a bucket's own
//     S3 credential (SEC-3); an overflow target's credential is the one
//     secret SEC-3 lets it carry.
//   - Only Seal and Open touch the key (identity.ManifestKey); the key
//     and the plaintext are never written anywhere.
//   - The envelope format is versioned by its first four bytes; Open
//     refuses one it does not know, and a lender stores any envelope
//     of the right shape (Store.Put), so a newer borrower's manifest is
//     kept by an older lender.
package manifest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// Format is the plaintext format this build writes and reads.
const Format = 1

// magicPrefix begins every envelope; the fourth byte is the envelope
// version. Envelope v1: "ldm1" ‖ 24-byte nonce ‖ XChaCha20-Poly1305
// ciphertext of the JSON manifest, with "ldm1" ‖ the node's 32 key
// bytes as additional data. The JSON is padded with spaces to a multiple
// of padTo bytes.
const (
	magicPrefix = "ldm"
	envelopeV1  = magicPrefix + "1"
)

// minEnvelope is the shortest envelope of any version: header, nonce
// and tag around an empty plaintext.
const minEnvelope = len(envelopeV1) + chacha20poly1305.NonceSizeX + chacha20poly1305.Overhead

// Manifest is the plaintext: Content plus what identifies its version.
type Manifest struct {
	Format    int       `json:"format"`
	Node      string    `json:"node"` // the owner's node ID
	Seq       int64     `json:"seq"`  // counts content changes from 1
	UpdatedAt time.Time `json:"updated_at"`
	Content
}

// Content is what the node's state says; its hash is the version's.
type Content struct {
	Settings Settings   `json:"settings"`
	Friends  []Friend   `json:"friends"`
	Overflow []Overflow `json:"overflow"`
	Buckets  []Bucket   `json:"buckets"`
}

// Settings are what lendisk.db holds about the node itself that a
// rebuilt node needs to keep: the relay region its address embeds, so
// its address does not change (SEC-14). The configuration file is the
// user's and is not here.
type Settings struct {
	Address      string          `json:"address,omitempty"`       // the node's tc… address
	TunnelRegion json.RawMessage `json:"tunnel_region,omitempty"` // the saved region, as lendisk.db holds it
}

// Friend is one friendship (PEER-2, LEND-8, RET-1): enough to reach the
// friend again and restore the terms both sides agreed.
type Friend struct {
	ID                   string    `json:"id"`      // node ID
	Address              string    `json:"address"` // the friend's tc… address (NET-3 hints)
	OwnNumber            int64     `json:"own_number"`
	PeerNumber           int64     `json:"peer_number"`
	OwnRetentionSeconds  int64     `json:"own_retention_seconds"`
	PeerRetentionSeconds int64     `json:"peer_retention_seconds"`
	Since                time.Time `json:"since"`
	// DialKey is the key the friend dials this node with (#69), learnt at
	// redeem and proven there: a node rebuilt from the manifest admits
	// the friend by it again (ID-5). Absent from a manifest written
	// before it was added, and for a friendship without one; readers
	// that predate it ignore it, so format stays 1.
	DialKey string `json:"dial_key,omitempty"`
}

// Overflow is an overflow target (S3-1), credential included (SEC-3
// allows it here and nowhere else), so recovery through a friend also
// recovers bucket access. The keys are absent when the daemon that
// built it had them unset.
type Overflow struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	// Added with #38, absent from older manifests (format stays 1):
	// what a rebuilt node needs to add the target again.
	Region           string  `json:"region,omitempty"`
	MaxBytes         int64   `json:"max_bytes,omitempty"`
	EnvPrefix        string  `json:"env_prefix,omitempty"`
	RetentionSeconds int64   `json:"retention_seconds,omitempty"`
	PricePerGB       float64 `json:"price_per_gb,omitempty"`
}

// Bucket is one of the node's own buckets with its sealed name, the
// name every lender stores it under (SEC-16, ID-7).
type Bucket struct {
	Name      string    `json:"name"`
	Sealed    string    `json:"sealed"`
	Copies    int       `json:"copies"`
	Profile   string    `json:"profile"`
	CreatedAt time.Time `json:"created_at"`
}

// Build reads the manifest's content from the node's state, and each
// overflow target's credential with getenv (S3-1, SEC-3): a target
// whose variables are unset is listed without it, and nil reads none.
func Build(ctx context.Context, db *state.DB, self *identity.Identity, getenv overflow.Getenv) (Content, error) {
	c := Content{Friends: []Friend{}, Overflow: []Overflow{}, Buckets: []Bucket{}}
	region, err := transport.SavedRegion(ctx, db)
	if err != nil {
		return c, err
	}
	if region != nil {
		raw, err := json.Marshal(region)
		if err != nil {
			return c, err
		}
		c.Settings = Settings{Address: transport.AddressOf(self, region), TunnelRegion: raw}
	}
	fs, err := db.Friendships(ctx)
	if err != nil {
		return c, err
	}
	for _, f := range fs {
		c.Friends = append(c.Friends, Friend{ID: f.Peer, Address: f.Address, OwnNumber: f.OwnNumber, PeerNumber: f.PeerNumber,
			OwnRetentionSeconds: int64(f.OwnRetention / time.Second), PeerRetentionSeconds: int64(f.PeerRetention / time.Second),
			Since: f.CreatedAt.UTC(), DialKey: f.DialKey})
	}
	targets, err := db.Overflows(ctx)
	if err != nil {
		return c, err
	}
	for _, o := range targets {
		mo := Overflow{Name: o.Name, Endpoint: o.Endpoint, Bucket: o.Bucket, Prefix: o.Prefix, Region: o.Region,
			MaxBytes: o.MaxBytes, EnvPrefix: o.EnvPrefix, RetentionSeconds: int64(o.Retention / time.Second), PricePerGB: o.PricePerGB}
		if getenv != nil {
			if cred, err := overflow.Lookup(getenv, o.EnvPrefix); err == nil {
				mo.AccessKey, mo.SecretKey = cred.AccessKey, cred.SecretKey
			}
		}
		c.Overflow = append(c.Overflow, mo)
	}
	bs, err := db.Buckets(ctx)
	if err != nil {
		return c, err
	}
	for _, b := range bs {
		c.Buckets = append(c.Buckets, Bucket{Name: b.Name, Sealed: self.SealedName(b.Name), Copies: b.Copies,
			Profile: b.Profile, CreatedAt: b.CreatedAt.UTC()})
	}
	return c, nil
}

// Hash is the content's version hash: SHA-256 of its JSON, in hex.
// Build lists rows in a fixed order, so equal state hashes equally.
func Hash(c Content) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // plain structs always marshal
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// Seal encrypts m for self: format and node are set from self, and a
// fresh nonce is read from random (nil means crypto/rand).
func Seal(self *identity.Identity, m *Manifest, random io.Reader) ([]byte, error) {
	if random == nil {
		random = rand.Reader
	}
	pm := *m
	pm.Format, pm.Node = Format, self.NodeID().String()
	plain, err := json.Marshal(pm)
	if err != nil {
		return nil, err
	}
	plain = pad(plain)
	aead, err := chacha20poly1305.NewX(self.ManifestKey())
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(envelopeV1)+aead.NonceSize(), minEnvelope+len(plain))
	copy(out, envelopeV1)
	nonce := out[len(envelopeV1):]
	if _, err := io.ReadFull(random, nonce); err != nil {
		return nil, fmt.Errorf("manifest nonce: %w", err)
	}
	return aead.Seal(out, nonce, plain, additional(self)), nil
}

// Open decrypts an envelope with self's key and checks that it is a
// manifest of self in a format this build reads.
func Open(self *identity.Identity, blob []byte) (*Manifest, error) {
	if len(blob) < minEnvelope || string(blob[:len(magicPrefix)]) != magicPrefix {
		return nil, errors.New("not a lendisk manifest")
	}
	if string(blob[:len(envelopeV1)]) != envelopeV1 {
		return nil, fmt.Errorf("manifest envelope version %q is newer than this lendisk: upgrade it", blob[len(magicPrefix)])
	}
	aead, err := chacha20poly1305.NewX(self.ManifestKey())
	if err != nil {
		return nil, err
	}
	nonce := blob[len(envelopeV1) : len(envelopeV1)+aead.NonceSize()]
	plain, err := aead.Open(nil, nonce, blob[len(envelopeV1)+aead.NonceSize():], additional(self))
	if err != nil {
		return nil, errors.New("the manifest does not open with this identity: it is another node's, or damaged")
	}
	var m Manifest
	if err := json.Unmarshal(bytes.TrimRight(plain, " "), &m); err != nil {
		return nil, fmt.Errorf("the manifest is malformed: %w", err)
	}
	if m.Format != Format {
		return nil, fmt.Errorf("manifest format %d is not one this lendisk reads (%d): upgrade it", m.Format, Format)
	}
	if m.Node != self.NodeID().String() {
		return nil, errors.New("the manifest names another node")
	}
	return &m, nil
}

// padTo quantises the plaintext's size, so the envelope's size tells a
// lender little (how many friends and buckets, roughly): JSON is padded
// with spaces, which it ignores after the value, to a multiple of it.
const padTo = 4096

func pad(plain []byte) []byte {
	n := (len(plain) + padTo - 1) / padTo * padTo
	if n == 0 {
		n = padTo
	}
	return append(plain, bytes.Repeat([]byte{' '}, n-len(plain))...)
}

// additional binds an envelope to its version and its owner.
func additional(self *identity.Identity) []byte {
	id := self.NodeID()
	return append([]byte(envelopeV1), id[:]...)
}
