// Package control owns the peer control API (DESIGN §3.7): the plain
// HTTP calls under /_lendisk/ on the tunnel port, the protocol version
// every call carries (NET-6), and the caller each call is answered for.
//
// Rules a caller must keep:
//
//   - Server answers for the Caller in the request context, which the
//     bridge's tunnel middleware sets from the connection's key and
//     lendisk.db on every request (SEC-11, SEC-12). Never take an
//     identity from a parameter: the one exception, redeem's own key,
//     is proven by RedeemProof, a key agreement with the inviting node.
//   - Only redeem is open to the invite role (SEC-13); the middleware
//     enforces it, and Server checks the role again.
//   - Nothing here returns another peer, ever (PEER-10).
package control

import (
	"context"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Protocol is this build's peer protocol version, major.minor (NET-6).
const Protocol = "0.1"

// ProtocolHeader carries the sender's protocol version on every control
// API request and response.
const ProtocolHeader = "Lendisk-Protocol"

// Paths of the control API. Only redeem and version are served so far;
// the others are refused as not yet served.
const (
	Prefix      = gateway.ControlPrefix
	RedeemPath  = Prefix + "redeem"
	VersionPath = Prefix + "version"
)

// Compatible reports whether a peer speaking theirs interoperates with
// ours: same major, minors at most one apart (NET-6). The error says
// which side to upgrade.
func Compatible(ours, theirs string) error {
	oM, om, err := parseVersion(ours)
	if err != nil {
		return err
	}
	tM, tm, err := parseVersion(theirs)
	if err != nil {
		return fmt.Errorf("peer protocol version %q: %w", theirs, err)
	}
	if oM == tM && om-tm <= 1 && tm-om <= 1 {
		return nil
	}
	older := "the peer"
	if oM < tM || oM == tM && om < tm {
		older = "this node"
	}
	return fmt.Errorf("peer protocol %s is incompatible with this node's %s (they must be at most one minor version apart): upgrade lendisk on %s", theirs, ours, older)
}

func parseVersion(v string) (major, minor int, err error) {
	a, b, ok := strings.Cut(v, ".")
	if ok {
		major, err = strconv.Atoi(a)
	}
	if ok && err == nil {
		minor, err = strconv.Atoi(b)
	}
	if !ok || err != nil || major < 0 || minor < 0 {
		return 0, 0, fmt.Errorf("not a protocol version: %q", v)
	}
	return major, minor, nil
}

// Caller is who a control API request is answered for: the key that
// authenticated its connection and the role lendisk.db gave that key
// when the request arrived.
type Caller struct {
	Peer identity.NodeID
	Role state.Role
}

type callerKey struct{}

// WithCaller returns ctx carrying c. Only the tunnel middleware calls it.
func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFrom returns the caller set by the middleware.
func CallerFrom(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

// RedeemRequest is what the invited node sends. Address names its own
// key, which Proof shows it holds.
type RedeemRequest struct {
	Address          string `json:"address"`
	Number           int64  `json:"number"`
	RetentionSeconds int64  `json:"retention_seconds"`
	Proof            []byte `json:"proof"`
}

// RedeemResponse is the inviting node's side of the new friendship.
type RedeemResponse struct {
	Number           int64 `json:"number"`
	RetentionSeconds int64 `json:"retention_seconds"`
}

// VersionResponse answers the version call.
type VersionResponse struct {
	Protocol string `json:"protocol"`
	Version  string `json:"version"`
}

// errorBody is every control API error.
type errorBody struct {
	Error string `json:"error"`
}

// proofInfo fixes the redeem proof's key derivation.
const proofInfo = "lendisk-redeem-v1"

// RedeemProof shows that the redeeming node holds the key its address
// names, without a signature (ID-1): HMAC-SHA256 keyed with
// HKDF-SHA256(X25519(self, other), "lendisk-redeem-v1") over the invite
// key. Either side computes it with its own private key and the other's
// public key.
func RedeemProof(self *identity.Identity, other, inviteKey identity.NodeID) ([]byte, error) {
	pub, err := ecdh.X25519().NewPublicKey(other[:])
	if err != nil {
		return nil, err
	}
	shared, err := self.PrivateKey().ECDH(pub)
	if err != nil {
		return nil, fmt.Errorf("key agreement with %s: %w", other, err)
	}
	k, err := hkdf.Key(sha256.New, shared, nil, proofInfo, 32)
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, k)
	m.Write(inviteKey[:])
	return m.Sum(nil), nil
}
