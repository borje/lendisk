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
//     is proven by RedeemProof, a key agreement with the inviting node,
//     and with it the redeemer's dial key, proven by DialProof (#69).
//   - Only redeem is open to the invite role (SEC-13); the middleware
//     enforces it, and Server checks the role again.
//   - manifest stores and returns the caller's own manifest only, keyed
//     by the resolved caller (Caller.Peer, the friend's node ID even on a
//     dial-key connection); no parameter names whose (ID-4). list is
//     keyed the same way: a friend pages its own buckets only (NET-7).
//   - quota applies the caller's own number for its friendship with
//     this node, keyed by the resolved caller, and nothing else; it
//     answers with this node's figures for that friendship (LEND-8).
//   - challenge reads the caller's own object only, keyed by the
//     resolved caller, through Held (Lending: never a tomb or another
//     friend's bucket), and answers an HMAC over it (VER-1). Each
//     friend's challenges are read one at a time and paced in bytes
//     (ChallengeRate, ChallengeBurst), so a friend cannot make this node
//     read without bound.
//   - remove ends the caller's friendship on this side (PEER-5): the
//     caller ended it on its own, and the call only tells this node.
//   - Nothing here returns another peer, ever (PEER-10).
package control

import (
	"context"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Protocol is this build's peer protocol version, major.minor (NET-6).
const Protocol = "0.1"

// ProtocolHeader carries the sender's protocol version on every control
// API request and response.
const ProtocolHeader = "Lendisk-Protocol"

// Paths of the control API: redeem, version, manifest, list, quota
// (QuotaPath, quota.go), challenge (ChallengePath, challenge.go) and
// remove. undelete (RET-3) is not served yet.
const (
	Prefix       = gateway.ControlPrefix
	RedeemPath   = Prefix + "redeem"
	VersionPath  = Prefix + "version"
	ManifestPath = Prefix + "manifest"
	ListPath     = Prefix + "list"
	// RemovePath is the call a node makes after it ended the friendship
	// with the callee, so the callee ends it too (PEER-5, #25).
	RemovePath = Prefix + "remove"
)

// LentLister is the lender's index of what it holds for each borrower
// (NET-7 list); gateway.LentIndex is the implementation. List must
// answer for borrower alone, which the control API takes from the
// caller, never from the request.
type LentLister interface {
	List(ctx context.Context, borrower identity.NodeID, sealed, after string, limit int) ([]gateway.LentObject, string, error)
}

// ListPage is one page of the list call: keys in order, and the key the
// next page starts after, "" after the last page.
type ListPage struct {
	Objects []ListObject `json:"objects"`
	Next    string       `json:"next,omitempty"`
}

// ListObject is one key of a borrower's bucket at the lender: its size,
// its SHA-256 (REPL-10) and, if tombstoned, when (RET-1).
type ListObject struct {
	Key          string     `json:"key"`
	Size         int64      `json:"size"`
	SHA256       string     `json:"sha256"`
	TombstonedAt *time.Time `json:"tombstoned_at,omitempty"`
	// Replaced are the hashes of the versions of a live key the
	// borrower's rewrites replaced at the lender, newest first, while
	// their tombs are kept (#74).
	Replaced []string `json:"replaced,omitempty"`
}

// MaxManifest bounds one node manifest envelope (ID-4). A friend keeps
// one here, so this bounds what the manifest call can make a lender
// store per friend; a manifest of hundreds of friends and buckets is a
// few tens of KiB.
const MaxManifest = 1 << 20

// ManifestStore keeps the node manifests friends push to this node
// (ID-4), one per friend, outside lendisk.db. The control API is its
// only caller; manifest.Store is the implementation.
type ManifestStore interface {
	// Put replaces peer's manifest with blob, durably. It returns
	// ErrBadManifest for a blob that is not a manifest envelope.
	Put(peer identity.NodeID, blob []byte) error
	// Get returns peer's manifest, or ErrNoManifest.
	Get(peer identity.NodeID) ([]byte, error)
}

var (
	// ErrNoManifest means no manifest is stored for the node.
	ErrNoManifest = errors.New("no manifest stored here for this node")
	// ErrBadManifest means a pushed body is not a manifest envelope.
	ErrBadManifest = errors.New("not a lendisk manifest envelope")
)

// ManifestReceipt answers a manifest push: what the lender stored.
type ManifestReceipt struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex
}

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
// key, which Proof shows it holds; Proof also binds DialKey, the key it
// dials the inviting node with (#69), which DialProof shows it holds.
// Prove fills all three.
type RedeemRequest struct {
	Address          string `json:"address"`
	Number           int64  `json:"number"`
	RetentionSeconds int64  `json:"retention_seconds"`
	Proof            []byte `json:"proof"`
	DialKey          string `json:"dial_key"`
	DialProof        []byte `json:"dial_proof"`
}

// Prove sets the request's dial key to self's for inviter and its two
// proofs, for the redeem of the invite with inviteKey at inviter.
func (r *RedeemRequest) Prove(self *identity.Identity, inviter, inviteKey identity.NodeID) error {
	dial := self.DialKeyFor(inviter)
	var err error
	if r.Proof, err = RedeemProof(self, inviter, inviteKey, dial.NodeID()); err != nil {
		return err
	}
	if r.DialProof, err = DialProof(dial, inviter, inviteKey, self.NodeID()); err != nil {
		return err
	}
	r.DialKey = dial.NodeID().String()
	return nil
}

// RedeemResponse is the inviting node's side of the new friendship,
// with its dial key for the redeeming node and DialProof that it holds
// it (#69).
type RedeemResponse struct {
	Number           int64  `json:"number"`
	RetentionSeconds int64  `json:"retention_seconds"`
	DialKey          string `json:"dial_key"`
	DialProof        []byte `json:"dial_proof"`
}

// InviterDialKey checks the inviting node's dial key in r against its
// proof for the redeem by self of the invite with inviteKey at inviter,
// and returns it. The answer came over the tunnel to inviter's own key,
// so only the proof of possession is checked here.
func (r RedeemResponse) InviterDialKey(self *identity.Identity, inviter, inviteKey identity.NodeID) (identity.NodeID, error) {
	if r.DialKey == "" {
		return identity.NodeID{}, errors.New("the inviting node sent no dial key: upgrade lendisk on the inviting node")
	}
	dial, err := identity.ParseNodeID(r.DialKey)
	if err != nil {
		return identity.NodeID{}, fmt.Errorf("the inviting node's dial key: %w", err)
	}
	if dial == inviter || dial == self.NodeID() || dial == self.DialKeyFor(inviter).NodeID() || dial == inviteKey {
		return identity.NodeID{}, errors.New("the inviting node's dial key is a node ID, the invite key or this node's dial key")
	}
	if !checkDialProof(self, dial, inviteKey, inviter, r.DialProof) {
		return identity.NodeID{}, errors.New("the inviting node's dial key is not proven")
	}
	return dial, nil
}

// VersionResponse answers the version call.
type VersionResponse struct {
	Protocol string `json:"protocol"`
	Version  string `json:"version"`
}

// errorBody is every control API error. Code, when set, names a refusal
// the caller acts on, beside its HTTP status.
type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

// CodeNoSuchBucket answers a list of a bucket the caller has none of at
// the lender: the lender holds nothing of it. Any other 404 (a node
// that lends nothing, or one without the call) is not that.
const CodeNoSuchBucket = "NoSuchBucket"

// The redeem proofs' key derivations; each distinct from every other
// info string (identity's included).
const (
	proofInfo     = "lendisk-redeem-v1"
	dialProofInfo = "lendisk-redeem-dial-v1"
)

// RedeemProof shows that the redeeming node holds the key its address
// names, without a signature (ID-1), and binds the dial key it reports
// (#69), so that key cannot be swapped: HMAC-SHA256 keyed with
// HKDF-SHA256(X25519(self, other), "lendisk-redeem-v1") over the invite
// key and the dial key. Either side computes it with its own private
// key and the other's public key.
func RedeemProof(self *identity.Identity, other, inviteKey, dialKey identity.NodeID) ([]byte, error) {
	return proof(self, other, proofInfo, inviteKey[:], dialKey[:])
}

// DialProof shows that a dial key's holder took part in this redeem
// for holder, the node the dial key stands for: the same construction
// keyed by X25519(dial key, other) with "lendisk-redeem-dial-v1", over
// the invite key and then holder's node ID. Without it a node could
// report another's dial key as its own and be named by that key's
// connections; without holder, a proof copied from another node's
// request would do (#69 review).
func DialProof(dial *identity.Identity, other, inviteKey, holder identity.NodeID) ([]byte, error) {
	return proof(dial, other, dialProofInfo, inviteKey[:], holder[:])
}

// checkDialProof checks, as self, got from the holder of dial for
// holder.
func checkDialProof(self *identity.Identity, dial, inviteKey, holder identity.NodeID, got []byte) bool {
	want, err := proof(self, dial, dialProofInfo, inviteKey[:], holder[:])
	return err == nil && hmac.Equal(want, got)
}

func proof(self *identity.Identity, other identity.NodeID, info string, msg ...[]byte) ([]byte, error) {
	pub, err := ecdh.X25519().NewPublicKey(other[:])
	if err != nil {
		return nil, err
	}
	shared, err := self.PrivateKey().ECDH(pub)
	if err != nil {
		return nil, fmt.Errorf("key agreement with %s: %w", other, err)
	}
	k, err := hkdf.Key(sha256.New, shared, nil, info, 32)
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, k)
	for _, b := range msg {
		m.Write(b)
	}
	return m.Sum(nil), nil
}
