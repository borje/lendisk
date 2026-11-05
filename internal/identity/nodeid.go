package identity

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
)

// NodeID is a node's X25519 public key.
//
// Display form: lowercase unpadded base32 (RFC 4648) of the 32 key bytes
// followed by a 3-byte checksum, 56 characters. The checksum is the first
// three bytes of SHA-256("lendisk-node-id-v1" || key). Because base32 works
// in 5-byte groups, the first 16 characters depend on the key alone, so
// they are also the `id16` used in lent bucket names (DESIGN §3).
type NodeID [32]byte

const (
	checksumLen  = 3
	nodeIDLen    = 56 // base32 of 35 bytes, no padding
	checksumInfo = "lendisk-node-id-v1"
)

var b32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func checksum(key []byte) []byte {
	h := sha256.New()
	h.Write([]byte(checksumInfo))
	h.Write(key)
	return h.Sum(nil)[:checksumLen]
}

// String returns the display form.
func (n NodeID) String() string {
	buf := append(n[:], checksum(n[:])...)
	return b32.EncodeToString(buf)
}

// Short is the first 16 characters, the `id16` of DESIGN §3.
func (n NodeID) Short() string { return n.String()[:16] }

// ErrBadNodeID is returned for any string that is not a valid node ID.
var ErrBadNodeID = errors.New("not a valid node ID")

// ParseNodeID accepts the display form, case-insensitively and ignoring
// spaces and dashes, and rejects it if the checksum does not match.
func ParseNodeID(s string) (NodeID, error) {
	var n NodeID
	s = strings.ToLower(s)
	s = strings.NewReplacer(" ", "", "-", "", "\t", "").Replace(s)
	if len(s) != nodeIDLen {
		return n, fmt.Errorf("%w: %d characters, want %d", ErrBadNodeID, len(s), nodeIDLen)
	}
	buf, err := b32.DecodeString(s)
	if err != nil {
		return n, fmt.Errorf("%w: %v", ErrBadNodeID, err)
	}
	key, sum := buf[:32], buf[32:]
	want := checksum(key)
	for i := range want {
		if sum[i] != want[i] {
			return n, fmt.Errorf("%w: checksum mismatch (mistyped?)", ErrBadNodeID)
		}
	}
	copy(n[:], key)
	return n, nil
}
