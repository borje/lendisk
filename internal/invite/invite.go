// Package invite owns the invite string (PEER-1) and the key an invited
// node dials with, derived from the invite's secret (SEC-6, SEC-13).
//
// Format, DESIGN §3.2: five fields joined by dots,
//
//	li1.<address>.<number>.<retention>.<secret>
//
// the format tag, the inviting node's `tc…` address, its number for the
// friendship in bytes (LEND-8), its retention offer in seconds (RET-1)
// and 32 random bytes in unpadded base64url. That is everything an invite
// carries; the expiry is kept by the inviting node alone.
//
// Rules: the secret is the only secret in the string (SEC-14). The
// inviting node never stores it, only the public key Key derives from it
// (state.Invite); never log an invite string or a secret.
package invite

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// Tag is the format tag, the first field.
const Tag = "li1"

// KeyInfo fixes the invite key derivation (HKDF-SHA256 over the secret).
const KeyInfo = "lendisk-invite-key-v1"

// SecretLen is the secret's length in bytes.
const SecretLen = 32

// Invite is a parsed invite.
type Invite struct {
	Address   string        // the inviting node's tc… address
	Number    int64         // bytes the inviting node lends (LEND-8)
	Retention time.Duration // how long the inviting node keeps a deleted object (RET-1), whole seconds
	Secret    [SecretLen]byte
}

// New makes an invite with a fresh secret read from rand.
func New(rand io.Reader, address string, number int64, retention time.Duration) (Invite, error) {
	inv := Invite{Address: address, Number: number, Retention: retention.Truncate(time.Second)}
	if _, err := io.ReadFull(rand, inv.Secret[:]); err != nil {
		return Invite{}, fmt.Errorf("making the invite secret: %w", err)
	}
	return inv, inv.check()
}

// String is the invite as given to the invited person.
func (inv Invite) String() string {
	return strings.Join([]string{
		Tag,
		inv.Address,
		strconv.FormatInt(inv.Number, 10),
		strconv.FormatInt(int64(inv.Retention/time.Second), 10),
		base64.RawURLEncoding.EncodeToString(inv.Secret[:]),
	}, ".")
}

// ErrBad is wrapped by every Parse error.
var ErrBad = errors.New("not a lendisk invite")

// Parse reads an invite string, ignoring surrounding whitespace.
func Parse(s string) (Invite, error) {
	f := strings.Split(strings.TrimSpace(s), ".")
	if len(f) != 5 || f[0] != Tag {
		return Invite{}, fmt.Errorf("%w: want five dot-separated fields starting %s.", ErrBad, Tag)
	}
	var inv Invite
	inv.Address = f[1]
	n, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return Invite{}, fmt.Errorf("%w: number %q", ErrBad, f[2])
	}
	inv.Number = n
	r, err := strconv.ParseInt(f[3], 10, 64)
	if err != nil || r > int64(1<<62)/int64(time.Second) {
		return Invite{}, fmt.Errorf("%w: retention %q", ErrBad, f[3])
	}
	inv.Retention = time.Duration(r) * time.Second
	sec, err := base64.RawURLEncoding.DecodeString(f[4])
	if err != nil || len(sec) != SecretLen {
		return Invite{}, fmt.Errorf("%w: the secret is damaged; copy the whole invite", ErrBad)
	}
	copy(inv.Secret[:], sec)
	if err := inv.check(); err != nil {
		return Invite{}, err
	}
	return inv, nil
}

func (inv Invite) check() error {
	switch {
	case !strings.HasPrefix(inv.Address, "tc") || len(inv.Address) < 3 ||
		strings.ContainsFunc(inv.Address, func(r rune) bool { return !isBase64URL(r) }):
		return fmt.Errorf("%w: the address is not a tc… address", ErrBad)
	case inv.Number <= 0:
		return fmt.Errorf("%w: the number must be positive", ErrBad)
	case inv.Retention <= 0:
		return fmt.Errorf("%w: the retention must be positive", ErrBad)
	}
	return nil
}

func isBase64URL(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_'
}

// Key is the invite key: the X25519 key the invited node dials with and
// the inviting node admits in the invite role until the invite is used or
// expires.
func (inv Invite) Key() (*identity.Identity, error) {
	return identity.FromSecret(inv.Secret[:], KeyInfo)
}
