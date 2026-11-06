package state

import (
	"context"
	"time"
)

// Role is what a peer key may do on the tunnel (SEC-10, SEC-13).
type Role string

const (
	RoleNone Role = ""     // unknown: refused
	RolePeer Role = "peer" // a friendship row
	// RoleInvite is an unexpired invite key, unused or used by a node
	// that is still a friend, so that node can retry redeem (#51).
	RoleInvite Role = "invite"
)

// NoRelationship is the message of the tunnel's refusal of a key with
// RoleNone. A borrower's replica client reads a 403 carrying it as
// "removed by the friend" (PEER-5), so the two sides must agree on it.
const NoRelationship = "no relationship with this node"

// PeerRole looks a tunnel key (a node ID in display form) up in the
// friendship and invite tables. Callers deciding admission must treat an
// error like RoleNone: a database that cannot be read admits nobody.
func (db *DB) PeerRole(ctx context.Context, key string, now time.Time) (Role, error) {
	var peer, invite bool
	err := db.QueryRowContext(ctx, `SELECT
		EXISTS (SELECT 1 FROM friendship WHERE peer_key = ?1),
		EXISTS (SELECT 1 FROM invite WHERE invite_key = ?1 AND expires_at > ?2
			AND (used_at IS NULL OR redeemed_by IN (SELECT peer_key FROM friendship)))`,
		key, now.Unix()).Scan(&peer, &invite)
	switch {
	case err != nil:
		return RoleNone, err
	case peer:
		return RolePeer, nil
	case invite:
		return RoleInvite, nil
	}
	return RoleNone, nil
}
