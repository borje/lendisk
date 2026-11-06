package state

import (
	"context"
	"database/sql"
	"errors"
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

// TunnelPeer resolves a key that authenticated a tunnel connection, in
// node-ID form, to whom it speaks for and that one's role (SEC-10,
// SEC-12): a friend's node ID or its dial key names the friend in the
// peer role (#69); an invite key names itself. Callers deciding
// admission must treat an error like RoleNone.
func (db *DB) TunnelPeer(ctx context.Context, key string, now time.Time) (string, Role, error) {
	role, err := db.PeerRole(ctx, key, now)
	if err != nil || role != RoleNone {
		return key, role, err
	}
	var peer string
	err = db.QueryRowContext(ctx, `SELECT peer_key FROM friendship WHERE dial_key = ?`, key).Scan(&peer)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return key, RoleNone, nil
	case err != nil:
		return key, RoleNone, err
	}
	return peer, RolePeer, nil
}

// PeerRole looks a node ID in display form up in the friendship and
// invite tables. A dial key has no role here: S3 accounts and control
// calls are keyed by node ID; TunnelPeer resolves a tunnel key. Callers
// deciding admission must treat an error like RoleNone: a database that
// cannot be read admits nobody.
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
