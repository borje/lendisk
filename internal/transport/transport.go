// Package transport owns the peer connection: the authenticated tunnel
// every byte between friends travels in (NET-1), behind an interface
// that a second implementation could meet (NET-8). Tailcat, over
// Tailscale's tailcat, is the only implementation.
//
// Rules a caller must keep:
//
//   - Admission fails closed (SEC-10). Admit must be given a non-nil
//     function before Listen, which refuses otherwise, and an
//     implementation with no admission function refuses every
//     connection. Use Admission for the function: it reads lendisk.db
//     on every lookup and rejects on any error.
//   - After committing a change to a friendship or an invite, call
//     Admission.Clear, so a key refused before is looked up again.
//   - Close an accepted connection whose PeerOf fails before reading
//     from it (SEC-12).
//   - A node serves with its identity key and dials each friend with
//     its dial key for that friend (identity.DialKeyFor, #69). PeerOf
//     names the key a connection authenticated with, which for a friend
//     is its dial key: resolve it with state.DB.TunnelPeer before acting
//     for anyone, and admit with the same lookup. To drop a friend,
//     Disconnect both its keys.
//   - The address is a pure function of the identity key and the saved
//     DERP region (SEC-14). EnsureRegion chooses the region once and
//     saves it in lendisk.db; never pick one per start.
package transport

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// Port is the one tunnel port a node serves (DESIGN §3.7). Dialling
// friends assumes it, so it is not a setting.
const Port = 9000

// Transport is the peer connection (NET-8).
type Transport interface {
	// Admit sets the admission function, asked about every key that
	// tries to connect. It must be called before Listen.
	Admit(fn func(peer identity.NodeID) bool)
	// Listen starts serving the tunnel port. It fails with
	// ErrNoAdmission when Admit was not given a function.
	Listen(ctx context.Context) (net.Listener, error)
	// Dial connects to a friend's tunnel port, with this node's dial
	// key. addr is the friend's address; it must name peer's key.
	Dial(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error)
	// PeerOf returns the key that authenticated an accepted connection.
	PeerOf(c net.Conn) (identity.NodeID, error)
	// Disconnect drops a peer's tunnel and reports whether it was
	// connected. It closes none of the peer's connections; the caller
	// closes them first (SEC-15).
	Disconnect(peer identity.NodeID) bool
	// PathInfo reports how a dialled peer is reached (NET-4).
	PathInfo(ctx context.Context, peer identity.NodeID) (Path, error)
	// Address is this node's address, as given to friends (ID-5).
	Address() string
	// Close stops listening and dialling.
	Close() error
}

// Path describes how a peer was reached by a path probe.
type Path struct {
	Direct   bool          // the probe's answer came over a direct path
	Endpoint string        // ip:port of the direct path, if Direct
	Relay    string        // the relay region's code, if not Direct
	Latency  time.Duration // round trip of the probe
}

// ErrNoAdmission is returned by Listen when no admission function is set.
var ErrNoAdmission = errors.New("transport: refusing to listen without an admission function (it would admit everyone)")
