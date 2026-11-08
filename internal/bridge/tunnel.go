package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// RoleOf resolves the key a tunnel connection authenticated with to whom
// it speaks for and what that one may do now (state.DB.TunnelPeer): a
// friend's dial key names the friend (#69). An error refuses the
// request.
type RoleOf func(ctx context.Context, key identity.NodeID) (identity.NodeID, state.Role, error)

// RolesIn is the RoleOf that reads db at now().
func RolesIn(db *state.DB, now func() time.Time) RoleOf {
	return func(ctx context.Context, key identity.NodeID) (identity.NodeID, state.Role, error) {
		p, r, err := db.TunnelPeer(ctx, key.String(), now())
		if err != nil || r == state.RoleNone {
			return key, r, err
		}
		peer, err := identity.ParseNodeID(p)
		if err != nil {
			return key, state.RoleNone, fmt.Errorf("the relationship of %s names %q: %w", key, p, err)
		}
		return peer, r, nil
	}
}

// PeerOf returns the key that authenticated an accepted tunnel
// connection (transport.Transport.PeerOf).
type PeerOf func(net.Conn) (identity.NodeID, error)

// TunnelHandler is the tunnel port's one middleware (SEC-11, SEC-12,
// SEC-13), in front of everything served there. On every request it
// takes the connection's key, resolves it in lendisk.db to a peer and
// its role, and refuses before routing if there is none; an invite key
// may call redeem and nothing else. Then /_lendisk/ goes to ctl, with
// the caller (the resolved peer: a friend's node ID, never its dial
// key) in the request context, and everything else to s3
// (Bridge.TunnelS3Handler in the daemon). A nil s3 answers S3 with 501.
// A friend at its cap of requests in flight (caps, shared with
// ServeTunnel; nil means the defaults) is answered 503 SlowDown.
func TunnelHandler(roles RoleOf, ctl http.Handler, s3 http.Handler, caps *Caps, log *slog.Logger) http.Handler {
	if caps == nil {
		caps = &Caps{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isControl := isControlPath(r)
		refuse := func(code int, msg string) {
			// Every refusal says what it speaks (NET-6), so the caller
			// can tell lendisk refused it from something else answering.
			w.Header().Set(control.ProtocolHeader, control.Protocol)
			if isControl {
				control.Error(w, code, msg)
				return
			}
			s3code := "AccessDenied"
			if code != http.StatusForbidden {
				s3code = http.StatusText(code)
			}
			writeError(w, s3err.APIError{Code: s3code, Description: "lendisk: " + msg, HTTPStatusCode: code})
		}
		key, ok := r.Context().Value(peerKey{}).(identity.NodeID)
		if !ok {
			refuse(http.StatusForbidden, "connection has no peer key")
			return
		}
		peer, role, err := roles(r.Context(), key)
		if err != nil {
			log.Error("tunnel request refused: relationship lookup failed", "peer", key.String(), "err", err)
			refuse(http.StatusServiceUnavailable, "cannot read relationships; try again later")
			return
		}
		switch role {
		case state.RolePeer:
		case state.RoleInvite:
			if r.Method != http.MethodPost || r.URL.Path != control.RedeemPath {
				log.Warn("tunnel request refused: an invite key may only redeem", "method", r.Method, "path", r.URL.Path)
				refuse(http.StatusForbidden, "an invite may only be redeemed")
				return
			}
		default:
			log.Warn("tunnel request refused: no relationship", "peer", key.String())
			if r.URL.Path == control.RedeemPath {
				// A used invite's key keeps its tunnel for a while.
				refuse(http.StatusForbidden, "no pending invite for this key: it was already used or has expired; ask for a new one")
				return
			}
			refuse(http.StatusForbidden, state.NoRelationship)
			return
		}
		if wasCut(r) {
			// The connection is being cut (SEC-15): whatever the row says
			// by now, nothing more is served on it.
			log.Warn("tunnel request refused: its connection was cut", "peer", key.String())
			writeCut(w, r)
			return
		}
		if !caps.request(peer) {
			log.Warn("tunnel request refused: the friend is at its cap of requests in flight", "peer", peer.String())
			w.Header().Set("Retry-After", "1")
			if isControl {
				refuse(http.StatusServiceUnavailable, "too many requests in flight from this node; retry shortly")
			} else {
				w.Header().Set(control.ProtocolHeader, control.Protocol)
				writeError(w, s3err.APIError{Code: "SlowDown", HTTPStatusCode: http.StatusServiceUnavailable,
					Description: "lendisk: too many requests in flight from this node; retry shortly"})
			}
			return
		}
		defer caps.requestDone(peer)
		r = r.WithContext(control.WithCaller(r.Context(), control.Caller{Peer: peer, Role: role}))
		switch {
		case isControl:
			ctl.ServeHTTP(w, r)
		case s3 != nil:
			s3.ServeHTTP(w, r)
		default:
			refuse(http.StatusNotImplemented, "S3 is not served on the tunnel yet")
		}
	})
}

type peerKey struct{}

func isControlPath(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, control.Prefix) }

// peerListener closes, unread, every connection without a peer key, and
// every connection over its key's cap; it tracks the rest in conns.
type peerListener struct {
	net.Listener
	peerOf PeerOf
	caps   *Caps
	conns  *Conns
	log    *slog.Logger
}

func (l *peerListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		p, err := l.peerOf(c)
		if err != nil {
			l.log.Warn("tunnel connection without a peer key; closed unread", "remote", c.RemoteAddr().String(), "err", err)
			c.Close()
			continue
		}
		friend := l.caps.friendOf(p)
		if !l.caps.conn(friend) {
			l.log.Warn("tunnel connection over the friend's cap; closed unread", "peer", friend.String(), "key", p.String())
			c.Close()
			continue
		}
		l.log.Debug("tunnel connection", "peer", p.String())
		pc := &peerConn{Conn: c, peer: p, done: func() { l.caps.connDone(friend) }, conns: l.conns}
		l.conns.add(pc)
		return pc, nil
	}
}

// ServeTunnel serves h on the tunnel listener ln until ctx is cancelled.
// Each request's context carries the key of its connection (SEC-12),
// which TunnelHandler reads. caps bounds each key's open connections; it
// is the Caps given to TunnelHandler, and nil means the defaults. conns
// tracks the open connections by key, so Conns.Cut can end them
// (SEC-15); nil tracks nothing.
func ServeTunnel(ctx context.Context, ln net.Listener, peerOf PeerOf, caps *Caps, conns *Conns, h http.Handler, log *slog.Logger) error {
	if caps == nil {
		caps = &Caps{}
	}
	srv := &http.Server{
		Handler:           counted(h),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelDebug),
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if pc, ok := c.(*peerConn); ok {
				return context.WithValue(context.WithValue(pc.context(ctx), peerKey{}, pc.peer), connKey{}, pc)
			}
			return ctx
		},
	}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	err := srv.Serve(&peerListener{Listener: ln, peerOf: peerOf, caps: caps, conns: conns, log: log})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
