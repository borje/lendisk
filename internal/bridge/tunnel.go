package bridge

import (
	"context"
	"errors"
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

// RoleOf looks up what a tunnel peer may do now (state.DB.PeerRole). An
// error refuses the request.
type RoleOf func(ctx context.Context, peer identity.NodeID) (state.Role, error)

// PeerOf returns the key that authenticated an accepted tunnel
// connection (transport.Transport.PeerOf).
type PeerOf func(net.Conn) (identity.NodeID, error)

// TunnelHandler is the tunnel port's one middleware (SEC-11, SEC-12,
// SEC-13), in front of everything served there. On every request it
// takes the connection's key, loads that key's role from lendisk.db and
// refuses before routing if there is none; an invite key may call
// redeem and nothing else. Then /_lendisk/ goes to ctl, with the caller
// in the request context, and everything else to s3
// (Bridge.TunnelS3Handler in the daemon). A nil s3 answers S3 with 501.
func TunnelHandler(roles RoleOf, ctl http.Handler, s3 http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isControl := strings.HasPrefix(r.URL.Path, control.Prefix)
		refuse := func(code int, msg string) {
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
		peer, ok := r.Context().Value(peerKey{}).(identity.NodeID)
		if !ok {
			refuse(http.StatusForbidden, "connection has no peer key")
			return
		}
		role, err := roles(r.Context(), peer)
		if err != nil {
			log.Error("tunnel request refused: relationship lookup failed", "peer", peer.String(), "err", err)
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
			log.Warn("tunnel request refused: no relationship", "peer", peer.String())
			if r.URL.Path == control.RedeemPath {
				// A used invite's key keeps its tunnel for a while.
				refuse(http.StatusForbidden, "no pending invite for this key: it was already used or has expired; ask for a new one")
				return
			}
			refuse(http.StatusForbidden, "no relationship with this node")
			return
		}
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

// peerConn is an accepted tunnel connection with its key.
type peerConn struct {
	net.Conn
	peer identity.NodeID
}

// peerListener closes, unread, every connection without a peer key.
type peerListener struct {
	net.Listener
	peerOf PeerOf
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
		l.log.Debug("tunnel connection", "peer", p.String())
		return &peerConn{Conn: c, peer: p}, nil
	}
}

// ServeTunnel serves h on the tunnel listener ln until ctx is cancelled.
// Each request's context carries the key of its connection (SEC-12),
// which TunnelHandler reads.
func ServeTunnel(ctx context.Context, ln net.Listener, peerOf PeerOf, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelDebug),
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if pc, ok := c.(*peerConn); ok {
				return context.WithValue(ctx, peerKey{}, pc.peer)
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
	err := srv.Serve(&peerListener{Listener: ln, peerOf: peerOf, log: log})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
