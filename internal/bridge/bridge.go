// Package bridge owns every TCP listener in front of the gateway's internal
// socket (§3.7): the loopback S3 endpoint (EP-1), the LAN listeners and
// their TLS certificate (EP-17, SEC-3), and the tunnel port, where the
// control API and S3 for friends are served (§3.7). It proxies each S3
// request, unbuffered, to gw.sock with the Host header and every signed
// header unchanged, so the client's SigV4 signature still verifies there
// (Q-G). lendisk never computes a signature.
//
// Rules: never use a ServeMux or anything else that cleans or rewrites the
// path, since the signature covers it; never set X-Forwarded-* headers;
// refuse the control API path on loopback and LAN listeners; on the
// tunnel, proxy an S3 request only when its access key is the
// connection's peer key (SEC-12).
package bridge

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/gateway"
)

// Bridge proxies HTTP requests to the gateway socket.
type Bridge struct {
	Socket string
	Log    *slog.Logger
	// Stall and MinProgress bound a request body on the tunnel: one that
	// brings fewer than MinProgress bytes in a Stall period ends the
	// request (defaults DefaultStall, DefaultMinProgress).
	Stall       time.Duration
	MinProgress int64
	// FloorRate and FloorWindow bound it further: one that brings fewer
	// than FloorRate bytes a second over a FloorWindow ends the request
	// (defaults DefaultFloorRate, DefaultFloorWindow).
	FloorRate   int64
	FloorWindow time.Duration
	proxy       *httputil.ReverseProxy
}

// New returns a bridge to the gateway on socket.
func New(socket string, log *slog.Logger) *Bridge {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		// Pass Accept-Encoding through untouched; never add gzip.
		DisableCompression:  true,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	b := &Bridge{Socket: socket, Log: log}
	b.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = "gw"
			pr.Out.Host = pr.In.Host // signed; must reach versitygw unchanged
		},
		Transport:     tr,
		FlushInterval: -1, // stream responses as they come
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if wasCut(r) {
				// The friend was removed mid-request (SEC-15): the
				// proxied request ended with it.
				log.Info("tunnel: a request in flight ended with its connection, cut as the friend was removed", "method", r.Method, "path", r.URL.Path)
				writeCut(w, r)
				return
			}
			if stalledBody(r) {
				log.Info("tunnel: a request body stalled; ended the request", "method", r.Method, "path", r.URL.Path)
				writeError(w, errStalled)
				return
			}
			if !errors.Is(err, context.Canceled) {
				log.Warn("proxy to gateway failed", "method", r.Method, "path", r.URL.Path, "err", err)
			}
			writeError(w, s3err.APIError{Code: "ServiceUnavailable",
				Description: "lendisk: gateway unavailable", HTTPStatusCode: http.StatusServiceUnavailable})
		},
	}
	return b
}

// LoopbackHandler serves the S3 endpoint for tools on this host (EP-1)
// and on the LAN listeners (EP-17). A friend's credential, a node ID as
// the access key, is refused here: it is served on the tunnel only,
// where the connection's key binds it (SEC-12) and the per-friend caps
// apply (#15, DESIGN §3.9).
func (b *Bridge) LoopbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gateway.IsControlPath(r.URL.Path) {
			writeError(w, s3err.APIError{Code: "AccessDenied",
				Description: "lendisk: control API not served here", HTTPStatusCode: http.StatusForbidden})
			return
		}
		creds := credentialsOf(r)
		if !creds.Readable {
			writeError(w, s3err.APIError{Code: "AccessDenied", HTTPStatusCode: http.StatusForbidden,
				Description: "lendisk: the request's credential cannot be read; sign it with SigV4"})
			return
		}
		if creds.Friend {
			b.Log.Warn("refused a friend's credential on a tool listener", "remote", r.RemoteAddr, "method", r.Method)
			writeError(w, s3err.APIError{Code: "AccessDenied", HTTPStatusCode: http.StatusForbidden,
				Description: "lendisk: a friend's node is served on the tunnel only, not on this listener"})
			return
		}
		b.proxy.ServeHTTP(w, r)
	})
}

// Serve accepts connections on ln with h until ctx is cancelled.
func Serve(ctx context.Context, ln net.Listener, h http.Handler, log *slog.Logger) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func writeError(w http.ResponseWriter, e s3err.APIError) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.HTTPStatusCode)
	w.Write(e.XMLBody("", ""))
}
