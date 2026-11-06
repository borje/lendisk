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
	proxy  *httputil.ReverseProxy
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
			if !errors.Is(err, context.Canceled) {
				log.Warn("proxy to gateway failed", "method", r.Method, "path", r.URL.Path, "err", err)
			}
			writeError(w, s3err.APIError{Code: "ServiceUnavailable",
				Description: "lendisk: gateway unavailable", HTTPStatusCode: http.StatusServiceUnavailable})
		},
	}
	return b
}

// LoopbackHandler serves the S3 endpoint for tools on this host (EP-1).
func (b *Bridge) LoopbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gateway.IsControlPath(r.URL.Path) {
			writeError(w, s3err.APIError{Code: "AccessDenied",
				Description: "lendisk: control API not served here", HTTPStatusCode: http.StatusForbidden})
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
