package bridge

import (
	"io"
	"net/http"
	"sync/atomic"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
)

// AccessKeyOf returns the access key a SigV4 request names, read without
// verifying anything and exactly as versitygw reads it
// (gateway.ReadCredentials): from the Authorization header or a
// presigned URL's X-Amz-Credential. It returns false when the request
// has neither, names two different keys, or carries a credential that
// cannot be read that way.
func AccessKeyOf(r *http.Request) (string, bool) {
	c := credentialsOf(r)
	if !c.Readable || len(c.Keys) == 0 || c.Keys[0] == "" {
		return "", false
	}
	for _, k := range c.Keys[1:] {
		if k != c.Keys[0] {
			return "", false
		}
	}
	return c.Keys[0], true
}

// credentialsOf reads r's credentials (gateway.ReadCredentials).
func credentialsOf(r *http.Request) gateway.Credentials {
	var query [][2]string
	for k, vs := range r.URL.Query() {
		for _, v := range vs {
			query = append(query, [2]string{k, v})
		}
	}
	return gateway.ReadCredentials(r.Header.Values("Authorization"), query)
}

// TunnelS3Handler serves S3 on the tunnel port behind TunnelHandler
// (SEC-12, §3.7): a request whose access key is not the connection's
// peer key, or that carries no credential, is refused with AccessDenied;
// anything else is proxied to gw.sock unchanged, where versitygw
// verifies the signature and its IAM service resolves the key (NET-7a).
// A request body is bounded by the connection's read deadline (bound):
// fasthttp behind gw.sock reads without a timeout, so the bound has to
// be here.
func (b *Bridge) TunnelS3Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, ok := control.CallerFrom(r.Context())
		if !ok {
			writeError(w, s3err.APIError{Code: "AccessDenied", Description: "lendisk: no peer for this request", HTTPStatusCode: http.StatusForbidden})
			return
		}
		key, ok := AccessKeyOf(r)
		if !ok {
			b.Log.Warn("tunnel S3 request refused: no credential", "peer", caller.Peer.String(), "method", r.Method)
			writeError(w, s3err.APIError{Code: "AccessDenied", HTTPStatusCode: http.StatusForbidden,
				Description: "lendisk: an S3 request on the tunnel must be signed (SigV4) with this node's key as the access key"})
			return
		}
		if key != caller.Peer.String() {
			b.Log.Warn("tunnel S3 request refused: the access key is not the connection's key", "peer", caller.Peer.String(), "method", r.Method)
			writeError(w, s3err.APIError{Code: "AccessDenied", HTTPStatusCode: http.StatusForbidden,
				Description: "lendisk: the access key must be the node ID this connection authenticated as"})
			return
		}
		defer b.bound(w, r, caller.Peer.String())()
		if b.Pace != nil {
			// LEND-5: the answer to this borrower at its own rate.
			w = &pacedAnswer{ResponseWriter: w, w: b.Pace.LendOut(r.Context(), caller.Peer.String(), w)}
		}
		b.proxy.ServeHTTP(w, r)
	})
}

// pacedAnswer is a response writer whose body is paced (LEND-5). Unwrap
// lets the proxy flush and set deadlines on the connection's writer.
type pacedAnswer struct {
	http.ResponseWriter
	w io.Writer
}

func (p *pacedAnswer) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pacedAnswer) Unwrap() http.ResponseWriter { return p.ResponseWriter }

// Late is a handler set once its target exists: the tunnel serves before
// the gateway is ready, and answers S3 with 503 until then.
type Late struct{ h atomic.Pointer[http.Handler] }

// Set installs h.
func (l *Late) Set(h http.Handler) { l.h.Store(&h) }

func (l *Late) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := l.h.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	writeError(w, s3err.APIError{Code: "ServiceUnavailable", Description: "lendisk: the store is starting; try again shortly",
		HTTPStatusCode: http.StatusServiceUnavailable})
}
