package bridge

import (
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/control"
)

// AccessKeyOf returns the access key a SigV4 request names, read without
// verifying anything: from the Authorization header's Credential= or a
// presigned URL's X-Amz-Credential. It returns false when the request
// has neither, or names two different keys.
func AccessKeyOf(r *http.Request) (string, bool) {
	var keys []string
	if h := r.Header.Get("Authorization"); h != "" {
		k, ok := headerAccessKey(h)
		if !ok {
			return "", false
		}
		keys = append(keys, k)
	}
	if q := r.URL.Query(); q.Has("X-Amz-Credential") {
		k, _, _ := strings.Cut(q.Get("X-Amz-Credential"), "/")
		keys = append(keys, k)
	}
	if len(keys) == 0 || keys[0] == "" {
		return "", false
	}
	for _, k := range keys[1:] {
		if k != keys[0] {
			return "", false
		}
	}
	return keys[0], true
}

// headerAccessKey reads Credential=<key>/… from a SigV4 Authorization
// header. Any other scheme (SigV2's "AWS key:sig") is not a credential
// form lendisk accepts on the tunnel.
func headerAccessKey(h string) (string, bool) {
	scheme, rest, ok := strings.Cut(h, " ")
	if !ok || scheme != "AWS4-HMAC-SHA256" {
		return "", false
	}
	for _, part := range strings.Split(rest, ",") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(part), "Credential="); ok {
			k, _, _ := strings.Cut(v, "/")
			return k, k != ""
		}
	}
	return "", false
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
		defer b.bound(w, r)()
		b.proxy.ServeHTTP(w, r)
	})
}

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
