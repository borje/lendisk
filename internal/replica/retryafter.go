package replica

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// A busy place's Retry-After (#93, LEND-2). minio-go keeps no response
// header in its ErrorResponse, so the client's transport notes the
// header of a 503 answer in the request's context, where the worker
// set a hint (withRetryHint) around its send; the worker then waits
// that long, capped by max_backoff, in place of its own backoff.

type retryHintKey struct{}

// retryHint is the last Retry-After noted in a context.
type retryHint struct {
	mu    sync.Mutex
	after time.Duration
}

// withRetryHint returns ctx carrying a hint, and a function returning
// the last Retry-After a 503 answer to a request on it gave (zero if
// none did).
func withRetryHint(ctx context.Context) (context.Context, func() time.Duration) {
	h := &retryHint{}
	return context.WithValue(ctx, retryHintKey{}, h), func() time.Duration {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.after
	}
}

// noteRetryAfter records d in ctx's hint, if it has one.
func noteRetryAfter(ctx context.Context, d time.Duration) {
	if h, ok := ctx.Value(retryHintKey{}).(*retryHint); ok {
		h.mu.Lock()
		h.after = d
		h.mu.Unlock()
	}
}

// hinting is a transport that notes a 503 answer's Retry-After in its
// request's context.
type hinting struct{ http.RoundTripper }

func (t hinting) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(r)
	if err == nil && resp.StatusCode == http.StatusServiceUnavailable {
		if d := retryAfter(resp.Header.Get("Retry-After"), time.Now()); d > 0 {
			noteRetryAfter(r.Context(), d)
		}
	}
	return resp, err
}

// retryAfter parses a Retry-After value, in seconds or an HTTP date,
// as a wait from now; zero if it is neither or already past.
func retryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n <= 0 {
			return 0
		}
		return time.Duration(min(n, int64(24*time.Hour/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		return max(at.Sub(now), 0)
	}
	return 0
}
