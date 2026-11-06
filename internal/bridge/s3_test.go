package bridge

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// fakeGateway stands in for gw.sock and records what reaches it.
type fakeGateway struct {
	mu   sync.Mutex
	seen []*http.Request
}

func startFakeGateway(t *testing.T) (string, *fakeGateway) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		g.mu.Lock()
		g.seen = append(g.seen, r)
		g.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock, g
}

func (g *fakeGateway) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.seen)
}

func (g *fakeGateway) last() *http.Request {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seen[len(g.seen)-1]
}

func sigv4(access string) string {
	return "AWS4-HMAC-SHA256 Credential=" + access + "/20261106/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=00ff"
}

// SEC-12 AC at the bridge: an S3 request on the tunnel is proxied to
// gw.sock, signed headers and Host unchanged, only when its access key is
// the connection's peer key; another key, or no credential at all, is
// refused before the gateway sees it.
func TestTunnelS3BindsAccessKeyToPeer(t *testing.T) {
	sock, gw := startFakeGateway(t)
	br := New(sock, quiet)
	l := startLenderS3(t, br.TunnelS3Handler())
	bob, carol := newID(t), newID(t)
	for _, f := range []string{bob.NodeID().String(), carol.NodeID().String()} {
		if err := l.db.AddFriendship(t.Context(), state.Friendship{Peer: f, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	c := l.tun.client(bob).HTTP
	send := func(path, auth string) int {
		t.Helper()
		r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://lender:9000"+path, nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		r.Header.Set("X-Amz-Date", "20261106T000000Z")
		resp, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := send("/sbucket/key", sigv4(bob.NodeID().String())); got != http.StatusOK {
		t.Fatalf("Bob's own key: %d", got)
	}
	r := gw.last()
	if r.Header.Get("Authorization") != sigv4(bob.NodeID().String()) || r.Host != "lender:9000" ||
		r.Header.Get("X-Amz-Date") != "20261106T000000Z" || r.URL.Path != "/sbucket/key" {
		t.Fatalf("gateway saw %s %s %v", r.Host, r.URL, r.Header)
	}
	q := "/sbucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + bob.NodeID().String() +
		"%2F20261106%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=00ff"
	if got := send(q, ""); got != http.StatusOK {
		t.Fatalf("Bob's presigned URL: %d", got)
	}

	before := gw.count()
	for name, auth := range map[string]string{
		"Carol's key on Bob's connection": sigv4(carol.NodeID().String()),
		"an own bucket's key":             sigv4("LDKAAAAAAAAAAAAAAAA"),
		"a SigV2 header":                  "AWS " + bob.NodeID().String() + ":c2ln",
		"no credential":                   "",
	} {
		if got := send("/sbucket/key", auth); got != http.StatusForbidden {
			t.Errorf("%s: %d, want 403", name, got)
		}
	}
	carolQuery := "/sbucket/key?X-Amz-Credential=" + carol.NodeID().String() + "%2F20261106%2Fus-east-1%2Fs3%2Faws4_request"
	if got := send(carolQuery, ""); got != http.StatusForbidden {
		t.Errorf("Carol's presigned URL on Bob's connection: %d", got)
	}
	if got := send(carolQuery, sigv4(bob.NodeID().String())); got != http.StatusForbidden {
		t.Errorf("a header and a query naming two keys: %d", got)
	}
	if gw.count() != before {
		t.Fatalf("%d refused requests reached the gateway", gw.count()-before)
	}
}

// #53: the loopback and LAN listeners refuse the control API path in
// every spelling that cleans to it, and pass everything else.
func TestLoopbackRefusesControlPathAfterCleaning(t *testing.T) {
	sock, g := startFakeGateway(t)
	h := New(sock, quiet).LoopbackHandler()
	serve := func(p string) int {
		r := httptest.NewRequest("GET", "/", nil)
		r.URL.Path = p
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	for _, p := range []string{"/_lendisk", "/_lendisk/", "/_lendisk/version", "//_lendisk/x",
		"/./_lendisk/redeem", "/b/../_lendisk/version", "_lendisk/x"} {
		if code := serve(p); code != http.StatusForbidden {
			t.Errorf("%q: %d", p, code)
		}
	}
	if n := g.count(); n != 0 {
		t.Fatalf("%d control requests reached the gateway", n)
	}
	for _, p := range []string{"/bucket/_lendisk/x", "/_lendiskish/x", "/bucket"} {
		if code := serve(p); code != http.StatusOK {
			t.Errorf("%q: %d", p, code)
		}
	}
}
