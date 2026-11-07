package bridge

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// slowGateway stands in for gw.sock: a request for a key ending in
// "slow" waits until release is closed; anything else answers at once.
type slowGateway struct {
	release chan struct{}
	mu      sync.Mutex
	waiting int
}

func startSlowGateway(t *testing.T) (string, *slowGateway) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	g := &slowGateway{release: make(chan struct{})}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "slow") {
			g.mu.Lock()
			g.waiting++
			g.mu.Unlock()
			<-g.release
		}
		w.WriteHeader(http.StatusOK)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock, g
}

func (g *slowGateway) held() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting
}

// #16 review, SEC-7: one friend cannot exhaust the tunnel. With caps of
// 3 connections and 2 requests in flight per friend, Bob holding 2 slow
// requests on his node ID gets SlowDown for a third and has a fourth
// connection closed unread, though it comes on his dial key: the two
// keys share one budget, while Carol on the tunnel and a tool on the loopback
// listener of the same bridge are served; once Bob's requests finish he
// is served again.
func TestOneFriendCannotExhaustTheTunnel(t *testing.T) {
	sock, gw := startSlowGateway(t)
	br := New(sock, quiet)
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{Conns: 3, Requests: 2})
	bob, carol := newID(t), newID(t)
	bobDial := bob.DialKeyFor(l.self.NodeID())
	for f, dial := range map[string]string{bob.NodeID().String(): bobDial.NodeID().String(), carol.NodeID().String(): ""} {
		if err := l.db.AddFriendship(t.Context(), state.Friendship{Peer: f, DialKey: dial, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	released := false
	release := func() {
		if !released {
			released = true
			close(gw.release)
		}
	}
	t.Cleanup(release)
	get := func(c *http.Client, path, access string) (*http.Response, error) {
		r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://lender:9000"+path, nil)
		r.Header.Set("Authorization", sigv4(access))
		return c.Do(r)
	}
	bobC := l.tun.client(bob).HTTP
	slow := make(chan int, 2)
	for range 2 {
		go func() {
			resp, err := get(bobC, "/sbucket/slow", bob.NodeID().String())
			if err != nil {
				slow <- 0
				return
			}
			resp.Body.Close()
			slow <- resp.StatusCode
		}()
	}
	for deadline := time.Now().Add(5 * time.Second); gw.held() < 2; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("Bob's slow requests did not reach the gateway (%d)", gw.held())
		}
	}
	resp, err := get(bobC, "/sbucket/key", bob.NodeID().String())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "SlowDown") || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("Bob's third request in flight: %d %s", resp.StatusCode, body)
	}

	// Bob now holds three connections; a fourth, on his dial key, is
	// closed unread.
	raw, err := net.Dial("tcp", l.tun.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	l.tun.mu.Lock()
	l.tun.keys[raw.LocalAddr().String()] = bobDial.NodeID()
	l.tun.mu.Unlock()
	raw.Write([]byte("GET /sbucket/key HTTP/1.1\r\nHost: lender\r\nAuthorization: " + sigv4(bob.NodeID().String()) + "\r\n\r\n"))
	raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := raw.Read(make([]byte, 64)); err == nil {
		t.Fatalf("Bob's fourth connection was served (%d bytes)", n)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("Bob's fourth connection was neither served nor closed")
	}

	// Carol and a loopback tool are served meanwhile.
	resp, err = get(l.tun.client(carol).HTTP, "/sbucket/key", carol.NodeID().String())
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Carol while Bob is at his caps: %v %v", resp, err)
	}
	resp.Body.Close()
	loop := httptest.NewServer(br.LoopbackHandler())
	defer loop.Close()
	r, _ := http.NewRequest("GET", loop.URL+"/photos/key", nil)
	r.Header.Set("Authorization", sigv4("LDKAAAAAAAAAAAAAAAAA"))
	if resp, err := http.DefaultClient.Do(r); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("a loopback tool while Bob is at his caps: %v %v", resp, err)
	} else {
		resp.Body.Close()
	}

	release()
	for range 2 {
		if code := <-slow; code != http.StatusOK {
			t.Fatalf("Bob's slow request: %d", code)
		}
	}
	resp, err = get(bobC, "/sbucket/key", bob.NodeID().String())
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Bob after his requests finished: %v %v", resp, err)
	}
	resp.Body.Close()
}
