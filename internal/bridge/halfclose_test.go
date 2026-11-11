package bridge

import (
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// heldGateway stands in for gw.sock: each request reports on arrived,
// then answers 200 "the answer" once release is closed, or reports on
// cancelled if its context ends first.
func heldGateway(t *testing.T) (sock string, arrived, cancelled, release chan struct{}) {
	t.Helper()
	sock = filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	arrived, cancelled, release = make(chan struct{}, 8), make(chan struct{}, 8), make(chan struct{})
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
			w.Write([]byte("the answer"))
		case <-r.Context().Done():
			cancelled <- struct{}{}
		}
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock, arrived, cancelled, release
}

// raw dials the fake tunnel as key and sends req on the connection.
func (f *fakeTunnel) raw(t *testing.T, key *identity.Identity, req string) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp", f.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	f.mu.Lock()
	f.keys[c.LocalAddr().String()] = key.NodeID()
	f.mu.Unlock()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	return c.(*net.TCPConn)
}

func signedGet(key *identity.Identity) string {
	return "GET /sbucket/key HTTP/1.1\r\nHost: lender\r\nAuthorization: " + sigv4(key.NodeID().String()) + "\r\n\r\n"
}

// #101: a friend that half-closes once its request is sent (nc -N
// through peer forward) reads the gateway's own answer in full, though
// net/http reads the end of stream while the gateway is still working:
// neither the relationship lookup nor the proxied request is cancelled
// by it.
func TestAHalfClosingFriendReadsTheGatewaysAnswer(t *testing.T) {
	sock, arrived, cancelled, release := heldGateway(t)
	l := startLenderS3(t, New(sock, quiet).TunnelS3Handler())
	bob := newID(t)
	l.befriend(t, bob)
	c := l.tun.raw(t, bob, signedGet(bob))
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the gateway")
	}
	// net/http's background read meets the end of stream meanwhile.
	time.Sleep(200 * time.Millisecond)
	close(release)
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	if !strings.HasPrefix(string(got), "HTTP/1.1 200 ") || !strings.HasSuffix(string(got), "the answer") {
		t.Fatalf("a half-closing friend read %q", got)
	}
	select {
	case <-cancelled:
		t.Fatal("the gateway's request was cancelled by the half-close")
	default:
	}
}

// #101: a friend whose connection resets while the gateway works on
// its request still ends the proxied request, so what the gateway
// holds for it is given back (LEND-2).
func TestAFriendWhoseConnectionResetsEndsItsProxiedRequest(t *testing.T) {
	sock, arrived, cancelled, _ := heldGateway(t)
	l := startLenderS3(t, New(sock, quiet).TunnelS3Handler())
	bob := newID(t)
	l.befriend(t, bob)
	c := l.tun.raw(t, bob, signedGet(bob))
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the gateway")
	}
	c.SetLinger(0) // close with a reset
	c.Close()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the reset did not end the gateway's request")
	}
}
