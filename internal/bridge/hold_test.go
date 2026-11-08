package bridge

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// streamingGateway stands in for gw.sock: GET of a key ending in "big"
// streams bytes until the request ends; a PUT reads its body whole;
// anything else answers at once.
func streamingGateway(t *testing.T) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "big") {
			chunk := bytes.Repeat([]byte("b"), 32<<10)
			for r.Context().Err() == nil {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock
}

// befriend adds friendship rows for each friend.
func (l *lender) befriend(t *testing.T, friends ...*identity.Identity) {
	t.Helper()
	for _, f := range friends {
		if err := l.db.AddFriendship(t.Context(), state.Friendship{Peer: f.NodeID().String(), CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
}

// untimed is key's tunnel client without a client timeout, which would
// close a connection the test means to leave unread.
func (f *fakeTunnel) untimed(key *identity.Identity) *http.Client {
	return &http.Client{Transport: f.client(key).HTTP.Transport}
}

// #76: a friend that stops reading a GetObject answer mid-body holds its
// request slot only until one chunk's write has waited the write bound:
// then the connection is closed and the slot is given back.
func TestAFriendThatStopsReadingAnAnswerLosesItsSlot(t *testing.T) {
	br := New(streamingGateway(t), quiet)
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{Requests: 1, WriteStall: 200 * time.Millisecond})
	bob := newID(t)
	l.befriend(t, bob)
	c := l.tun.untimed(bob)
	get := func(path string) *http.Response {
		t.Helper()
		r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://lender:9000"+path, nil)
		r.Header.Set("Authorization", sigv4(bob.NodeID().String()))
		resp, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	big := get("/sbucket/big")
	if big.StatusCode != http.StatusOK {
		t.Fatalf("GetObject: %d", big.StatusCode)
	}
	defer big.Body.Close()
	// Bob reads nothing more. His one slot comes back once the
	// lender's write gives up; until then he is told to slow down.
	for give := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		resp := get("/sbucket/small")
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			break
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("Bob's second request: %d", resp.StatusCode)
		}
		if time.Now().After(give) {
			t.Fatal("Bob still holds his slot with an answer he does not read")
		}
	}
	if _, err := io.Copy(io.Discard, big.Body); err == nil {
		t.Fatal("the unread answer ended whole; want its connection closed")
	}
}

// #76, LEND-2, SEC-5: a PUT body trickling fast enough for the stall
// bound but below the floor rate over a window is ended with
// RequestTimeout, and gives its slot back; a body at speed is served.
func TestABodyTricklingBelowTheFloorRateIsEnded(t *testing.T) {
	br := New(streamingGateway(t), quiet)
	br.Stall, br.MinProgress = time.Minute, 1 // the stall bound never ends it
	br.FloorRate, br.FloorWindow = 64<<10, 300*time.Millisecond
	l := startLenderCaps(t, br.TunnelS3Handler(), &Caps{Requests: 1})
	bob := newID(t)
	l.befriend(t, bob)
	c := l.tun.untimed(bob)
	const size = 256 << 10
	put := func(body io.Reader) (int, string) {
		t.Helper()
		r, _ := http.NewRequestWithContext(t.Context(), "PUT", "http://lender:9000/sbucket/key", body)
		r.ContentLength = size
		r.Header.Set("Authorization", sigv4(bob.NodeID().String()))
		resp, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(b)
	}
	pr, pw := io.Pipe()
	defer pw.CloseWithError(io.ErrClosedPipe)
	go func() {
		// 20 KiB/s: 13 s for the whole body at this rate.
		for i := 0; i < size; i += 1 << 10 {
			if _, err := pw.Write(make([]byte, 1<<10)); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		pw.Close()
	}()
	if code, body := put(pr); code != http.StatusBadRequest || !strings.Contains(body, "RequestTimeout") {
		t.Fatalf("a trickling body: %d %s; want 400 RequestTimeout", code, body)
	}
	if code, body := put(bytes.NewReader(make([]byte, size))); code != http.StatusOK {
		t.Fatalf("a body at speed after it: %d %s", code, body)
	}
}

// #76 (#15 re-review): the per-friend cap's lookup of whom a key speaks
// for runs off the accept loop, so a lookup that hangs on one key's
// connection (a slow database) does not keep the tunnel from accepting
// and serving another friend's.
func TestASlowCapLookupDoesNotStallOtherAccepts(t *testing.T) {
	bob, carol := newID(t), newID(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	caps := &Caps{Of: func(ctx context.Context, key identity.NodeID) (identity.NodeID, state.Role, error) {
		if key == bob.NodeID() {
			close(entered)
			<-release
		}
		return key, state.RolePeer, nil
	}}
	br := New(streamingGateway(t), quiet)
	l := startLenderCaps(t, br.TunnelS3Handler(), caps)
	l.befriend(t, bob, carol)
	raw, err := net.Dial("tcp", l.tun.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	l.tun.mu.Lock()
	l.tun.keys[raw.LocalAddr().String()] = bob.NodeID()
	l.tun.mu.Unlock()
	<-entered
	// Bob's lookup hangs; Carol is accepted and served meanwhile.
	r, _ := http.NewRequestWithContext(t.Context(), "GET", "http://lender:9000/sbucket/key", nil)
	r.Header.Set("Authorization", sigv4(carol.NodeID().String()))
	resp, err := l.tun.client(carol).HTTP.Do(r)
	if err != nil {
		t.Fatalf("Carol while Bob's lookup hangs: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Carol while Bob's lookup hangs: %d", resp.StatusCode)
	}
	// Once it answers, Bob is served too.
	close(release)
	raw.Write([]byte("GET /sbucket/key HTTP/1.1\r\nHost: lender\r\nAuthorization: " + sigv4(bob.NodeID().String()) + "\r\n\r\n"))
	got, err := http.ReadResponse(bufio.NewReader(raw), nil)
	if err != nil {
		t.Fatalf("Bob after his lookup: %v", err)
	}
	got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("Bob after his lookup: %d", got.StatusCode)
	}
}
