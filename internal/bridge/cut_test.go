package bridge

import (
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
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// blockingGateway stands in for gw.sock: each request reads its body
// until its context ends, which it reports on cancelled.
func blockingGateway(t *testing.T) (sock string, arrived, cancelled chan struct{}) {
	t.Helper()
	sock = filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	arrived, cancelled = make(chan struct{}, 8), make(chan struct{}, 8)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusOK)
			return
		}
		arrived <- struct{}{}
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		cancelled <- struct{}{}
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return sock, arrived, cancelled
}

// SEC-15 (#25, from #14): once a friend's row is gone, Cut cancels the
// in-flight requests of every connection that authenticated with its
// node ID or its dial key, the proxied half to gw.sock included, answers
// them with the removed answer, and closes those connections; another
// friend's connection is untouched.
func TestCutCancelsInFlightRequestsAndClosesTheKeysConnections(t *testing.T) {
	sock, arrived, cancelled := blockingGateway(t)
	conns := &Conns{}
	l := startLenderConns(t, New(sock, quiet).TunnelS3Handler(), nil, conns)
	lenderKey := l.self.NodeID()
	bob, carol := newID(t), newID(t)
	bobDial := bob.DialKeyFor(lenderKey)
	for _, f := range []state.Friendship{
		{Peer: bob.NodeID().String(), DialKey: bobDial.NodeID().String(), CreatedAt: time.Now()},
		{Peer: carol.NodeID().String(), CreatedAt: time.Now()},
	} {
		if err := l.db.AddFriendship(t.Context(), f); err != nil {
			t.Fatal(err)
		}
	}
	send := func(c *http.Client, method, auth string, body io.Reader) (*http.Response, error) {
		r, _ := http.NewRequestWithContext(t.Context(), method, "http://lender:9000/sbucket/key", body)
		r.Header.Set("Authorization", auth)
		return c.Do(r)
	}
	ok := func(c *http.Client, who string) {
		t.Helper()
		resp, err := send(c, http.MethodGet, sigv4(who), nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET as %s: %v %v", who, resp, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	// Bob holds an idle connection on his node ID, Carol one of hers, and
	// Bob's dial key a PUT whose body never ends.
	bobNode, bobOnDial, carolC := l.tun.client(bob).HTTP, l.tun.client(bobDial).HTTP, l.tun.client(carol).HTTP
	ok(bobNode, bob.NodeID().String())
	ok(carolC, carol.NodeID().String())
	// A running transfer: the body trickles in until the request ends.
	pr, pw := io.Pipe()
	defer pw.Close()
	go func() {
		for {
			if _, err := pw.Write([]byte(strings.Repeat("x", 1024))); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	type answer struct {
		resp *http.Response
		err  error
	}
	put := make(chan answer, 1)
	go func() {
		resp, err := send(bobOnDial, http.MethodPut, sigv4(bob.NodeID().String()), pr)
		put <- answer{resp, err}
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the PUT never reached the gateway")
	}
	if n := conns.count(bob.NodeID()) + conns.count(bobDial.NodeID()); n != 2 {
		t.Fatalf("Bob's tracked connections: %d, want 2", n)
	}

	if _, err := l.db.RemoveFriendship(t.Context(), bob.NodeID().String(), false, time.Now()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if n := conns.Cut(bob.NodeID(), bobDial.NodeID()); n != 2 {
		t.Fatalf("Cut closed %d connections, want 2", n)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the proxied half of Bob's PUT was not cancelled")
	}
	select {
	case a := <-put:
		if a.err != nil {
			t.Fatalf("Bob's PUT in flight: %v; want the removed answer", a.err)
		}
		body, _ := io.ReadAll(a.resp.Body)
		a.resp.Body.Close()
		if a.resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), state.NoRelationship) {
			t.Fatalf("Bob's PUT in flight was answered %d %s", a.resp.StatusCode, body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Bob's PUT in flight was not ended")
	}
	t.Logf("Bob's PUT ended %v after the cut", time.Since(start).Round(time.Millisecond))
	if n := conns.count(bob.NodeID()) + conns.count(bobDial.NodeID()); n != 0 {
		t.Fatalf("Bob's connections left open: %d", n)
	}
	if n := conns.count(carol.NodeID()); n != 1 {
		t.Fatalf("Carol's connections after Bob's cut: %d, want 1", n)
	}
	ok(carolC, carol.NodeID().String())
	// Bob's next request, on a new connection, gets the removed answer.
	resp, err := send(bobNode, http.MethodGet, sigv4(bob.NodeID().String()), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("Bob after removal: %d", resp.StatusCode)
	}
}

// SEC-15, spike #3's TestCloseThenDisconnect over the real tunnel (#25):
// a removed friend's connection is closed by Cut before the tunnel is
// told to disconnect it; the client sees its connection end at once, the
// disconnect finds the client still connected, and the key cannot dial
// again.
func TestCutThenDisconnect(t *testing.T) {
	d := transporttest.Start(t)
	db, err := state.Open(t.Context(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srvID, cliID := newID(t), newID(t)
	dial := cliID.DialKeyFor(srvID.NodeID())
	if err := db.AddFriendship(t.Context(), state.Friendship{Peer: cliID.NodeID().String(),
		DialKey: dial.NodeID().String(), CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	adm := transport.NewAdmission(func(ctx context.Context, key identity.NodeID) (bool, error) {
		_, r, err := db.TunnelPeer(ctx, key.String(), time.Now())
		return r != state.RoleNone, err
	}, quiet)
	srv, err := transport.NewTailcat(srvID, d.Region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.Admit(adm.Admit)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ln, err := srv.Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conns := &Conns{}
	roles := RolesIn(db, time.Now)
	caps := &Caps{Of: roles}
	go ServeTunnel(ctx, ln, srv.PeerOf, caps, conns, TunnelHandler(roles, http.NotFoundHandler(), nil, caps, quiet), quiet)

	cli, err := transport.NewTailcat(cliID, d.Region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	dctx, dcancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer dcancel()
	cc, err := cli.Dial(dctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	// One request, so the server has accepted and tracks the connection.
	io.WriteString(cc, "GET /_lendisk/version HTTP/1.1\r\nHost: x\r\nLendisk-Protocol: 0.1\r\n\r\n")
	cc.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 4096)
	if _, err := cc.Read(buf); err != nil {
		t.Fatalf("first answer: %v", err)
	}

	if _, err := db.RemoveFriendship(t.Context(), cliID.NodeID().String(), false, time.Now()); err != nil {
		t.Fatal(err)
	}
	adm.Clear()
	start := time.Now()
	if n := conns.Cut(cliID.NodeID(), dial.NodeID()); n != 1 {
		t.Fatalf("Cut closed %d connections, want 1", n)
	}
	cc.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, err := cc.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("client read after the cut: %v; want EOF", err)
		}
	}
	t.Logf("client saw EOF %v after the cut", time.Since(start).Round(time.Millisecond))
	if !srv.Disconnect(dial.NodeID()) {
		t.Fatal("Disconnect after the cut: the client was not connected")
	}
	srv.Disconnect(cliID.NodeID())
	c2, err := transport.NewTailcat(cliID, d.Region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c2.Close() })
	cli.Close()
	rctx, rcancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer rcancel()
	if conn, err := c2.Dial(rctx, srvID.NodeID(), srv.Address()); err == nil {
		conn.Close()
		t.Fatal("the removed friend's dial key dialled again")
	}
}
