package bridge

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
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
	// Bob's client keeps sending its body, so it closes nothing and
	// gets the removed answer once the connection closes after Linger.
	conns := &Conns{Linger: time.Second}
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

// SEC-15, PEER-5 (#84): a handler that hijacked its connection, as the
// remove call does to wait for the caller to close it after the answer,
// keeps its reads when the connection is cut: Cut neither ends them
// nor closes the connection before the handler does, within Grace. So
// the remove call's answer is not lost to a farewell pass that cuts
// the caller meanwhile.
func TestCutLeavesAHijackedConnectionsReadsToItsHandler(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tun := &fakeTunnel{keys: map[string]identity.NodeID{}, ln: ln}
	conns := &Conns{Grace: 10 * time.Second}
	hijacked, readEnd := make(chan struct{}), make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		rc.Flush()
		c, brw, err := rc.Hijack()
		if err != nil {
			readEnd <- err
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		close(hijacked)
		_, err = io.Copy(io.Discard, brw)
		readEnd <- err
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)
	go func() { done <- ServeTunnel(ctx, ln, tun.peerOf, nil, conns, h, quiet) }()
	t.Cleanup(func() { cancel(); <-done })

	bob := newID(t)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tun.mu.Lock()
	tun.keys[conn.LocalAddr().String()] = bob.NodeID()
	tun.mu.Unlock()
	io.WriteString(conn, "POST /_lendisk/remove HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
	select {
	case <-hijacked:
	case err := <-readEnd:
		t.Fatalf("hijack: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never hijacked")
	}
	cut := make(chan int, 1)
	go func() { cut <- conns.Cut(bob.NodeID()) }()
	select {
	case err := <-readEnd:
		t.Fatalf("the cut ended the hijacked connection's read: %v", err)
	case n := <-cut:
		t.Fatalf("Cut returned (%d) before the handler closed its connection", n)
	case <-time.After(300 * time.Millisecond):
	}
	// The client reads its answer and closes the connection.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if resp, err := http.ReadResponse(bufio.NewReader(conn), nil); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("the answer: %v %v", resp, err)
	}
	conn.Close()
	select {
	case err := <-readEnd:
		if err != nil {
			t.Fatalf("the handler's read ended with %v, want the client's close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's read did not end at the client's close")
	}
	select {
	case n := <-cut:
		if n != 1 {
			t.Fatalf("Cut: %d connections, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Cut did not return after the handler closed its connection")
	}
}

// SEC-15, PEER-5 (#84, CI on 2b1f67e): the removed answer to a request
// in flight is written and flushed, but a connection closed at once may
// still hold it in the tunnel's send buffer when the disconnect after
// Cut drops the tunnel, and the client then waits for an answer that
// never comes. So Cut closes the connection only once the client read
// the answer and closed it, waiting Linger from when the answer was
// written, past the grace.
func TestCutClosesOnlyOnceTheClientHasTheRemovedAnswer(t *testing.T) {
	sock, arrived, _ := blockingGateway(t)
	conns := &Conns{}
	l := startLenderConns(t, New(sock, quiet).TunnelS3Handler(), nil, conns)
	bob := newID(t)
	if err := l.db.AddFriendship(t.Context(), state.Friendship{Peer: bob.NodeID().String(), CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", l.tun.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	l.tun.mu.Lock()
	l.tun.keys[conn.LocalAddr().String()] = bob.NodeID()
	l.tun.mu.Unlock()
	// A PUT whose body trickles in until the connection closes.
	io.WriteString(conn, "PUT /sbucket/key HTTP/1.1\r\nHost: lender:9000\r\nAuthorization: "+sigv4(bob.NodeID().String())+
		"\r\nContent-Length: 104857600\r\n\r\n")
	go func() {
		chunk := []byte(strings.Repeat("x", 1024))
		for {
			if _, err := conn.Write(chunk); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the PUT never reached the gateway")
	}
	if _, err := l.db.RemoveFriendship(t.Context(), bob.NodeID().String(), false, time.Now()); err != nil {
		t.Fatal(err)
	}
	cut := make(chan int, 1)
	go func() { cut <- conns.Cut(bob.NodeID()) }()
	time.Sleep(500 * time.Millisecond) // the answer is written meanwhile
	select {
	case n := <-cut:
		t.Fatalf("Cut closed the connection (%d) before the client read the removed answer", n)
	default:
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("the removed answer: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), state.NoRelationship) {
		t.Fatalf("the answer: %d %s", resp.StatusCode, body)
	}
	conn.Close()
	select {
	case n := <-cut:
		if n != 1 {
			t.Fatalf("Cut: %d connections, want 1", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Cut did not return once the client closed the connection")
	}
}

// deadlineConn records the last read deadline set; setting readEnded
// signals entering and then waits a moment, widening the window between
// a cut's check and its deadline.
type deadlineConn struct {
	net.Conn
	mu      sync.Mutex
	last    time.Time
	cutting chan struct{}
}

func (d *deadlineConn) SetReadDeadline(t time.Time) error {
	if t.Equal(readEnded) {
		close(d.cutting)
		time.Sleep(50 * time.Millisecond)
	}
	d.mu.Lock()
	d.last = t
	d.mu.Unlock()
	return nil
}

// #84 review: a cut that checked the connection was not hijacked must
// not then overwrite the deadline its handler set once it hijacked it,
// which would end the wait for the client and drop the answer again.
func TestCutDoesNotOverwriteAHijackersDeadline(t *testing.T) {
	dc := &deadlineConn{cutting: make(chan struct{})}
	pc := &peerConn{Conn: dc}
	done := make(chan struct{})
	go func() { pc.cutNow(); close(done) }()
	<-dc.cutting
	pc.hijack()
	want := time.Now().Add(5 * time.Second)
	pc.SetReadDeadline(want)
	<-done
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if !dc.last.Equal(want) {
		t.Fatalf("the read deadline after the hijacker set it: %v, want %v", dc.last, want)
	}
}
