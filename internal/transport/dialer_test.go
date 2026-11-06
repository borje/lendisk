package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// An invited node dials with a key that is not its identity and has no
// region of its own; such a dialer never listens.
func TestDialerReachesServerAndNeverListens(t *testing.T) {
	d := transporttest.Start(t)
	srvID, inviteKey := newID(t), newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(p identity.NodeID) bool { return p == inviteKey.NodeID() })
	l := listen(t, srv)

	dl, err := NewDialer(inviteKey, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.Admit(func(identity.NodeID) bool { return true })
	if _, err := dl.Listen(t.Context()); err == nil {
		t.Fatal("a dialer listened")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case sc := <-l.conns:
		defer sc.Close()
		if p, _ := srv.PeerOf(sc); p != inviteKey.NodeID() {
			t.Fatalf("connection names %v", p)
		}
	case <-ctx.Done():
		t.Fatal("no connection accepted")
	}
}

func TestKeyOfAddress(t *testing.T) {
	d := transporttest.Start(t)
	id := newID(t)
	got, err := KeyOf(AddressOf(id, d.Region))
	if err != nil || got != id.NodeID() {
		t.Fatalf("KeyOf = %v, %v", got, err)
	}
	if _, err := KeyOf("tcnotcbor"); err == nil {
		t.Fatal("KeyOf accepted garbage")
	}
}

// A rejection is remembered for DeniedFor only, so a relationship
// written by another process (a one-shot command) is seen without a
// Clear (SEC-15's 60 seconds).
func TestAdmissionRejectionExpires(t *testing.T) {
	k := newID(t).NodeID()
	answer, calls := false, 0
	adm := NewAdmission(func(context.Context, identity.NodeID) (bool, error) {
		calls++
		return answer, nil
	}, quiet)
	now := time.Unix(1_800_000_000, 0)
	adm.now = func() time.Time { return now }
	adm.Admit(k)
	answer = true
	now = now.Add(DeniedFor - time.Second)
	if adm.Admit(k) || calls != 1 {
		t.Fatalf("rejection forgotten early: calls %d", calls)
	}
	now = now.Add(time.Second)
	if !adm.Admit(k) || calls != 2 {
		t.Fatalf("rejection not forgotten after DeniedFor: calls %d", calls)
	}
}

// A friend whose daemon restarts has forgotten every client: tailcat
// admits a client as a WireGuard peer only at its first ping. After a
// failed dial the transport checks the cached client with a dial of its
// own; that fails too, so the client is replaced and a later dial
// starts afresh and reaches the restarted friend (#18: replication
// resumes after a lender's restart; #52: only a failed check replaces).
func TestDialReachesRestartedServer(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	serve := func() (*Tailcat, *listener) {
		srv := newTailcat(t, srvID, d)
		srv.Admit(func(identity.NodeID) bool { return true })
		return srv, listen(t, srv)
	}
	srv, l := serve()
	dl, err := NewDialer(me, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dial := func(timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()
		c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
		if err == nil {
			c.Close()
		}
		return err
	}
	if err := dial(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	<-l.conns
	srv.Close()
	srv, l = serve()
	// The first dial after the restart may go to the stale session.
	start := time.Now()
	for dial(3*time.Second) != nil {
		if time.Since(start) > 30*time.Second {
			t.Fatal("the restarted server is never reached")
		}
	}
	select {
	case c := <-l.conns:
		c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("no connection accepted after the restart")
	}
}

// echo checks that the dialled connection c and its accepted end s still
// carry bytes both ways.
func echo(t *testing.T, c, s net.Conn, msg string) {
	t.Helper()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	s.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("writing %q: %v", msg, err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(s, buf); err != nil || string(buf) != msg {
		t.Fatalf("server read %q, %v", buf, err)
	}
	if _, err := s.Write(buf); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != msg {
		t.Fatalf("client read %q, %v", buf, err)
	}
}

// dialed opens a connection from dl to srv and returns both ends. The
// accepted end is the one that reads the hello the dialled end sends:
// the transport's own probes are accepted too, and closed at once.
func dialed(t *testing.T, dl, srv *Tailcat, peer identity.NodeID, l *listener) (net.Conn, net.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, peer, srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case s := <-l.conns:
			s.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 2)
			if _, err := io.ReadFull(s, buf); err != nil || string(buf) != "hi" {
				s.Close()
				continue
			}
			t.Cleanup(func() { s.Close() })
			return c, s
		case <-ctx.Done():
			t.Fatal("no connection accepted")
		}
	}
}

func (t *Tailcat) client(peer identity.NodeID) *tailcatClient {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.clients[peer]
}

func (t *Tailcat) probing(c *tailcatClient) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return c.probing
}

func (c *tailcatClient) isClosed() bool {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	return c.closed
}

// NET-1, NET-8 (#52): two users share one friend's client. A dial whose
// own context ends (the caller gave up) keeps the client, and the other
// user's open connection keeps working.
func TestCancelledDialKeepsTheSharedClient(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(identity.NodeID) bool { return true })
	l := listen(t, srv)
	dl, err := NewDialer(me, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	peer := srvID.NodeID()
	a, sa := dialed(t, dl, srv, peer, l)
	shared := dl.client(peer)
	echo(t, a, sa, "before")
	for _, timeout := range []time.Duration{0, time.Millisecond} {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		if c, err := dl.Dial(ctx, peer, srv.Address()); err == nil {
			c.Close()
		}
		cancel()
		if dl.client(peer) != shared {
			t.Fatalf("a dial that ran out of time (%v) dropped the shared client", timeout)
		}
		echo(t, a, sa, "after")
	}
	// The transport checks the client with a dial of its own; it passes.
	for deadline := time.Now().Add(15 * time.Second); dl.probing(shared); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the client's check never ended")
		}
	}
	b, sb := dialed(t, dl, srv, peer, l)
	if dl.client(peer) != shared {
		t.Fatal("the next dial built a new client")
	}
	echo(t, b, sb, "b")
	echo(t, a, sa, "a")
}

// NET-1, NET-8 (#52): when a friend's address changes the client is
// replaced, but the old one stays up until its last open connection
// closes.
func TestReplacedClientKeepsOpenConnections(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(identity.NodeID) bool { return true })
	l := listen(t, srv)
	dl, err := NewDialer(me, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	peer := srvID.NodeID()
	a, sa := dialed(t, dl, srv, peer, l)
	old := dl.client(peer)
	// The friend's address changes (another relay, where it is not).
	moved := AddressOf(srvID, transporttest.Start(t).Region)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	if c, err := dl.Dial(ctx, peer, moved); err == nil {
		c.Close()
		t.Fatal("dialled the friend at a relay it is not on")
	}
	cancel()
	if dl.client(peer) == old {
		t.Fatal("the client for the old address is still used")
	}
	echo(t, a, sa, "still open")
	if old.isClosed() {
		t.Fatal("the replaced client was closed under an open connection")
	}
	a.Close()
	if !old.isClosed() {
		t.Fatal("the replaced client outlived its last connection")
	}
	// Back at the first address, a fresh client reaches it.
	b, sb := dialed(t, dl, srv, peer, l)
	echo(t, b, sb, "back")
}
