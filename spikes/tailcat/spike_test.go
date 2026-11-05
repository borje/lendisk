package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

// These tests run two tailcat nodes in one process against a local
// DERP+STUN relay on 127.0.0.1, so they need no internet. demo.sh runs
// the same checks as two processes, locally or over the real relays.

func localRegion(t *testing.T) *tailcfg.DERPRegion {
	t.Helper()
	d, err := StartLocalDERP(t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d.Region
}

func tlogf(t *testing.T, name string) func(string, ...any) {
	return func(format string, args ...any) {
		if os.Getenv("TCSPIKE_VERBOSE") != "" {
			t.Logf("["+name+"] "+format, args...)
		}
	}
}

// server is a started tunnel server whose port 9000 listener records
// what reaches it.
type server struct {
	*tailcat.Server
	accepted atomic.Int64 // connections the listener handed out
	conns    chan net.Conn
}

func startServer(t *testing.T, reg *tailcfg.DERPRegion, k key.NodePrivate, admit func(key.NodePublic) bool) *server {
	t.Helper()
	s, err := StartServer(t.Context(), ServerConfig{Key: k, Region: reg, Admit: admit, Logf: tlogf(t, "server")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ln, err := s.Listen(t.Context(), "tcp", fmt.Sprintf(":%d", TunnelPort))
	if err != nil {
		t.Fatal(err)
	}
	srv := &server{Server: s, conns: make(chan net.Conn, 8)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			srv.accepted.Add(1)
			srv.conns <- c
		}
	}()
	return srv
}

func newClient(t *testing.T, s *tailcat.Server, k key.NodePrivate) *tailcat.Client {
	c := &tailcat.Client{Server: s.TailcatAddr(), Key: k, Logf: tlogf(t, "client")}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestNilHookRefusesToStart(t *testing.T) {
	reg := localRegion(t)
	s, err := StartServer(t.Context(), ServerConfig{Key: key.NewNode(), Region: reg, Admit: nil})
	if !errors.Is(err, ErrNilHook) || s != nil {
		t.Fatalf("StartServer with nil hook = %v, %v; want nil, ErrNilHook", s, err)
	}
}

// An unknown key is refused at the meow, before any WireGuard peer
// exists for it, so no TCP connection and no byte reach the listener.
func TestDenyAllRefusesBeforeAnyByte(t *testing.T) {
	reg := localRegion(t)
	var calls atomic.Int64
	srv := startServer(t, reg, key.NewNode(), func(k key.NodePublic) bool {
		calls.Add(1)
		return DenyAll(k)
	})
	ck := key.NewNode()
	c := newClient(t, srv.Server, ck)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := c.DialTCPPort(ctx, TunnelPort)
	if err == nil {
		conn.Write([]byte("should never arrive\n"))
		conn.Close()
		t.Fatal("dial succeeded with a deny-all hook")
	}
	t.Logf("dial error: %v", err)
	if calls.Load() == 0 {
		t.Fatal("hook was never consulted")
	}
	if _, ok := srv.Status().Peer[ck.Public()]; ok {
		t.Fatal("rejected key became a peer")
	}
	if n := srv.accepted.Load(); n != 0 {
		t.Fatalf("listener accepted %d connections; want 0", n)
	}
	t.Logf("hook called %d times in 5s (client retries ~1/s); no peer, no connection, so no byte", calls.Load())
}

func decode(t *testing.T, a tailcat.Addr) tailcat.ConnInfo {
	t.Helper()
	ci, err := tailcat.ParseAddr(a)
	if err != nil {
		t.Fatal(err)
	}
	return ci
}

// With the pre-shared key disabled and a saved region, the address is a
// function of the key and the region: identical across restarts, and
// carrying only public values.
func TestAddressStableAndPublic(t *testing.T) {
	reg := localRegion(t)
	k := key.NewNode()
	var addrs []tailcat.Addr
	for range 2 {
		s, err := StartServer(t.Context(), ServerConfig{Key: k, Region: reg, Admit: DenyAll, Logf: tlogf(t, "server")})
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, s.TailcatAddr())
		s.Close()
	}
	if addrs[0] != addrs[1] {
		t.Fatalf("address changed across restarts:\n%s\n%s", addrs[0], addrs[1])
	}
	ci := decode(t, addrs[0])
	if !ci.PresharedKey.IsZero() {
		t.Fatal("address carries a pre-shared key")
	}
	if ci.ServerPublic.NodePublic != k.Public() {
		t.Fatalf("ServerPublic = %v; want %v", ci.ServerPublic.NodePublic, k.Public())
	}
	if !ci.ServerDiscoPublic.Equal(tailcat.DiscoPublicForNode(k)) {
		t.Fatal("ServerDiscoPublic is not the disco key derived from the node key")
	}
	raw, _ := tailcat.ParseAddrRaw(addrs[0])
	t.Logf("address %s\ncarries %+v", addrs[0], raw)

	// Contrast: tailcat's default generates a fresh pre-shared key per
	// start and puts it in the address.
	var def []tailcat.Addr
	for range 2 {
		s := &tailcat.Server{Key: k, Region: reg, AllowClient: DenyAll, Logf: tlogf(t, "default")}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		def = append(def, s.TailcatAddr())
		s.Close()
	}
	if def[0] == def[1] || decode(t, def[0]).PresharedKey.IsZero() {
		t.Fatal("expected tailcat's default to embed a new pre-shared key per start")
	}
}

// accept dials as ck and returns both ends of one tunnel connection,
// after a line has crossed in each direction.
func accept(t *testing.T, srv *server, c *tailcat.Client) (client, server net.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cc, err := c.DialTCPPort(ctx, TunnelPort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	var sc net.Conn
	select {
	case sc = <-srv.conns:
	case <-ctx.Done():
		t.Fatal("no connection accepted")
	}
	t.Cleanup(func() { sc.Close() })
	io.WriteString(cc, "ping\n")
	sc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if line, err := bufio.NewReader(sc).ReadString('\n'); err != nil || line != "ping\n" {
		t.Fatalf("server read %q, %v", line, err)
	}
	io.WriteString(sc, "pong\n")
	cc.SetReadDeadline(time.Now().Add(5 * time.Second))
	if line, err := bufio.NewReader(cc).ReadString('\n'); err != nil || line != "pong\n" {
		t.Fatalf("client read %q, %v", line, err)
	}
	return cc, sc
}

func TestPeerKeyOfAcceptedConn(t *testing.T) {
	reg := localRegion(t)
	var allow tailcat.KeySet
	ck := key.NewNode()
	allow.Add(ck.Public())
	srv := startServer(t, reg, key.NewNode(), allow.Contains)
	_, sc := accept(t, srv, newClient(t, srv.Server, ck))
	got, err := PeerOf(srv.Server, sc)
	if err != nil {
		t.Fatal(err)
	}
	if got != ck.Public() {
		t.Fatalf("PeerKey = %v; want %v", got, ck.Public())
	}
	t.Logf("accepted %v reports peer key %v", sc.RemoteAddr(), got)

	// A remote address that is no peer's has no key.
	if k, ok := srv.PeerKey(&net.TCPAddr{IP: net.ParseIP("fd7a:115c:a1e0::1"), Port: 1}); ok {
		t.Fatalf("PeerKey of a stranger's address = %v, true", k)
	}
}

// isTimeout reports whether err is a read deadline expiring, as opposed
// to EOF or a reset: the connection is still open, just silent.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func readWithin(c net.Conn, d time.Duration) (string, error) {
	c.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	return string(buf[:n]), err
}

// DisconnectClient alone resets nothing: neither end of an open
// connection sees EOF or a reset. It cuts only the server-to-client
// direction; what the client sends can still reach the server's open
// connection (see TestDisconnectInboundStillDelivered). Closing the
// server's end after the disconnect does not reach the client either.
func TestDisconnectDoesNotResetConns(t *testing.T) {
	reg := localRegion(t)
	var allow tailcat.KeySet
	ck := key.NewNode()
	allow.Add(ck.Public())
	srv := startServer(t, reg, key.NewNode(), allow.Contains)
	cc, sc := accept(t, srv, newClient(t, srv.Server, ck))

	allow.Remove(ck.Public())
	if !srv.DisconnectClient(ck.Public()) {
		t.Fatal("DisconnectClient: client not connected")
	}
	time.Sleep(time.Second) // let any in-flight state settle

	if _, err := cc.Write([]byte("after disconnect\n")); err != nil {
		t.Fatalf("client write after disconnect: %v", err)
	}
	got, err := readWithin(sc, 3*time.Second)
	switch {
	case err == nil:
		t.Logf("server still received %q from the disconnected client", got)
	case isTimeout(err):
		t.Log("server read timed out")
	default:
		t.Fatalf("server read after disconnect = %q, %v; want data or a timeout, not EOF/reset", got, err)
	}
	if _, err := sc.Write([]byte("reply\n")); err != nil {
		t.Fatalf("server write after disconnect: %v", err)
	}
	if got, err := readWithin(cc, 3*time.Second); !isTimeout(err) {
		t.Fatalf("client read after disconnect = %q, %v; want a timeout (open, server's packets dropped)", got, err)
	}
	if k, ok := srv.PeerKey(sc.RemoteAddr()); ok {
		t.Fatalf("PeerKey after disconnect = %v, true; want false", k)
	}
	t.Log("after DisconnectClient both ends are still open; no EOF, no reset; PeerKey no longer resolves")

	sc.Close()
	if got, err := readWithin(cc, 3*time.Second); !isTimeout(err) {
		t.Fatalf("client read after server close = %q, %v; want a timeout (FIN dropped)", got, err)
	}
	t.Log("closing the server end after DisconnectClient does not reach the client: it stalls")
}

// After DisconnectClient the WireGuard session survives in the server's
// engine, so the disconnected client's packets are still decrypted and
// delivered: to an open UDP flow, and as a new UDP flow, whose PeerKey
// no longer resolves. A new TCP connection does not complete, because
// the server's SYN-ACK is dropped.
func TestDisconnectInboundStillDelivered(t *testing.T) {
	reg := localRegion(t)
	var allow tailcat.KeySet
	ck := key.NewNode()
	allow.Add(ck.Public())
	srv := startServer(t, reg, key.NewNode(), allow.Contains)
	ln, err := srv.Listen(t.Context(), "udp", ":9001")
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(t, srv.Server, ck)
	pc, err := c.DialUDPPort(t.Context(), 9001)
	if err != nil {
		t.Fatal(err)
	}
	pc.Write([]byte("before"))
	sc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readWithin(sc, 5*time.Second); err != nil || got != "before" {
		t.Fatalf("first datagram = %q, %v", got, err)
	}

	allow.Remove(ck.Public())
	srv.DisconnectClient(ck.Public())
	time.Sleep(time.Second)
	if _, ok := srv.Status().Peer[ck.Public()]; ok {
		t.Log("Status still lists the disconnected key as an engine peer")
	}

	delivered := 0
	for range 5 {
		pc.Write([]byte("after"))
		sc.Write([]byte("reply"))
		if got, err := readWithin(sc, time.Second); err == nil && got == "after" {
			delivered++
		}
	}
	if got, err := readWithin(pc, time.Second); err == nil {
		t.Fatalf("client received %q after disconnect", got)
	}
	t.Logf("open flow: %d of 5 datagrams from the disconnected client delivered; none back", delivered)
	if delivered == 0 {
		t.Fatal("expected inbound datagrams to still arrive (tailcat behaviour changed?)")
	}

	pc2, err := c.DialUDPPort(t.Context(), 9001)
	if err != nil {
		t.Fatal(err)
	}
	pc2.Write([]byte("new flow"))
	type res struct {
		data string
		ok   bool
	}
	ch := make(chan res, 1)
	go func() {
		c2, err := ln.Accept()
		if err != nil {
			return
		}
		got, _ := readWithin(c2, 2*time.Second)
		_, ok := srv.PeerKey(c2.RemoteAddr())
		ch <- res{got, ok}
	}()
	select {
	case r := <-ch:
		t.Logf("new UDP flow from the disconnected client accepted: %q, PeerKey ok=%v", r.data, r.ok)
		if r.ok {
			t.Fatal("PeerKey resolved for a disconnected client")
		}
	case <-time.After(4 * time.Second):
		t.Log("new UDP flow not delivered")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	if tc, err := c.DialTCPPort(ctx, TunnelPort); err == nil {
		tc.Close()
		t.Fatal("new TCP connection completed after disconnect")
	}
	if n := srv.accepted.Load(); n != 0 {
		t.Fatalf("TCP listener accepted %d connections after disconnect", n)
	}
}

// How long inbound packets survive DisconnectClient. Slow; run with
// TCSPIKE_LONG=1.
func TestDisconnectInboundDuration(t *testing.T) {
	if os.Getenv("TCSPIKE_LONG") == "" {
		t.Skip("set TCSPIKE_LONG=1 to run (about 5 minutes)")
	}
	reg := localRegion(t)
	var allow tailcat.KeySet
	ck := key.NewNode()
	allow.Add(ck.Public())
	srv := startServer(t, reg, key.NewNode(), allow.Contains)
	ln, err := srv.Listen(t.Context(), "udp", ":9001")
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(t, srv.Server, ck)
	pc, err := c.DialUDPPort(t.Context(), 9001)
	if err != nil {
		t.Fatal(err)
	}
	pc.Write([]byte("before"))
	sc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	readWithin(sc, 5*time.Second)
	allow.Remove(ck.Public())
	srv.DisconnectClient(ck.Public())
	start := time.Now()
	var last time.Duration
	for time.Since(start) < 300*time.Second {
		pc.Write([]byte("after"))
		if _, err := readWithin(sc, 5*time.Second); err == nil {
			last = time.Since(start)
		}
	}
	t.Logf("last datagram from the disconnected client delivered %v after DisconnectClient", last.Round(time.Second))
}

// lendisk's order (SEC-15): close the key's connections first, then
// disconnect. The client sees EOF at once.
func TestCloseThenDisconnect(t *testing.T) {
	reg := localRegion(t)
	var allow tailcat.KeySet
	ck := key.NewNode()
	allow.Add(ck.Public())
	srv := startServer(t, reg, key.NewNode(), allow.Contains)
	cc, sc := accept(t, srv, newClient(t, srv.Server, ck))

	allow.Remove(ck.Public())
	start := time.Now()
	sc.Close()
	got, err := readWithin(cc, 5*time.Second)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("client read after server close = %q, %v; want EOF", got, err)
	}
	t.Logf("client saw EOF %v after the server closed its end", time.Since(start).Round(time.Millisecond))
	if !srv.DisconnectClient(ck.Public()) {
		t.Fatal("DisconnectClient: client not connected")
	}
	// The key is now rejected; a fresh dial is refused at the meow.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	c2 := newClient(t, srv.Server, ck)
	if conn, err := c2.DialTCPPort(ctx, TunnelPort); err == nil {
		conn.Close()
		t.Fatal("revoked key dialled again")
	}
}

// The FileAdmitter fails closed on a missing file and a malformed line.
func TestFileAdmitterFailsClosed(t *testing.T) {
	dir := t.TempDir()
	k := key.NewNode().Public()
	kt, _ := k.MarshalText()
	a := FileAdmitter{Path: dir + "/allowed"}
	if a.Admit(k) {
		t.Fatal("admitted with no file")
	}
	os.WriteFile(a.Path, []byte("garbage\n"+string(kt)+"\n"), 0o600)
	if a.Admit(k) {
		t.Fatal("admitted past a malformed line")
	}
	os.WriteFile(a.Path, []byte(string(kt)+"\n"), 0o600)
	if !a.Admit(k) {
		t.Fatal("rejected a listed key")
	}
}
