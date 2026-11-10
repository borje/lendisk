package relay

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"tailscale.com/derp"
	"tailscale.com/envknob"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport"
)

// setOf is the relay set of one relays.list entry.
func setOf(e Entry) *transport.RelaySet {
	return &transport.RelaySet{List: []transport.RelayServer{{Host: e.Host, Port: e.Port, STUNPort: -1, Cert: e.Cert}}}
}

// serveEcho starts a tunnel server for id homed at the relay of e,
// echoing what each connection sends, and returns its address.
func serveEcho(t *testing.T, id *identity.Identity, e Entry) string {
	t.Helper()
	m, err := setOf(e).DERPMap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := transport.NewTailcat(id, m.Regions[900], quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.Admit(func(identity.NodeID) bool { return true })
	ln, err := srv.Listen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return srv.Address()
}

// echoes dials the server at addr with key dk through set, and reports
// whether a message comes back within d.
func echoes(t *testing.T, dk *identity.Identity, set *transport.RelaySet, srv identity.NodeID, addr string, d time.Duration) error {
	t.Helper()
	dl, err := transport.NewDialer(dk, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.SetRelays(set, false)
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	c, err := dl.Dial(ctx, srv, addr)
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(d))
	if _, err := c.Write([]byte("hello")); err != nil {
		return err
	}
	buf := make([]byte, 5)
	_, err = io.ReadFull(c, buf)
	return err
}

// logBuffer collects a logger's text output.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// #99: a relay with relay.allow carries a tunnel to a listed node from
// a key it has never heard of (a friend's dial key, an invite key), and
// a tunnel from a listed node's dial key to a server that is not
// listed; it carries nothing between two keys neither of which is
// listed, and logs the pair so the operator sees whom to add.
func TestAllowlistedRelayCarriesOnlyTunnelsWithAListedEnd(t *testing.T) {
	listed, stranger := newID(t), newID(t)
	var logs logBuffer
	e := run(t, filepath.Join(t.TempDir(), "relay"), func(c *Config) {
		c.Allow = []identity.NodeID{listed.NodeID()}
		c.Log = slog.New(slog.NewTextHandler(&logs, nil))
	})
	set := setOf(e)
	listedAddr := serveEcho(t, listed, e)
	if err := echoes(t, newID(t), set, listed.NodeID(), listedAddr, 30*time.Second); err != nil {
		t.Fatalf("an unknown key could not reach a listed node: %v", err)
	}
	strangerAddr := serveEcho(t, stranger, e)
	if err := echoes(t, newID(t), set, stranger.NodeID(), strangerAddr, 8*time.Second); err == nil {
		t.Fatal("two keys outside relay.allow were relayed")
	}
	if !strings.Contains(logs.String(), "relay.allow") || !strings.Contains(logs.String(), stranger.NodeID().String()) {
		t.Fatalf("the refused pair was not logged:\n%s", logs.String())
	}
	if err := echoes(t, listed, set, stranger.NodeID(), strangerAddr, 30*time.Second); err != nil {
		t.Fatalf("a listed key could not reach a server outside the list: %v", err)
	}
}

// frame is one DERP frame as a client writes it.
func frame(typ derp.FrameType, payload ...[]byte) []byte {
	p := bytes.Join(payload, nil)
	b := []byte{byte(typ), 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(len(p)))
	return append(b, p...)
}

// oneByte reads one byte at a time, as a slow connection may deliver.
type oneByte struct{ r io.Reader }

func (o oneByte) Read(p []byte) (int, error) { return o.r.Read(p[:min(1, len(p))]) }

// #99: the filter takes out exactly the packets with no listed end,
// whatever the reads' sizes, and passes every other frame unchanged:
// the ClientInfo, keep-alives, packets to or from a listed node, and a
// frame too large to drop, which derpserver refuses itself.
func TestFilterDropsOnlyPacketsWithNoListedEnd(t *testing.T) {
	listed, other, client := newID(t).NodeID(), newID(t).NodeID(), newID(t).NodeID()
	info := frame(derp.FrameClientInfo, client[:], bytes.Repeat([]byte{1}, 40))
	keepAlive := frame(derp.FrameKeepAlive)
	toListed := frame(derp.FrameSendPacket, listed[:], []byte("to the listed node"))
	toOther := frame(derp.FrameSendPacket, other[:], []byte("between strangers"))
	huge := frame(derp.FrameSendPacket, other[:], make([]byte, maxDropped))
	in := slices.Concat(info, keepAlive, toOther, toListed, toOther, keepAlive, huge)
	want := slices.Concat(info, keepAlive, toListed, keepAlive, huge)
	for _, slow := range []bool{false, true} {
		var r io.Reader = bytes.NewReader(in)
		if slow {
			r = oneByte{r}
		}
		got, err := io.ReadAll(&filterReader{r: r, allow: allowSet{listed: true}, log: quiet})
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("slow %v: %d bytes, %v; want %d bytes", slow, len(got), err, len(want))
		}
	}
	// From a listed node, every packet passes.
	fromListed := slices.Concat(frame(derp.FrameClientInfo, listed[:], make([]byte, 40)), toOther)
	if got, _ := io.ReadAll(&filterReader{r: bytes.NewReader(fromListed), allow: allowSet{listed: true}, log: quiet}); !bytes.Equal(got, fromListed) {
		t.Fatal("a listed node's packet to a stranger was dropped")
	}
	// A frame cut short is an error, not a clean end.
	if _, err := io.ReadAll(&filterReader{r: bytes.NewReader(toOther[:20]), allow: allowSet{listed: true}, log: quiet}); err != io.ErrUnexpectedEOF {
		t.Fatalf("a truncated frame: %v", err)
	}
}

// #99 (#42 review): a node refuses a relay whose certificate does not
// match the sha256-raw pin of its relays.list entry, so no tunnel comes
// up through it; the right pin carries one.
func TestWrongPinRelayIsRefused(t *testing.T) {
	e := run(t, filepath.Join(t.TempDir(), "relay"))
	srv := newID(t)
	addr := serveEcho(t, srv, e)
	wrong := e
	last := wrong.Cert[len(wrong.Cert)-1]
	wrong.Cert = wrong.Cert[:len(wrong.Cert)-1] + map[bool]string{true: "1", false: "0"}[last == '0']
	err := echoes(t, newID(t), setOf(wrong), srv.NodeID(), addr, 8*time.Second)
	if err == nil {
		t.Fatal("a relay with another certificate than the pinned one carried a tunnel")
	}
	if strings.Contains(err.Error(), transport.ErrRelayNotAllowed.Error()) {
		t.Fatalf("refused by the relay policy, not by the pin: %v", err)
	}
	if err := echoes(t, newID(t), setOf(e), srv.NodeID(), addr, 30*time.Second); err != nil {
		t.Fatalf("the right pin: %v", err)
	}
}

// #99: with letsencrypt, the relay offers no h2 in ALPN, since HTTP/2
// is off and DERP upgrades an HTTP/1.1 connection; ACME's TLS-ALPN
// challenge is still offered.
func TestLetsEncryptRelayOffersNoH2(t *testing.T) {
	c := Config{Hostname: "relay.example", Listen: ":443", TLS: "letsencrypt", Dir: t.TempDir(), Log: quiet}
	tc, _, err := c.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(tc.NextProtos, "h2") || !slices.Contains(tc.NextProtos, "acme-tls/1") || !slices.Contains(tc.NextProtos, "http/1.1") {
		t.Fatalf("NextProtos %v", tc.NextProtos)
	}
}

// #99: relay.rate_limit caps what one key sends through the relay:
// 768 KiB at 256 KiB a second take well over a second, where the
// loopback carries them at once. The engines advertise no local
// address and the relay runs no STUN server, so they know no endpoint
// of each other and no direct path goes round the relay.
func TestRelayRateLimitsEachKey(t *testing.T) {
	envknob.SetenvForTest(t, "TS_DEBUG_OMIT_LOCAL_ADDRS", "true")
	const limit, size = 256 << 10, 768 << 10
	e := run(t, filepath.Join(t.TempDir(), "relay"), func(c *Config) { c.RateLimit = limit })
	srv := newID(t)
	addr := serveEcho(t, srv, e)
	dl, err := transport.NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.SetRelays(setOf(e), false)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, srv.NodeID(), addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	start := time.Now()
	go c.Write(make([]byte, size))
	if _, err := io.CopyN(io.Discard, c, size); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 1500*time.Millisecond {
		t.Fatalf("%d KiB echoed in %v at a limit of %d KiB/s", size>>10, d, limit>>10)
	}
}
