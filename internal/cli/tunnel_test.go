package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport"
)

// syncBuffer is a bytes.Buffer safe for a daemon writing while the test
// reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// serveLogged runs the daemon until the returned stop is called, after the
// tunnel is listening and the gateway serving: a gateway stopped while
// starting can outlive the daemon and refuse the next one in this
// process. stop returns the daemon's log.
func (n *node) serveLogged(args ...string) (stop func() string) {
	n.t.Helper()
	var out, errOut syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx, regions: n.regions,
		tuneBridge: n.tuneBridge, tuneAlerts: n.tuneAlerts, tuneEnding: n.tuneEnding, clock: n.clock}
	done := make(chan int)
	go func() {
		done <- a.run(append([]string{"serve", "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", "127.0.0.1:0"}, args...))
	}()
	deadline := time.After(20 * time.Second)
	for !strings.Contains(out.String(), "tunnel listening") || !strings.Contains(out.String(), "gateway serving") {
		select {
		case code := <-done:
			cancel()
			n.t.Fatalf("serve exited %d before listening: %s%s", code, out.String(), errOut.String())
		case <-deadline:
			cancel()
			n.t.Fatalf("serve not listening after 20s: %s", out.String())
		case <-time.After(20 * time.Millisecond):
		}
	}
	return func() string {
		cancel()
		if code := <-done; code != 0 {
			n.t.Fatalf("serve exit %d: %s", code, errOut.String())
		}
		return out.String()
	}
}

func (n *node) init() {
	n.t.Helper()
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		n.t.Fatal(e)
	}
}

// SEC-14 AC: status shows the node's address, with or without the
// daemon, and it is identical before and after a restart. The address
// carries the identity's public key and no pre-shared key, and the
// region it embeds is saved in lendisk.db, not the config file.
func TestStatusShowsStableAddress(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	n.init()
	conf, err := os.ReadFile(n.config)
	if err != nil {
		t.Fatal(err)
	}
	addr := n.status().Node.Address
	if !strings.HasPrefix(addr, "tc") {
		t.Fatalf("status address %q", addr)
	}
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := identity.FromPhrase(testPhrase)
	if b, _ := ci.ServerPublic.MarshalText(); !bytes.Equal(b, nodeKeyText(want)) {
		t.Fatalf("address key %s; want the identity's", b)
	}
	if !ci.PresharedKey.IsZero() {
		t.Fatal("address carries a pre-shared key")
	}
	if _, text, _ := n.run("", "", "status"); !strings.Contains(text, "Address: "+addr+"\n") {
		t.Fatalf("status text has no address:\n%s", text)
	}

	for i := range 2 {
		log := n.serveLogged()()
		if !strings.Contains(log, addr) {
			t.Fatalf("serve %d logged another address:\n%s", i, log)
		}
		if got := n.status().Node.Address; got != addr {
			t.Fatalf("address changed across restart %d:\n%s\n%s", i, addr, got)
		}
	}
	if after, _ := os.ReadFile(n.config); !bytes.Equal(after, conf) {
		t.Fatal("the config file was written")
	}
}

func nodeKeyText(id *identity.Identity) []byte {
	pk := id.PublicKey()
	return []byte("nodekey:" + hex.EncodeToString(pk[:]))
}

// SEC-10 AC, first state: on a node with zero peers, an unknown identity
// holding only the address is refused before any application data.
func TestServeRefusesStrangerOnFreshNode(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	n.init()
	stop := n.serveLogged("--log-level", "debug")
	addr := n.status().Node.Address

	stranger := &tailcat.Client{Server: tailcat.Addr(addr), Key: key.NewNode(), Logf: func(string, ...any) {}}
	defer stranger.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if c, err := stranger.DialTCPPort(ctx, transport.Port); err == nil {
		c.Close()
		t.Fatal("stranger connected to a node with zero peers")
	}
	log := stop()
	if !strings.Contains(log, "tunnel refused unknown key") {
		t.Fatalf("no refusal logged:\n%s", log)
	}
	if strings.Contains(log, "tunnel connection") {
		t.Fatalf("a tunnel connection was accepted:\n%s", log)
	}
}

type failingRelays struct{}

func (failingRelays) DERPMap(context.Context) (*tailcfg.DERPMap, error) {
	return nil, errors.New("offline")
}

func (failingRelays) Pick(context.Context, *tailcfg.DERPMap) (tailcfg.DERPRegionID, error) {
	return 0, errors.New("offline")
}

// init works offline; the region is chosen when the relays are reachable.
func TestInitOfflineDefersRegion(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	online := n.regions
	n.regions = failingRelays{}
	code, _, errOut := n.run("abandon\nabandon\nart\n", testPhrase, "init")
	if code != 0 {
		t.Fatalf("init offline: %d %s", code, errOut)
	}
	if !strings.Contains(errOut, "offline") {
		t.Fatalf("init did not say why there is no address: %s", errOut)
	}
	if a := n.status().Node.Address; a != "" {
		t.Fatalf("address %q without a region", a)
	}
	n.regions = online
	n.serveLogged()()
	if a := n.status().Node.Address; !strings.HasPrefix(a, "tc") {
		t.Fatalf("no address after serve chose a region: %q", a)
	}
}
