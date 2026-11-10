package relay

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport"
)

var quiet = slog.New(slog.DiscardHandler)

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// run starts a self-signed relay on 127.0.0.1 and returns its entry;
// set, if given, changes its config first.
func run(t *testing.T, dir string, set ...func(*Config)) Entry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ready := make(chan Entry, 1)
	c := Config{Hostname: "127.0.0.1", Listen: "127.0.0.1:0", STUNPort: 0, TLS: "self-signed", Dir: dir, Log: quiet}
	for _, f := range set {
		f(&c)
	}
	go func() {
		done <- Run(ctx, c, func(e Entry) { ready <- e })
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the relay stopped with %v", err)
		}
	})
	select {
	case e := <-ready:
		return e
	case err := <-done:
		t.Fatalf("the relay did not start: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not start")
	}
	return Entry{}
}

// NET-5 (#42): `lendisk relay` is a DERP server two nodes can use as
// their only relay: listed by the entry it prints, its self-signed
// certificate pinned by hash, it carries a tunnel between them. The
// certificate is kept, so the entry survives a restart.
func TestSelfHostedRelayCarriesATunnel(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "relay")
	e := run(t, dir)
	if e.Host != "127.0.0.1" || e.Port == 0 || len(e.Cert) != len("sha256-raw:")+64 {
		t.Fatalf("entry %s", e)
	}
	for _, f := range []string{"relay.key", "relay.crt"} {
		if fi, err := os.Stat(filepath.Join(dir, f)); err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("%s: %v, %v", f, fi, err)
		}
	}
	set := &transport.RelaySet{List: []transport.RelayServer{{Host: e.Host, Port: e.Port, STUNPort: -1, Cert: e.Cert}}}
	m, err := set.DERPMap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	region := m.Regions[900]

	srvID := newID(t)
	srv, err := transport.NewTailcat(srvID, region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
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
	dl, err := transport.NewDialer(newID(t), quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.SetRelays(set, false)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte("through the relay")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("through the relay"))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "through the relay" {
		t.Fatalf("read %q, %v", buf, err)
	}

	// A second relay on the same directory keeps the certificate.
	if again := run(t, dir); again.Cert != e.Cert {
		t.Fatalf("the pin changed across a restart: %s, then %s", e.Cert, again.Cert)
	}
}

// #42 review L5: an existing relay directory open to others is made
// private, and a leftover temporary key file open to others is not
// reused, so the key is 0600 from its first byte.
func TestRelayKeepsItsDirectoryPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "relay")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "relay.key.tmp"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, dir)
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("relay dir: %v %v", fi, err)
	}
	if fi, err := os.Stat(filepath.Join(dir, "relay.key")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("relay key: %v %v", fi, err)
	}
}

// NET-5 (#42): a relay with a setting it cannot serve refuses to start
// and says which.
func TestRelayRefusesBadSettings(t *testing.T) {
	for _, c := range []Config{
		{Listen: ":443", TLS: "self-signed"},
		{Hostname: "192.0.2.1", Listen: ":443", TLS: "letsencrypt"},
		{Hostname: "relay.example", Listen: "443", TLS: "self-signed"},
		{Hostname: "relay.example", Listen: ":443", TLS: "files", CertFile: "relay.crt"},
		{Hostname: "relay.example", Listen: ":443", TLS: "none"},
	} {
		if err := c.Check(); err == nil {
			t.Errorf("%+v was accepted", c)
		}
	}
}
