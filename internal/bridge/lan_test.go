package bridge

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/config"
)

// freePort returns a port that is free on ip right now.
func freePort(t *testing.T, ip string) int {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Skipf("cannot listen on %s: %v", ip, err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// echo answers every request with "ok" and counts them.
type echo struct{ n atomic.Int32 }

func (e *echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.n.Add(1)
	io.WriteString(w, "ok")
}

func startLAN(t *testing.T, l *LAN) (stop func() error) {
	t.Helper()
	if l.Log == nil {
		l.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errc := make(chan error, 1)
	go func() { errc <- l.Run(ctx, ready) }()
	select {
	case <-ready:
	case err := <-errc:
		cancel()
		t.Fatalf("LAN did not start: %v", err)
	}
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() { cancel(); err = <-errc })
		return err
	}
	t.Cleanup(func() { stop() })
	return stop
}

func pool(t *testing.T, certFile string) *x509.CertPool {
	t.Helper()
	b, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		t.Fatal("no certificate in " + certFile)
	}
	return p
}

func get(c *http.Client, url string) (int, string, error) {
	resp, err := c.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

func tlsClient(p *x509.CertPool) *http.Client {
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: p}}}
}

// EP-17, SEC-3: tls = generate makes a certificate for the configured
// addresses and the host name, key at 0600, kept while the address set
// is unchanged and regenerated when it changes. A plaintext request on
// the TLS port is refused and never reaches the handler.
func TestGeneratedCertificate(t *testing.T) {
	dir := t.TempDir()
	port := freePort(t, "127.0.0.1")
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	h := &echo{}
	stop := startLAN(t, &LAN{Entries: []config.Listen{{Address: addr, TLS: "generate"}}, ConfigDir: dir, Handler: h})

	certFile := filepath.Join(dir, CertFile)
	fi, err := os.Stat(filepath.Join(dir, KeyFile))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key: %v %v", fi.Mode(), err)
	}
	cert, err := ReadCert(dir)
	if err != nil {
		t.Fatal(err)
	}
	host, _ := os.Hostname()
	if len(cert.IPAddresses) != 1 || !cert.IPAddresses[0].Equal(net.ParseIP("127.0.0.1")) ||
		(host != "" && !slices.Contains(cert.DNSNames, host)) {
		t.Fatalf("SANs: %v %v", cert.IPAddresses, cert.DNSNames)
	}
	if cert.NotAfter.Before(time.Now().AddDate(9, 11, 0)) {
		t.Errorf("valid until %v, want ten years", cert.NotAfter)
	}
	code, body, err := get(tlsClient(pool(t, certFile)), "https://"+addr+"/bucket/key")
	if err != nil || code != 200 || body != "ok" {
		t.Fatalf("https: %d %q %v", code, body, err)
	}

	before := h.n.Load()
	code, body, err = get(&http.Client{Timeout: 5 * time.Second}, "http://"+addr+"/bucket/key")
	if err != nil || code != http.StatusBadRequest || !strings.Contains(body, "HTTPS") {
		t.Errorf("plaintext request: %d %q", code, body)
	}
	if h.n.Load() != before {
		t.Error("plaintext request reached the handler")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// Same address set: same certificate.
	fp := Fingerprint(cert.Raw)
	startLAN(t, &LAN{Entries: []config.Listen{{Address: addr, TLS: "generate"}}, ConfigDir: dir, Handler: h})()
	if c2, _ := ReadCert(dir); Fingerprint(c2.Raw) != fp {
		t.Error("certificate regenerated for an unchanged address set")
	}
	// A new address: regenerated, and the old one no longer trusted.
	port2 := freePort(t, "127.0.0.1")
	addr2 := net.JoinHostPort("127.0.0.1", strconv.Itoa(port2))
	ip6 := net.JoinHostPort("::1", strconv.Itoa(freePort(t, "::1")))
	startLAN(t, &LAN{Entries: []config.Listen{{Address: addr2, TLS: "generate"}, {Address: ip6, TLS: "generate"}}, ConfigDir: dir, Handler: h})
	c3, _ := ReadCert(dir)
	if Fingerprint(c3.Raw) == fp || len(c3.IPAddresses) != 2 {
		t.Fatalf("not regenerated: %v", c3.IPAddresses)
	}
	if _, _, err := get(tlsClient(pool(t, certFile)), "https://"+ip6+"/"); err != nil {
		t.Errorf("IPv6 https: %v", err)
	}
}

// SEC-3, #53: a generated key that is reused is checked like a
// tls = files key: one others can read is refused, not served.
func TestReusedGeneratedKeyIsChecked(t *testing.T) {
	dir := t.TempDir()
	ips := []net.IP{net.ParseIP("127.0.0.1")}
	if _, made, err := ensureCert(dir, ips, nil, time.Now(), rand.Reader); err != nil || !made {
		t.Fatalf("first: made %v, %v", made, err)
	}
	if _, made, err := ensureCert(dir, ips, nil, time.Now(), rand.Reader); err != nil || made {
		t.Fatalf("reuse: made %v, %v", made, err)
	}
	os.Chmod(filepath.Join(dir, KeyFile), 0o644)
	if _, _, err := ensureCert(dir, ips, nil, time.Now(), rand.Reader); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("readable key reused: %v", err)
	}
}

// EP-17: tls = off serves plain HTTP.
func TestPlaintextListener(t *testing.T) {
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t, "127.0.0.1")))
	dir := t.TempDir()
	startLAN(t, &LAN{Entries: []config.Listen{{Address: addr, TLS: "off"}}, ConfigDir: dir, Handler: &echo{}})
	if code, body, err := get(&http.Client{}, "http://"+addr+"/"); err != nil || code != 200 || body != "ok" {
		t.Fatalf("http: %d %q %v", code, body, err)
	}
	if _, err := os.Stat(filepath.Join(dir, CertFile)); !os.IsNotExist(err) {
		t.Error("certificate generated with TLS off")
	}
}

// EP-17, SEC-3: tls = files serves the user's pair and refuses to start
// when the key is readable by anyone but the service user.
func TestUserCertificate(t *testing.T) {
	src := t.TempDir()
	if _, _, err := ensureCert(src, []net.IP{net.ParseIP("127.0.0.1")}, nil, time.Now(), rand.Reader); err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(src, CertFile), filepath.Join(src, KeyFile)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t, "127.0.0.1")))
	entries := []config.Listen{{Address: addr, TLS: "files", Cert: certFile, Key: keyFile}}

	os.Chmod(keyFile, 0o640)
	l := &LAN{Entries: entries, ConfigDir: t.TempDir(), Handler: &echo{}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	err := l.Run(context.Background(), make(chan struct{}))
	if err == nil || !strings.Contains(err.Error(), keyFile) {
		t.Fatalf("group-readable key accepted: %v", err)
	}

	os.Chmod(keyFile, 0o600)
	dir := t.TempDir()
	startLAN(t, &LAN{Entries: entries, ConfigDir: dir, Handler: &echo{}})
	if code, _, err := get(tlsClient(pool(t, certFile)), "https://"+addr+"/"); err != nil || code != 200 {
		t.Fatalf("https with user certificate: %d %v", code, err)
	}
	if _, err := os.Stat(filepath.Join(dir, CertFile)); !os.IsNotExist(err) {
		t.Error("certificate generated for tls = files")
	}
}

// EP-17: an interface's addresses are resolved at start and again when
// they change: listeners follow, and the certificate is regenerated.
func TestInterfaceAddressesFollowed(t *testing.T) {
	var mu sync.Mutex
	ips := []net.IP{net.ParseIP("127.0.0.2")}
	lookup := func(name string) ([]net.IP, error) {
		mu.Lock()
		defer mu.Unlock()
		if name != "eth9" {
			return nil, &net.OpError{Op: "route", Err: os.ErrNotExist}
		}
		return slices.Clone(ips), nil
	}
	port := freePort(t, "127.0.0.2")
	freePort(t, "127.0.0.3")
	dir := t.TempDir()
	startLAN(t, &LAN{Entries: []config.Listen{{Interface: "eth9", Port: port, TLS: "generate"}},
		ConfigDir: dir, Handler: &echo{}, Lookup: lookup, Poll: 20 * time.Millisecond})
	url := func(ip string) string { return "https://" + net.JoinHostPort(ip, strconv.Itoa(port)) + "/" }
	if _, _, err := get(tlsClient(pool(t, filepath.Join(dir, CertFile))), url("127.0.0.2")); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	ips = []net.IP{net.ParseIP("127.0.0.3")}
	mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, _, err3 := get(tlsClient(pool(t, filepath.Join(dir, CertFile))), url("127.0.0.3"))
		_, err2 := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.2", strconv.Itoa(port)), time.Second)
		if err3 == nil && err2 != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("listeners did not follow the interface: new %v, old still open %v", err3, err2 == nil)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c, _ := ReadCert(dir)
	if len(c.IPAddresses) != 1 || !c.IPAddresses[0].Equal(net.ParseIP("127.0.0.3")) {
		t.Fatalf("certificate SANs %v", c.IPAddresses)
	}

	// Resolve reports interface addresses with the entry's port.
	got := Resolve([]config.Listen{{Interface: "eth9", Port: port, TLS: "off"}, {Interface: "nope", Port: 1, TLS: "off"},
		{Address: "192.168.1.10:9000", TLS: "generate"}}, lookup)
	if len(got) != 2 || got[0].URL() != "http://127.0.0.3:"+strconv.Itoa(port) || got[1].URL() != "https://192.168.1.10:9000" {
		t.Fatalf("Resolve: %+v", got)
	}
}
