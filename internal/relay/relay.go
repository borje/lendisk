// Package relay runs a self-hosted relay (NET-5, `lendisk relay`): a
// DERP server and a STUN server, as Tailscale's derper runs them, for
// friends whose nodes cannot reach each other directly. It is a relay
// for anyone who knows its address: it forwards WireGuard packets it
// cannot read, between keys it does not check, and sees only their IP
// addresses and volume (NET-5a). Nodes use it by listing it in
// relays.list, with the entry Run reports.
//
// Rules a caller must keep:
//
//   - Config.Dir is private to the relay (0700): it holds the
//     self-signed certificate's key and the ACME account key.
//   - The entry names Hostname: friends' nodes dial it, so it must be
//     the name or IP they reach this host by.
package relay

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/acme/autocert"
	"tailscale.com/derp/derpserver"
	"tailscale.com/net/stunserver"
	"tailscale.com/types/key"
)

// Config is how the relay serves.
type Config struct {
	// Hostname is the DNS name or IP address friends' nodes dial; the
	// certificate names it.
	Hostname string
	// Listen is the TCP address DERP is served on over HTTPS, such as
	// ":443".
	Listen string
	// STUNPort is the UDP port of the STUN server, which nodes use to
	// learn their public addresses; 0 serves none.
	STUNPort int
	// TLS is "self-signed" (made once and kept in Dir; nodes pin it by
	// its hash), "letsencrypt" (needs Hostname to be a DNS name that
	// points here, and Listen on port 443) or "files" (CertFile and
	// KeyFile, renewed by something else and re-read at each handshake).
	TLS               string
	CertFile, KeyFile string
	// Dir keeps the self-signed certificate and the ACME cache.
	Dir string
	Log *slog.Logger
}

// Entry is how a node lists a relay in relays.list, in the compact
// form of the config's array of tables.
type Entry struct {
	Host     string
	Port     int
	STUNPort int
	Cert     string // sha256-raw:<hex> for a self-signed certificate
}

func (e Entry) String() string {
	s := fmt.Sprintf("host=%s,port=%d", e.Host, e.Port)
	if e.STUNPort != 0 {
		s += fmt.Sprintf(",stun_port=%d", e.STUNPort)
	} else {
		s += ",stun_port=-1"
	}
	if e.Cert != "" {
		s += ",cert=" + e.Cert
	}
	return s
}

// Check reports what is wrong with c, before anything listens.
func (c Config) Check() error {
	var errs []error
	if strings.TrimSpace(c.Hostname) == "" {
		errs = append(errs, errors.New("relay.hostname is empty: give the DNS name or IP address friends' nodes reach this host by"))
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		errs = append(errs, fmt.Errorf("relay.listen = %q: want host:port, such as :443", c.Listen))
	}
	if c.STUNPort < 0 || c.STUNPort > 65535 {
		errs = append(errs, fmt.Errorf("relay.stun_port = %d: want 1 to 65535, or 0 for no STUN server", c.STUNPort))
	}
	switch c.TLS {
	case "self-signed":
	case "letsencrypt":
		if _, err := netip.ParseAddr(c.Hostname); err == nil {
			errs = append(errs, errors.New("relay.tls = \"letsencrypt\" needs relay.hostname to be a DNS name, not an IP address; use tls = \"self-signed\""))
		}
	case "files":
		if !filepath.IsAbs(c.CertFile) || !filepath.IsAbs(c.KeyFile) {
			errs = append(errs, errors.New("relay.tls = \"files\" needs relay.cert and relay.key as absolute paths"))
		}
	default:
		errs = append(errs, fmt.Errorf("relay.tls = %q: want self-signed, letsencrypt or files", c.TLS))
	}
	return errors.Join(errs...)
}

// Run serves until ctx ends. ready, if set, is called once the relay
// listens, with the entry nodes list it by.
func Run(ctx context.Context, c Config, ready func(Entry)) error {
	if err := c.Check(); err != nil {
		return err
	}
	if err := os.MkdirAll(c.Dir, 0o700); err != nil {
		return err
	}
	tlsConf, pin, err := c.tlsConfig()
	if err != nil {
		return err
	}
	logf := func(format string, args ...any) {
		c.Log.Debug(strings.TrimSpace(fmt.Sprintf(format, args...)), "component", "derp")
	}
	d := derpserver.New(key.NewNode(), logf)
	defer d.Close()

	mux := http.NewServeMux()
	mux.Handle("/derp", derpserver.Handler(d))
	mux.HandleFunc("/derp/probe", derpserver.ProbeHandler)
	mux.HandleFunc("/derp/latency-check", derpserver.ProbeHandler)
	mux.HandleFunc("/generate_204", derpserver.ServeNoContent)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "This is a lendisk relay (a DERP server). It forwards encrypted packets between lendisk nodes and reads none of them.")
	})

	ln, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return fmt.Errorf("relay.listen %s: %w", c.Listen, err)
	}
	hs := &http.Server{Handler: mux, TLSConfig: tlsConf, ReadHeaderTimeout: 10 * time.Second,
		// DERP upgrades an HTTP/1.1 connection; HTTP/2 is off, as in derper.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
		ErrorLog:     slog.NewLogLogger(c.Log.Handler(), slog.LevelDebug)}
	errc := make(chan error, 2)
	go func() { errc <- hs.ServeTLS(ln, "", "") }()

	e := Entry{Host: c.Hostname, Port: ln.Addr().(*net.TCPAddr).Port, Cert: pin}
	if c.STUNPort != 0 {
		stun := stunserver.New(ctx)
		host, _, _ := net.SplitHostPort(c.Listen)
		if err := stun.Listen(net.JoinHostPort(host, strconv.Itoa(c.STUNPort))); err != nil {
			hs.Close()
			return fmt.Errorf("relay.stun_port %d: %w", c.STUNPort, err)
		}
		go func() { errc <- stun.Serve() }()
		e.STUNPort = stun.LocalAddr().(*net.UDPAddr).Port
	}
	c.Log.Info("relay serving; nodes list it in relays.list", "entry", e.String(), "listen", ln.Addr().String(), "tls", c.TLS)
	if ready != nil {
		ready(e)
	}
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hs.Shutdown(sctx)
	hs.Close()
	if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
		return fmt.Errorf("relay stopped: %w", err)
	}
	return nil
}

// tlsConfig is the relay's TLS, and for a self-signed certificate the
// pin nodes list it with.
func (c Config) tlsConfig() (*tls.Config, string, error) {
	switch c.TLS {
	case "letsencrypt":
		m := &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist(c.Hostname),
			Cache:      autocert.DirCache(filepath.Join(c.Dir, "acme")),
		}
		return m.TLSConfig(), "", nil
	case "files":
		if _, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile); err != nil {
			return nil, "", fmt.Errorf("relay.cert and relay.key: %w", err)
		}
		return &tls.Config{GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
			return &cert, err
		}}, "", nil
	}
	cert, err := c.selfSigned()
	if err != nil {
		return nil, "", err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, fmt.Sprintf("sha256-raw:%x", sha256.Sum256(cert.Certificate[0])), nil
}

// selfSigned loads the certificate kept in Dir, or makes one for
// Hostname and keeps it, so the pin nodes hold stays valid across
// restarts. A kept certificate for another host name is replaced.
func (c Config) selfSigned() (tls.Certificate, error) {
	certFile, keyFile := filepath.Join(c.Dir, "relay.crt"), filepath.Join(c.Dir, "relay.key")
	if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
		if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil && leaf.VerifyHostname(c.Hostname) == nil {
			return cert, nil
		}
		c.Log.Warn("the kept relay certificate names another host; making a new one, so nodes must list the new entry", "hostname", c.Hostname)
	} else if !errors.Is(err, fs.ErrNotExist) {
		c.Log.Warn("the kept relay certificate cannot be read; making a new one, so nodes must list the new entry", "err", err)
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: c.Hostname},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(20, 0, 0), // pinned by hash; renewal would only change the pin
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip, err := netip.ParseAddr(c.Hostname); err == nil {
		tmpl.IPAddresses = []net.IP{ip.AsSlice()}
	} else {
		tmpl.DNSNames = []string{c.Hostname}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	if err := writePrivate(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})); err != nil {
		return tls.Certificate{}, err
	}
	if err := writePrivate(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
		return tls.Certificate{}, err
	}
	return tls.LoadX509KeyPair(certFile, keyFile)
}

// writePrivate writes b to path at 0600 from the first byte.
func writePrivate(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
