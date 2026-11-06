package bridge

import (
	"bytes"
	"context"
	"crypto/tls"

	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/borje/lendisk/internal/config"
)

// Lookup returns the addresses of a network interface.
type Lookup func(name string) ([]net.IP, error)

// InterfaceAddrs is the system Lookup: the interface's unicast addresses,
// link-local ones left out (an IPv6 link-local address needs a zone no
// backup tool's URL can carry). Pure Go (netlink on Linux), no cgo.
func InterfaceAddrs(name string) ([]net.IP, error) {
	ifi, err := net.InterfaceByName(name)
	if err != nil {
		return nil, err
	}
	addrs, err := ifi.Addrs()
	if err != nil {
		return nil, err
	}
	var out []net.IP
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok || n.IP.IsLinkLocalUnicast() || n.IP.IsUnspecified() || n.IP.IsMulticast() {
			continue
		}
		out = append(out, n.IP)
	}
	return out, nil
}

// Addr is one network address an [[endpoint.listen]] entry listens on.
type Addr struct {
	Entry    config.Listen
	HostPort string
	ip       net.IP
}

// URL is the address as an S3 endpoint URL: https unless TLS is off.
func (a Addr) URL() string {
	if a.Entry.TLS == "off" {
		return "http://" + a.HostPort
	}
	return "https://" + a.HostPort
}

// Plaintext reports whether the listener serves without TLS.
func (a Addr) Plaintext() bool { return a.Entry.TLS == "off" }

// Resolve lists the addresses entries listen on now, in entry order. An
// interface that is missing or has no address contributes none; lookup
// errors are returned per interface name.
func Resolve(entries []config.Listen, lookup Lookup) []Addr {
	out, _ := resolve(entries, lookup)
	return out
}

func resolve(entries []config.Listen, lookup Lookup) ([]Addr, map[string]error) {
	if lookup == nil {
		lookup = InterfaceAddrs
	}
	var out []Addr
	errs := map[string]error{}
	for _, e := range entries {
		if e.Interface == "" {
			host, _, _ := net.SplitHostPort(e.Address)
			out = append(out, Addr{Entry: e, HostPort: e.Address, ip: net.ParseIP(host)})
			continue
		}
		ips, err := lookup(e.Interface)
		if err != nil {
			errs[e.Interface] = err
		}
		sortIPs(ips)
		for _, ip := range ips {
			out = append(out, Addr{Entry: e, HostPort: net.JoinHostPort(ip.String(), strconv.Itoa(e.Port)), ip: ip})
		}
	}
	return out, errs
}

func sortIPs(ips []net.IP) {
	slices.SortFunc(ips, func(a, b net.IP) int { return bytes.Compare(a.To16(), b.To16()) })
}

// LAN runs the bridge's listeners on the [[endpoint.listen]] entries
// (EP-17): plain HTTP, or TLS with a generated certificate or the user's.
// Interface entries are resolved at start and every Poll after; listeners
// open and close as addresses come and go, and the generated certificate
// is regenerated when its address set changes.
type LAN struct {
	Entries   []config.Listen
	ConfigDir string       // holds endpoint.crt and endpoint.key
	Handler   http.Handler // the bridge's handler for tool listeners
	Log       *slog.Logger
	Lookup    Lookup        // nil means InterfaceAddrs
	Poll      time.Duration // zero means 10s
}

type lanListener struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Run opens the listeners, closes ready, and serves until ctx ends. It
// fails before ready if a configured address cannot be bound or a
// certificate cannot be made or loaded.
func (l *LAN) Run(ctx context.Context, ready chan<- struct{}) error {
	poll := l.Poll
	if poll == 0 {
		poll = 10 * time.Second
	}
	user := map[config.Listen]*tls.Certificate{}
	for _, e := range l.Entries {
		if e.TLS == "files" {
			pair, err := loadUserPair(e.Cert, e.Key)
			if err != nil {
				return err
			}
			user[e] = pair
		}
	}
	var names []string
	if h, err := os.Hostname(); err == nil && h != "" {
		names = []string{h}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	open := map[string]*lanListener{}
	defer func() {
		for _, ln := range open {
			ln.cancel()
			<-ln.done
		}
	}()
	// The generated certificate, swapped in place when it is regenerated
	// so listeners already open serve the new one.
	var gen atomic.Pointer[tls.Certificate]
	genConfig := &tls.Config{MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return gen.Load(), nil }}
	var genIPs []net.IP
	failed := map[string]string{} // address or interface -> last error logged

	update := func(start bool) error {
		addrs, lookupErrs := resolve(l.Entries, l.Lookup)
		for name, err := range lookupErrs {
			if failed[name] != err.Error() {
				l.Log.Warn("endpoint.listen interface has no usable address", "interface", name, "err", err)
				failed[name] = err.Error()
			}
		}
		var ips []net.IP
		for _, a := range addrs {
			if a.Entry.TLS == "generate" && !slices.ContainsFunc(ips, a.ip.Equal) {
				ips = append(ips, a.ip)
			}
		}
		sortIPs(ips)
		if len(ips) > 0 && !slices.EqualFunc(ips, genIPs, net.IP.Equal) {
			pair, changed, err := ensureCert(l.ConfigDir, ips, names)
			if err != nil {
				return fmt.Errorf("endpoint certificate: %w", err)
			}
			if changed {
				l.Log.Info("generated endpoint certificate", "path", l.ConfigDir+"/"+CertFile,
					"addresses", ips, "sha256", Fingerprint(pair.Certificate[0]))
			}
			gen.Store(pair)
			genIPs = ips
		}
		want := map[string]Addr{}
		for _, a := range addrs {
			want[a.HostPort] = a
		}
		for hp, ln := range open {
			if _, ok := want[hp]; !ok {
				ln.cancel()
				<-ln.done
				delete(open, hp)
				l.Log.Info("S3 endpoint stopped listening; the address is gone", "address", hp)
			}
		}
		for _, a := range addrs {
			if open[a.HostPort] != nil {
				continue
			}
			nl, err := net.Listen("tcp", a.HostPort)
			if err != nil {
				if start && a.Entry.Interface == "" {
					return fmt.Errorf("S3 endpoint on %s: %w", a.HostPort, err)
				}
				if failed[a.HostPort] != err.Error() {
					l.Log.Warn("S3 endpoint cannot listen; retrying", "address", a.HostPort, "err", err)
					failed[a.HostPort] = err.Error()
				}
				continue
			}
			delete(failed, a.HostPort)
			switch a.Entry.TLS {
			case "generate":
				nl = tls.NewListener(nl, genConfig)
			case "files":
				nl = tls.NewListener(nl, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*user[a.Entry]}})
			}
			lctx, lcancel := context.WithCancel(ctx)
			ln := &lanListener{cancel: lcancel, done: make(chan struct{})}
			open[a.HostPort] = ln
			go func() {
				defer close(ln.done)
				if err := Serve(lctx, nl, l.Handler, l.Log); err != nil && lctx.Err() == nil {
					select {
					case errc <- fmt.Errorf("S3 endpoint on %s: %w", a.HostPort, err):
					default:
					}
				}
			}()
			tlsAttr := a.Entry.TLS
			if a.Plaintext() {
				tlsAttr = "off (plaintext)"
			}
			l.Log.Info("S3 endpoint listening", "address", a.HostPort, "tls", tlsAttr)
		}
		return nil
	}

	if err := update(true); err != nil {
		return err
	}
	close(ready)
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			return err
		case <-t.C:
			if err := update(false); err != nil {
				l.Log.Error("endpoint.listen update failed", "err", err)
			}
		}
	}
}

// LANHandler serves the S3 endpoint on [[endpoint.listen]] addresses
// (EP-17): the same as on loopback, the control API refused.
func (b *Bridge) LANHandler() http.Handler { return b.LoopbackHandler() }
