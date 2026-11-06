// Command tcspike is a throwaway prototype for lendisk issue #3: it pins
// tailcat and shows the admission behaviour DESIGN.md §3.9 relies on
// (SEC-10 to SEC-15). It is not lendisk code.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
	"tailscale.com/wgengine/filter"
)

// TunnelPort is the one port lendisk serves inside the tailcat Server
// (DESIGN.md §3.7).
const TunnelPort = 9000

// ServerConfig is what lendisk would pass to its tunnel at start.
type ServerConfig struct {
	Key    key.NodePrivate     // the identity's WireGuard key (ID-1)
	Region *tailcfg.DERPRegion // saved at init, never picked per start (SEC-14)
	// Admit is the admission hook (SEC-10). It must be non-nil; a nil
	// hook would make tailcat admit every client.
	Admit func(key.NodePublic) bool
	Logf  logger.Logf
}

// ErrNilHook is returned by StartServer when no admission hook is set.
var ErrNilHook = errors.New("tcspike: refusing to start: admission hook is nil (tailcat would admit every client)")

// StartServer is the lendisk-side guard: it never starts a tailcat
// Server without a hook, never with a pre-shared key, and never with a
// region chosen at start.
func StartServer(ctx context.Context, cfg ServerConfig) (*tailcat.Server, error) {
	if cfg.Admit == nil {
		return nil, ErrNilHook
	}
	if cfg.Key.IsZero() {
		return nil, errors.New("tcspike: refusing to start: no identity key")
	}
	if cfg.Region == nil {
		return nil, errors.New("tcspike: refusing to start: no saved DERP region")
	}
	s := &tailcat.Server{
		Key:                 cfg.Key,
		DisablePresharedKey: true,
		Region:              cfg.Region,
		AllowClient:         cfg.Admit,
		Logf:                cfg.Logf,
		// Only the tunnel port; the packet filter drops everything else.
		ServedTCPPorts: []filter.PortRange{{First: TunnelPort, Last: TunnelPort}},
		ServedUDPPorts: []filter.PortRange{},
	}
	if err := s.StartContext(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// FileAdmitter stands in for the lendisk.db lookup: it re-reads a file
// of allowed public keys (one per line) on every call and rejects on any
// error, so it fails closed like SEC-10 requires.
type FileAdmitter struct {
	Path string
	Logf logger.Logf
}

func (a FileAdmitter) Admit(k key.NodePublic) bool {
	f, err := os.Open(a.Path)
	if err != nil {
		a.logf("admit %v: reject (%v)", k.ShortString(), err)
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var allowed key.NodePublic
		if err := allowed.UnmarshalText([]byte(line)); err != nil {
			a.logf("admit %v: reject (bad line %q: %v)", k.ShortString(), line, err)
			return false
		}
		if allowed == k {
			a.logf("admit %v: accept", k.ShortString())
			return true
		}
	}
	if err := sc.Err(); err != nil {
		a.logf("admit %v: reject (%v)", k.ShortString(), err)
		return false
	}
	a.logf("admit %v: reject (unknown key)", k.ShortString())
	return false
}

func (a FileAdmitter) logf(format string, args ...any) {
	if a.Logf != nil {
		a.Logf(format, args...)
	}
}

// DenyAll is the hook from the issue's acceptance list: set, and false
// for everything.
func DenyAll(key.NodePublic) bool { return false }

// PeerOf is SEC-12's first step: the key that authenticated the
// connection, or an error, in which case the caller closes the
// connection before reading a byte.
func PeerOf(s *tailcat.Server, c net.Conn) (key.NodePublic, error) {
	k, ok := s.PeerKey(c.RemoteAddr())
	if !ok {
		return key.NodePublic{}, fmt.Errorf("no peer key for %v", c.RemoteAddr())
	}
	return k, nil
}
