package transport

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// ParseExplicitAddress checks a friend's explicit address (NET-3), as
// `lendisk peer address` takes it, and returns it in the form it is
// kept: ip:port, or name:port with a DNS name (such as a dynamic DNS
// name, #99), lower-cased and without a trailing dot. A name is not
// looked up here; the transport looks it up at each dial.
func ParseExplicitAddress(s string) (string, error) {
	s = strings.TrimSpace(s)
	if ap, err := netip.ParseAddrPort(s); err == nil {
		if ap.Port() == 0 || ap.Addr().IsUnspecified() || ap.Addr().IsMulticast() {
			return "", fmt.Errorf("%q cannot be dialled: give a host's IP address or DNS name and a port from 1 to 65535", s)
		}
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String(), nil
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", fmt.Errorf("%q is not host:port, such as 203.0.113.5:41641, [2001:db8::5]:41641 or home.dyndns.example:41641", s)
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return "", fmt.Errorf("%q cannot be dialled: give a port from 1 to 65535", s)
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if !isDNSName(host) {
		return "", fmt.Errorf("%q: %q is neither an IP address nor a DNS name", s, host)
	}
	return net.JoinHostPort(host, port), nil
}

// isDNSName reports whether s is a host name: dot-separated labels of
// letters, digits and inner hyphens, 63 bytes each, 253 in all, and
// not all digits (which would be a mistyped IPv4 address).
func isDNSName(s string) bool {
	if s == "" || len(s) > 253 || strings.Trim(s, "0123456789.") == "" {
		return false
	}
	for _, l := range strings.Split(s, ".") {
		if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return false
		}
		for _, c := range l {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// endpointsOf are a friend's explicit addresses as UDP endpoints:
// each ip:port as it is, and each name:port as the IPs the name
// resolves to now, every lookup bounded by resolveTimeout. A name that
// does not resolve, or an address that is neither, is left out and
// logged; the friend is then reached by hole punching and the relay.
// The result is sorted and without duplicates.
func (t *Tailcat) endpointsOf(ctx context.Context, peer string, addrs []string) []netip.AddrPort {
	t.relays.mu.Lock()
	lookup := t.relays.lookup
	t.relays.mu.Unlock()
	var eps []netip.AddrPort
	for _, s := range addrs {
		if ap, err := netip.ParseAddrPort(s); err == nil {
			eps = append(eps, netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()))
			continue
		}
		host, port, err := net.SplitHostPort(s)
		p, perr := strconv.ParseUint(port, 10, 16)
		if err != nil || perr != nil || p == 0 || lookup == nil {
			t.log.Warn("transport: ignoring a friend's explicit address that is not host:port", "peer", peer, "address", s)
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, resolveTimeout)
		ips, err := lookup(rctx, host)
		cancel()
		if err != nil || len(ips) == 0 {
			t.log.Warn("transport: a friend's explicit address does not resolve now; it is reached by hole punching and the relay",
				"peer", peer, "address", s, "err", err)
			continue
		}
		for _, ip := range ips {
			if ip = ip.Unmap(); !ip.IsUnspecified() && !ip.IsMulticast() {
				eps = append(eps, netip.AddrPortFrom(ip, uint16(p)))
			}
		}
	}
	slices.SortFunc(eps, func(a, b netip.AddrPort) int { return a.Compare(b) })
	return slices.Compact(eps)
}
