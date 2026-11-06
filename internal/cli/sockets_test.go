package cli

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/borje/lendisk/internal/store"
)

// sock is one of this process's IP sockets, from /proc.
type sock struct {
	proto  string // tcp or udp
	local  string // host:port
	listen bool
}

// ownSockets lists the IP sockets this process holds, as `ss -lntup`
// would for its PID (EP-5, SEC-8).
func ownSockets() ([]sock, error) {
	fds, err := filepath.Glob("/proc/self/fd/*")
	if err != nil {
		return nil, err
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		l, err := os.Readlink(fd)
		if err == nil && strings.HasPrefix(l, "socket:[") {
			inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
		}
	}
	var out []sock
	for _, f := range []string{"tcp", "tcp6", "udp", "udp6"} {
		fh, err := os.Open("/proc/self/net/" + f)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(fh)
		sc.Scan() // header
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) < 10 || !inodes[fields[9]] {
				continue
			}
			local, err := parseProcAddr(fields[1])
			if err != nil {
				fh.Close()
				return nil, err
			}
			out = append(out, sock{proto: strings.TrimSuffix(f, "6"), local: local, listen: fields[3] == "0A"})
		}
		fh.Close()
	}
	return out, nil
}

// parseProcAddr decodes /proc/net's "0100007F:2328" (little-endian words).
func parseProcAddr(s string) (string, error) {
	ipHex, portHex, ok := strings.Cut(s, ":")
	if !ok {
		return "", fmt.Errorf("bad address %q", s)
	}
	raw, err := hex.DecodeString(ipHex)
	if err != nil {
		return "", err
	}
	for i := 0; i+4 <= len(raw); i += 4 {
		raw[i], raw[i+1], raw[i+2], raw[i+3] = raw[i+3], raw[i+2], raw[i+1], raw[i]
	}
	port, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil {
		return "", err
	}
	ip := net.IP(raw)
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), nil
}

func statfsAs(fs string) func(string) (store.Space, error) {
	return func(string) (store.Space, error) {
		return store.Space{Total: 1 << 40, Free: 1 << 40, FSType: fs}, nil
	}
}
