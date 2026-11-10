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

// sock is one of this process's sockets, from /proc.
type sock struct {
	proto  string // tcp, udp or unix
	local  string // host:port, or a unix socket's path (empty if unnamed)
	listen bool
}

// ownSockets lists the sockets this process holds, as `ss -lntup` and
// `ss -lxp` would for its PID (EP-5, SEC-8).
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
	// Unix sockets: Num RefCount Protocol Flags Type St Inode [Path].
	// A listening one has __SO_ACCEPTCON (0x10000) in Flags.
	fh, err := os.Open("/proc/self/net/unix")
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	sc.Scan() // header
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 7 || !inodes[fields[6]] {
			continue
		}
		flags, err := strconv.ParseUint(fields[3], 16, 32)
		if err != nil {
			return nil, err
		}
		var path string
		if len(fields) > 7 {
			path = fields[7]
		}
		out = append(out, sock{proto: "unix", local: path, listen: flags&0x10000 != 0})
	}
	return out, sc.Err()
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
