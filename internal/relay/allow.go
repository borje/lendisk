package relay

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"tailscale.com/derp"
	"tailscale.com/derp/derpserver"

	"github.com/borje/lendisk/internal/identity"
)

// The allowlist (relay.allow, #99). A node connects to its relay with
// more keys than its node ID: one dial key per friend
// (identity.DialKeyFor) and, while redeeming an invite, the invite key.
// Those keys cannot be known in advance, so the relay cannot admit
// connections by key, as derper's --verify-clients does. It filters
// what they send instead: a packet is forwarded only when its sender or
// its destination is a listed node. Every lendisk tunnel has a listed
// end, the server whose address names this relay, so friends' tunnels
// pass; two keys neither of which is listed, the free relay an abuser
// wants, get nothing through. A listed node still decides for itself
// whom it admits (SEC-10).

// allowSet is the node IDs whose packets the relay forwards.
type allowSet map[identity.NodeID]bool

// allows reports whether a packet from src to dst is forwarded.
func (a allowSet) allows(src, dst identity.NodeID) bool { return a[src] || a[dst] }

// filterReader is the client-to-relay half of one DERP connection,
// with the packets the allowlist refuses taken out. It reads the DERP
// frames (a type byte, a 4-byte big-endian length, the payload): the
// first ClientInfo frame names the connection's key in its first 32
// bytes, which derpserver then checks against the frame's sealed box;
// each SendPacket frame names its destination in its first 32 bytes.
// Every other frame passes unchanged, for derpserver to judge.
type filterReader struct {
	r     io.Reader
	allow allowSet
	log   *slog.Logger
	// The connection's key, once its ClientInfo frame was read.
	src    identity.NodeID
	hasSrc bool
	logged bool   // a refused packet was logged for this connection
	out    []byte // header and head of the frame being passed
	pass   int64  // payload bytes of that frame still to pass
}

// maxDropped is the largest frame the filter drops itself; a larger
// one is passed for derpserver to refuse as oversized, which closes
// the connection, so no client makes the filter read without bound.
const maxDropped = derp.MaxPacketSize + derp.KeyLen

func (f *filterReader) Read(p []byte) (int, error) {
	for {
		if len(f.out) > 0 {
			n := copy(p, f.out)
			f.out = f.out[n:]
			return n, nil
		}
		if f.pass > 0 {
			if int64(len(p)) > f.pass {
				p = p[:f.pass]
			}
			n, err := f.r.Read(p)
			f.pass -= int64(n)
			if err == io.EOF && f.pass > 0 {
				err = io.ErrUnexpectedEOF
			}
			return n, err
		}
		var hdr [derp.FrameHeaderLen]byte
		if _, err := io.ReadFull(f.r, hdr[:]); err != nil {
			return 0, err
		}
		typ, n := derp.FrameType(hdr[0]), int64(binary.BigEndian.Uint32(hdr[1:]))
		keyed := (typ == derp.FrameClientInfo && !f.hasSrc) || typ == derp.FrameSendPacket
		if !keyed || n < derp.KeyLen {
			f.out, f.pass = hdr[:], n
			continue
		}
		var k identity.NodeID
		if _, err := io.ReadFull(f.r, k[:]); err != nil {
			return 0, io.ErrUnexpectedEOF
		}
		if typ == derp.FrameClientInfo {
			f.src, f.hasSrc = k, true
		} else if !f.allow.allows(f.src, k) && n <= maxDropped {
			if !f.logged {
				f.logged = true
				f.log.Info("relay: dropping packets between two nodes not in relay.allow; add the node whose address names this relay",
					"from", f.src.String(), "to", k.String())
			}
			if _, err := io.CopyN(io.Discard, f.r, n-derp.KeyLen); err != nil {
				return 0, io.ErrUnexpectedEOF
			}
			continue
		}
		f.out, f.pass = append(hdr[:], k[:]...), n-derp.KeyLen
	}
}

// filteredHandler is derpserver.Handler with the client's frames read
// through a filterReader. The rest follows derpserver.Handler: probes,
// the upgrade (DERP, or WebSocket, which derpserver also serves as
// plain DERP after the upgrade), the 101 answer unless the client
// starts fast, and the connection served on its own goroutine.
func filteredHandler(s *derpserver.Server, allow allowSet, lg *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/derp/probe", "/derp/latency-check":
			derpserver.ProbeHandler(w, r)
			return
		}
		up := strings.ToLower(r.Header.Get("Upgrade"))
		if up != "websocket" && up != "derp" {
			http.Error(w, "DERP requires connection upgrade", http.StatusUpgradeRequired)
			return
		}
		fastStart := r.Header.Get(derp.FastStartHeader) == "1"
		h, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "HTTP does not support general TCP support", http.StatusInternalServerError)
			return
		}
		netConn, brw, err := h.Hijack()
		if err != nil {
			http.Error(w, "HTTP does not support general TCP support", http.StatusInternalServerError)
			return
		}
		if !fastStart {
			netConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := fmt.Fprintf(netConn, "HTTP/1.1 101 Switching Protocols\r\n"+
				"Upgrade: DERP\r\n"+
				"Connection: Upgrade\r\n"+
				"Derp-Version: %v\r\n"+
				"Derp-Public-Key: %s\r\n\r\n",
				derp.ProtocolVersion, s.PublicKey().UntypedHexString()); err != nil {
				netConn.Close()
				return
			}
		}
		ctx := context.Background()
		if v := r.Header.Get(derp.IdealNodeHeader); v != "" {
			ctx = derpserver.IdealNodeContextKey.WithValue(ctx, v)
		}
		// brw.Reader holds what the client sent with its request, a
		// fast-starting client's ClientInfo among it, then reads on.
		fr := &filterReader{r: brw.Reader, allow: allow, log: lg}
		go serveFiltered(ctx, s, netConn, bufio.NewReadWriter(bufio.NewReaderSize(fr, 1<<10), nil), lg)
	})
}

// serveFiltered serves one hijacked connection, recovering a panic as
// net/http would, so one bad connection does not end the relay.
func serveFiltered(ctx context.Context, s *derpserver.Server, nc net.Conn, brw *bufio.ReadWriter, lg *slog.Logger) {
	defer func() {
		if e := recover(); e != nil {
			lg.Error("relay: panic serving a connection", "remote", nc.RemoteAddr().String(), "panic", fmt.Sprint(e), "stack", string(debug.Stack()))
			nc.Close()
		}
	}()
	s.Accept(ctx, nc, brw, nc.RemoteAddr().String())
}
