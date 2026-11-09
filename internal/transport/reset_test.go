package transport

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// syncBuffer is a log buffer safe for the transport's goroutines.
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

// #89, #92: a tunnel connection resets with linger 0 and logs nothing;
// one Reset cannot reach (not gVisor's, or gonet changed) is closed as
// usual and the fallback is logged as a warning, since the friend then
// reads on what the tunnel holds of an abandoned send.
func TestAResetThatFallsBackToACloseIsLogged(t *testing.T) {
	d := transporttest.Start(t)
	srvID := newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(identity.NodeID) bool { return true })
	l := listen(t, srv)
	var logs syncBuffer
	dl, err := NewDialer(newID(t), slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case sc := <-l.conns:
		defer sc.Close()
	case <-ctx.Done():
		t.Fatal("no connection accepted")
	}
	tc, ok := c.(*tunnelConn)
	if !ok {
		t.Fatalf("dialled a %T", c)
	}
	if err := tc.Reset(); err != nil {
		t.Fatal(err)
	}
	if s := logs.String(); strings.Contains(s, "reset") {
		t.Fatalf("a tunnel connection's reset logged:\n%s", s)
	}

	a, b := net.Pipe()
	tl := &tunnelConn{Conn: a, c: &tailcatClient{t: dl, refs: 1, conns: 1}}
	tl.Reset()
	if _, err := b.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("the peer of a connection reset as a close reads %v", err)
	}
	if s := logs.String(); !strings.Contains(s, "level=WARN") || !strings.Contains(s, "could not be reset") {
		t.Fatalf("a reset that fell back to a close was not logged:\n%s", s)
	}
}
