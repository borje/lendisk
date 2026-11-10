package bridge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/replica"
)

// refusingGateway stands in for gw.sock as versitygw answers a write it
// refuses (LEND-2): it answers a bodiless request 200 and a write e,
// read up to its head only, without reading the body, and closes the
// connection; with e nil it closes without an answer. It stops reading
// a moment before it answers, so the bridge's write of the body fails
// before the answer can be read: the order that, under load, about half
// the runs against the real gateway saw (#100), and about half here.
func refusingGateway(t *testing.T, e *s3err.APIError) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "gw.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				for {
					rq, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					if rq.ContentLength <= 0 {
						// The bucket's create, and anything else bodiless.
						fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
						continue
					}
					// The bridge has filled the socket by now and waits
					// in its write. Reading stops, and what is read off
					// the socket after that wakes the write to fail.
					time.Sleep(20 * time.Millisecond)
					c.(*net.UnixConn).CloseRead()
					io.CopyN(io.Discard, c, 64<<10)
					time.Sleep(20 * time.Millisecond)
					if e == nil {
						return
					}
					body := e.XMLBody("", "")
					fmt.Fprintf(c, "HTTP/1.1 %d %s\r\nContent-Type: application/xml\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
						e.HTTPStatusCode, http.StatusText(e.HTTPStatusCode), len(body))
					c.Write(body)
					return
				}
			}()
		}
	}()
	return sock
}

// putThroughBridge puts a 3 MiB object as bob, with the replica client,
// through the tunnel to a bridge in front of gw.
func putThroughBridge(t *testing.T, gw string) error {
	t.Helper()
	br := New(gw, quiet)
	l := startLenderS3(t, br.TunnelS3Handler())
	bob := newID(t)
	l.befriend(t, bob)
	dial := func(ctx context.Context, _ identity.NodeID, _ string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", l.tun.ln.Addr().String())
		if err == nil {
			l.tun.mu.Lock()
			l.tun.keys[c.LocalAddr().String()] = bob.NodeID()
			l.tun.mu.Unlock()
		}
		return c, err
	}
	f, err := replica.NewFriend(bob, l.self.NodeID(), "lender", dial, replica.Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("x"), 3<<20)
	sum := sha256.Sum256(body)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return f.Put(ctx, replica.Object{Bucket: "laptop", Key: "data/big", Size: int64(len(body)),
		SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(body)})
}

// #100, LEND-2: a lender that answers 503 SlowDown before reading a
// large body, and closes gw.sock, is relayed as busy, not as the
// bridge's own "gateway unavailable".
func TestABusyAnswerBeforeALargeBodyIsReadReachesTheBorrower(t *testing.T) {
	gw := refusingGateway(t, &s3err.APIError{Code: "SlowDown", HTTPStatusCode: http.StatusServiceUnavailable,
		Description: "lendisk: the writes in flight leave no room; retry shortly"})
	if err := putThroughBridge(t, gw); !errors.Is(err, replica.ErrBusy) {
		t.Fatalf("a 3 MiB put the lender answers SlowDown unread: %v; want ErrBusy", err)
	}
}

// #100, LEND-2: the same for 507 QuotaExceeded, which pauses the
// borrower for quota.
func TestAQuotaAnswerBeforeALargeBodyIsReadReachesTheBorrower(t *testing.T) {
	gw := refusingGateway(t, &s3err.APIError{Code: "QuotaExceeded", HTTPStatusCode: http.StatusInsufficientStorage,
		Description: "lendisk: quota exceeded"})
	if err := putThroughBridge(t, gw); !errors.Is(err, replica.ErrQuota) {
		t.Fatalf("a 3 MiB put the lender answers 507 unread: %v; want ErrQuota", err)
	}
}

// #100: a gateway that stops reading and closes without an answer is
// still the bridge's "gateway unavailable", at once: the held-back
// write ends when the transport finds no answer.
func TestAGatewayThatClosesWithoutAnAnswerIsUnavailable(t *testing.T) {
	start := time.Now()
	err := putThroughBridge(t, refusingGateway(t, nil))
	if err == nil || !strings.Contains(err.Error(), "gateway unavailable") {
		t.Fatalf("a 3 MiB put the lender drops unanswered: %v; want gateway unavailable", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("the put failed after %v; want at once", el)
	}
}
