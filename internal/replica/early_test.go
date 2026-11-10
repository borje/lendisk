package replica

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
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// resettingFriend stands in for a lender's tunnel port that refuses a
// put before reading its body: it reads the request's head and 64 KiB
// of the body, writes answer unless it is empty, and resets the
// connection (a close with the body unread), as net/http does 500 ms
// after an early answer. The reset reaches the borrower while its
// write of the body waits on a full window.
func resettingFriend(t *testing.T, answer string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
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
				rq, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				if rq.ContentLength < 1<<20 {
					fmt.Fprintf(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n") // the bucket's create
					rq, err = http.ReadRequest(br)
					if err != nil {
						return
					}
				}
				io.CopyN(io.Discard, rq.Body, 64<<10)
				if answer != "" {
					io.WriteString(c, answer)
					time.Sleep(200 * time.Millisecond)
				}
				c.(*net.TCPConn).SetLinger(0)
			}()
		}
	}()
	return ln.Addr().String()
}

// putTo puts a 64 MiB object to the friend at addr with a 10 s stall
// timeout, reading what arrives late by delay.
func putTo(t *testing.T, addr string, delay time.Duration) error {
	t.Helper()
	self, lender := newID(t), newID(t)
	dial := func(ctx context.Context, _ identity.NodeID, _ string) (net.Conn, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil || delay == 0 {
			return c, err
		}
		return &lateConn{Conn: c, delay: delay}, nil
	}
	f, err := NewFriend(self, lender.NodeID(), "lender", dial, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("b"), 64<<20)
	sum := sha256.Sum256(body)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return f.Put(ctx, Object{Bucket: "laptop", Key: "data/big", Size: int64(len(body)),
		SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(body)})
}

// lateConn reads what arrives on it delay late, as a borrower whose
// reading goroutine is not scheduled for that long.
type lateConn struct {
	net.Conn
	delay time.Duration
}

func (c *lateConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		time.Sleep(c.delay)
	}
	return n, err
}

// #109, LEND-2: a friend's busy answer, written before the friend reset
// the connection on the body it did not read, reaches a borrower that
// reads it after its write of the body failed on the reset.
func TestABusyAnswerOutlivesTheResetOfTheWriteItRefused(t *testing.T) {
	body := "<Error><Code>SlowDown</Code><Message>retry shortly</Message><Resource></Resource></Error>\n"
	answer := fmt.Sprintf("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/xml\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s",
		len(body), body)
	if err := putTo(t, resettingFriend(t, answer), time.Second); !errors.Is(err, ErrBusy) {
		t.Fatalf("a put the friend answers SlowDown and resets, read 1 s late: %v; want ErrBusy", err)
	}
}

// #109: a friend that resets without an answer fails the put at once:
// the held write ends when the transport finds no answer, long before
// its bound (the stall timeout, 10 s here).
func TestAResetWithoutAnAnswerFailsAtOnce(t *testing.T) {
	start := time.Now()
	err := putTo(t, resettingFriend(t, ""), 0)
	if err == nil {
		t.Fatal("a put the friend reset unanswered succeeded")
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("the put failed after %v (%v); want at once", el, err)
	}
}
