package cli

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/gateway"
)

// VER-3 (#27 review): `lendisk verify` waits for the daemon, and stops
// waiting when it cannot come: it exits 1 when the daemon stops while it
// waits, and, interrupted, takes its request back, though its own
// context has ended. A stand-in socket plays the daemon that never
// answers.
func TestVerifyStopsWaitingForAGoneDaemon(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	n.addBucket(t, "laptop", "restic")
	peer := befriendDirect(t, n, 1<<30, 1)[0]
	sock := gateway.SocketPath(n.data)
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		t.Fatal(err)
	}
	listen := func() net.Listener {
		t.Helper()
		os.Remove(sock)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				c.Close()
			}
		}()
		return ln
	}
	pending := func() int {
		t.Helper()
		var k int
		if err := openDB(t, n).QueryRow(`SELECT count(*) FROM verify_request WHERE done_at IS NULL`).Scan(&k); err != nil {
			t.Fatal(err)
		}
		return k
	}

	// The daemon stops while verify waits.
	ln := listen()
	polls := 0
	n.sleep = func(ctx context.Context, d time.Duration) error {
		if polls++; polls == 2 {
			ln.Close()
		}
		return nil
	}
	code, _, e := n.run("", "", "verify", "laptop", "--peer", peer, "--timeout", "2m")
	if code != 1 || !strings.Contains(e, "daemon stopped") {
		t.Fatalf("verify as the daemon stops: %d %s", code, e)
	}
	if k := pending(); k != 0 {
		t.Fatalf("%d requests left behind", k)
	}

	// Interrupted while it waits.
	ln = listen()
	defer ln.Close()
	n.sleep = func(ctx context.Context, d time.Duration) error { return context.Canceled }
	if code, _, _ := n.run("", "", "verify", "laptop", "--peer", peer, "--timeout", "2m"); code == 0 {
		t.Fatal("an interrupted verify succeeded")
	}
	if k := pending(); k != 0 {
		t.Fatalf("%d requests left behind after an interrupt", k)
	}
}
