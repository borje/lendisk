package transport

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"tailscale.com/envknob"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// #69 review: a node dialling two friends on one relay runs two tailcat
// clients, each with its own relay connection. With one dial key for
// both, the relay gave a key's packets to whichever of its connections
// wrote last: while A uploaded to C, C's client wrote all the time, so
// the download A asked of B went to it and was dropped, and stalled.
// With a dial key per friend, both carry at once. Direct paths are
// switched off, so every packet crosses the relay, as it does between
// friends behind hard NATs.
func TestTwoFriendsOnOneRelayCarryAtOnce(t *testing.T) {
	envknob.SetenvForTest(t, "TS_DEBUG_ALWAYS_USE_DERP", "true")
	d := transporttest.Start(t)
	a := newTailcat(t, newID(t), d)
	const download = 8 << 20
	serve := func(handle func(net.Conn)) (*identity.Identity, string) {
		id := newID(t)
		srv := newTailcat(t, id, d)
		srv.Admit(func(identity.NodeID) bool { return true })
		ln, err := srv.Listen(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { handle(c); c.Close() }()
			}
		}()
		return id, srv.Address()
	}
	// B sends download bytes once asked; C swallows what it is sent.
	bID, bAddr := serve(func(c net.Conn) {
		if _, err := io.ReadFull(c, make([]byte, 1)); err == nil {
			io.CopyN(c, rand.Reader, download)
		}
	})
	cID, cAddr := serve(func(c net.Conn) { io.Copy(io.Discard, c) })

	dial := func(id *identity.Identity, addr string) net.Conn {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		c, err := a.Dial(ctx, id.NodeID(), addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	toB, toC := dial(bID, bAddr), dial(cID, cAddr)

	// A uploads to C without pause until the download is done.
	var stop atomic.Bool
	uploaded := make(chan int64, 1)
	go func() {
		buf := make([]byte, 64<<10)
		var n int64
		for !stop.Load() {
			toC.SetWriteDeadline(time.Now().Add(5 * time.Second))
			w, err := toC.Write(buf)
			n += int64(w)
			if err != nil {
				break
			}
		}
		uploaded <- n
	}()
	time.Sleep(time.Second) // the upload is under way

	toB.SetDeadline(time.Now().Add(30 * time.Second))
	start := time.Now()
	if _, err := toB.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	got, err := io.Copy(io.Discard, io.LimitReader(toB, download))
	stop.Store(true)
	up := <-uploaded
	if err != nil || got != download {
		t.Fatalf("downloaded %d of %d bytes from B while uploading to C (%d bytes up): %v", got, download, up, err)
	}
	t.Logf("downloaded %d bytes from B in %v while uploading %d bytes to C", got, time.Since(start).Round(time.Millisecond), up)
	if up == 0 {
		t.Fatal("nothing was uploaded to C during the download")
	}
}
