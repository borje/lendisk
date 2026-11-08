package control_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

// PEER-5 (#25): the remove call is how a friend that ended the
// friendship tells this node. It ends the friendship here too, as ended
// by the friend (and so told already), commits before it answers, calls
// Changed and Ended, and answers for the caller alone: no parameter
// names whom. An invite key cannot call it; a caller with no friendship
// is answered as told, since there is nothing left to end.
func TestRemoveCallEndsTheFriendshipOnThisSide(t *testing.T) {
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bob, carol, invited := newID(t), newID(t), newID(t)
	for _, f := range []string{bob.NodeID().String(), carol.NodeID().String()} {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	var changed, ended atomic.Int32
	now := time.Unix(1_800_000_000, 0)
	l := newLender(t)
	l.srv.DB, l.srv.Now = db, func() time.Time { return now }
	l.srv.Changed = func() { changed.Add(1) }
	l.srv.Ended = func() { ended.Add(1) }

	if err := l.client(t, control.Caller{Peer: invited.NodeID(), Role: state.RoleInvite}).Remove(t.Context()); err == nil {
		t.Fatal("an invite key ended a friendship")
	} else if ce := (*control.CallError)(nil); !errors.As(err, &ce) || ce.Status != http.StatusForbidden {
		t.Fatalf("remove by an invite key: %v", err)
	}
	if err := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer}).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetFriendship(t.Context(), bob.NodeID().String()); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("Bob is still a friend: %v", err)
	}
	fs, err := db.FormerFriends(t.Context())
	if err != nil || len(fs) != 1 || fs[0].Peer != bob.NodeID().String() || !fs[0].ByPeer || !fs[0].NotifiedAt.Equal(now) {
		t.Fatalf("former friends: %+v, %v", fs, err)
	}
	// Ended runs once the client closed the connection after the answer.
	for deadline := time.Now().Add(5 * time.Second); ended.Load() == 0 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	if changed.Load() != 1 || ended.Load() != 1 {
		t.Fatalf("Changed %d, Ended %d after the remove call", changed.Load(), ended.Load())
	}
	if _, err := db.GetFriendship(t.Context(), carol.NodeID().String()); err != nil {
		t.Fatalf("Carol after Bob's remove call: %v", err)
	}
	// Again (an answer lost): told already, nothing changes.
	if err := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer}).Remove(t.Context()); err != nil {
		t.Fatalf("Bob's remove call again: %v", err)
	}
	if changed.Load() != 1 || ended.Load() != 1 {
		t.Fatalf("Changed %d, Ended %d after a second remove call", changed.Load(), ended.Load())
	}
}

// removeServer serves srv over loopback TCP as if every connection
// were peer's, and returns its address.
func removeServer(t *testing.T, srv *control.Server, peer control.Caller) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(w, r.WithContext(control.WithCaller(r.Context(), peer)))
	}))
	t.Cleanup(ts.Close)
	return ts.Listener.Addr().String()
}

const rawRemove = "POST " + control.RemovePath + " HTTP/1.1\r\nHost: lendisk\r\n" + control.ProtocolHeader + ": " +
	control.Protocol + "\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// PEER-5, SEC-15 (#84): ending the friendship on a remove call cuts the
// caller's connections and drops its tunnel (Ended), which loses an
// answer still on its way. So Ended runs only once the caller has the
// answer: after it closed the connection, as a client does after an
// answer with Connection: close, or after AnswerWait at the most.
func TestRemoveCallEndsOnlyOnceTheCallerHasTheAnswer(t *testing.T) {
	db, err := state.Open(t.Context(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	bob, carol := newID(t), newID(t)
	for _, f := range []string{bob.NodeID().String(), carol.NodeID().String()} {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	var ended atomic.Int32
	srv := &control.Server{DB: db, Self: newID(t), Log: slog.New(slog.DiscardHandler), Ended: func() { ended.Add(1) },
		AnswerWait: 30 * time.Second}
	addr := removeServer(t, srv, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, rawRemove); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Bob's friendship was not ended", func() bool {
		_, err := db.GetFriendship(t.Context(), bob.NodeID().String())
		return errors.Is(err, state.ErrNotFound)
	})
	time.Sleep(300 * time.Millisecond)
	if n := ended.Load(); n != 0 {
		t.Fatalf("Ended ran %d times before the caller read the answer", n)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !resp.Close {
		t.Fatalf("the answer: %d close=%v %s", resp.StatusCode, resp.Close, body)
	}
	time.Sleep(100 * time.Millisecond)
	if n := ended.Load(); n != 0 {
		t.Fatalf("Ended ran %d times before the caller closed the connection", n)
	}
	conn.Close()
	waitFor(t, "Ended did not run after the caller closed the connection", func() bool { return ended.Load() == 1 })

	// A caller that keeps the connection open delays Ended by AnswerWait
	// at the most.
	srv2 := &control.Server{DB: db, Self: srv.Self, Log: srv.Log, Ended: srv.Ended, AnswerWait: 200 * time.Millisecond}
	c2, err := net.Dial("tcp", removeServer(t, srv2, control.Caller{Peer: carol.NodeID(), Role: state.RolePeer}))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, err := io.WriteString(c2, rawRemove); err != nil {
		t.Fatal(err)
	}
	c2.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err = http.ReadResponse(bufio.NewReader(c2), nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Carol's answer: %v %v", resp, err)
	}
	waitFor(t, "Ended did not run after AnswerWait", func() bool { return ended.Load() == 2 })
	// The connection was closed by then.
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("the connection is still open after AnswerWait")
	}
}

// #84: a caller tells a call that never reached the peer
// (ErrNotConnected: unreachable, or its key refused at connect) from one
// whose request was sent whole and got no answer (ErrNoAnswer); an
// answer, a refusal included, is neither.
func TestClientTellsNoConnectionFromNoAnswer(t *testing.T) {
	refused := &control.Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("tunnel: no handshake")
		}}}}
	err := refused.Remove(t.Context())
	if !errors.Is(err, control.ErrNotConnected) || errors.Is(err, control.ErrNoAnswer) {
		t.Fatalf("a dial that failed: %v", err)
	}
	if !strings.Contains(err.Error(), "tunnel: no handshake") {
		t.Fatalf("the error lost its cause: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { // reads the request and never answers
				defer c.Close()
				io.Copy(io.Discard, c)
			}()
		}
	}()
	silent := &control.Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
		}}}}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	err = silent.Remove(ctx)
	if !errors.Is(err, control.ErrNoAnswer) || errors.Is(err, control.ErrNotConnected) {
		t.Fatalf("a request sent and never answered: %v", err)
	}

	l := newLender(t)
	err = l.client(t, control.Caller{Peer: newID(t).NodeID(), Role: state.RoleInvite}).Remove(t.Context())
	if errors.Is(err, control.ErrNoAnswer) || errors.Is(err, control.ErrNotConnected) {
		t.Fatalf("a refusal: %v", err)
	}
}
