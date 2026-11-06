package transport

import (
	"context"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

// An invited node dials with a key that is not its identity and has no
// region of its own; such a dialer never listens.
func TestDialerReachesServerAndNeverListens(t *testing.T) {
	d := transporttest.Start(t)
	srvID, inviteKey := newID(t), newID(t)
	srv := newTailcat(t, srvID, d)
	srv.Admit(func(p identity.NodeID) bool { return p == inviteKey.NodeID() })
	l := listen(t, srv)

	dl, err := NewDialer(inviteKey, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dl.Admit(func(identity.NodeID) bool { return true })
	if _, err := dl.Listen(t.Context()); err == nil {
		t.Fatal("a dialer listened")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case sc := <-l.conns:
		defer sc.Close()
		if p, _ := srv.PeerOf(sc); p != inviteKey.NodeID() {
			t.Fatalf("connection names %v", p)
		}
	case <-ctx.Done():
		t.Fatal("no connection accepted")
	}
}

func TestKeyOfAddress(t *testing.T) {
	d := transporttest.Start(t)
	id := newID(t)
	got, err := KeyOf(AddressOf(id, d.Region))
	if err != nil || got != id.NodeID() {
		t.Fatalf("KeyOf = %v, %v", got, err)
	}
	if _, err := KeyOf("tcnotcbor"); err == nil {
		t.Fatal("KeyOf accepted garbage")
	}
}

// A rejection is remembered for DeniedFor only, so a relationship
// written by another process (a one-shot command) is seen without a
// Clear (SEC-15's 60 seconds).
func TestAdmissionRejectionExpires(t *testing.T) {
	k := newID(t).NodeID()
	answer, calls := false, 0
	adm := NewAdmission(func(context.Context, identity.NodeID) (bool, error) {
		calls++
		return answer, nil
	}, quiet)
	now := time.Unix(1_800_000_000, 0)
	adm.now = func() time.Time { return now }
	adm.Admit(k)
	answer = true
	now = now.Add(DeniedFor - time.Second)
	if adm.Admit(k) || calls != 1 {
		t.Fatalf("rejection forgotten early: calls %d", calls)
	}
	now = now.Add(time.Second)
	if !adm.Admit(k) || calls != 2 {
		t.Fatalf("rejection not forgotten after DeniedFor: calls %d", calls)
	}
}

// A friend whose daemon restarts has forgotten every client: tailcat
// admits a client as a WireGuard peer only at its first ping. A failed
// dial drops the cached client, so the next one starts afresh and
// reaches the restarted friend (#18: replication resumes after a
// lender's restart).
func TestDialReachesRestartedServer(t *testing.T) {
	d := transporttest.Start(t)
	srvID, me := newID(t), newID(t)
	serve := func() (*Tailcat, *listener) {
		srv := newTailcat(t, srvID, d)
		srv.Admit(func(identity.NodeID) bool { return true })
		return srv, listen(t, srv)
	}
	srv, l := serve()
	dl, err := NewDialer(me, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()
	dial := func(timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()
		c, err := dl.Dial(ctx, srvID.NodeID(), srv.Address())
		if err == nil {
			c.Close()
		}
		return err
	}
	if err := dial(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	<-l.conns
	srv.Close()
	srv, l = serve()
	// The first dial after the restart may go to the stale session.
	start := time.Now()
	for dial(3*time.Second) != nil {
		if time.Since(start) > 30*time.Second {
			t.Fatal("the restarted server is never reached")
		}
	}
	select {
	case c := <-l.conns:
		c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("no connection accepted after the restart")
	}
}
