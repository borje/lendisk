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
