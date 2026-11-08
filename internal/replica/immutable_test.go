package replica

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/borje/lendisk/internal/state"
)

// refuser is a place that answers every put with a 412, as a broken or
// hostile lender might, and whose Head fails until failHeads is spent,
// then reports holding other bytes.
type refuser struct {
	*place
	mu        sync.Mutex
	failHeads int
	heads     int
}

func (p *refuser) Put(ctx context.Context, o Object) error { return ErrImmutable }

func (p *refuser) Head(ctx context.Context, bucket, key string) (string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.heads++
	if p.failHeads > 0 {
		p.failHeads--
		return "", false, errors.New("connection reset by peer")
	}
	return hashOf("their " + key), true, nil
}

func (p *refuser) headCount() int { p.mu.Lock(); defer p.mu.Unlock(); return p.heads }

// EP-11, LEND-4 (#22 review): a friend answering 412 to every key is not
// the user's doing. Each refusal is reported as a changed key the friend
// reports (ReportedBy), with the hash it claims, and counted as the
// friend's refusal, so place_refused names it too. A Head that fails
// after the 412 is retried with the network backoff (the place shows
// retrying meanwhile) rather than recorded with no hash.
func TestAFriendRefusingEveryKeyIsNamed(t *testing.T) {
	fx := newFixture(t, "B")
	b := &refuser{place: fx.places["B"], failHeads: 3}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	fx.write("laptop", "data/1", "one")
	fx.write("laptop", "data/2", "two")
	fx.run()
	fx.drained("B")
	if n := b.headCount(); n < 5 {
		t.Fatalf("%d heads: the failed ones were not retried", n)
	}
	cs, err := fx.db.ChangedKeys(t.Context())
	if err != nil || len(cs) != 2 {
		t.Fatalf("changed keys %+v %v", cs, err)
	}
	for _, c := range cs {
		if strings.Join(c.ReportedBy, ",") != "B" || c.Places["B"] != hashOf("their "+c.Key) {
			t.Errorf("changed key %+v: want B's word with its hash", c)
		}
	}
	if r, _ := fx.db.RefusedAtPlaces(t.Context()); r["B"] != 2 {
		t.Fatalf("place_refused counts %v", r)
	}
	if c := fx.condition("B"); c.Condition != state.ConditionOK {
		t.Fatalf("condition %+v", c)
	}
}

// LEND-4, RET-1 (#22 review): a key that is not rewritable, deleted and
// written again with other bytes, is sent marked as replacing the
// version the place acknowledged before its acknowledged delete: the
// lender keeps that version in a tomb and stores other bytes over a
// tomb only so marked. A first write carries no mark.
func TestAPutAfterADeleteNamesTheTombedVersion(t *testing.T) {
	fx := newFixture(t, "B")
	b := &immutablePlace{place: fx.places["B"]}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	fx.run()
	fx.write("files", "doc", "one")
	fx.drained("B")
	fx.remove("files", "doc")
	fx.drained("B")
	fx.write("files", "doc", "two")
	fx.drained("B")
	want := []string{"doc=", "doc=" + hashOf("one")}
	if strings.Join(b.sentMarks(), " ") != strings.Join(want, " ") {
		t.Fatalf("marks %v, want %v", b.sentMarks(), want)
	}
	if h, ok, _ := fx.db.AckedHash(t.Context(), "B", "files", "doc"); !ok || h != hashOf("two") {
		t.Fatalf("acked %q %v", h, ok)
	}
	if cs, _ := fx.db.ChangedKeys(t.Context()); len(cs) != 0 {
		t.Fatalf("changed keys %+v", cs)
	}
}
