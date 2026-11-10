package verify

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// lender is a friend as the verifier sees it: what it holds of the
// node's bucket, by key, and the challenges it was sent.
type lender struct {
	mu    sync.Mutex
	held  map[string][]byte
	asked []control.ChallengeRequest
	fail  error // answered instead, when set
	// during, when set, runs inside each call before it is answered:
	// what happens on the node and at the friend while a challenge is
	// in flight.
	during func(req control.ChallengeRequest)
}

func (l *lender) call(_ context.Context, _ state.Friendship, req control.ChallengeRequest) (control.ChallengeResponse, error) {
	if l.during != nil {
		l.during(req)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked = append(l.asked, req)
	if l.fail != nil {
		return control.ChallengeResponse{}, l.fail
	}
	b, ok := l.held[req.Key]
	if !ok {
		return control.ChallengeResponse{}, &control.CallError{Status: 404, Code: control.CodeNoSuchKey}
	}
	if !req.Whole {
		if req.Offset >= int64(len(b)) {
			return control.ChallengeResponse{}, &control.CallError{Status: 416, Code: control.CodeInvalidRange}
		}
		b = b[req.Offset:min(req.Offset+req.Length, int64(len(b)))]
	}
	return control.ChallengeResponse{HMAC: control.ChallengeMAC(req.Nonce, b), Size: int64(len(b))}, nil
}

func (l *lender) requests() []control.ChallengeRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]control.ChallengeRequest{}, l.asked...)
}

type node struct {
	t      *testing.T
	db     *state.DB
	store  string
	self   *identity.Identity
	now    time.Time
	mu     sync.Mutex
	b      *lender
	v      *Verifier
	alerts atomic.Int32
}

const peerB = "B"

// newNode is a node with one friend, B, holding the objects of bucket
// laptop it acknowledged, each also in the node's store.
func newNode(t *testing.T, objects int) *node {
	t.Helper()
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p, _ := identity.NewPhrase()
	self, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	n := &node{t: t, db: db, store: t.TempDir(), self: self, now: time.Unix(1_800_000_000, 0),
		b: &lender{held: map[string][]byte{}}}
	db.Clock = n.clock
	if err := db.AddFriendship(ctx, state.Friendship{Peer: peerB, Address: "tcB", OwnNumber: 1 << 30, PeerNumber: 1 << 30,
		CreatedAt: n.now}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 1, AccessKey: "AK", SecretKey: "s",
		CreatedAt: n.now}); err != nil {
		t.Fatal(err)
	}
	for i := range objects {
		body := make([]byte, 1000+i*100)
		rand.Read(body)
		n.put("data/"+string(rune('a'+i)), body)
	}
	n.v = &Verifier{DB: db, StoreDir: n.store, Self: self, Call: n.b.call, Now: n.clock, Log: slog.New(slog.DiscardHandler),
		Alerted: func() { n.alerts.Add(1) }}
	return n
}

func (n *node) clock() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

func (n *node) advance(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.now = n.now.Add(d)
}

// put writes key to the store, captures it, and has B acknowledge and
// hold it.
func (n *node) put(key string, body []byte) {
	n.t.Helper()
	ctx := n.t.Context()
	p := filepath.Join(n.store, "laptop", filepath.FromSlash(key))
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, body, 0o600); err != nil {
		n.t.Fatal(err)
	}
	seqs, err := n.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpPut, Class: "data"}})
	if err != nil {
		n.t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	h := hex.EncodeToString(sum[:])
	if _, err := n.db.FinishPut(ctx, seqs[0], int64(len(body)), h); err != nil {
		n.t.Fatal(err)
	}
	if err := n.db.AckPlacement(ctx, peerB, seqs[0], h, n.now); err != nil {
		n.t.Fatal(err)
	}
	n.b.held[key] = bytes.Clone(body)
}

func (n *node) check(full bool) state.VerifyOutcome {
	n.t.Helper()
	return n.v.Check(n.t.Context(), peerB, "laptop", full)
}

func (n *node) record() state.ReplicaCheck {
	n.t.Helper()
	cs, err := n.db.ReplicaChecks(n.t.Context())
	if err != nil || len(cs) != 1 {
		n.t.Fatalf("checks %+v %v", cs, err)
	}
	return cs[0]
}

// VER-1 (#27): a challenge names the bucket by its sealed name (SEC-16),
// a random object of those B acknowledged, a random range within it of
// at most Range bytes and a fresh nonce; B's HMAC, checked against the
// node's own copy, passes and is recorded as verified. Up to Samples
// distinct objects are challenged.
func TestAChallengePassesAgainstTheLocalCopy(t *testing.T) {
	n := newNode(t, 12)
	n.v.Samples = 5
	o := n.check(false)
	if o.Outcome != state.VerifyPassed || o.Checked != 5 {
		t.Fatalf("outcome %+v", o)
	}
	rs := n.b.requests()
	if len(rs) != 5 {
		t.Fatalf("%d challenges", len(rs))
	}
	keys, nonces := map[string]bool{}, map[string]bool{}
	for _, r := range rs {
		if r.Bucket != n.self.SealedName("laptop") || r.Whole || r.Length < 1 || r.Length > DefaultRange ||
			len(r.Nonce) != 32 || r.Offset < 0 || r.Offset+r.Length > int64(len(n.b.held[r.Key])) {
			t.Fatalf("challenge %+v", r)
		}
		keys[r.Key], nonces[string(r.Nonce)] = true, true
	}
	if len(keys) != 5 || len(nonces) != 5 {
		t.Fatalf("keys %v, %d nonces: not distinct", keys, len(nonces))
	}
	if c := n.record(); c.VerifiedAt != n.now || c.CheckedAt != n.now || c.Unverified() || n.alerts.Load() != 0 {
		t.Fatalf("record %+v", c)
	}
}

// VER-1 (#27): a friend that no longer holds an object it acknowledged,
// or answers for other bytes, fails: the replica is marked unverified
// and the alert service told at once.
func TestAFailedChallengeMarksTheReplicaUnverified(t *testing.T) {
	for name, spoil := range map[string]func(b *lender){
		"lost": func(b *lender) { clear(b.held) },
		"changed": func(b *lender) {
			for k := range b.held {
				b.held[k] = bytes.Repeat([]byte{1}, len(b.held[k]))
			}
		},
		"shorter": func(b *lender) {
			for k := range b.held {
				b.held[k] = b.held[k][:1]
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			n := newNode(t, 3)
			spoil(n.b)
			o := n.check(false)
			if o.Outcome != state.VerifyFailed || o.Detail == "" {
				t.Fatalf("outcome %+v", o)
			}
			if c := n.record(); !c.Unverified() || c.UnverifiedAt != n.now || c.Detail != o.Detail || !c.VerifiedAt.IsZero() {
				t.Fatalf("record %+v", c)
			}
			if n.alerts.Load() != 1 {
				t.Fatalf("Alerted %d times", n.alerts.Load())
			}
		})
	}
}

// VER-1 (#27): a challenge that gets no answer (a network error, a
// refusal, a node without the call) decides nothing: it is recorded as
// an error, retried within the hour, and never counted as a failure.
func TestATransientErrorIsNoFailedProof(t *testing.T) {
	for name, err := range map[string]error{
		"network":   errors.New("connection reset by peer"),
		"busy":      &control.CallError{Status: 503, RetryAfter: time.Hour},
		"no call":   &control.CallError{Status: 404, Message: "no such control API call on this node"},
		"lender ko": &control.CallError{Status: 500},
	} {
		t.Run(name, func(t *testing.T) {
			n := newNode(t, 3)
			n.b.fail = err
			o := n.check(false)
			if o.Outcome != state.VerifyError {
				t.Fatalf("outcome %+v", o)
			}
			c := n.record()
			if c.Unverified() || c.Error == "" || c.ErrorAt != n.now || !c.CheckedAt.IsZero() || n.alerts.Load() != 0 {
				t.Fatalf("record %+v", c)
			}
		})
	}
}

// VER-1, VER-5 (#27): when the friend's answer disagrees with the local
// copy and the local copy no longer matches its capture hash, the fault
// is this node's own store (bit rot), not the friend's: no failure. An
// object with no local copy (a recovered node, EP-10) cannot be checked
// and is skipped; with nothing checkable the check decides nothing.
func TestTheLocalCopysOwnFaultsAreNotTheFriends(t *testing.T) {
	n := newNode(t, 2)
	for k := range n.b.held {
		p := filepath.Join(n.store, "laptop", k)
		if k == "data/a" {
			os.WriteFile(p, bytes.Repeat([]byte{9}, len(n.b.held[k])), 0o600)
		} else {
			os.Remove(p)
		}
	}
	o := n.check(false)
	if o.Outcome != state.VerifyNothing || o.Checked != 0 || o.Skipped != 2 {
		t.Fatalf("outcome %+v", o)
	}
	if c := n.record(); c.Unverified() || n.alerts.Load() != 0 {
		t.Fatalf("record %+v", c)
	}
}

// VER-1, ID-5 (#71): recovery trusts a friend's list as it trusts an
// acknowledgement, so a friend that deleted an object yet still lists
// it counts as holding it. The challenge is the backstop: an object
// recovery recorded acked at B is challenged like any other once the
// read-through has fetched it (from another friend), and B's answer
// that it does not hold it marks B's replica unverified, so
// re-placement moves its copies. Not yet fetched, it is skipped.
func TestARecoveredAckIsChallenged(t *testing.T) {
	ctx := t.Context()
	n := newNode(t, 0)
	body := []byte("an object B listed after deleting it")
	sum := sha256.Sum256(body)
	h := hex.EncodeToString(sum[:])
	const key = "data/recovered"
	if err := n.db.AddRelists(ctx, []string{peerB, "C"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	for _, place := range []string{peerB, "C"} {
		if _, err := n.db.RecordListed(ctx, place, "laptop", []state.Listed{{Key: key, Size: int64(len(body)), SHA256: h,
			Class: "data"}}, 1, "", true); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.db.SettleFinished(ctx); err != nil {
		t.Fatal(err)
	}
	if o := n.check(false); o.Outcome != state.VerifyNothing || o.Skipped != 1 {
		t.Fatalf("before the read-through fetched it: %+v", o)
	}
	p := filepath.Join(n.store, "laptop", filepath.FromSlash(key))
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, body, 0o600); err != nil {
		t.Fatal(err)
	}
	o := n.check(false)
	if o.Outcome != state.VerifyFailed || o.Checked != 1 || !strings.Contains(o.Detail, key) {
		t.Fatalf("outcome %+v", o)
	}
	if c := n.record(); !c.Unverified() || n.alerts.Load() != 1 {
		t.Fatalf("record %+v", c)
	}
}

// VER-3 (#27): a full check asks for every object B acknowledged, whole,
// each with its own nonce, and compares B's HMAC with the same computed
// over the local copy: no object's bytes cross the tunnel. Passing, it
// records a full verification and clears an unverified mark; one object
// lost fails it.
func TestAFullCheckCoversEveryObject(t *testing.T) {
	n := newNode(t, 7)
	if err := n.db.RecordUnverified(t.Context(), peerB, "laptop", "earlier", n.now); err != nil {
		t.Fatal(err)
	}
	n.advance(time.Hour)
	o := n.check(true)
	if o.Outcome != state.VerifyPassed || o.Checked != 7 || o.Skipped != 0 {
		t.Fatalf("outcome %+v", o)
	}
	rs := n.b.requests()
	seen := map[string]bool{}
	for _, r := range rs {
		if !r.Whole || r.Offset != 0 || r.Length != 0 {
			t.Fatalf("challenge %+v", r)
		}
		seen[r.Key] = true
	}
	if len(rs) != 7 || len(seen) != 7 {
		t.Fatalf("%d challenges of %d keys", len(rs), len(seen))
	}
	if c := n.record(); c.Unverified() || c.FullAt != n.now || c.VerifiedAt != n.now {
		t.Fatalf("record %+v", c)
	}
	delete(n.b.held, "data/d")
	if o := n.check(true); o.Outcome != state.VerifyFailed || !strings.Contains(o.Detail, "data/d") {
		t.Fatalf("outcome %+v", o)
	}
}

// VER-1, VER-3 (#27): Run challenges each replica when it is due, daily
// on the injected clock, and runs the checks `lendisk verify` asks for
// through lendisk.db, recording their outcome there.
func TestRunChallengesDailyAndOnRequest(t *testing.T) {
	n := newNode(t, 3)
	n.v.Poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); n.v.Run(ctx) }()
	defer func() { cancel(); <-done }()
	eventually := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("%s: never", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	// The daily challenge's ranged requests only: the rolling full
	// check (VER-1a, #105) adds whole-object ones on its own schedule.
	ranged := func() int {
		c := 0
		for _, r := range n.b.requests() {
			if !r.Whole {
				c++
			}
		}
		return c
	}
	eventually("the first challenge", func() bool { return ranged() > 0 && !n.record().VerifiedAt.IsZero() })
	first := ranged()
	n.advance(23 * time.Hour)
	n.v.Kick()
	time.Sleep(50 * time.Millisecond)
	if got := ranged(); got != first {
		t.Fatalf("challenged again within the day: %d, then %d", first, got)
	}
	n.advance(time.Hour)
	eventually("the next day's challenge", func() bool { return ranged() > first })

	id, err := n.db.RequestVerify(t.Context(), peerB, "laptop", true, n.now)
	if err != nil {
		t.Fatal(err)
	}
	n.v.Kick()
	var o state.VerifyOutcome
	eventually("the requested full check", func() bool {
		var done bool
		o, done, err = n.db.VerifyResult(t.Context(), id)
		return err == nil && done
	})
	if o.Outcome != state.VerifyPassed || o.Checked != 3 {
		t.Fatalf("outcome %+v", o)
	}
}

// VER-1 (#27 review): an object that stops being the one the friend
// holds while its challenge is in flight (the user deletes or rewrites
// it, a prune's delete reaches the friend, re-placement drops the copy
// there) is no failed proof, though the friend truthfully answers that
// it no longer holds those bytes: the row is read again before a
// failure is recorded, and the object is skipped.
func TestAnObjectThatChangesMidChallengeIsSkipped(t *testing.T) {
	for name, change := range map[string]func(n *node, key string){
		"deleted": func(n *node, key string) {
			ctx := n.t.Context()
			seqs, err := n.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpDelete, Class: "data"}})
			if err != nil {
				n.t.Fatal(err)
			}
			if err := n.db.FinishDelete(ctx, seqs...); err != nil {
				n.t.Fatal(err)
			}
			os.Remove(filepath.Join(n.store, "laptop", filepath.FromSlash(key)))
			delete(n.b.held, key)
		},
		"rewritten": func(n *node, key string) {
			ctx := n.t.Context()
			body := bytes.Repeat([]byte{5}, 900)
			os.WriteFile(filepath.Join(n.store, "laptop", filepath.FromSlash(key)), body, 0o600)
			seqs, err := n.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpPut, Class: "data"}})
			if err != nil {
				n.t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			if _, err := n.db.FinishPut(ctx, seqs[0], int64(len(body)), hex.EncodeToString(sum[:])); err != nil {
				n.t.Fatal(err)
			}
		},
		"dropped there": func(n *node, key string) {
			if _, err := n.db.Exec(`UPDATE placement SET state = 'dropping' WHERE place = ? AND key = ?`, peerB, key); err != nil {
				n.t.Fatal(err)
			}
			delete(n.b.held, key)
		},
	} {
		for _, full := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, full %v", name, full), func(t *testing.T) {
				n := newNode(t, 1)
				n.b.during = func(req control.ChallengeRequest) {
					n.b.during = nil
					change(n, req.Key)
				}
				o := n.check(full)
				if o.Outcome == state.VerifyFailed || o.Skipped != 1 || o.Checked != 0 {
					t.Fatalf("outcome %+v", o)
				}
				if c := n.record(); c.Unverified() || n.alerts.Load() != 0 {
					t.Fatalf("record %+v", c)
				}
			})
		}
	}
}
