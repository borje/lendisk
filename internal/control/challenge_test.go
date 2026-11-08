package control_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// held is a lender's copies of borrowers' objects, by borrower, sealed
// name and key, recording whom each read was for; block, when set, holds
// each read until it is closed.
type held struct {
	mu    sync.Mutex
	objs  map[identity.NodeID]map[string]map[string][]byte
	asked []identity.NodeID
	block chan struct{}
	start chan struct{}
}

func (h *held) Read(_ context.Context, borrower identity.NodeID, sealed, key string, offset, length int64) (io.ReadCloser, error) {
	h.mu.Lock()
	h.asked = append(h.asked, borrower)
	h.mu.Unlock()
	if h.start != nil {
		h.start <- struct{}{}
	}
	if h.block != nil {
		<-h.block
	}
	b, ok := h.objs[borrower][sealed]
	if !ok {
		return nil, gateway.ErrNoLentBucket
	}
	o, ok := b[key]
	if !ok {
		return nil, gateway.ErrNoLentKey
	}
	if length < 0 {
		return io.NopCloser(bytes.NewReader(o)), nil
	}
	if offset >= int64(len(o)) {
		return nil, gateway.ErrBadRange
	}
	return io.NopCloser(bytes.NewReader(o[offset:min(offset+length, int64(len(o)))])), nil
}

func mac(nonce, b []byte) []byte {
	m := hmac.New(sha256.New, nonce)
	m.Write(b)
	return m.Sum(nil)
}

func callCode(err error) (int, string) {
	var ce *control.CallError
	if !errors.As(err, &ce) {
		return 0, ""
	}
	return ce.Status, ce.Code
}

// VER-1, NET-7 (#27): the challenge names the caller's own bucket by
// sealed name, a key, a byte range and a fresh nonce; the lender reads
// that range of what it holds and answers HMAC-SHA256 keyed with the
// nonce over it, or over the whole object for a full check. It answers
// for the caller alone: a friend naming another's bucket finds none. A
// key it does not hold, or a range past its end, is said so by code, the
// answer a borrower counts as a failed proof. Ranges are bounded; an
// invite key is refused.
func TestChallengeAnswersAnHMACOverTheHeldBytes(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	bob, carol := newID(t), newID(t)
	obj := bytes.Repeat([]byte("0123456789"), 1000)
	sealed := bob.SealedName("laptop")
	h := &held{objs: map[identity.NodeID]map[string]map[string][]byte{bob.NodeID(): {sealed: {"data/ab/x": obj}}}}
	l.srv.Held = h
	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	asCarol := l.client(t, control.Caller{Peer: carol.NodeID(), Role: state.RolePeer})
	nonce := bytes.Repeat([]byte{7}, 32)

	got, err := asBob.Challenge(ctx, control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Offset: 100, Length: 500})
	if err != nil {
		t.Fatal(err)
	}
	if !hmac.Equal(got.HMAC, mac(nonce, obj[100:600])) || got.Size != 500 {
		t.Fatalf("ranged answer %+v", got)
	}
	got, err = asBob.Challenge(ctx, control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Whole: true})
	if err != nil {
		t.Fatal(err)
	}
	if !hmac.Equal(got.HMAC, mac(nonce, obj)) || got.Size != int64(len(obj)) {
		t.Fatalf("whole answer %+v", got)
	}
	if want := control.ChallengeMAC(nonce, obj); !hmac.Equal(want, got.HMAC) {
		t.Fatal("ChallengeMAC differs from the lender's answer")
	}

	// Answered for the caller alone.
	if _, err := asCarol.Challenge(ctx, control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Length: 10}); err == nil {
		t.Fatal("Carol challenged Bob's bucket")
	} else if s, c := callCode(err); s != http.StatusNotFound || c != control.CodeNoSuchBucket {
		t.Fatalf("Carol naming Bob's bucket: %v", err)
	}
	if h.asked[len(h.asked)-1] != carol.NodeID() {
		t.Fatalf("read for %v", h.asked)
	}
	for name, c := range map[string]struct {
		req          control.ChallengeRequest
		status       int
		code         string
		failureProof bool
	}{
		"a key not held":   {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/y", Nonce: nonce, Length: 1}, 404, control.CodeNoSuchKey, true},
		"past its end":     {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Offset: 1 << 20, Length: 1}, 416, control.CodeInvalidRange, true},
		"a range too long": {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Length: control.MaxChallengeRange + 1}, 400, "", false},
		"an empty range":   {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce}, 400, "", false},
		"a negative offset": {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Offset: -1, Length: 1},
			400, "", false},
		"a short nonce":       {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce[:8], Length: 1}, 400, "", false},
		"a whole with range":  {control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Whole: true, Length: 1}, 400, "", false},
		"a reserved key":      {control.ChallengeRequest{Bucket: sealed, Key: ".lendisk/tomb/x", Nonce: nonce, Length: 1}, 400, "", false},
		"no key":              {control.ChallengeRequest{Bucket: sealed, Nonce: nonce, Length: 1}, 400, "", false},
		"a bucket not sealed": {control.ChallengeRequest{Bucket: "laptop", Key: "data/ab/x", Nonce: nonce, Length: 1}, 404, control.CodeNoSuchBucket, true},
	} {
		_, err := asBob.Challenge(ctx, c.req)
		if s, code := callCode(err); s != c.status || code != c.code {
			t.Errorf("%s: %v, want %d %q", name, err, c.status, c.code)
		}
		if control.FailedProof(err) != c.failureProof {
			t.Errorf("%s: FailedProof is %v", name, !c.failureProof)
		}
	}
	asInvite := l.client(t, control.Caller{Peer: newID(t).NodeID(), Role: state.RoleInvite})
	if _, err := asInvite.Challenge(ctx, control.ChallengeRequest{Bucket: sealed, Key: "data/ab/x", Nonce: nonce, Length: 1}); !isStatus(err, http.StatusForbidden) {
		t.Fatalf("an invite key: %v", err)
	}
	resp, err := asBob.Do(ctx, http.MethodGet, control.ChallengePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", resp.StatusCode)
	}
	resp, err = asBob.Do(ctx, http.MethodPost, control.ChallengePath, strings.NewReader("not json"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("junk: %d", resp.StatusCode)
	}
	// A network error or a refusal is no failed proof.
	if control.FailedProof(errors.New("connection reset")) || control.FailedProof(&control.CallError{Status: 503}) ||
		control.FailedProof(&control.CallError{Status: 404}) {
		t.Fatal("a transient error counts as a failed proof")
	}
}

// VER-1, LEND-5 (#27 review): a hostile borrower cannot make the lender
// read without bound. One challenge per friend is read at a time, and
// what a friend's challenges read is paced: a burst, then a rate, each
// call counting at least a minimum. A call over the pace is answered 503
// with how long to wait, reading nothing, while other friends are
// served.
func TestChallengesArePacedPerFriend(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	bob, carol := newID(t), newID(t)
	obj := make([]byte, 5000)
	h := &held{objs: map[identity.NodeID]map[string]map[string][]byte{
		bob.NodeID():   {bob.SealedName("laptop"): {"k": obj}},
		carol.NodeID(): {carol.SealedName("laptop"): {"k": obj}},
	}}
	now := time.Unix(1_800_000_000, 0)
	var mu sync.Mutex
	l.srv.Held, l.srv.ChallengeRate, l.srv.ChallengeBurst = h, 1000, 1000
	l.srv.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	asCarol := l.client(t, control.Caller{Peer: carol.NodeID(), Role: state.RolePeer})
	nonce := bytes.Repeat([]byte{1}, 16)
	whole := func(c *control.Client, who *identity.Identity) error {
		_, err := c.Challenge(ctx, control.ChallengeRequest{Bucket: who.SealedName("laptop"), Key: "k", Nonce: nonce, Whole: true})
		return err
	}
	if err := whole(asBob, bob); err != nil {
		t.Fatal(err)
	}
	reads := len(h.asked)
	err := whole(asBob, bob)
	var ce *control.CallError
	if !errors.As(err, &ce) || ce.Status != http.StatusServiceUnavailable || ce.RetryAfter != 4*time.Second {
		t.Fatalf("a second call over the pace: %v (%+v)", err, ce)
	}
	if len(h.asked) != reads {
		t.Fatal("a refused call read the store")
	}
	if err := whole(asCarol, carol); err != nil {
		t.Fatalf("Carol paced by Bob's calls: %v", err)
	}
	mu.Lock()
	now = now.Add(4 * time.Second)
	mu.Unlock()
	if err := whole(asBob, bob); err != nil {
		t.Fatalf("after the wait: %v", err)
	}

	// One at a time per friend.
	l2 := newLender(t)
	h2 := &held{objs: h.objs, block: make(chan struct{}), start: make(chan struct{}, 1)}
	l2.srv.Held = h2
	asBob2 := l2.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	done := make(chan error, 1)
	go func() { done <- whole(asBob2, bob) }()
	<-h2.start
	h2.start = nil
	if err := whole(asBob2, bob); !isStatus(err, http.StatusServiceUnavailable) {
		t.Fatalf("a second call while the first reads: %v", err)
	}
	close(h2.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
