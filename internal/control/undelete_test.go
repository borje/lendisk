package control_test

import (
	"context"
	"errors"
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

// tombs is a lender's tombstoned keys of borrowers' buckets, recording
// what each undelete asked; block, when set, holds each call until it is
// closed.
type tombs struct {
	mu    sync.Mutex
	keys  map[identity.NodeID]map[string][]string
	asked []identity.NodeID
	since []time.Time
	block chan struct{}
	start chan struct{}
}

func (u *tombs) Undelete(_ context.Context, borrower identity.NodeID, sealed string, since time.Time, rewrites bool,
	after string, limit int) (gateway.Undeleted, error) {
	u.mu.Lock()
	u.asked = append(u.asked, borrower)
	u.since = append(u.since, since)
	u.mu.Unlock()
	if u.start != nil {
		u.start <- struct{}{}
	}
	if u.block != nil {
		<-u.block
	}
	ks, ok := u.keys[borrower][sealed]
	if !ok {
		return gateway.Undeleted{}, gateway.ErrNoLentBucket
	}
	var out gateway.Undeleted
	for _, k := range ks {
		if k <= after {
			continue
		}
		if len(out.Restored) == limit {
			out.Next = out.Restored[limit-1].Key
			break
		}
		out.Restored = append(out.Restored, gateway.LentObject{Key: k, Size: 1, SHA256: strings.Repeat("a", 64), TombstonedAt: since})
	}
	return out, nil
}

// RET-3, NET-7 (#32): undelete names the caller's own bucket by sealed
// name and a time; the lender brings back that bucket's tombs since then
// and answers what it restored, a page at a time. It answers for the
// caller alone: a friend naming another's bucket finds none. An invite
// key is refused, a malformed request is 400, and a lender whose
// gateway is not serving yet asks for a retry.
func TestUndeleteCallRestoresTheCallersOwnTombs(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	bob, carol := newID(t), newID(t)
	sealed := bob.SealedName("laptop")
	u := &tombs{keys: map[identity.NodeID]map[string][]string{bob.NodeID(): {sealed: {"data/aa/1", "data/bb/2", "snapshots/3"}}}}
	l.srv.Undeleter = u
	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	asCarol := l.client(t, control.Caller{Peer: carol.NodeID(), Role: state.RolePeer})
	since := time.Unix(1_800_000_000, 0)

	page, err := asBob.Undelete(ctx, control.UndeleteRequest{Bucket: sealed, Since: since.Unix(), Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Restored) != 2 || page.Restored[1].Key != "data/bb/2" || page.Next != "data/bb/2" ||
		page.Restored[0].TombstonedAt == nil || !page.Restored[0].TombstonedAt.Equal(since) || !u.since[0].Equal(since) {
		t.Fatalf("first page %+v, asked since %v", page, u.since)
	}
	page, err = asBob.Undelete(ctx, control.UndeleteRequest{Bucket: sealed, Since: since.Unix(), After: page.Next})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Restored) != 1 || page.Restored[0].Key != "snapshots/3" || page.Next != "" {
		t.Fatalf("second page %+v", page)
	}

	// Answered for the caller alone.
	if _, err := asCarol.Undelete(ctx, control.UndeleteRequest{Bucket: sealed}); err == nil {
		t.Fatal("Carol undeleted Bob's bucket")
	} else if s, c := callCode(err); s != http.StatusNotFound || c != control.CodeNoSuchBucket {
		t.Fatalf("Carol naming Bob's bucket: %v", err)
	}
	if u.asked[len(u.asked)-1] != carol.NodeID() {
		t.Fatalf("undeleted for %v", u.asked)
	}
	for name, req := range map[string]control.UndeleteRequest{
		"a negative since": {Bucket: sealed, Since: -1},
		"a page too long":  {Bucket: sealed, Limit: gateway.MaxListPage + 1},
		"a negative page":  {Bucket: sealed, Limit: -1},
		"no bucket":        {},
	} {
		if _, err := asBob.Undelete(ctx, req); !isStatus(err, http.StatusBadRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	asInvite := l.client(t, control.Caller{Peer: newID(t).NodeID(), Role: state.RoleInvite})
	if _, err := asInvite.Undelete(ctx, control.UndeleteRequest{Bucket: sealed}); !isStatus(err, http.StatusForbidden) {
		t.Fatalf("an invite key: %v", err)
	}
	for method, body := range map[string]string{http.MethodGet: "", http.MethodPost: "not json"} {
		var r *strings.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		var resp *http.Response
		if r == nil {
			resp, err = asBob.Do(ctx, method, control.UndeletePath, nil)
		} else {
			resp, err = asBob.Do(ctx, method, control.UndeletePath, r)
		}
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if want := map[string]int{http.MethodGet: 405, http.MethodPost: 400}[method]; resp.StatusCode != want {
			t.Fatalf("%s %q: %d", method, body, resp.StatusCode)
		}
	}

	// A lender that lends nothing, or whose gateway is still starting.
	l.srv.Undeleter = nil
	if _, err := asBob.Undelete(ctx, control.UndeleteRequest{Bucket: sealed}); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("no undeleter: %v", err)
	}
	l.srv.Undeleter = notServing{}
	var ce *control.CallError
	if _, err := asBob.Undelete(ctx, control.UndeleteRequest{Bucket: sealed}); !errors.As(err, &ce) ||
		ce.Status != http.StatusServiceUnavailable || ce.RetryAfter == 0 {
		t.Fatalf("not serving yet: %v", err)
	}
}

type notServing struct{}

func (notServing) Undelete(context.Context, identity.NodeID, string, time.Time, bool, string, int) (gateway.Undeleted, error) {
	return gateway.Undeleted{}, gateway.ErrNotServing
}

// RET-3, LEND-5 (#32): a hostile friend cannot make the lender walk its
// tombstones without bound. One undelete per friend runs at a time, and
// the keys a friend's calls look at are paced: a burst, then a rate,
// each call counting at least a minimum. A call over the pace is answered
// 503 with how long to wait and looks at nothing, while other friends
// are served.
func TestUndeleteIsPacedPerFriend(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	bob, carol := newID(t), newID(t)
	many := make([]string, 30)
	for i := range many {
		many[i] = "data/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	u := &tombs{keys: map[identity.NodeID]map[string][]string{
		bob.NodeID():   {bob.SealedName("laptop"): many},
		carol.NodeID(): {carol.SealedName("laptop"): many},
	}}
	now := time.Unix(1_800_000_000, 0)
	var mu sync.Mutex
	l.srv.Undeleter, l.srv.UndeleteRate, l.srv.UndeleteBurst = u, 10, 20
	l.srv.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	asCarol := l.client(t, control.Caller{Peer: carol.NodeID(), Role: state.RolePeer})
	call := func(c *control.Client, who *identity.Identity) error {
		_, err := c.Undelete(ctx, control.UndeleteRequest{Bucket: who.SealedName("laptop")})
		return err
	}
	if err := call(asBob, bob); err != nil { // 30 keys: 10 over the burst
		t.Fatal(err)
	}
	calls := len(u.asked)
	err := call(asBob, bob)
	var ce *control.CallError
	if !errors.As(err, &ce) || ce.Status != http.StatusServiceUnavailable || ce.RetryAfter != time.Second {
		t.Fatalf("a second call over the pace: %v (%+v)", err, ce)
	}
	if len(u.asked) != calls {
		t.Fatal("a refused call reached the tombs")
	}
	if err := call(asCarol, carol); err != nil {
		t.Fatalf("Carol paced by Bob's calls: %v", err)
	}
	mu.Lock()
	now = now.Add(time.Second)
	mu.Unlock()
	if err := call(asBob, bob); err != nil {
		t.Fatalf("after the wait: %v", err)
	}
	// Calls that restore nothing still count their minimum.
	empty := func() error {
		_, err := asCarol.Undelete(ctx, control.UndeleteRequest{Bucket: carol.SealedName("laptop"), After: "zzz"})
		return err
	}
	mu.Lock()
	now = now.Add(time.Hour)
	mu.Unlock()
	n := 0
	for ; n < 100 && empty() == nil; n++ {
	}
	if n == 100 || n < 2 {
		t.Fatalf("%d empty calls admitted at once", n)
	}

	// One at a time per friend.
	l2 := newLender(t)
	u2 := &tombs{keys: u.keys, block: make(chan struct{}), start: make(chan struct{}, 1)}
	l2.srv.Undeleter = u2
	asBob2 := l2.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	done := make(chan error, 1)
	go func() { done <- call(asBob2, bob) }()
	<-u2.start
	u2.start = nil
	if err := call(asBob2, bob); !isStatus(err, http.StatusServiceUnavailable) {
		t.Fatalf("a second call while the first runs: %v", err)
	}
	close(u2.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// partial is an Undeleter that revives one key and then fails.
type partial struct{}

func (partial) Undelete(_ context.Context, _ identity.NodeID, _ string, since time.Time, _ bool, _ string, _ int) (gateway.Undeleted, error) {
	return gateway.Undeleted{Restored: []gateway.LentObject{{Key: "data/aa/1", Size: 1, SHA256: strings.Repeat("a", 64), TombstonedAt: since}}},
		errors.New("the disk went away")
}

// RET-3 (#32 review): a page that ends in an error after it revived keys
// still answers them, with next at the last key handled: the borrower
// records them and goes on, rather than losing them to a 500.
func TestAnUndeletePageThatFailsLateAnswersWhatItDid(t *testing.T) {
	l := newLender(t)
	bob := newID(t)
	l.srv.Undeleter = partial{}
	asBob := l.client(t, control.Caller{Peer: bob.NodeID(), Role: state.RolePeer})
	page, err := asBob.Undelete(t.Context(), control.UndeleteRequest{Bucket: bob.SealedName("laptop")})
	if err != nil || len(page.Restored) != 1 || page.Next != "data/aa/1" {
		t.Fatalf("%+v %v", page, err)
	}
}
