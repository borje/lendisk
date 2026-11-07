package control_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// index is a lender's index of two borrowers' buckets, by borrower and
// sealed name, recording who each call was answered for.
type index struct {
	held  map[identity.NodeID]map[string][]gateway.LentObject
	asked []identity.NodeID
}

func (x *index) List(_ context.Context, borrower identity.NodeID, sealed, after string, limit int) ([]gateway.LentObject, string, error) {
	x.asked = append(x.asked, borrower)
	objs, ok := x.held[borrower][sealed]
	if !ok {
		return nil, "", gateway.ErrNoLentBucket
	}
	var out []gateway.LentObject
	for _, o := range objs {
		if o.Key > after && len(out) < limit {
			out = append(out, o)
		}
	}
	next := ""
	if len(out) == limit && out[len(out)-1].Key != objs[len(objs)-1].Key {
		next = out[len(out)-1].Key
	}
	return out, next, nil
}

// NET-7 list, ID-5 (#29): a friend pages its own bucket at a lender
// with nothing but its key: each key's size and hash. The call is keyed
// by the resolved caller alone, so a friend naming another friend's
// bucket, by sealed name or by a parameter, finds no bucket; an invite
// key may not call it.
func TestListCallPagesCallersOwnBucketOnly(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	a, b := newID(t), newID(t)
	objs := []gateway.LentObject{{Key: "config", Size: 155, SHA256: "aa"}, {Key: "data/01/x", Size: 9, SHA256: "bb"},
		{Key: "index/1", Size: 3, SHA256: "cc"}}
	x := &index{held: map[identity.NodeID]map[string][]gateway.LentObject{a.NodeID(): {a.SealedName("laptop"): objs}}}
	l.srv.Lent = x
	ca := l.client(t, control.Caller{Peer: a.NodeID(), Role: state.RolePeer})
	cb := l.client(t, control.Caller{Peer: b.NodeID(), Role: state.RolePeer})

	var got []control.ListObject
	after := ""
	for {
		p, err := ca.ListPage(ctx, a.SealedName("laptop"), after, 2)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, p.Objects...)
		if p.Next == "" {
			break
		}
		after = p.Next
	}
	if len(got) != 3 || got[0].Key != "config" || got[0].Size != 155 || got[0].SHA256 != "aa" || got[2].Key != "index/1" ||
		got[0].TombstonedAt != nil {
		t.Fatalf("listed %+v", got)
	}

	if _, err := cb.ListPage(ctx, a.SealedName("laptop"), "", 10); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("B listed A's bucket: %v", err)
	}
	// No parameter names whose bucket.
	resp, err := cb.Do(ctx, http.MethodGet, control.ListPath+"?"+url.Values{"bucket": {a.SealedName("laptop")},
		"peer": {a.NodeID().String()}, "borrower": {a.NodeID().String()}}.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("B naming A: %d", resp.StatusCode)
	}
	for _, who := range x.asked[len(x.asked)-2:] {
		if who != b.NodeID() {
			t.Fatalf("answered B's call for %s", who)
		}
	}

	inv := l.client(t, control.Caller{Peer: newID(t).NodeID(), Role: state.RoleInvite})
	if _, err := inv.ListPage(ctx, a.SealedName("laptop"), "", 10); !isStatus(err, http.StatusForbidden) {
		t.Fatalf("an invite key listed: %v", err)
	}
	if _, err := ca.ListPage(ctx, a.SealedName("laptop"), "", 5000); !isStatus(err, http.StatusBadRequest) {
		t.Fatalf("an oversized page: %v", err)
	}
	// #29 review: "no such bucket of yours" has its own code; any other
	// 404 (a node lending nothing, an older node without the call) does
	// not, so a borrower does not read it as "holds nothing".
	var ce *control.CallError
	if _, err := cb.ListPage(ctx, a.SealedName("laptop"), "", 10); !errors.As(err, &ce) || ce.Code != control.CodeNoSuchBucket {
		t.Fatalf("no such bucket: %v", err)
	}
	l.srv.Lent = nil
	if _, err := ca.ListPage(ctx, a.SealedName("laptop"), "", 10); !errors.As(err, &ce) || ce.Status != http.StatusNotFound ||
		ce.Code == control.CodeNoSuchBucket {
		t.Fatalf("a node lending nothing: %v", err)
	}
	resp, err = ca.Do(ctx, http.MethodGet, control.Prefix+"no-such-call", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown call: %d", resp.StatusCode)
	}
}

// #29 review: a list that does not move past the key it was asked to
// start after, or whose keys do not rise above it, is refused, so a
// hostile lender cannot keep a recovery listing forever.
func TestListPageMustMoveOn(t *testing.T) {
	for name, body := range map[string]string{
		"next not past after": `{"objects":[],"next":"m"}`,
		"next before after":   `{"objects":[],"next":"a"}`,
		"key not past after":  `{"objects":[{"key":"m","size":1,"sha256":"x"}]}`,
		"keys not rising":     `{"objects":[{"key":"q","size":1,"sha256":"x"},{"key":"p","size":1,"sha256":"x"}]}`,
		"repeated key":        `{"objects":[{"key":"q","size":1,"sha256":"x"},{"key":"q","size":1,"sha256":"x"}]}`,
		"next behind keys":    `{"objects":[{"key":"q","size":1,"sha256":"x"}],"next":"p"}`,
		"page too long":       oversized(),
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(control.ProtocolHeader, control.Protocol)
			w.Write([]byte(body))
		}))
		c := &control.Client{HTTP: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", ts.Listener.Addr().String())
			},
		}}}
		if _, err := c.ListPage(t.Context(), "s", "m", 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
		ts.Close()
	}
}

// oversized is a page of one key more than a page may hold, short keys
// rising past "m".
func oversized() string {
	var b strings.Builder
	b.WriteString(`{"objects":[`)
	for i := 0; i <= gateway.MaxListPage; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"key":"n%05d","size":1,"sha256":"x"}`, i)
	}
	b.WriteString(`]}`)
	return b.String()
}
