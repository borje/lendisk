package control_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
)

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// lender serves the control API with a real manifest store; client
// returns a control client whose requests arrive as caller would after
// the tunnel middleware (SEC-11, SEC-12).
type lender struct {
	srv   *control.Server
	store *manifest.Store
}

func newLender(t *testing.T) *lender {
	t.Helper()
	st := &manifest.Store{Dir: manifest.Dir(t.TempDir())}
	return &lender{store: st, srv: &control.Server{Self: newID(t), Manifests: st, Log: slog.New(slog.DiscardHandler)}}
}

func (l *lender) client(t *testing.T, caller control.Caller) *control.Client {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.srv.ServeHTTP(w, r.WithContext(control.WithCaller(r.Context(), caller)))
	}))
	t.Cleanup(ts.Close)
	return &control.Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", ts.Listener.Addr().String())
		},
	}}}
}

// blob is a well-formed manifest envelope sealed by id.
func blob(t *testing.T, id *identity.Identity, buckets ...string) []byte {
	t.Helper()
	m := &manifest.Manifest{Seq: 1}
	for _, b := range buckets {
		m.Buckets = append(m.Buckets, manifest.Bucket{Name: b, Sealed: id.SealedName(b), Copies: 2})
	}
	b, err := manifest.Seal(id, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ID-4, NET-7: a friend stores its manifest with the manifest call and
// fetches it back with nothing but its key; the lender keeps each
// friend's apart, and answers each with its own only.
func TestManifestCallStoresAndReturnsCallersOwn(t *testing.T) {
	ctx := t.Context()
	l := newLender(t)
	a, b := newID(t), newID(t)
	ca := l.client(t, control.Caller{Peer: a.NodeID(), Role: state.RolePeer})
	cb := l.client(t, control.Caller{Peer: b.NodeID(), Role: state.RolePeer})

	if _, err := ca.GetManifest(ctx); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("before any push: %v", err)
	}
	ma := blob(t, a, "laptop")
	rc, err := ca.PutManifest(ctx, ma)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Size != int64(len(ma)) {
		t.Fatalf("receipt %+v for %d bytes", rc, len(ma))
	}
	if got, err := ca.GetManifest(ctx); err != nil || !bytes.Equal(got, ma) {
		t.Fatalf("fetch: %v", err)
	}
	// Another friend sees none, and its push leaves A's alone.
	if _, err := cb.GetManifest(ctx); !isStatus(err, http.StatusNotFound) {
		t.Fatalf("B fetched: %v", err)
	}
	mb := blob(t, b)
	if _, err := cb.PutManifest(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if got, _ := ca.GetManifest(ctx); !bytes.Equal(got, ma) {
		t.Fatal("B's push replaced A's manifest")
	}
	if got, _ := cb.GetManifest(ctx); !bytes.Equal(got, mb) {
		t.Fatal("B's manifest not returned to B")
	}
	// A newer push replaces the older.
	ma2 := blob(t, a, "laptop", "desktop")
	if _, err := ca.PutManifest(ctx, ma2); err != nil {
		t.Fatal(err)
	}
	if got, _ := ca.GetManifest(ctx); !bytes.Equal(got, ma2) {
		t.Fatal("the second push did not replace the first")
	}
	// The lender learns that a manifest exists and its size only.
	stored, _ := l.store.Get(a.NodeID())
	if bytes.Contains(stored, []byte("laptop")) || bytes.Contains(stored, []byte(a.SealedName("laptop"))) {
		t.Fatal("the stored manifest shows a bucket name")
	}
}

// SEC-13: an invite key may not push or fetch a manifest, even if the
// middleware let it through.
func TestManifestCallRefusesInviteKey(t *testing.T) {
	l := newLender(t)
	ik := newID(t)
	c := l.client(t, control.Caller{Peer: ik.NodeID(), Role: state.RoleInvite})
	if _, err := c.PutManifest(t.Context(), blob(t, ik)); !isStatus(err, http.StatusForbidden) {
		t.Fatalf("push with an invite key: %v", err)
	}
	if _, err := c.GetManifest(t.Context()); !isStatus(err, http.StatusForbidden) {
		t.Fatalf("fetch with an invite key: %v", err)
	}
	if _, err := l.store.Get(ik.NodeID()); !errors.Is(err, control.ErrNoManifest) {
		t.Fatalf("stored for an invite key: %v", err)
	}
}

// SEC-5: a friend cannot use the manifest call to store anything else
// or anything large: one envelope of at most MaxManifest bytes each.
func TestManifestCallRefusesOversizedOrForeignBody(t *testing.T) {
	l := newLender(t)
	a := newID(t)
	c := l.client(t, control.Caller{Peer: a.NodeID(), Role: state.RolePeer})
	big := append([]byte("ldm1"), make([]byte, control.MaxManifest)...)
	if _, err := c.PutManifest(t.Context(), big); !isStatus(err, http.StatusRequestEntityTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	for _, body := range [][]byte{nil, []byte("hello, this is not a manifest at all, just bytes"), []byte("ldm1")} {
		if _, err := c.PutManifest(t.Context(), body); !isStatus(err, http.StatusBadRequest) {
			t.Fatalf("body %q: %v", body, err)
		}
	}
	if _, err := l.store.Get(a.NodeID()); !errors.Is(err, control.ErrNoManifest) {
		t.Fatalf("something was stored: %v", err)
	}
	// Methods other than GET and PUT are refused.
	resp, err := c.Do(t.Context(), http.MethodDelete, control.ManifestPath, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE: %d", resp.StatusCode)
	}
}

func isStatus(err error, code int) bool {
	var ce *control.CallError
	return errors.As(err, &ce) && ce.Status == code
}
