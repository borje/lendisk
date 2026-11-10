package seed

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/state"
)

// fakeGateway serves S3 puts on a unix socket, recording each key in
// order and refusing those in refuse with BadDigest.
type fakeGateway struct {
	mu     sync.Mutex
	puts   []string
	refuse map[string]bool
	sock   string
}

func newFakeGateway(t *testing.T, refuse ...string) *fakeGateway {
	t.Helper()
	dir, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	g := &fakeGateway{refuse: map[string]bool{}, sock: filepath.Join(dir, "gw.sock")}
	for _, k := range refuse {
		g.refuse[k] = true
	}
	ln, err := net.Listen("unix", g.sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		if r.Method != http.MethodPut || len(parts) < 2 || parts[1] == "" {
			return // creating the bucket
		}
		key := parts[1]
		g.mu.Lock()
		g.puts = append(g.puts, key)
		refused := g.refuse[key]
		g.mu.Unlock()
		if refused {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>BadDigest</Code><Message>no</Message></Error>`)
			return
		}
		w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
	})}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return g
}

func (g *fakeGateway) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", g.sock)
}

func (g *fakeGateway) put() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.puts)
}

// writeDisk writes files and a manifest from a for b of them, exported
// at at, into dir.
func writeDisk(t *testing.T, dir string, at time.Time, class map[string]string) Manifest {
	t.Helper()
	var objs []Object
	for _, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range class {
			if !yield(k) {
				return
			}
		}
	}) {
		p := filepath.Join(dir, filepath.FromSlash(k))
		os.MkdirAll(filepath.Dir(p), 0o700)
		if err := os.WriteFile(p, []byte(k), 0o600); err != nil {
			t.Fatal(err)
		}
		objs = append(objs, Object{Key: k, Size: int64(len(k)), SHA256: hexSum([]byte(k)), Class: class[k]})
	}
	return Manifest{Format: Format, Layout: LayoutPlain, ExportedAt: at, Objects: objs}
}

var repoClasses = map[string]string{"config": "metadata", "index/1": "metadata", "snapshots/1": "snapshot",
	"data/1": "data", "data/2": "data"}

// REPL-8, REPL-12 (#95): an import puts every data object before any
// metadata, whatever the manifest's order; when a data object is
// refused, no metadata is put (an index naming the missing pack would
// land before it), and the network brings both later.
func TestImportPutsDataBeforeMetadata(t *testing.T) {
	a, b := newID(t), newID(t)
	for _, c := range []struct {
		refuse   []string
		want     []string
		deferred int64
	}{
		{nil, []string{"data/1", "data/2", "config", "index/1", "snapshots/1"}, 0},
		{[]string{"data/1"}, []string{"data/1", "data/2"}, 3},
	} {
		g := newFakeGateway(t, c.refuse...)
		dir := t.TempDir()
		m := writeDisk(t, dir, time.Now(), repoClasses)
		m.Borrower, m.Lender, m.Bucket = a.NodeID().String(), b.NodeID().String(), a.SealedName("laptop")
		im := &Importer{Self: b, DB: openDB(t), Dial: g.dial}
		r, err := im.Import(context.Background(), dir, m)
		if err != nil {
			t.Fatal(err)
		}
		if got := g.put(); !slices.Equal(got, c.want) {
			t.Errorf("refusing %v: put %v; want %v", c.refuse, got, c.want)
		}
		if r.Deferred != c.deferred || len(r.Refused) != len(c.refuse) {
			t.Errorf("refusing %v: %+v", c.refuse, r)
		}
	}
}

// REPL-8 (#95): a disk older than one imported before from the same
// borrower's bucket is refused whole, so it cannot bring back keys the
// borrower has deleted since; the same disk again is not. A key the
// borrower deleted here (a tombstone) is not put from the disk: the
// gateway would revive it (a put of the same hash to a tombstoned key
// is a revival, tomb.go), so the import leaves it to the network.
func TestImportRefusesAnOlderDiskAndDeletedKeys(t *testing.T) {
	ctx := context.Background()
	a, b := newID(t), newID(t)
	db := openDB(t)
	g := newFakeGateway(t)
	im := &Importer{Self: b, DB: db, Dial: g.dial}
	t0 := time.Now().UTC()
	disk := func(at time.Time) (string, Manifest) {
		dir := t.TempDir()
		m := writeDisk(t, dir, at, map[string]string{"data/1": "data", "data/2": "data", "config": "metadata"})
		m.Borrower, m.Lender, m.Bucket = a.NodeID().String(), b.NodeID().String(), a.SealedName("laptop")
		return dir, m
	}
	newDir, newer := disk(t0)
	if _, err := db.AddTombstone(ctx, state.Tombstone{Peer: a.NodeID().String(), Sealed: a.SealedName("laptop"), Key: "data/2",
		Tomb: "t", SHA256: hexSum([]byte("data/2")), Size: 6, DeletedAt: t0, PurgeAfter: t0.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	r, err := im.Import(ctx, newDir, newer)
	if err != nil {
		t.Fatal(err)
	}
	if got := g.put(); !slices.Equal(got, []string{"data/1"}) {
		t.Fatalf("put %v; want data/1 only (data/2 is deleted here, config waits for it)", got)
	}
	if len(r.Refused) != 1 || r.Refused[0].Key != "data/2" || !strings.Contains(r.Refused[0].Reason, "deleted") {
		t.Fatalf("refused %+v", r.Refused)
	}
	if _, err := im.Import(ctx, newDir, newer); err != nil {
		t.Fatalf("the same disk again: %v", err)
	}
	oldDir, older := disk(t0.Add(-time.Hour))
	before := len(g.put())
	if _, err := im.Import(ctx, oldDir, older); !errors.Is(err, state.ErrOlderSeed) || !strings.Contains(err.Error(), "deleted since") {
		t.Fatalf("an older disk: %v", err)
	}
	if len(g.put()) != before {
		t.Fatal("an older disk put objects")
	}
}

// REPL-8 (#95): rows the export skipped (a key whose copy changed while
// it ran) do not keep the seed from ending confirmed; they are sent by
// the network after. The manifest gives each object's class.
func TestASeedWhoseExportSkippedRowsEndsConfirmed(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := t.TempDir()
	a, b := newID(t), newID(t)
	bKey := b.NodeID().String()
	befriend(t, db, b.NodeID())
	put(t, db, store, "laptop", "config", []byte("config"))
	put(t, db, store, "laptop", "data/1", []byte("data/1"))
	put(t, db, store, "laptop", "data/2", []byte("data/2"))
	os.WriteFile(filepath.Join(store, "laptop", "data", "2"), []byte("changed"), 0o600)
	dst := t.TempDir()
	if r, err := (&Exporter{DB: db, Self: a, StoreDir: store}).Export(ctx, "laptop", b.NodeID(), false, dst); err != nil || r.Changed != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	m, err := ReadManifest(dst, b)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range m.Objects {
		if want := map[string]string{"config": "metadata", "data/1": "data"}[o.Key]; o.Class != want {
			t.Errorf("%s has class %q; want %q", o.Key, o.Class, want)
		}
	}
	c := &Confirmer{DB: db, Self: a, Page: pages([]control.ListObject{
		{Key: "config", SHA256: hexSum([]byte("config"))}, {Key: "data/1", SHA256: hexSum([]byte("data/1"))}})}
	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetSeed(ctx, bKey, "laptop"); s.Outcome != state.SeedConfirmed {
		t.Fatalf("seed %+v; want confirmed", s)
	}
	if p, ok, _ := db.NextPlacement(ctx, bKey); !ok || p.Key != "data/2" {
		t.Fatalf("next for B: %+v %v; want the skipped data/2", p, ok)
	}
}

// The manifest's class is additive to format 1: a manifest without it
// reads, and its objects count as data.
func TestAManifestWithoutClassesReads(t *testing.T) {
	var o Object
	if err := json.Unmarshal([]byte(`{"key":"k","size":1,"sha256":"x"}`), &o); err != nil || o.Class != "" || o.meta() {
		t.Fatalf("%+v %v", o, err)
	}
}
