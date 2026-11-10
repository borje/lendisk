package seed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func openDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func befriend(t *testing.T, db *state.DB, peer identity.NodeID) {
	t.Helper()
	if err := db.AddFriendship(context.Background(), state.Friendship{Peer: peer.String(), Address: "tc" + peer.String(),
		OwnNumber: 1 << 40, PeerNumber: 1 << 40, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func hexSum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// put stores body as bucket/key in the store and captures it, as the
// decorator would; it returns the queue seq.
func put(t *testing.T, db *state.DB, store, bucket, key string, body []byte) int64 {
	t.Helper()
	ctx := context.Background()
	p := filepath.Join(store, bucket, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o600); err != nil {
		t.Fatal(err)
	}
	class := "data"
	if !strings.HasPrefix(key, "data/") {
		class = "metadata"
	}
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: class}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), hexSum(body)); err != nil {
		t.Fatal(err)
	}
	return seqs[0]
}

// files lists the regular files under dir, with their modes.
func files(t *testing.T, dir string) map[string]fs.FileMode {
	t.Helper()
	out := map[string]fs.FileMode{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			if fi.Mode().Perm() != 0o700 {
				t.Errorf("directory %s is %v; want 0700", rel, fi.Mode().Perm())
			}
			return nil
		}
		out[filepath.ToSlash(rel)] = fi.Mode().Perm()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// REPL-8 (#36): export writes exactly the keys placed at the friend,
// each its latest captured version, one file per key at its key path
// (restic's layout), 0600 in 0700 directories, with an authenticated
// manifest only that friend accepts; a key whose store copy changed
// since its capture is left out for the network.
func TestExportWritesExactlyTheKeysPlacedAtTheFriend(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := t.TempDir()
	a, b, c := newID(t), newID(t), newID(t)
	befriend(t, db, b.NodeID())
	put(t, db, store, "laptop", "config", []byte("cfg"))
	put(t, db, store, "laptop", "data/00/0001", []byte("one"))
	put(t, db, store, "laptop", "data/00/0002", []byte("old"))
	put(t, db, store, "laptop", "data/00/0002", []byte("two")) // rewritten: the latest goes
	put(t, db, store, "other", "x", []byte("not this bucket"))
	befriend(t, db, c.NodeID()) // holds none of the above
	put(t, db, store, "laptop", "snapshots/s1", []byte("snap"))
	if err := os.WriteFile(filepath.Join(store, "laptop", "snapshots", "s1"), []byte("SNAP"), 0o600); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "disk")
	e := &Exporter{DB: db, Self: a, StoreDir: store}
	r, err := e.Export(ctx, "laptop", b.NodeID(), false, dst)
	if err != nil {
		t.Fatal(err)
	}
	if r.Objects != 3 || r.Changed != 1 || r.Bytes != 9 {
		t.Fatalf("result %+v; want config, data/00/0001 and data/00/0002 (9 bytes), snapshots/s1 changed", r)
	}
	got := files(t, dst)
	want := []string{".lendisk/seed.json", "config", "data/00/0001", "data/00/0002"}
	if keys := slices.Sorted(func(yield func(string) bool) {
		for k := range got {
			if !yield(k) {
				return
			}
		}
	}); !slices.Equal(keys, want) {
		t.Fatalf("export holds %v; want %v", keys, want)
	}
	for k, m := range got {
		if m != 0o600 {
			t.Errorf("%s is %v; want 0600", k, m)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "data", "00", "0002")); string(b) != "two" {
		t.Fatalf("data/00/0002 holds %q", b)
	}

	m, err := ReadManifest(dst, b)
	if err != nil {
		t.Fatal(err)
	}
	if m.Borrower != a.NodeID().String() || m.Bucket != a.SealedName("laptop") || m.Layout != LayoutPlain || len(m.Objects) != 3 {
		t.Fatalf("manifest %+v", m)
	}
	if _, err := ReadManifest(dst, c); err == nil || !strings.Contains(err.Error(), "exported for node") {
		t.Fatalf("another friend read the manifest: %v", err)
	}
	// Changing a hash on the disk breaks the MAC.
	mp := filepath.Join(dst, filepath.FromSlash(ManifestPath))
	raw, _ := os.ReadFile(mp)
	os.WriteFile(mp, []byte(strings.Replace(string(raw), hexSum([]byte("two")), hexSum([]byte("TWO")), 1)), 0o600)
	if _, err := ReadManifest(dst, b); err == nil || !strings.Contains(err.Error(), "does not authenticate") {
		t.Fatalf("a changed manifest was read: %v", err)
	}

	// The seed holds B's rows of laptop; another export into the same
	// directory is refused and leaves the seed as it was.
	s, err := db.GetSeed(ctx, b.NodeID().String(), "laptop")
	if err != nil || s.Objects != 3 || !s.DoneAt.IsZero() {
		t.Fatalf("seed %+v %v", s, err)
	}
	if _, err := e.Export(ctx, "laptop", b.NodeID(), false, dst); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("export into a used directory: %v", err)
	}
	if _, err := e.Export(ctx, "other", c.NodeID(), false, t.TempDir()); err != ErrNothingPlaced {
		t.Fatalf("export of what C does not hold: %v", err)
	}
	if _, err := db.GetSeed(ctx, c.NodeID().String(), "other"); err != state.ErrNotFound {
		t.Fatalf("a failed export left a seed: %v", err)
	}
}

// LEND-6's layout for a Kopia bucket: blobs sharded as Kopia's
// filesystem storage keeps them, with its .shards file.
func TestExportOfAKopiaBucketIsInKopiasLayout(t *testing.T) {
	db := openDB(t)
	store := t.TempDir()
	a, b := newID(t), newID(t)
	befriend(t, db, b.NodeID())
	put(t, db, store, "vault", "kopia.repository", []byte("repo"))
	put(t, db, store, "vault", "p0123456789abcdef0123456789abcdef", []byte("pack"))
	dst := filepath.Join(t.TempDir(), "disk")
	if _, err := (&Exporter{DB: db, Self: a, StoreDir: store}).Export(context.Background(), "vault", b.NodeID(), true, dst); err != nil {
		t.Fatal(err)
	}
	got := files(t, dst)
	for _, f := range []string{"kopia.repository.f", "p/012/3456789abcdef0123456789abcdef.f", ".shards", ManifestPath} {
		if _, ok := got[f]; !ok {
			t.Errorf("no %s in %v", f, got)
		}
	}
	m, err := ReadManifest(dst, b)
	if err != nil || m.Layout != LayoutKopia || m.Path("kopia.repository") != "kopia.repository.f" {
		t.Fatalf("manifest %+v %v", m, err)
	}
}

// pages serves a fixed list as the friend's, a page of two keys at a
// time; nil objects means the friend holds no such bucket yet.
func pages(objs []control.ListObject) func(context.Context, state.Friendship, string, string) (control.ListPage, error) {
	return func(_ context.Context, _ state.Friendship, _, after string) (control.ListPage, error) {
		if objs == nil {
			return control.ListPage{}, &control.CallError{Status: 404, Code: control.CodeNoSuchBucket}
		}
		var p control.ListPage
		for _, o := range objs {
			if o.Key > after && len(p.Objects) < 2 {
				p.Objects = append(p.Objects, o)
			}
		}
		if len(p.Objects) == 2 && p.Objects[1].Key != objs[len(objs)-1].Key {
			p.Next = p.Objects[1].Key
		}
		return p, nil
	}
}

// REPL-8 (#36): the daemon acknowledges only the held rows the
// friend's list shows with the captured hash; the rest stay queued. The
// seed ends when none is left, when an import that confirmed some has
// confirmed nothing more for an hour, or when nothing was confirmed for
// SeedExpiry; then the place's FIFO goes on with what is left.
func TestOnlyRowsTheListShowsWithTheirHashAreAcknowledged(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := t.TempDir()
	a, b := newID(t), newID(t)
	bKey := b.NodeID().String()
	befriend(t, db, b.NodeID())
	for _, k := range []string{"config", "data/1", "data/2", "data/3"} {
		put(t, db, store, "laptop", k, []byte(k))
	}
	if _, err := (&Exporter{DB: db, Self: a, StoreDir: store}).Export(ctx, "laptop", b.NodeID(), false, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	var listed []control.ListObject
	c := &Confirmer{DB: db, Self: a, Now: func() time.Time { return now },
		Page: func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
			if sealed != a.SealedName("laptop") || f.Peer != bKey {
				t.Errorf("listed %s at %s", sealed, f.Peer)
			}
			return pages(listed)(ctx, f, sealed, after)
		}}
	round := func() state.Seed {
		t.Helper()
		if err := c.Round(ctx); err != nil {
			t.Fatal(err)
		}
		s, err := db.GetSeed(ctx, bKey, "laptop")
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	// Not imported yet: nothing confirmed, still held.
	if s := round(); s.Confirmed != 0 || !s.DoneAt.IsZero() || s.Error != "" {
		t.Fatalf("before the import: %+v", s)
	}
	if _, ok, _ := db.NextPlacement(ctx, bKey); ok {
		t.Fatal("a held row is in the FIFO")
	}
	// Imported in part: config and data/1 as captured, data/2 with
	// other bytes, data/3 tombstoned.
	tomb := now
	listed = []control.ListObject{
		{Key: "config", SHA256: hexSum([]byte("config"))},
		{Key: "data/1", SHA256: hexSum([]byte("data/1"))},
		{Key: "data/2", SHA256: hexSum([]byte("other"))},
		{Key: "data/3", SHA256: hexSum([]byte("data/3")), TombstonedAt: &tomb},
	}
	if s := round(); s.Confirmed != 2 || !s.DoneAt.IsZero() {
		t.Fatalf("after the import: %+v", s)
	}
	for k, want := range map[string]bool{"config": true, "data/1": true, "data/2": false, "data/3": false} {
		if _, ok, _ := db.AckedHash(ctx, bKey, "laptop", k); ok != want {
			t.Errorf("%s acknowledged: %v; want %v", k, ok, want)
		}
	}
	// Nothing more for SeedQuietAfter: the rest go by the network.
	now = now.Add(state.SeedQuietAfter - time.Minute)
	if s := round(); !s.DoneAt.IsZero() {
		t.Fatalf("ended before the quiet hour: %+v", s)
	}
	now = now.Add(time.Minute)
	if s := round(); s.Outcome != state.SeedQuiet {
		t.Fatalf("after the quiet hour: %+v", s)
	}
	var sent []string
	for {
		p, ok, err := db.NextPlacement(ctx, bKey)
		if err != nil || !ok {
			break
		}
		sent = append(sent, p.Key)
		db.AckPlacement(ctx, bKey, p.Seq, p.SHA256, now)
	}
	if !slices.Equal(sent, []string{"data/2", "data/3"}) {
		t.Fatalf("sent after the seed: %v; want data/2 and data/3 only", sent)
	}
}

// A seed whose list confirms everything ends at once; one never
// imported ends after SeedExpiry; one of a former friend ends.
func TestSeedEnds(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	store := t.TempDir()
	a, b := newID(t), newID(t)
	bKey := b.NodeID().String()
	befriend(t, db, b.NodeID())
	put(t, db, store, "laptop", "config", []byte("config"))
	put(t, db, store, "other", "config", []byte("other"))
	e := &Exporter{DB: db, Self: a, StoreDir: store}
	for _, bucket := range []string{"laptop", "other"} {
		if _, err := e.Export(ctx, bucket, b.NodeID(), false, t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	c := &Confirmer{DB: db, Self: a, Now: func() time.Time { return now },
		Page: func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
			if sealed == a.SealedName("laptop") {
				return pages([]control.ListObject{{Key: "config", SHA256: hexSum([]byte("config"))}})(ctx, f, sealed, after)
			}
			return pages(nil)(ctx, f, sealed, after)
		}}
	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetSeed(ctx, bKey, "laptop"); s.Outcome != state.SeedConfirmed {
		t.Fatalf("laptop: %+v", s)
	}
	if s, _ := db.GetSeed(ctx, bKey, "other"); !s.DoneAt.IsZero() {
		t.Fatalf("other ended at once: %+v", s)
	}
	now = now.Add(state.SeedExpiry)
	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetSeed(ctx, bKey, "other"); s.Outcome != state.SeedExpired {
		t.Fatalf("other after the expiry: %+v", s)
	}

	if _, err := e.Export(ctx, "other", b.NodeID(), false, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM friendship`); err != nil {
		t.Fatal(err)
	}
	if err := c.Round(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetSeed(ctx, bKey, "other"); s.Outcome != state.SeedRemoved {
		t.Fatalf("other of a former friend: %+v", s)
	}
}

// seedDisk writes a manifest from a for b, of objs, into dir.
func seedDisk(t *testing.T, dir string, a, b *identity.Identity, objs ...Object) {
	t.Helper()
	secret, err := a.S3Secret(b.NodeID())
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{Format: Format, Borrower: a.NodeID().String(), Lender: b.NodeID().String(),
		Bucket: a.SealedName("laptop"), Layout: LayoutPlain, Objects: objs}
	if err := writeManifest(dir, m, secret); err != nil {
		t.Fatal(err)
	}
}

// #36 review: import reads only the regular files the export wrote. A
// symbolic link, to a file or through a directory, a FIFO and a missing
// file are refused without being opened or followed, and a manifest
// naming a key outside the disk is refused whole, as is a manifest that
// is a link.
func TestImportFollowsNoLinkAndOpensNoDevice(t *testing.T) {
	a, b := newID(t), newID(t)
	dir := t.TempDir()
	outside := t.TempDir()
	body := []byte("the lender's own file")
	secretFile := filepath.Join(outside, "secret")
	os.WriteFile(secretFile, body, 0o600)
	if err := os.Symlink(secretFile, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "dirlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	obj := func(key string) Object { return Object{Key: key, Size: int64(len(body)), SHA256: hexSum(body)} }
	seedDisk(t, dir, a, b, obj("link"), obj("dirlink/secret"), obj("fifo"), obj("missing"))
	m, err := ReadManifest(dir, b)
	if err != nil {
		t.Fatal(err)
	}
	dialled := false
	im := &Importer{Self: b, DB: openDB(t), Dial: func(context.Context) (net.Conn, error) {
		dialled = true
		return nil, errors.New("no gateway in this test")
	}}
	done := make(chan struct{})
	var r ImportResult
	go func() {
		defer close(done)
		r, err = im.Import(context.Background(), dir, m)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("import blocked (on the FIFO?)")
	}
	if err != nil || r.Objects != 0 || len(r.Refused) != 4 || dialled {
		t.Fatalf("import: %+v %v, dialled %v; want all four refused, nothing sent", r, err, dialled)
	}
	for _, f := range r.Refused {
		want := "not a regular file"
		if f.Key == "missing" {
			want = "missing"
		}
		if !strings.Contains(f.Reason, want) {
			t.Errorf("%s refused as %q; want %q", f.Key, f.Reason, want)
		}
	}

	for _, key := range []string{"../x", "/etc/passwd", "a/../../x"} {
		d := t.TempDir()
		seedDisk(t, d, a, b, Object{Key: key, Size: 1, SHA256: hexSum([]byte("x"))})
		if _, err := ReadManifest(d, b); err == nil || !strings.Contains(err.Error(), "no file of the export") {
			t.Errorf("manifest naming %q: %v", key, err)
		}
	}
	d := t.TempDir()
	seedDisk(t, d, a, b)
	real := filepath.Join(d, ".lendisk", "real.json")
	os.Rename(filepath.Join(d, filepath.FromSlash(ManifestPath)), real)
	os.Symlink(real, filepath.Join(d, filepath.FromSlash(ManifestPath)))
	if _, err := ReadManifest(d, b); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Errorf("a linked manifest: %v", err)
	}
}
