package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
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

func openDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ID-4: a manifest sealed by a node opens with the same identity
// restored from its phrase, and with nothing else: another identity,
// a flipped bit or another node's envelope fails. The envelope shows no
// bucket name, sealed or not.
func TestSealedManifestOpensOnlyWithItsIdentity(t *testing.T) {
	phrase, _ := identity.NewPhrase()
	a, _ := identity.FromPhrase(phrase)
	m := &Manifest{Seq: 3, UpdatedAt: time.Unix(1_800_000_000, 0).UTC(), Content: Content{
		Friends: []Friend{{ID: newID(t).NodeID().String(), Address: "tcfriend", OwnNumber: 10 << 30}},
		Buckets: []Bucket{{Name: "laptop", Sealed: a.SealedName("laptop"), Copies: 2, Profile: "restic"}},
	}}
	b, err := Seal(a, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte("ldm1")) {
		t.Fatalf("envelope does not start with its format: %q", b[:4])
	}
	for _, s := range []string{"laptop", a.SealedName("laptop"), "tcfriend", "restic"} {
		if bytes.Contains(b, []byte(s)) {
			t.Fatalf("envelope shows %q", s)
		}
	}
	restored, _ := identity.FromPhrase(phrase)
	got, err := Open(restored, b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != Format || got.Node != a.NodeID().String() || got.Seq != 3 ||
		len(got.Buckets) != 1 || got.Buckets[0] != m.Buckets[0] || got.Friends[0] != m.Friends[0] {
		t.Fatalf("opened %+v", got)
	}
	if _, err := Open(newID(t), b); err == nil {
		t.Fatal("another identity opened the manifest")
	}
	for _, i := range []int{0, 3, 5, len(b) - 1} {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if _, err := Open(restored, bad); err == nil {
			t.Fatalf("a flipped bit at %d was accepted", i)
		}
	}
	// Two seals of one manifest differ (fresh nonce) and both open.
	b2, _ := Seal(a, m, nil)
	if bytes.Equal(b, b2) {
		t.Fatal("two seals are identical: the nonce is not fresh")
	}
	if _, err := Open(restored, b2); err != nil {
		t.Fatal(err)
	}
}

// Randomness is injected: a fixed reader gives a fixed envelope.
func TestSealUsesInjectedRandomness(t *testing.T) {
	a := newID(t)
	m := &Manifest{Seq: 1}
	x, _ := Seal(a, m, bytes.NewReader(make([]byte, 64)))
	y, _ := Seal(a, m, bytes.NewReader(make([]byte, 64)))
	if !bytes.Equal(x, y) {
		t.Fatal("the same randomness gave two envelopes")
	}
}

// ID-4: the manifest is built from the state database: every friend
// with its address and terms, every bucket with its copies and sealed
// name (ID-7, from #14), the address settings; no placement and no
// bucket credential (SEC-3).
func TestBuildListsFriendsAndBucketsNotPlacement(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	self := newID(t)
	friend := newID(t).NodeID().String()
	created := time.Unix(1_800_000_000, 0)
	if err := db.AddFriendship(ctx, state.Friendship{Peer: friend, Address: "tcfriendaddr", OwnNumber: 10 << 30,
		PeerNumber: 20 << 30, OwnRetention: 30 * 24 * time.Hour, PeerRetention: 60 * 24 * time.Hour, CreatedAt: created}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "AKIDSECRETACCESS",
		SecretKey: "verysecretkey", CreatedAt: created}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state)
		VALUES (1, ?, 'laptop', 'data/placed-object-key', 'put', 'queued')`, friend); err != nil {
		t.Fatal(err)
	}
	c, err := Build(ctx, db, self)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Friends) != 1 || c.Friends[0] != (Friend{ID: friend, Address: "tcfriendaddr", OwnNumber: 10 << 30,
		PeerNumber: 20 << 30, OwnRetentionSeconds: 30 * 86400, PeerRetentionSeconds: 60 * 86400, Since: created.UTC()}) {
		t.Fatalf("friends %+v", c.Friends)
	}
	if len(c.Buckets) != 1 || c.Buckets[0] != (Bucket{Name: "laptop", Sealed: self.SealedName("laptop"), Copies: 2,
		Profile: "restic", CreatedAt: created.UTC()}) {
		t.Fatalf("buckets %+v", c.Buckets)
	}
	if c.Overflow == nil || len(c.Overflow) != 0 {
		t.Fatalf("overflow %+v, want an empty list", c.Overflow)
	}
	raw, _ := json.Marshal(c)
	for _, s := range []string{"placed-object-key", "AKIDSECRETACCESS", "verysecretkey", "placement"} {
		if strings.Contains(string(raw), s) {
			t.Fatalf("manifest carries %q: %s", s, raw)
		}
	}
	// The hash is of the content: stable, and moved by a change.
	h := Hash(c)
	again, _ := Build(ctx, db, self)
	if Hash(again) != h {
		t.Fatal("the same state hashed differently")
	}
	db.AddBucket(ctx, state.Bucket{Name: "desktop", Profile: "restic", Copies: 1, AccessKey: "A2", SecretKey: "s2", CreatedAt: created})
	changed, _ := Build(ctx, db, self)
	if Hash(changed) == h {
		t.Fatal("a new bucket left the hash alone")
	}
}

// ID-4, SEC-3: the lender keeps each friend's manifest as a file in a
// private directory of its store, 0600 in 0700 directories, named by
// the friend's node ID; the directory's name is not a valid bucket
// name, so versitygw serves it to no account.
func TestStoreKeepsManifestsPrivately(t *testing.T) {
	storeDir := t.TempDir()
	st := &Store{Dir: Dir(storeDir)}
	a := newID(t)
	if _, err := st.Get(a.NodeID()); !errors.Is(err, control.ErrNoManifest) {
		t.Fatalf("empty store: %v", err)
	}
	b := blobFor(t, a)
	if err := st.Put(a.NodeID(), b); err != nil {
		t.Fatal(err)
	}
	if got, err := st.Get(a.NodeID()); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("get: %v", err)
	}
	if rel, _ := filepath.Rel(storeDir, st.Dir); !strings.HasPrefix(rel, ".") {
		t.Fatalf("manifests live in %s, a name a bucket could have", rel)
	}
	for p, mode := range map[string]os.FileMode{
		filepath.Dir(st.Dir): 0o700,
		st.Dir:               0o700,
		filepath.Join(st.Dir, a.NodeID().String()): 0o600,
	} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != mode {
			t.Errorf("%s mode %04o, want %04o", p, fi.Mode().Perm(), mode)
		}
	}
	// Only the manifest is left behind: no temporary files.
	ents, _ := os.ReadDir(st.Dir)
	if len(ents) != 1 {
		t.Fatalf("directory holds %d entries", len(ents))
	}
	for _, bad := range [][]byte{nil, []byte("ldm1"), []byte("not a manifest, but long enough to be one if it were")} {
		if err := st.Put(a.NodeID(), bad); !errors.Is(err, control.ErrBadManifest) {
			t.Fatalf("put %q: %v", bad, err)
		}
	}
	if got, _ := st.Get(a.NodeID()); !bytes.Equal(got, b) {
		t.Fatal("a refused put changed the stored manifest")
	}
}

func blobFor(t *testing.T, id *identity.Identity) []byte {
	t.Helper()
	b, err := Seal(id, &Manifest{Seq: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
