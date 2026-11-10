package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// S3-1, SEC-3 (#38 review): recovery restores each overflow target's row
// from the manifest, all but the credential, so the rebuilt node's own
// manifest still names the target (its endpoint and, once the user puts
// the credential in the environment, its keys) rather than erasing the
// only copy; restoring again changes nothing.
func TestRestoreBringsOverflowTargetsBackWithoutTheCredential(t *testing.T) {
	ctx := t.Context()
	self := newID(t)
	m := &manifest.Manifest{Seq: 3, Content: manifest.Content{Overflow: []manifest.Overflow{{
		Name: "b2", Endpoint: "https://s3.example", Bucket: "offsite", Prefix: "lendisk", Region: "eu-1",
		AccessKey: "AKID", SecretKey: "SECRET", MaxBytes: 450 << 30, EnvPrefix: "MY_B2_", RetentionSeconds: 30 * 86400,
		PricePerGB: 0.006,
	}, {Name: "old", Endpoint: "https://s3.other", Bucket: "x"}}}}
	db := openDB(t)
	for range 2 {
		if _, err := Restore(ctx, db, m); err != nil {
			t.Fatal(err)
		}
	}
	o, err := db.GetOverflow(ctx, "b2")
	if err != nil || o.Endpoint != "https://s3.example" || o.Bucket != "offsite" || o.Prefix != "lendisk" || o.Region != "eu-1" ||
		o.MaxBytes != 450<<30 || o.EnvPrefix != "MY_B2_" || o.Retention != 30*24*time.Hour || o.PricePerGB != 0.006 {
		t.Fatalf("restored %+v, %v", o, err)
	}
	// A manifest that named no variables: the default ones.
	if old, err := db.GetOverflow(ctx, "old"); err != nil || old.EnvPrefix != "LENDISK_OVERFLOW_OLD_" {
		t.Fatalf("restored %+v, %v", old, err)
	}
	// The rebuilt node's manifest names both, with the keys from its
	// environment, never from the database.
	env := map[string]string{"MY_B2_ACCESS_KEY": "AKID", "MY_B2_SECRET_KEY": "SECRET"}
	c, err := manifest.Build(ctx, db, self, func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil || len(c.Overflow) != 2 || c.Overflow[0].AccessKey != "AKID" || c.Overflow[1].Endpoint != "https://s3.other" {
		t.Fatalf("rebuilt manifest %+v, %v", c.Overflow, err)
	}
	if none, _ := manifest.Build(ctx, db, self, nil); none.Overflow[0].SecretKey != "" {
		t.Fatal("the credential came from the database")
	}
}

// #96 (from #38): recovery lists each overflow target, as it lists each
// friend, and rebuilds its placement rows from the listing, so the
// copies there count as held and are not placed again as if missing.
// The provider lists keys and sizes only: a key named by its hash has
// that hash, any other is read for the hash lendisk stored with it,
// and an object without one is not counted.
func TestRecoveryListsEachOverflowTargetAndRebuildsItsPlacement(t *testing.T) {
	ctx := t.Context()
	srv := overflowtest.Start(t, "spill", false)
	self := newID(t)
	sealed := self.SealedName("laptop")
	pack := []byte("a pack of data")
	sum := sha256.Sum256(pack)
	packKey := "data/" + hex.EncodeToString(sum[:2]) + "/" + hex.EncodeToString(sum[:])
	config := []byte("the repository's config")
	csum := sha256.Sum256(config)
	for _, o := range []struct {
		key  string
		body []byte
		meta map[string]string
	}{
		{packKey, pack, nil}, // named by its hash: no HEAD needed
		{"config", config, map[string]string{replica.MetaSHA256: hex.EncodeToString(csum[:])}},
		{"keys/stray", []byte("not put by lendisk"), nil}, // no hash: not counted
		{"locks/x", []byte("a lock"), nil},                // local-only: never a copy
	} {
		if err := srv.Put(ctx, replica.OverflowKey("lendisk", sealed, o.key), o.body, o.meta); err != nil {
			t.Fatal(err)
		}
	}
	m := &manifest.Manifest{Seq: 2, Content: manifest.Content{
		Overflow: []manifest.Overflow{{Name: "spill", Endpoint: srv.Endpoint, Bucket: srv.Bucket, Prefix: "lendisk",
			MaxBytes: 1 << 30, EnvPrefix: "SPILL_"}},
		Buckets: []manifest.Bucket{{Name: "laptop", Sealed: sealed, Copies: 2, Profile: "restic"}},
	}}
	db := openDB(t)
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "AK", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, db, m); err != nil {
		t.Fatal(err)
	}
	place := state.OverflowPlace("spill")

	// A lister that cannot reach targets (recover without the
	// credential in its environment) leaves the list pending.
	l := &Lister{DB: db, Self: self, Now: time.Now, Log: quiet}
	res, err := l.Round(ctx)
	if err != nil || len(res) != 1 || res[0].Place != place || res[0].Err == nil {
		t.Fatalf("without a client: %+v, %v", res, err)
	}
	if n, _ := l.Pending(ctx); n != 1 {
		t.Fatalf("%d lists pending, want the target's", n)
	}

	env := map[string]string{"SPILL_ACCESS_KEY": srv.Access, "SPILL_SECRET_KEY": srv.Secret}
	l = &Lister{DB: db, Self: self, Now: time.Now, Log: quiet, MinBackoff: time.Nanosecond, Overflow: func(o state.Overflow) (Target, error) {
		cred, err := overflow.Lookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok }, o.EnvPrefix)
		if err != nil {
			return nil, err
		}
		return replica.NewOverflow(o, cred, self.SealedName, replica.Options{})
	}}
	res, err = l.Round(ctx)
	if err != nil || len(res) != 1 || res[0].Err != nil {
		t.Fatalf("listing the target: %+v, %v", res, err)
	}
	if res[0].Listed != 2 || res[0].Rejected != 1 {
		t.Fatalf("listed %d, rejected %d: want the pack and config, and the stray key refused", res[0].Listed, res[0].Rejected)
	}
	if n, _ := l.Pending(ctx); n != 0 {
		t.Fatalf("%d lists pending after the round", n)
	}
	// The pack counts; config, metadata no friend lists, does not (#96
	// review) until a friend's list names it.
	held, err := db.HeldAt(ctx, "laptop")
	if err != nil || held[place] != 1 {
		t.Fatalf("held %v, %v: want the pack at %s", held, err, place)
	}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "F", Address: "tcF", OwnNumber: 1 << 30, PeerNumber: 1 << 30,
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordListed(ctx, "F", "laptop", []state.Listed{{Key: "config", Size: int64(len(config)),
		SHA256: hex.EncodeToString(csum[:]), Class: "metadata"}}, 1, "", true); err != nil {
		t.Fatal(err)
	}
	if held, err := db.HeldAt(ctx, "laptop"); err != nil || held[place] != 2 {
		t.Fatalf("held %v, %v: want the pack and config at %s once a friend lists config", held, err, place)
	}
	keys, err := db.RecoveredKeys(ctx, "laptop")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		want := hex.EncodeToString(sum[:])
		if k.Key == "config" {
			want = hex.EncodeToString(csum[:])
		}
		if k.SHA256 != want || k.Places[place] != want {
			t.Errorf("%s recovered as %+v", k.Key, k)
		}
	}
	// Re-placement finds the copies held there: it places none there
	// again.
	if _, err := db.Replace(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	var queued int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM placement WHERE place = ? AND state != 'acked'`, place).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("%d rows placed at the target again after recovery", queued)
	}
}
