package recovery

import (
	"testing"
	"time"

	"github.com/borje/lendisk/internal/manifest"
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
