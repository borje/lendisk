package replica

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
	"github.com/borje/lendisk/internal/state"
)

// S3-4, RET-3 (#39): at a versioned target a delete is a delete marker
// only; undelete lists the keys deleted since a time with the version
// before each marker, and copies that version onto the key, which
// makes it current again and leaves the marker for the lifecycle rule.
func TestAVersionedTargetUndeletesByCopyingTheVersionBeforeTheMarker(t *testing.T) {
	s := startS3(t, true)
	undeleteAt(t, s, "lendisk")
}

// The same at a versioned provider running elsewhere: CI's RustFS
// (.github/rustfs.sh); skipped without its variables.
func TestAVersionedProviderUndeletesByCopyingTheVersionBeforeTheMarker(t *testing.T) {
	const p = "LENDISK_TEST_OVERFLOW_VERSIONED_"
	ep := os.Getenv(p + "ENDPOINT")
	if ep == "" {
		t.Skip(p + "ENDPOINT not set (CI runs .github/rustfs.sh)")
	}
	s := overflowtest.Connect(t, ep, os.Getenv(p+"REGION"), os.Getenv(p+"BUCKET"), os.Getenv(p+"ACCESS_KEY"), os.Getenv(p+"SECRET_KEY"))
	undeleteAt(t, s, fmt.Sprintf("ci/%d", time.Now().UnixNano()))
}

func undeleteAt(t *testing.T, s *overflowtest.Server, prefix string) {
	self := newID(t)
	c, err := NewOverflow(state.Overflow{Name: "b2", Endpoint: s.Endpoint, Region: s.Region, Bucket: s.Bucket, Prefix: prefix},
		overflow.Credential{AccessKey: s.Access, SecretKey: s.Secret}, self.SealedName, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	seal := self.SealedName
	ctx := t.Context()
	body := []byte("pack bytes to bring back")
	o := object("laptop", "data/ab/abcdef", body)
	if err := c.Put(ctx, o); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, object("laptop", "data/cd/kept", []byte("never deleted"))); err != nil {
		t.Fatal(err)
	}
	// A second deleted key: a provider may list every marker before the
	// versions (RustFS does), not each key's versions together.
	if err := c.Put(ctx, object("laptop", "index/zz", []byte("an index"))); err != nil {
		t.Fatal(err)
	}
	before := time.Now().Add(-time.Minute)
	for _, k := range []string{"data/ab/abcdef", "index/zz"} {
		if err := c.Delete(ctx, "laptop", k); err != nil {
			t.Fatal(err)
		}
	}
	if ds, err := c.ListDeleted(ctx, "laptop", time.Now().Add(time.Hour)); err != nil || len(ds) != 0 {
		t.Fatalf("deleted since a later time: %+v, %v", ds, err)
	}
	ds, err := c.ListDeleted(ctx, "laptop", before)
	if err != nil || len(ds) != 2 || ds[1].Key != "index/zz" || ds[1].SHA256 != object("", "", []byte("an index")).SHA256 {
		t.Fatalf("deleted: %+v, %v", ds, err)
	}
	d := ds[0]
	if d.Key != "data/ab/abcdef" || d.SHA256 != o.SHA256 || d.Size != int64(len(body)) || d.VersionID == "" {
		t.Fatalf("deleted %+v", d)
	}
	if err := c.Restore(ctx, "laptop", d); err != nil {
		t.Fatal(err)
	}
	if h, ok, err := c.Head(ctx, "laptop", d.Key); err != nil || !ok || h != o.SHA256 {
		t.Fatalf("after restore: %q %v %v", h, ok, err)
	}
	key := prefix + "/" + seal("laptop") + "/" + d.Key
	if v, m, err := s.Versions(ctx, key); err != nil || v != 2 || m != 1 {
		t.Fatalf("after restore: %d versions, %d markers (%v); want the marker kept", v, m, err)
	}
	if ds, err := c.ListDeleted(ctx, "laptop", before); err != nil || len(ds) != 1 || ds[0].Key != "index/zz" {
		t.Fatalf("after restoring one: %+v, %v", ds, err)
	}
	live, err := c.ListLive(ctx, "laptop")
	if err != nil || len(live) != 2 {
		t.Fatalf("live %v, %v", live, err)
	}
}

// S3-6 (#39): Stat reads the provider's stored checksums: a single-part
// object's ETag is the MD5 of its bytes, and lendisk's hash rides along.
func TestStatReadsTheProvidersStoredChecksums(t *testing.T) {
	s := startS3(t, false)
	c, _ := overflowClient(t, s, s.Secret)
	ctx := t.Context()
	body := []byte("single part object")
	o := object("laptop", "data/x", body)
	if err := c.Put(ctx, o); err != nil {
		t.Fatal(err)
	}
	st, ok, err := c.Stat(ctx, "laptop", "data/x")
	if err != nil || !ok {
		t.Fatalf("stat: %v %v", ok, err)
	}
	sum := md5.Sum(body)
	if st.SHA256 != o.SHA256 || st.Size != int64(len(body)) || !st.MD5ETag() || st.ETag != hex.EncodeToString(sum[:]) {
		t.Fatalf("stat %+v", st)
	}
	if st.ChecksumSHA256 != "" && st.ChecksumSHA256 != o.SHA256 {
		t.Fatalf("stored SHA-256 %q, want %q", st.ChecksumSHA256, o.SHA256)
	}
	if _, ok, err := c.Stat(ctx, "laptop", "data/none"); ok || err != nil {
		t.Fatalf("stat of nothing: %v %v", ok, err)
	}
}
