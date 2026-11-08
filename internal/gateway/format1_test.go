package gateway

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/borje/lendisk/internal/profile"
)

// COMPAT-1 (#79): a kopia bucket that captures a format-1 blob is
// marked, once, at the clock's time; the write is accepted and captured
// in its class as any other, completed uploads included. The upgrade's
// marker blob ends the mark. A restic bucket's n* key marks nothing.
func TestFormat1BlobMarksTheBucketWithoutRefusingIt(t *testing.T) {
	h := newHarness(t, map[string]string{"vault": "kopia", "laptop": "restic"})
	// The clock moves a minute at a time, within the signatures' skew.
	var clock atomic.Int64
	clock.Store(time.Now().Add(-time.Minute).Unix())
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	h.clock = now
	h.serve(false)
	ctx := context.Background()
	put := func(bucket, key string) {
		t.Helper()
		if _, err := h.client(bucket).PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader([]byte("blob " + key))}); err != nil {
			t.Fatalf("put %s/%s: %v", bucket, key, err)
		}
	}
	put("vault", "p0123-s01")
	put("laptop", "n0123")
	if b, _ := h.db.GetBucket(ctx, "vault"); b.Format1() {
		t.Fatalf("marked by a pack: %+v", b)
	}
	if b, _ := h.db.GetBucket(ctx, "laptop"); b.Format1() {
		t.Fatalf("restic bucket marked: %+v", b)
	}

	// A compaction log arrives as a multipart upload; then an index blob.
	c := h.client("vault")
	up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("vault"), Key: aws.String("m0123")})
	if err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	part, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String("vault"), Key: aws.String("m0123"), UploadId: up.UploadId,
		PartNumber: &one, Body: bytes.NewReader([]byte("log"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String("vault"), Key: aws.String("m0123"),
		UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: &one, ETag: part.ETag}}}}); err != nil {
		t.Fatal(err)
	}
	h.settled()
	b, _ := h.db.GetBucket(ctx, "vault")
	marked := now()
	if !b.Format1() || b.Format1Key != "m0123" || !b.Format1At.Equal(marked) {
		t.Fatalf("after m0123: %+v", b)
	}
	clock.Add(60)
	put("vault", "n0123-s01-c1")
	put("vault", "l0123")
	if b, _ := h.db.GetBucket(ctx, "vault"); b.Format1Key != "m0123" || !b.Format1At.Equal(marked) {
		t.Fatalf("marked again: %+v", b)
	}
	classes := map[string]string{}
	for _, e := range h.entries("vault") {
		classes[e.Key] = e.Class
	}
	want := map[string]profile.Class{"m0123": profile.Data, "n0123-s01-c1": profile.Snapshot, "l0123": profile.Data}
	for k, c := range want {
		if classes[k] != string(c) {
			t.Errorf("%s captured as %q, want %s", k, classes[k], c)
		}
	}

	put("vault", "n00000000000000000000000000000000-repository_unreadable_by_this_kopia_version_upgrade_required")
	b, _ = h.db.GetBucket(ctx, "vault")
	if b.Format1() || !b.UpgradedAt.Equal(now()) {
		t.Fatalf("after the upgrade's marker: %+v", b)
	}

	// Kopia 0.9-0.11 upgraded without the marker: its first epoch blob
	// (x*), which format 1 never writes, ends the mark too.
	clock.Add(60)
	h.db.ExecContext(ctx, `UPDATE buckets SET upgraded_at = NULL WHERE name = 'vault'`)
	if b, _ := h.db.GetBucket(ctx, "vault"); !b.Format1() {
		t.Fatalf("not format 1 again: %+v", b)
	}
	put("vault", "xn0_0123-s01-c1")
	if b, _ := h.db.GetBucket(ctx, "vault"); b.Format1() || !b.UpgradedAt.Equal(now()) {
		t.Fatalf("after an epoch blob: %+v", b)
	}
}
