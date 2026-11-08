package gateway

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// EP-7 (#74): the decorator records at capture whether the bucket's
// profile names the key rewritable, beside its class: Kopia's
// kopia.maintenance is, its index blobs are not; restic's config is.
func TestCaptureRecordsRewritableKeys(t *testing.T) {
	h := newHarness(t, map[string]string{"vault": "kopia", "laptop": "restic"})
	h.serve(false)
	ctx := context.Background()
	put := func(bucket, key string) {
		t.Helper()
		if _, err := h.client(bucket).PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: strings.NewReader("v1 of " + key)}); err != nil {
			t.Fatal(err)
		}
	}
	put("vault", "kopia.maintenance")
	put("vault", "n0123")
	put("laptop", "config")
	put("laptop", "keys/0123")
	want := map[string]bool{"kopia.maintenance": true, "n0123": false, "config": true, "keys/0123": false}
	for _, b := range []string{"vault", "laptop"} {
		for _, e := range h.entries(b) {
			if e.Rewritable != want[e.Key] {
				t.Errorf("%s/%s captured rewritable %v, want %v", b, e.Key, e.Rewritable, want[e.Key])
			}
			if e.Class != "metadata" && e.Class != "snapshot" {
				t.Errorf("%s/%s captured as %s", b, e.Key, e.Class)
			}
		}
	}
}
