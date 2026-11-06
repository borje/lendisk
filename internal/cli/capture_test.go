package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// EP-7 through serve: a profile in <config>/profiles/ overrides the
// compiled-in one. REPL-3: status shows each bucket's queue.
func TestStatusShowsQueueAndProfilesOverride(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	laptop := n.addBucket(t, "laptop", "restic")
	n.addBucket(t, "other", "generic")
	dir := filepath.Join(filepath.Dir(n.config), "profiles")
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, "restic.toml"), []byte(`encrypted = true
local_only = ["locks/*", "scratch/*"]
metadata = ["config"]
`), 0o600)
	d := n.serve(t)
	c := s3client(d.addr, laptop)
	ctx := context.Background()
	for _, k := range []string{"data/ab/one", "config", "scratch/x", "locks/y"} {
		if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(k),
			Body: strings.NewReader("12345")}); err != nil {
			t.Fatal(err)
		}
	}
	st := n.status()
	q := map[string]StatusQueue{}
	for _, b := range st.Buckets {
		q[b.Name] = b.Queue
	}
	if q["laptop"] != (StatusQueue{Objects: 2, Bytes: 10}) || q["other"] != (StatusQueue{}) {
		t.Fatalf("status queues %+v", q)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "queue 2 objects") {
		t.Fatalf("status text:\n%s", text)
	}
}
