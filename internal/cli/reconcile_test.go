package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/gateway"
)

// reconciledOf waits until the bucket's reconcile record satisfies ok,
// polling status --json, and returns it.
func (n *node) reconciledOf(t *testing.T, bucket string, ok func(StatusReconciled) bool) StatusReconciled {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		for _, b := range n.status().Buckets {
			if b.Name == bucket && ok(b.Reconciled) {
				return b.Reconciled
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("bucket %s not reconciled as wanted: %+v", bucket, n.status().Buckets)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// EP-14 AC (#8): a file dropped into a bucket directory while the
// daemon is stopped is enqueued after restart, and an object removed
// from disk behind its back is enqueued as a delete; status counts both,
// in text and --json.
func TestStoreChangesWhileStoppedAreReconciledAtStart(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	n.mustInit(t)
	laptop := n.addBucket(t, "laptop", "restic")
	d := n.serve(t)
	c := s3client(d.addr, laptop)
	for i := range 20 {
		if _, err := c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String(fmt.Sprintf("data/%02d", i)), Body: strings.NewReader("pack")}); err != nil {
			t.Fatal(err)
		}
	}
	d.stop()

	dir := filepath.Join(n.data, "store", "laptop")
	if err := os.MkdirAll(filepath.Join(dir, "data", "ab"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data", "ab", "dropped"), []byte("dropped pack"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "data", "03")); err != nil {
		t.Fatal(err)
	}
	d = n.serve(t)
	r := n.reconciledOf(t, "laptop", func(r StatusReconciled) bool { return r.Writes+r.Deletes > 1 })
	if r.Writes != 1 || r.Deletes != 1 || r.HeldDeletes != 0 || r.LastPass == "" {
		t.Fatalf("reconciled %+v; want 1 write and 1 delete", r)
	}
	for _, b := range n.status().Buckets {
		if b.Name == "laptop" && b.Queue.Objects != 22 {
			t.Fatalf("queue %+v; want 20 captured puts, the dropped file's and data/03's delete", b.Queue)
		}
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "reconciled outside the endpoint: 1 write, 1 delete") {
		t.Fatalf("status text:\n%s", text)
	}
	d.stop()
}

// EP-14 (#8): an emptied bucket directory deletes nothing at friends: the
// pass holds the deletes, status and an alert say so with the command,
// which refuses a smaller count; once allowed, the daemon enqueues them
// without a restart.
func TestEmptiedBucketHeldUntilTheUserAllowsDeletes(t *testing.T) {
	serving(t, 0)
	n := newNode(t)
	n.mustInit(t)
	laptop := n.addBucket(t, "laptop", "restic")
	n.tuneReconcile = func(s *gateway.ReconcileSettings) { s.Poll = 20 * time.Millisecond }
	n.tuneAlerts = func(s *alert.Service) { s.Interval = 20 * time.Millisecond }
	d := n.serve(t)
	c := s3client(d.addr, laptop)
	for i := range 5 {
		if _, err := c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String(fmt.Sprintf("data/%02d", i)), Body: strings.NewReader("pack")}); err != nil {
			t.Fatal(err)
		}
	}
	d.stop()
	dir := filepath.Join(n.data, "store", "laptop", "data")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	d = n.serve(t)
	r := n.reconciledOf(t, "laptop", func(r StatusReconciled) bool { return r.HeldDeletes > 0 })
	if r.HeldDeletes != 5 || r.HeldOf != 5 || r.Deletes != 0 || r.Allow != "lendisk bucket reconcile laptop --allow-deletes 5" {
		t.Fatalf("reconciled %+v", r)
	}
	deadline := time.Now().Add(60 * time.Second)
	for !hasAlert(n.status(), alert.KindReconcileHeld) {
		if time.Now().After(deadline) {
			t.Fatalf("no reconcile_held alert: %+v", n.status().Alerts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "! 5 of 5 objects held here are missing from the store") ||
		!strings.Contains(text, "lendisk bucket reconcile laptop --allow-deletes 5") {
		t.Fatalf("status text:\n%s", text)
	}
	if code, _, e := n.run("", "", "bucket", "reconcile", "laptop", "--allow-deletes", "4"); code != 1 ||
		!strings.Contains(e, "5 deletes are held back") {
		t.Fatalf("allowing fewer: exit %d: %s", code, e)
	}
	if code, _, e := n.run("", "", "bucket", "reconcile", "laptop"); code != 2 {
		t.Fatalf("no --allow-deletes: exit %d: %s", code, e)
	}
	code, out, e := n.run("", "", "bucket", "reconcile", "laptop", "--allow-deletes", "5")
	if code != 0 || !strings.Contains(out, "up to 5") {
		t.Fatalf("allow: exit %d: %s%s", code, out, e)
	}
	// The pass enqueues the deletes before it records that nothing is
	// held any more: wait for both.
	r = n.reconciledOf(t, "laptop", func(r StatusReconciled) bool { return r.Deletes == 5 && r.HeldDeletes == 0 })
	if r.HeldDeletes != 0 {
		t.Fatalf("still held after the allowed pass: %+v", r)
	}
	d.stop()
	if code, _, e := n.run("", "", "bucket", "reconcile", "laptop", "--allow-deletes", "5"); code != 1 ||
		!strings.Contains(e, "nothing to allow") {
		t.Fatalf("allowing with nothing held: exit %d: %s", code, e)
	}
}

func hasAlert(st Status, kind string) bool {
	for _, a := range st.Alerts {
		if a.Kind == kind {
			return true
		}
	}
	return false
}
