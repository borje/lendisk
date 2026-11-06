package gateway

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

func newIdentity(t *testing.T) *identity.Identity {
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

// befriend makes friend a friend of the harness's lender.
func (h *harness) befriend(friend *identity.Identity) {
	h.t.Helper()
	if err := h.db.AddFriendship(context.Background(), state.Friendship{Peer: friend.NodeID().String(), CreatedAt: time.Now()}); err != nil {
		h.t.Fatal(err)
	}
}

// borrowerClient signs as borrower with the given access key and secret
// on the gateway socket directly; the bridge's peer-key check is not in
// this path.
func (h *harness) borrowerClient(access, secret string) *s3.Client {
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", h.socket)
	}}}
	return s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://gw"), UsePathStyle: true, HTTPClient: hc,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""), RetryMaxAttempts: 1,
	})
}

// pairClient is borrower's client for the harness's lender, with the
// derived credential (NET-7a).
func (h *harness) pairClient(borrower *identity.Identity) *s3.Client {
	h.t.Helper()
	secret, err := borrower.S3Secret(h.self.NodeID())
	if err != nil {
		h.t.Fatal(err)
	}
	return h.borrowerClient(borrower.NodeID().String(), secret)
}

func bucketNames(t *testing.T, c *s3.Client) []string {
	t.Helper()
	out, err := c.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	return names
}

// PEER-2, PEER-8, NET-7a, SEC-16: a friend creates its bucket by sealed
// name and puts, gets, heads and lists objects in it; the store holds it
// as lent-<id16>-<sealed>, owned by the friend's account, and the
// friend's listing shows its sealed names alone.
func TestFriendBorrowsUnderSealedName(t *testing.T) {
	h := newHarness(t, map[string]string{"photos": "restic"})
	h.self = newIdentity(t)
	b := newIdentity(t)
	h.befriend(b)
	h.serve(false)
	ctx := context.Background()
	c := h.pairClient(b)
	sealed := b.SealedName("laptop")
	bucket := aws.String(sealed)

	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	body := []byte("encrypted pack")
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("data/ab/pack"), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("data/ab/pack")})
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := io.ReadAll(got.Body); !bytes.Equal(data, body) {
		t.Fatalf("read back %q", data)
	}
	if head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("data/ab/pack")}); err != nil || aws.ToInt64(head.ContentLength) != int64(len(body)) {
		t.Fatalf("head: %v %v", head, err)
	}
	if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Contents) != 1 || aws.ToString(list.Contents[0].Key) != "data/ab/pack" || aws.ToString(list.Name) != sealed {
		t.Fatalf("listing %+v name %q", list.Contents, aws.ToString(list.Name))
	}
	// A retried create is the friend's own bucket again.
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil && code(err) != "BucketAlreadyOwnedByYou" {
		t.Fatalf("retried create: %v", err)
	}
	if names := bucketNames(t, c); !slices.Equal(names, []string{sealed}) {
		t.Fatalf("friend lists %v", names)
	}

	stored := LentName(b.NodeID(), sealed)
	if len(stored) != 49 || stored[:5] != "lent-" || stored[5:21] != b.NodeID().Short() {
		t.Fatalf("store name %q", stored)
	}
	if data, err := os.ReadFile(filepath.Join(h.store, stored, "data", "ab", "pack")); err != nil || !bytes.Equal(data, body) {
		t.Fatalf("store file: %q %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(h.store, sealed)); !os.IsNotExist(err) {
		t.Fatal("the store holds a bucket under the bare sealed name")
	}
	// The lender's own bucket is untouched and not captured from a
	// friend's write.
	if es := h.entries(stored); len(es) != 0 {
		t.Fatalf("a lent bucket's write was queued: %+v", es)
	}
}

// PEER-8 AC: one borrower cannot list, read, delete or create in
// another's buckets, nor see or name the lender's own.
func TestBorrowersAreIsolated(t *testing.T) {
	h := newHarness(t, map[string]string{"photos": "restic"})
	h.self = newIdentity(t)
	a, b := newIdentity(t), newIdentity(t)
	h.befriend(a)
	h.befriend(b)
	h.serve(false)
	ctx := context.Background()
	ca, cb := h.pairClient(a), h.pairClient(b)
	aSealed := a.SealedName("laptop")
	if _, err := ca.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(aSealed)}); err != nil {
		t.Fatal(err)
	}
	if _, err := ca.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(aSealed), Key: aws.String("config"), Body: bytes.NewReader([]byte("a's"))}); err != nil {
		t.Fatal(err)
	}
	// The lender's own bucket has an object too.
	own := h.client("photos")
	if _, err := own.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("photos"), Key: aws.String("config"), Body: bytes.NewReader([]byte("lender's"))}); err != nil {
		t.Fatal(err)
	}

	if names := bucketNames(t, cb); len(names) != 0 {
		t.Fatalf("B lists %v", names)
	}
	for _, name := range []string{aSealed, "photos", LentName(a.NodeID(), aSealed)} {
		bk := aws.String(name)
		if _, err := cb.GetObject(ctx, &s3.GetObjectInput{Bucket: bk, Key: aws.String("config")}); err == nil {
			t.Errorf("B read %s/config", name)
		}
		if _, err := cb.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bk}); err == nil {
			t.Errorf("B heads %s", name)
		}
		if _, err := cb.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bk}); err == nil {
			t.Errorf("B lists %s", name)
		}
		if _, err := cb.PutObject(ctx, &s3.PutObjectInput{Bucket: bk, Key: aws.String("planted"), Body: bytes.NewReader([]byte("x"))}); err == nil {
			t.Errorf("B wrote into %s", name)
		}
		cb.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bk, Key: aws.String("config")})
	}
	// Naming the lender's own bucket or A's store name is not a sealed
	// name, so B cannot create it either.
	for _, name := range []string{"photos", LentName(a.NodeID(), aSealed), "laptop"} {
		if _, err := cb.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err == nil {
			t.Errorf("B created %s", name)
		}
	}
	// B creating the same sealed label gets its own bucket, not A's.
	if _, err := cb.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(aSealed)}); err != nil {
		t.Fatal(err)
	}
	if _, err := cb.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(aSealed), Key: aws.String("config")}); code(err) != "NoSuchKey" {
		t.Fatalf("B reading its own same-named bucket: %v", err)
	}
	// Nothing of A's or the lender's changed, and only names that are
	// the lender's own or lent-<id16>-<sealed> are in the store.
	for _, p := range []string{filepath.Join(h.store, LentName(a.NodeID(), aSealed), "config"), filepath.Join(h.store, "photos", "config")} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
	entries, _ := os.ReadDir(h.store)
	for _, e := range entries {
		n := e.Name()
		if n != "photos" && n != LentName(a.NodeID(), aSealed) && n != LentName(b.NodeID(), aSealed) && n[0] != '.' {
			t.Errorf("store holds %s", n)
		}
	}
	if _, err := os.Stat(filepath.Join(h.store, LentName(a.NodeID(), aSealed), "planted")); !os.IsNotExist(err) {
		t.Fatal("B planted an object in A's bucket")
	}
	// The lender's own key does not reach A's bucket either.
	if _, err := own.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(LentName(a.NodeID(), aSealed)), Key: aws.String("config")}); err == nil {
		t.Fatal("an own bucket's key read a lent bucket")
	}
}

// NET-7a, SEC-11: a node ID resolves only while a friendship exists, with
// a secret computed on demand; a stranger's correctly derived credential
// and a friend's wrong secret are refused, and a removed friend's next
// request is refused.
func TestNodeIDAccountNeedsFriendship(t *testing.T) {
	h := newHarness(t, nil)
	h.self = newIdentity(t)
	friend, stranger := newIdentity(t), newIdentity(t)
	h.befriend(friend)
	h.serve(false)
	ctx := context.Background()
	if _, err := h.pairClient(friend).ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil {
		t.Fatalf("friend: %v", err)
	}
	if _, err := h.pairClient(stranger).ListBuckets(ctx, &s3.ListBucketsInput{}); code(err) != "InvalidAccessKeyId" {
		t.Fatalf("stranger: %v", err)
	}
	if _, err := h.borrowerClient(friend.NodeID().String(), "wrong").ListBuckets(ctx, &s3.ListBucketsInput{}); code(err) != "SignatureDoesNotMatch" {
		t.Fatalf("wrong secret: %v", err)
	}
	if _, err := h.db.ExecContext(ctx, `DELETE FROM friendship WHERE peer_key = ?`, friend.NodeID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pairClient(friend).ListBuckets(ctx, &s3.ListBucketsInput{}); code(err) != "InvalidAccessKeyId" {
		t.Fatalf("removed friend: %v", err)
	}
}
