package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/transport"
)

// hashMeta is the hash metadata a borrower sends with an object (REPL-10).
func hashMeta(body []byte) map[string]string {
	return map[string]string{replica.MetaSHA256: sha256Hex(body)}
}

func sha256Hex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// The lender's policy seen by the borrower's own replica client through
// the tunnel (#16): A lends B 1K.
//
//   - LEND-1, LEND-2, Q-I.5: once B's objects fill the quota, the next is
//     refused with 507 QuotaExceeded, which B's client takes as "paused
//     for quota" (replica.ErrQuota), not as a retry.
//   - LEND-4, EP-11: a put of other bytes to a stored key is an
//     immutability refusal (ErrImmutable); the same bytes again are
//     accepted; bytes that do not match the hash sent are rejected
//     (ErrRejected) and not kept.
//   - LEND-4: a put without the hash metadata is refused.
func TestLenderPolicyAsTheBorrowerSeesIt(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	s := a.invite("--quota", "1K")
	if code, _, errOut := b.run("\n", "", "peer", "add", s); code != 0 {
		t.Fatalf("peer add: %d %s", code, errOut)
	}
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID := b.identity()
	dl, err := transport.NewDialer(bID, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	f, err := replica.NewFriend(bID, aKey, aAddr, dl.Dial, replica.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	put := func(key string, body []byte, hash string) error {
		return f.Put(ctx, replica.Object{Bucket: "laptop", Key: key, Size: int64(len(body)), SHA256: hash, Body: bytes.NewReader(body)})
	}
	first := bytes.Repeat([]byte("1"), 600)
	if err := put("data/one", first, sha256Hex(first)); err != nil {
		t.Fatalf("first put: %v", err)
	}
	second := bytes.Repeat([]byte("2"), 600)
	if err := put("data/two", second, sha256Hex(second)); !errors.Is(err, replica.ErrQuota) {
		t.Fatalf("put over quota: %v", err)
	}
	if err := put("data/one", second[:10], sha256Hex(second[:10])); !errors.Is(err, replica.ErrImmutable) {
		t.Fatalf("other bytes to a stored key: %v", err)
	}
	if err := put("data/one", first, sha256Hex(first)); err != nil {
		t.Fatalf("the same bytes again: %v", err)
	}
	if err := put("data/three", second[:10], sha256Hex(first[:10])); !errors.Is(err, replica.ErrRejected) {
		t.Fatalf("bytes that do not match their hash: %v", err)
	}
	if got := lentObjects(t, a, bID, "laptop"); len(got) != 1 || got["data/one"] == "" {
		t.Fatalf("A holds %v; want data/one alone", got)
	}

	creds, _ := pairCreds(t, bID, aKey)
	cb := s3Over(tunnelHTTP(t, bID, aAddr), creds)
	sealed := aws.String(bID.SealedName("laptop"))
	if _, err := cb.PutObject(ctx, &s3.PutObjectInput{Bucket: sealed, Key: aws.String("data/four"), Body: strings.NewReader("x")}); s3Code(err) != "InvalidRequest" {
		t.Fatalf("a put without the hash: %v", err)
	}
}

func (n *node) identity() *identity.Identity {
	n.t.Helper()
	id, err := identity.Load(filepath.Dir(n.config))
	if err != nil {
		n.t.Fatal(err)
	}
	return id
}

// befriend makes borrower a friend of lender, which must be serving.
func befriend(t *testing.T, lender, borrower *node) {
	t.Helper()
	s := lender.invite("--quota", "10G")
	if code, _, errOut := borrower.run("\n", "", "peer", "add", s); code != 0 {
		t.Fatalf("peer add: %d %s", code, errOut)
	}
}

// tunnelHTTP is an HTTP client that reaches lenderAddr through the
// tunnel, dialling with dialKey: what a borrower's replica client uses
// (§3.7 "Dialling a friend"). One per key: two tailcat clients with one
// key displace each other at the relay.
func tunnelHTTP(t *testing.T, dialKey *identity.Identity, lenderAddr string) *http.Client {
	t.Helper()
	dl, err := transport.NewDialer(dialKey, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	them, err := transport.KeyOf(lenderAddr)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dl.Dial(ctx, them, lenderAddr) },
	}}
}

// s3Over is a stock S3 client on hc, signing with creds.
func s3Over(hc *http.Client, creds aws.CredentialsProvider) *s3.Client {
	return s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://lender:9000"), UsePathStyle: true,
		HTTPClient: hc, Credentials: creds, RetryMaxAttempts: 1})
}

// pairCreds is borrower's derived credential at lender (NET-7a).
func pairCreds(t *testing.T, borrower *identity.Identity, lender identity.NodeID) (aws.CredentialsProvider, string) {
	t.Helper()
	secret, err := borrower.S3Secret(lender)
	if err != nil {
		t.Fatal(err)
	}
	return credentials.NewStaticCredentialsProvider(borrower.NodeID().String(), secret, ""), secret
}

func s3Code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// The lending store over the tunnel (#14). B and C befriend A; B
// creates its bucket at A under its sealed name and puts, gets, heads and
// lists objects with a stock S3 client through the tunnel, signing with
// the derived credential.
//
//   - NET-7a AC: neither side writes the per-pair secret to any file.
//   - SEC-12 AC: B's connection carrying C's valid credential is refused,
//     and so is a request with no credential.
//   - PEER-8 AC: C cannot list, read, write into or delete from B's
//     bucket, nor see A's own; each lists its own buckets only.
//   - SEC-16 AC: A's store, database and logs never hold "laptop"; the
//     store holds lent-<id16>-<sealed>.
func TestFriendBorrowsOverTunnel(t *testing.T) {
	a, b, c := friendNode(t), friendNode(t), friendNode(t)
	if code, _, errOut := a.run("", "", "bucket", "add", "photos", "--profile", "restic"); code != 0 {
		t.Fatal(errOut)
	}
	stop := a.serveLogged()
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	befriend(t, a, b)
	befriend(t, a, c)
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID, cID := b.identity(), c.identity()
	bCreds, bSecret := pairCreds(t, bID, aKey)
	cCreds, cSecret := pairCreds(t, cID, aKey)
	ctx := t.Context()

	bConn, cConn := tunnelHTTP(t, bID, aAddr), tunnelHTTP(t, cID, aAddr)
	cb := s3Over(bConn, bCreds)
	sealed := bID.SealedName("laptop")
	bucket := aws.String(sealed)
	if _, err := cb.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
		t.Fatalf("create: %v", err)
	}
	body := bytes.Repeat([]byte("restic pack "), 10_000)
	if _, err := cb.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String("data/ab/abcd"), Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := cb.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("data/ab/abcd")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if data, _ := io.ReadAll(got.Body); !bytes.Equal(data, body) {
		t.Fatal("read back other bytes")
	}
	if h, err := cb.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("data/ab/abcd")}); err != nil || aws.ToInt64(h.ContentLength) != int64(len(body)) {
		t.Fatalf("head: %v", err)
	}
	if l, err := cb.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket}); err != nil || len(l.Contents) != 1 {
		t.Fatalf("list: %v", err)
	}
	if lb, err := cb.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil || len(lb.Buckets) != 1 || aws.ToString(lb.Buckets[0].Name) != sealed {
		t.Fatalf("B's buckets: %v", err)
	}

	// SEC-12 AC: B's own connection, C's valid credential.
	stolen := s3Over(bConn, cCreds)
	if _, err := stolen.ListBuckets(ctx, &s3.ListBucketsInput{}); s3Code(err) != "AccessDenied" {
		t.Fatalf("C's credential on B's connection: %v", err)
	}
	// No credential form at all.
	anon := s3Over(bConn, aws.AnonymousCredentials{})
	if _, err := anon.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("data/ab/abcd")}); s3Code(err) != "AccessDenied" {
		t.Fatalf("unsigned request: %v", err)
	}

	// PEER-8 AC from C, on its own connection with its own credential.
	cc := s3Over(cConn, cCreds)
	if lb, err := cc.ListBuckets(ctx, &s3.ListBucketsInput{}); err != nil || len(lb.Buckets) != 0 {
		t.Fatalf("C lists %v, %v", lb, err)
	}
	for _, name := range []string{sealed, "photos", gateway.LentName(bID.NodeID(), sealed)} {
		bk := aws.String(name)
		if _, err := cc.GetObject(ctx, &s3.GetObjectInput{Bucket: bk, Key: aws.String("data/ab/abcd")}); err == nil {
			t.Errorf("C read from %s", name)
		}
		if _, err := cc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bk}); err == nil {
			t.Errorf("C listed %s", name)
		}
		if _, err := cc.PutObject(ctx, &s3.PutObjectInput{Bucket: bk, Key: aws.String("planted"), Body: strings.NewReader("x")}); err == nil {
			t.Errorf("C wrote into %s", name)
		}
		cc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bk, Key: aws.String("data/ab/abcd")})
	}
	if _, err := cc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")}); err == nil {
		t.Error("C created a bucket under a name that is not sealed")
	}
	if _, err := cb.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("data/ab/abcd")}); err != nil {
		t.Fatalf("B's object after C's attempts: %v", err)
	}

	stored := filepath.Join(a.data, "store", gateway.LentName(bID.NodeID(), sealed))
	if fi, err := os.Stat(filepath.Join(stored, "data", "ab", "abcd")); err != nil || fi.Size() != int64(len(body)) {
		t.Fatalf("A's store does not hold B's object at %s: %v", stored, err)
	}
	_, statusText, _ := a.run("", "", "status")
	_, statusJSON, _ := a.run("", "", "status", "--json")
	log := stop()
	stopped = true

	// SEC-16 AC: nothing of A's holds the real name.
	assertNotIn(t, a, "laptop")
	for what, s := range map[string]string{"log": log, "status": statusText, "status --json": statusJSON} {
		if strings.Contains(s, "laptop") {
			t.Errorf("A's %s holds the bucket name", what)
		}
	}
	// NET-7a AC: no per-pair secret in any file on either side.
	for _, n := range []*node{a, b, c} {
		assertNotIn(t, n, bSecret, cSecret)
	}
	if strings.Contains(log, bSecret) || strings.Contains(log, cSecret) {
		t.Error("A's log holds a per-pair secret")
	}
	entries, _ := os.ReadDir(filepath.Join(a.data, "store"))
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Contains(names, gateway.LentName(bID.NodeID(), sealed)) {
		t.Fatalf("store holds %v", names)
	}
	t.Logf("A's store: %v", names)
}
