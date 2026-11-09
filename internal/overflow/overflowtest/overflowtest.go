// Package overflowtest runs an S3 service in the test process to stand
// in for an overflow target's provider (S3-1, #38): versitygw's posix
// backend on a loopback TCP port, with one bucket, versioning on or off.
// Tests only.
//
// Rules a caller must keep: versitygw runs once per process, so a test
// that starts one holds the process's gateway for its whole run (in
// internal/cli, `serving`), and runs its daemons as processes.
package overflowtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"
)

// Server is a running S3 service with one bucket.
type Server struct {
	Endpoint       string // http://127.0.0.1:port
	Region         string // "" for the default
	Bucket         string
	Access, Secret string
	client         *s3.Client
}

// Start runs the service until t ends, with bucket created and, when
// versioning, versioning enabled on it.
func Start(t *testing.T, bucket string, versioning bool) *Server {
	t.Helper()
	root, vers := t.TempDir(), t.TempDir()
	opts := posix.PosixOpts{AbsolutePaths: true, CopyObjectThreshold: 5 << 30} // undelete copies a version (S3-4)
	if versioning {
		opts.VersioningDir = vers
	}
	be, err := posix.New(root, meta.XattrMeta{}, opts)
	if err != nil {
		t.Fatalf("overflowtest: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	s := &Server{Endpoint: "http://" + addr, Bucket: bucket, Access: "overflowtest-access", Secret: "overflowtest-secret-0123456789"}
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- embedgw.RunVersityGW(ctx, be, &embedgw.Config{RootUserAccess: s.Access, RootUserSecret: s.Secret,
			Ports: []string{addr}, Quiet: true, MaxConnections: 1000, MaxRequests: 1000, MultipartMaxParts: 10000})
	}()
	t.Cleanup(func() {
		cancel()
		<-errc
		be.Shutdown()
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			break
		}
		select {
		case err := <-errc:
			errc <- err
			t.Fatalf("overflowtest: versitygw stopped: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("overflowtest: versitygw not listening after 30s")
		}
	}
	s.client = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(s.Endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(s.Access, s.Secret, ""), RetryMaxAttempts: 1})
	if _, err := s.client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("overflowtest: creating bucket %s: %v", bucket, err)
	}
	if versioning {
		if _, err := s.client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
			t.Fatalf("overflowtest: enabling versioning: %v", err)
		}
	}
	return s
}

// Connect is a Server for a bucket at a provider running elsewhere,
// such as a Garage or RustFS container in CI (DESIGN §3.11): the bucket
// must exist, and the credential must be able to read and list it.
func Connect(t *testing.T, endpoint, region, bucket, access, secret string) *Server {
	t.Helper()
	s := &Server{Endpoint: endpoint, Region: region, Bucket: bucket, Access: access, Secret: secret}
	s.client = s3.New(s3.Options{Region: cmpOr(region, "us-east-1"), BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""), RetryMaxAttempts: 1,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	if _, err := s.client.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("overflowtest: bucket %s at %s: %v", bucket, endpoint, err)
	}
	return s
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// Keys lists every key in the bucket under prefix with its size, in key
// order.
func (s *Server) Keys(ctx context.Context, prefix string) (map[string]int64, error) {
	out := map[string]int64{}
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.Bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			out[aws.ToString(o.Key)] = aws.ToInt64(o.Size)
		}
	}
	return out, nil
}

// SortedKeys is Keys' keys in order.
func SortedKeys(m map[string]int64) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// Get reads key's bytes and its x-amz-meta-lendisk-sha256.
func (s *Server) Get(ctx context.Context, key string) ([]byte, string, error) {
	o, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", err
	}
	defer o.Body.Close()
	b, err := io.ReadAll(o.Body)
	return b, o.Metadata["lendisk-sha256"], err
}

// Put stores body at key with metadata meta, as a provider's own fault
// or another writer would (#39: rot a copy for verification to find).
func (s *Server) Put(ctx context.Context, key string, body []byte, meta map[string]string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), Metadata: meta})
	return err
}

// Versions counts the versions and delete markers of key.
func (s *Server) Versions(ctx context.Context, key string) (versions, markers int, err error) {
	out, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(s.Bucket), Prefix: aws.String(key)})
	if err != nil {
		return 0, 0, err
	}
	for _, v := range out.Versions {
		if aws.ToString(v.Key) == key {
			versions++
		}
	}
	for _, m := range out.DeleteMarkers {
		if aws.ToString(m.Key) == key {
			markers++
		}
	}
	return versions, markers, nil
}

// Download copies every key under prefix into dir as files, with prefix
// taken off: what a lendisk-free restore does with an S3 client (S3-2).
func (s *Server) Download(ctx context.Context, prefix string, write func(rel string, b []byte) error) error {
	keys, err := s.Keys(ctx, prefix)
	if err != nil {
		return err
	}
	for _, k := range SortedKeys(keys) {
		b, _, err := s.Get(ctx, k)
		if err != nil {
			return fmt.Errorf("getting %s: %w", k, err)
		}
		rel := k[len(prefix):]
		if rel == "" {
			return errors.New("a key equal to the prefix")
		}
		if err := write(rel, b); err != nil {
			return err
		}
	}
	return nil
}
