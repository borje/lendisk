package replica

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/overflow/overflowtest"
	"github.com/borje/lendisk/internal/state"
)

// versitygw runs once per process: the overflow tests take turns.
var s3Held sync.Mutex

func startS3(t *testing.T, versioning bool) *overflowtest.Server {
	t.Helper()
	s3Held.Lock()
	t.Cleanup(s3Held.Unlock)
	return overflowtest.Start(t, "offsite", versioning)
}

func overflowClient(t *testing.T, s *overflowtest.Server, secret string) (*Client, func(string) string) {
	t.Helper()
	self := newID(t)
	o := state.Overflow{Name: "b2", Endpoint: s.Endpoint, Bucket: s.Bucket, Prefix: "/lendisk/"}
	c, err := NewOverflow(o, overflow.Credential{AccessKey: s.Access, SecretKey: secret}, self.SealedName, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c, self.SealedName
}

func object(bucket, key string, body []byte) Object {
	sum := sha256.Sum256(body)
	return Object{Bucket: bucket, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), Body: bytes.NewReader(body)}
}

// S3-2, S3-3: an object goes to <prefix>/<sealed name>/<key> in the
// user's bucket, with its SHA-256 as metadata and as the request
// checksum the provider verifies: bytes that do not match it are
// refused, as ErrRejected.
func TestOverflowPutsUnderThePrefixWithAVerifiedChecksum(t *testing.T) {
	s := startS3(t, true)
	c, seal := overflowClient(t, s, s.Secret)
	ctx := t.Context()
	body := []byte("pack bytes")
	if err := c.Put(ctx, object("laptop", "data/ab/abcdef", body)); err != nil {
		t.Fatal(err)
	}
	key := "lendisk/" + seal("laptop") + "/data/ab/abcdef"
	got, sha, err := s.Get(ctx, key)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("at %s: %q, %v", key, got, err)
	}
	if want := object("", "", body).SHA256; sha != want {
		t.Fatalf("lendisk-sha256 %q, want %q", sha, want)
	}
	if h, ok, err := c.Head(ctx, "laptop", "data/ab/abcdef"); err != nil || !ok || h != object("", "", body).SHA256 {
		t.Fatalf("Head = %q %v %v", h, ok, err)
	}
	// Bytes that are not those the hash names.
	bad := object("laptop", "data/ab/bad", []byte("other bytes"))
	bad.SHA256 = object("", "", body).SHA256
	if err := c.Put(ctx, bad); !errors.Is(err, ErrRejected) {
		t.Fatalf("a corrupted put: %v, want ErrRejected", err)
	}
	if _, ok, _ := c.Head(ctx, "laptop", "data/ab/bad"); ok {
		t.Fatal("the provider stored a corrupted transfer")
	}
	// A delete leaves a delete marker in a versioned bucket (S3-4).
	if err := c.Delete(ctx, "laptop", "data/ab/abcdef"); err != nil {
		t.Fatal(err)
	}
	if v, m, err := s.Versions(ctx, key); err != nil || v != 1 || m != 1 {
		t.Fatalf("after delete: %d versions, %d markers, %v", v, m, err)
	}
	if err := c.Probe(ctx); err != nil {
		t.Fatalf("probe: %v", err)
	}
}

// S3-3, REPL-6: a large object goes in parts, each with its own SHA-256,
// through the same multipart calls as a friend's.
func TestOverflowTakesAnObjectInParts(t *testing.T) {
	s := startS3(t, false)
	c, seal := overflowClient(t, s, s.Secret)
	ctx := t.Context()
	body := make([]byte, 5<<20+1000)
	rand.Read(body)
	o := object("laptop", "data/big", body)
	id, err := c.Create(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	p1, err := c.PutPart(ctx, "laptop", "data/big", id, 1, bytes.NewReader(body[:5<<20]), 5<<20)
	if err != nil {
		t.Fatal(err)
	}
	if parts, err := c.Parts(ctx, "laptop", "data/big", id); err != nil || len(parts) != 1 {
		t.Fatalf("parts %v, %v", parts, err)
	}
	p2, err := c.PutPart(ctx, "laptop", "data/big", id, 2, bytes.NewReader(body[5<<20:]), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, "laptop", "data/big", id, []Part{p1, p2}); err != nil {
		t.Fatal(err)
	}
	got, sha, err := s.Get(ctx, "lendisk/"+seal("laptop")+"/data/big")
	if err != nil || !bytes.Equal(got, body) || sha != o.SHA256 {
		t.Fatalf("assembled object: %d bytes, sha %q, %v", len(got), sha, err)
	}
}

// S3-1, S3-4, S3-5: overflow add's check puts, reads and deletes a test
// object, reports versioning, tests version removal on the test
// object's own version, and leaves nothing live behind.
func TestCheckOverflowReportsVersioningAndVersionRemoval(t *testing.T) {
	s := startS3(t, true)
	c, _ := overflowClient(t, s, s.Secret)
	ctx := t.Context()
	ch, err := c.CheckOverflow(ctx, "b2", 30)
	if err != nil {
		t.Fatal(err)
	}
	if ch.Versioning != state.VersioningEnabled {
		t.Errorf("versioning %q", ch.Versioning)
	}
	// The root credential can remove versions: reported, with the advice.
	if ch.VersionRemoval != state.VersionRemovalAllowed || !strings.Contains(strings.Join(ch.Notes, "\n"), "can remove object versions") {
		t.Errorf("version removal %q, notes %q", ch.VersionRemoval, ch.Notes)
	}
	if ch.Lifecycle == "" {
		t.Error("no lifecycle finding")
	}
	keys, err := s.Keys(ctx, "lendisk/")
	if err != nil || len(keys) != 0 {
		t.Fatalf("left behind: %v, %v", keys, err)
	}
}

func TestCheckOverflowWarnsWhenVersioningIsOff(t *testing.T) {
	s := startS3(t, false)
	c, _ := overflowClient(t, s, s.Secret)
	ch, err := c.CheckOverflow(t.Context(), "b2", 0)
	if err != nil {
		t.Fatal(err)
	}
	if notes := strings.Join(ch.Notes, "\n"); ch.Versioning != state.VersioningOff || !strings.Contains(notes, "warning: versioning is off") ||
		!strings.Contains(notes, "a delete there is final") {
		t.Fatalf("check %+v", ch)
	}
}

// S3-1: a wrong credential fails the check, and a put with it is the
// provider's refusal, retried, never a friend ending the friendship.
func TestOverflowWrongCredentialIsRefusedNotRemoved(t *testing.T) {
	s := startS3(t, false)
	c, _ := overflowClient(t, s, "not-the-secret")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if _, err := c.CheckOverflow(ctx, "b2", 0); err == nil || !strings.Contains(err.Error(), "refused the credential") {
		t.Fatalf("check with a wrong secret: %v", err)
	}
	err := c.Put(ctx, object("laptop", "data/x", []byte("x")))
	if err == nil || errors.Is(err, ErrRemoved) || errors.Is(err, ErrQuota) {
		t.Fatalf("put with a wrong secret: %v", err)
	}
	if strings.Contains(err.Error(), "not-the-secret") {
		t.Fatalf("the error shows the secret: %v", err)
	}
}

// SEC-3, #38 review: a provider's error may name the access key (Garage's
// 403 does); the overflow client's errors never show the credential, and
// still say what kind of refusal they are.
func TestOverflowErrorsNeverShowTheCredential(t *testing.T) {
	o := state.Overflow{Name: "b2", Endpoint: "http://127.0.0.1:1", Bucket: "offsite"}
	c, err := NewOverflow(o, overflow.Credential{AccessKey: "GKLEAKEDACCESS", SecretKey: "LEAKEDSECRET"}, func(b string) string { return b }, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []minio.ErrorResponse{
		{StatusCode: 403, Code: "AccessDenied", Message: "Forbidden: No such key: GKLEAKEDACCESS"},
		{StatusCode: 400, Code: "BadDigest", Message: "checksum mismatch for GKLEAKEDACCESS / LEAKEDSECRET"},
	} {
		err := c.classify(fmt.Errorf("sending x: %w", in))
		if strings.Contains(err.Error(), "LEAKED") {
			t.Fatalf("error shows the credential: %v", err)
		}
		var er minio.ErrorResponse
		if !errors.As(err, &er) || er.Code != in.Code {
			t.Fatalf("the provider's error is lost: %v", err)
		}
	}
	if err := c.classify(fmt.Errorf("x: %w", minio.ErrorResponse{StatusCode: 400, Code: "BadDigest"})); !errors.Is(err, ErrRejected) {
		t.Fatalf("BadDigest: %v", err)
	}
}
