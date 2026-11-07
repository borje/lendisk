package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
)

// routedActions records the metrics action versitygw's router names for
// each request it routes: what a request reached, as versitygw sees it.
type actionLog struct {
	mu   sync.Mutex
	seen []string
}

func (a *actionLog) Send(_ fiber.Ctx, _ error, action string, _ int64, _ int) { a.add(action) }
func (a *actionLog) SendWithBucket(_ fiber.Ctx, _ error, action string, _ int64, _ int, _ string) {
	a.add(action)
}
func (a *actionLog) Close() {}
func (a *actionLog) add(s string) {
	a.mu.Lock()
	a.seen = append(a.seen, s)
	a.mu.Unlock()
}
func (a *actionLog) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.seen
	a.seen = nil
	return s
}

// touchedBackend is the unsupported backend, counting the calls
// versitygw makes before any controller: its access and CORS checks.
type touchedBackend struct {
	backend.BackendUnsupported
	calls atomic.Int32
}

func (b *touchedBackend) GetBucketAcl(ctx context.Context, in *s3.GetBucketAclInput) ([]byte, error) {
	b.calls.Add(1)
	return b.BackendUnsupported.GetBucketAcl(ctx, in)
}
func (b *touchedBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	b.calls.Add(1)
	return b.BackendUnsupported.GetBucketPolicy(ctx, bucket)
}
func (b *touchedBackend) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	b.calls.Add(1)
	return b.BackendUnsupported.GetBucketCors(ctx, bucket)
}

// noUsers resolves no access key, so every signed request fails
// authentication after routing.
type noUsers struct{}

func (noUsers) CreateAccount(auth.Account) error { return errAdmin }
func (noUsers) GetUserAccount(string) (auth.Account, error) {
	return auth.Account{}, auth.ErrNoSuchUser
}
func (noUsers) ResolveAccounts(k []string) ([]string, error)      { return k, nil }
func (noUsers) UpdateUserAccount(string, auth.MutableProps) error { return errAdmin }
func (noUsers) DeleteUserAccount(string) error                    { return errAdmin }
func (noUsers) ListUserAccounts() ([]auth.Account, error)         { return nil, errAdmin }
func (noUsers) Shutdown() error                                   { return nil }

// router is versitygw's own router at the pinned version, on a unix
// socket, over a backend that stores nothing, with or without the gate.
type router struct {
	sock    string
	actions *actionLog
	be      *touchedBackend
}

func startRouter(t *testing.T, gated bool) *router {
	t.Helper()
	dir, err := os.MkdirTemp("", "gate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := &router{sock: filepath.Join(dir, "s3.sock"), actions: &actionLog{}, be: &touchedBackend{}}
	opts := []s3api.Option{s3api.WithQuiet(), s3api.WithConcurrencyLimiter(1000, 1000)}
	if gated {
		opts = append(opts, Gate())
	}
	srv, err := s3api.New(r.be, middlewares.RootUserConfig{Access: "root", Secret: "rootsecret"}, "us-east-1",
		noUsers{}, nil, nil, nil, r.actions, opts...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.ServeMultiPort([]string{r.sock}) }()
	t.Cleanup(func() { srv.ShutDown(); <-done })
	for deadline := time.Now().Add(5 * time.Second); ; {
		if c, err := net.Dial("unix", r.sock); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("versitygw's router did not listen")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r
}

// answer is what the router said to one request, and what it routed.
type answer struct {
	status  int
	body    string
	actions []string
	touched int32
}

// send writes the request line exactly as given, so the router decodes
// the path itself.
func (r *router) send(t *testing.T, method, target string, hdr map[string]string) answer {
	t.Helper()
	r.actions.take()
	r.be.calls.Store(0)
	c, err := net.Dial("unix", r.sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: gw\r\nConnection: close\r\nContent-Length: 0\r\n", method, target)
	for k, v := range hdr {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	if _, err := c.Write(b.Bytes()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: method})
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return answer{status: resp.StatusCode, body: string(body), actions: r.actions.take(), touched: r.be.calls.Load()}
}

// target is op's example request line target.
func (op Op) target() string {
	p := map[Level]string{Service: "/", Bucket: "/sbucket", Object: "/sbucket/data/ab/cd"}[op.Level]
	if op.Query != "" {
		p += "?" + op.Query
	}
	return p
}

func (op Op) headers(h map[string]string) map[string]string {
	if op.Header != "" {
		h[op.Header] = "sbucket/other"
	}
	return h
}

// signedAs is a SigV4 Authorization naming access, which versitygw
// will not verify: enough for the gate, which reads it unverified.
func signedAs(access string) map[string]string {
	return map[string]string{
		"Authorization": "AWS4-HMAC-SHA256 Credential=" + access + "/20261107/us-east-1/s3/aws4_request, " +
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=00ff",
		"X-Amz-Date":           "20261107T000000Z",
		"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
	}
}

// SEC-9: each example in Surface reaches the action it names through
// versitygw's own router, so the table speaks of what versitygw does.
func TestSurfaceExamplesRouteAsNamed(t *testing.T) {
	r := startRouter(t, false)
	for _, op := range Surface {
		a := r.send(t, op.Method, op.target(), op.headers(map[string]string{}))
		if !slices.Equal(a.actions, []string{op.Action}) {
			t.Errorf("%s %s routed to %v, want %s (%d %s)", op.Method, op.target(), a.actions, op.Action, a.status, a.body)
		}
	}
}

// NET-7, SEC-9 AC: from a borrower's access key the gate lets through
// exactly the admitted ops and refuses every other one before versitygw
// routes it or the backend is touched.
func TestGateAdmitsNET7ListOnly(t *testing.T) {
	r := startRouter(t, true)
	friend := newIdentity(t).NodeID().String()
	for _, op := range Surface {
		a := r.send(t, op.Method, op.target(), op.headers(signedAs(friend)))
		switch op.Class {
		case Admitted:
			if !slices.Equal(a.actions, []string{op.Action}) {
				t.Errorf("admitted %s (%s %s) did not reach versitygw: %d %s", op.Action, op.Method, op.target(), a.status, a.body)
			}
		case Refused:
			if a.status != http.StatusForbidden || len(a.actions) != 0 || a.touched != 0 || !strings.Contains(a.body, "lendisk:") {
				t.Errorf("refused %s (%s %s) from a borrower: %d, routed %v, backend touched %d times: %s",
					op.Action, op.Method, op.target(), a.status, a.actions, a.touched, a.body)
			}
		}
	}
}

// NET-7, RET-2: an admitted op is admitted only in its exact shape. A
// version id, another op's query key, a copy source, object-lock, ACL,
// tagging or encryption headers, or a key under .lendisk/ is refused from
// a borrower; the same request from an own bucket's key is not the
// borrower list's business and reaches versitygw.
func TestGateRefusesBorrowerVariants(t *testing.T) {
	r := startRouter(t, true)
	friend := newIdentity(t).NodeID().String()
	with := func(h map[string]string, kv ...string) map[string]string {
		for i := 0; i+1 < len(kv); i += 2 {
			h[kv[i]] = kv[i+1]
		}
		return h
	}
	cases := []struct {
		name, method, target string
		hdr                  []string
	}{
		{"versioned get", "GET", "/sbucket/k?versionId=v1", nil},
		{"versioned head", "HEAD", "/sbucket/k?versionId=v1", nil},
		{"versioned delete", "DELETE", "/sbucket/k?versionId=null", nil},
		{"get a part", "GET", "/sbucket/k?partNumber=1", nil},
		{"list v2 with versions", "GET", "/sbucket?list-type=2&versions", nil},
		{"list v2 with tagging", "GET", "/sbucket?list-type=2&tagging", nil},
		{"list type 1", "GET", "/sbucket?list-type=1", nil},
		{"uploads and uploadId", "POST", "/sbucket/k?uploads&uploadId=u", nil},
		{"put with tagging query", "PUT", "/sbucket/k?tagging", nil},
		{"put with a copy source", "PUT", "/sbucket/k", []string{"X-Amz-Copy-Source", "sbucket/x"}},
		{"part with a copy source", "PUT", "/sbucket/k?uploadId=u&partNumber=1", []string{"X-Amz-Copy-Source", "sbucket/x"}},
		{"put with lock mode", "PUT", "/sbucket/k", []string{"X-Amz-Object-Lock-Mode", "COMPLIANCE"}},
		{"put with legal hold", "PUT", "/sbucket/k", []string{"X-Amz-Object-Lock-Legal-Hold", "ON"}},
		{"put with tags", "PUT", "/sbucket/k", []string{"X-Amz-Tagging", "a=b"}},
		{"upload with tags", "POST", "/sbucket/k?uploads", []string{"X-Amz-Tagging", "a=b"}},
		{"put with an ACL", "PUT", "/sbucket/k", []string{"X-Amz-Acl", "public-read"}},
		{"bucket with a grant", "PUT", "/sbucket", []string{"X-Amz-Grant-Read", "id=x"}},
		{"put with SSE-C", "PUT", "/sbucket/k", []string{"X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256"}},
		{"delete bypassing governance", "DELETE", "/sbucket/k", []string{"X-Amz-Bypass-Governance-Retention", "true"}},
		{"get under .lendisk", "GET", "/sbucket/.lendisk/tomb/k", nil},
		{"put under .lendisk", "PUT", "/sbucket/.lendisk/tomb/k", nil},
		{"delete under .LENDISK", "DELETE", "/sbucket/.LENDISK/tomb/k", nil},
		{"delete under encoded .lendisk", "DELETE", "/sbucket/%2Elendisk/tomb/k", nil},
		{"delete through ..", "DELETE", "/sbucket/data/../.lendisk/tomb/k", nil},
		{"delete through encoded ..", "DELETE", "/sbucket/data/%2E%2E/.lendisk/tomb/k", nil},
		{"key with an empty segment", "PUT", "/sbucket/a//b", nil},
		{"delete the bucket by a trailing slash", "DELETE", "/sbucket/", nil},
		{"options", "OPTIONS", "/sbucket/k", []string{"Origin", "http://x", "Access-Control-Request-Method", "PUT"}},
		{"trace", "TRACE", "/sbucket/k", nil},
	}
	for _, c := range cases {
		a := r.send(t, c.method, c.target, with(signedAs(friend), c.hdr...))
		if a.status != http.StatusForbidden && a.status != http.StatusBadRequest || len(a.actions) != 0 || a.touched != 0 || c.method != "HEAD" && !strings.Contains(a.body, "lendisk:") {
			t.Errorf("%s (%s %s) from a borrower: %d, routed %v, touched %d: %s", c.name, c.method, c.target, a.status, a.actions, a.touched, a.body)
		}
	}
	// The borrower list is keyed on the node ID: an own bucket's key
	// reaches versitygw with a version id.
	a := r.send(t, "GET", "/sbucket/k?versionId=v1", signedAs("LDKAAAAAAAAAAAAAAAAA"))
	if !slices.Equal(a.actions, []string{"s3_GetObject"}) {
		t.Fatalf("an own key's versioned get: %d %v", a.status, a.actions)
	}
	// A friend's admitted request with the signature in the query, and
	// with the SDK's x-id, passes.
	q := "/sbucket/k?x-id=GetObject&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + friend +
		"%2F20261107%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261107T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=00ff"
	if a := r.send(t, "GET", q, nil); !slices.Equal(a.actions, []string{"s3_GetObject"}) {
		t.Fatalf("a presigned get: %d %v %s", a.status, a.actions, a.body)
	}
	// A presigned URL naming the friend is the friend, however the
	// header reads.
	if a := r.send(t, "GET", "/sbucket/k?versionId=v1&X-Amz-Credential="+friend+"%2F20261107%2Fus-east-1%2Fs3%2Faws4_request", nil); a.status != http.StatusForbidden || len(a.actions) != 0 {
		t.Fatalf("a presigned versioned get: %d %v", a.status, a.actions)
	}
}

// #54 review: the control path is refused through fiber's own path
// handling, in each spelling versitygw would decode to it, not only when
// refuse is called with the decoded string.
func TestGateRefusesControlPathAsFiberDecodes(t *testing.T) {
	r := startRouter(t, true)
	for _, target := range []string{"/_lendisk/version", "/%5Flendisk/x", "/%5flendisk/x", "//_lendisk/x", "/_lendisk%2Fx",
		"/_lendisk", "/./_lendisk/x", "/b/%2E%2E/_lendisk/x", "/b/..%2F_lendisk/x"} {
		for _, hdr := range []map[string]string{{}, signedAs(newIdentity(t).NodeID().String())} {
			a := r.send(t, "GET", target, hdr)
			if a.status != http.StatusForbidden || len(a.actions) != 0 || !strings.Contains(a.body, "control API") {
				t.Errorf("GET %s: %d, routed %v: %s", target, a.status, a.actions, a.body)
			}
		}
	}
	if a := r.send(t, "GET", "/sbucket/_lendisk/x", nil); len(a.actions) != 1 {
		t.Fatalf("a key named _lendisk was refused: %d %s", a.status, a.body)
	}
}

// storeTree lists every file and directory under dir with its size
// and modification time.
func storeTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		out[p] = fmt.Sprintf("%v %d %d", fi.Mode(), fi.Size(), fi.ModTime().UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// #14 review, NET-7: the calls gateway.Lending maps to a borrower's own
// bucket but does not refuse (object lock, retention, legal hold,
// tagging, versioning, CORS, website) and every other category outside
// the list are refused by the gate, with a stock S3 client on the real
// store: AccessDenied with the gate's reason, and nothing in the store
// changes.
func TestBorrowerRefusedPerCategory(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	body := []byte("a pack")
	if err := l.put("data/ab/pack", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	k := aws.String("data/ab/pack")
	b := l.bucket
	tags := &types.Tagging{TagSet: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}
	cats := map[string][]func() error{
		"object lock": {
			func() error {
				_, err := l.c.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: b,
					ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled}})
				return err
			},
			func() error {
				_, err := l.c.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: b})
				return err
			},
		},
		"retention": {
			func() error {
				_, err := l.c.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: b, Key: k,
					Retention: &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: aws.Time(time.Now().Add(time.Hour))}})
				return err
			},
			func() error {
				_, err := l.c.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: b, Key: k})
				return err
			},
		},
		"legal hold": {
			func() error {
				_, err := l.c.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: b, Key: k,
					LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
				return err
			},
			func() error {
				_, err := l.c.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: b, Key: k})
				return err
			},
		},
		"tagging": {
			func() error {
				_, err := l.c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: b, Key: k, Tagging: tags})
				return err
			},
			func() error {
				_, err := l.c.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{Bucket: b, Key: k})
				return err
			},
			func() error {
				_, err := l.c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: b, Key: k})
				return err
			},
			func() error {
				_, err := l.c.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{Bucket: b, Tagging: tags})
				return err
			},
			func() error {
				_, err := l.c.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("data/tagged"), Body: bytes.NewReader(body),
					Metadata: hashMeta(body), Tagging: aws.String("a=b")})
				return err
			},
		},
		"versioning": {
			func() error {
				_, err := l.c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b,
					VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
				return err
			},
			func() error {
				_, err := l.c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k, VersionId: aws.String("null")})
				return err
			},
			func() error {
				_, err := l.c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k, VersionId: aws.String("null")})
				return err
			},
		},
		"CORS": {
			func() error {
				_, err := l.c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: b, CORSConfiguration: &types.CORSConfiguration{
					CORSRules: []types.CORSRule{{AllowedMethods: []string{"GET"}, AllowedOrigins: []string{"*"}}}}})
				return err
			},
			func() error {
				_, err := l.c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.DeleteBucketCors(ctx, &s3.DeleteBucketCorsInput{Bucket: b})
				return err
			},
		},
		"website": {
			func() error {
				_, err := l.c.PutBucketWebsite(ctx, &s3.PutBucketWebsiteInput{Bucket: b, WebsiteConfiguration: &types.WebsiteConfiguration{
					IndexDocument: &types.IndexDocument{Suffix: aws.String("index.html")}}})
				return err
			},
			func() error {
				_, err := l.c.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.DeleteBucketWebsite(ctx, &s3.DeleteBucketWebsiteInput{Bucket: b})
				return err
			},
		},
		"ACL and policy": {
			func() error {
				_, err := l.c.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: b, Key: k, ACL: types.ObjectCannedACLPublicRead})
				return err
			},
			func() error {
				_, err := l.c.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: b, Key: k})
				return err
			},
			func() error {
				_, err := l.c.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: b, ACL: types.BucketCannedACLPublicRead})
				return err
			},
			func() error {
				_, err := l.c.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{Bucket: b, Policy: aws.String(`{"Statement":[]}`)})
				return err
			},
		},
		"lifecycle": {
			func() error {
				_, err := l.c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: b,
					LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{Status: types.ExpirationStatusEnabled,
						Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)}}}}})
				return err
			},
		},
		"bulk delete and bucket delete": {
			func() error {
				_, err := l.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: k}}}})
				return err
			},
			func() error {
				_, err := l.c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: b})
				return err
			},
		},
		"copies": {
			func() error {
				_, err := l.c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("data/copy"), CopySource: aws.String(*b + "/data/ab/pack")})
				return err
			},
		},
		"listings outside the list": {
			func() error {
				_, err := l.c.ListObjects(ctx, &s3.ListObjectsInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: b})
				return err
			},
			func() error {
				_, err := l.c.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{Bucket: b, Key: k,
					ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectSize}})
				return err
			},
			func() error {
				_, err := l.c.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: b})
				return err
			},
		},
	}
	before := storeTree(t, l.h.store)
	for cat, calls := range cats {
		for i, call := range calls {
			err := call()
			var ae smithy.APIError
			if !errors.As(err, &ae) || ae.ErrorCode() != "AccessDenied" || !strings.Contains(ae.ErrorMessage(), "friend's node") {
				t.Errorf("%s #%d: %v; want the gate's AccessDenied", cat, i, err)
			}
		}
	}
	if after := storeTree(t, l.h.store); len(after) != len(before) {
		t.Fatalf("the store changed: %d entries before, %d after", len(before), len(after))
	} else {
		for p, v := range before {
			if after[p] != v {
				t.Errorf("%s changed: %s, then %s", p, v, after[p])
			}
		}
	}
	// The admitted list still works.
	if got, err := l.c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k, Range: aws.String("bytes=0-1")}); err != nil {
		t.Fatalf("ranged get: %v", err)
	} else if data, _ := io.ReadAll(got.Body); string(data) != "a " {
		t.Fatalf("ranged get read %q", data)
	}
}
