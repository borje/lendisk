package gateway

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// harness is a store, a state database with own buckets and, once
// started, a gateway serving them on its socket.
type harness struct {
	t      *testing.T
	store  string
	db     *state.DB
	socket string
	keys   map[string][2]string
	self   *identity.Identity // the lender, for friends' accounts; nil serves none
}

func newHarness(t *testing.T, buckets map[string]string) *harness {
	t.Helper()
	return newHarnessIn(t, t.TempDir(), buckets)
}

func newHarnessIn(t *testing.T, dir string, buckets map[string]string) *harness {
	t.Helper()
	h := &harness{t: t, store: filepath.Join(dir, "store"), keys: map[string][2]string{}}
	os.MkdirAll(h.store, 0o700)
	db, err := state.Open(context.Background(), dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	h.db = db
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	for name, prof := range buckets {
		access, secret := "LDK"+strings.ToUpper(randomHex(8)), randomHex(20)
		if err := db.AddBucket(context.Background(), state.Bucket{Name: name, Profile: prof, Copies: 2,
			AccessKey: access, SecretKey: secret, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if err := CreateBucket(context.Background(), be, name, access); err != nil {
			t.Fatal(err)
		}
		h.keys[name] = [2]string{access, secret}
	}
	return h
}

// serve runs the gateway until stop is called or the test ends.
func (h *harness) serve(noCapture bool) (stop func()) {
	h.t.Helper()
	g := &Gateway{StoreDir: h.store, Socket: filepath.Join(h.t.TempDir(), "gw.sock"), DB: h.db, Self: h.self, Log: quiet,
		ResolveEvery: 20 * time.Millisecond, noCapture: noCapture}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errc := make(chan error, 1)
	go func() { errc <- g.Run(ctx, ready) }()
	select {
	case <-ready:
	case err := <-errc:
		h.t.Fatalf("gateway: %v", err)
	case <-time.After(30 * time.Second):
		h.t.Fatal("gateway not ready")
	}
	h.socket = g.Socket
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); within(h.t, "the gateway stopping", errc) }) }
	h.t.Cleanup(stop)
	return stop
}

func (h *harness) client(bucket string) *s3.Client {
	k := h.keys[bucket]
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", h.socket)
	}}}
	return s3.New(s3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String("http://gw"), UsePathStyle: true, HTTPClient: hc,
		Credentials: credentials.NewStaticCredentialsProvider(k[0], k[1], ""), RetryMaxAttempts: 1,
	})
}

func (h *harness) entries(bucket string) []state.Entry {
	h.t.Helper()
	es, err := h.db.QueueEntries(context.Background(), bucket)
	if err != nil {
		h.t.Fatal(err)
	}
	return es
}

// settled waits until no queue row is pending: a completed upload's row
// is finished in the background after the client's answer.
func (h *harness) settled() {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for p, _ := h.db.PendingCaptures(context.Background()); len(p) != 0; p, _ = h.db.PendingCaptures(context.Background()) {
		if time.Now().After(deadline) {
			h.t.Fatalf("still pending: %+v", p)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func sum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// EP-2, EP-3, EP-7: successful puts, completed uploads, deletes and each
// deleted entry of a multi-delete are queued, with the stream's SHA-256;
// parts, aborted uploads, failed requests and local-only keys are not.
func TestCaptureWritesAndDeletes(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	c := h.client("laptop")
	ctx := context.Background()
	b := aws.String("laptop")
	put := func(key string, body []byte, opt ...func(*s3.PutObjectInput)) error {
		in := &s3.PutObjectInput{Bucket: b, Key: aws.String(key), Body: bytes.NewReader(body)}
		for _, o := range opt {
			o(in)
		}
		_, err := c.PutObject(ctx, in)
		return err
	}
	pack := bytes.Repeat([]byte("pack"), 100_000)
	if err := put("data/aa/pack1", pack); err != nil {
		t.Fatal(err)
	}
	if err := put("locks/lock1", []byte("lock")); err != nil {
		t.Fatal(err)
	}
	// Local-only keys are stored and served (EP-7).
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: aws.String("locks/lock1")})
	if err != nil {
		t.Fatal(err)
	}
	if body, _ := io.ReadAll(got.Body); string(body) != "lock" {
		t.Fatalf("lock reads %q", body)
	}
	// A failed request is not queued.
	if err := put("data/aa/pack1", []byte("other"), func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") }); err == nil {
		t.Fatal("conditional put over an existing key succeeded")
	}
	if err := put("config", []byte("cfg")); err != nil {
		t.Fatal(err)
	}

	// Multipart: two parts uploaded, then completed; a third upload aborted.
	part1, part2 := bytes.Repeat([]byte{1}, 5<<20), []byte("tail")
	up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("data/bb/big")})
	if err != nil {
		t.Fatal(err)
	}
	var done []types.CompletedPart
	for i, p := range [][]byte{part1, part2} {
		n := int32(i + 1)
		r, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String("data/bb/big"), UploadId: up.UploadId,
			PartNumber: &n, Body: bytes.NewReader(p)})
		if err != nil {
			t.Fatal(err)
		}
		done = append(done, types.CompletedPart{PartNumber: &n, ETag: r.ETag})
	}
	if n := len(h.entries("laptop")); n != 2 {
		t.Fatalf("%d entries before completion, want 2 (parts are not queued)", n)
	}
	complete := &s3.CompleteMultipartUploadInput{Bucket: b, Key: aws.String("data/bb/big"),
		UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: done}}
	if _, err := c.CompleteMultipartUpload(ctx, complete); err != nil {
		t.Fatal(err)
	}
	// A client's retry of the completion (its answer was lost) succeeds
	// in the store and is not queued again.
	if _, err := c.CompleteMultipartUpload(ctx, complete); err != nil {
		t.Fatalf("retried completion: %v", err)
	}
	ab, _ := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("data/cc/aborted")})
	one := int32(1)
	c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String("data/cc/aborted"), UploadId: ab.UploadId,
		PartNumber: &one, Body: strings.NewReader("x")})
	if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: b, Key: aws.String("data/cc/aborted"),
		UploadId: ab.UploadId}); err != nil {
		t.Fatal(err)
	}

	// Deletes: one, then a multi-delete with a local-only key in it.
	put("data/dd/gone", []byte("g"))
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: aws.String("data/dd/gone")}); err != nil {
		t.Fatal(err)
	}
	put("index/ee", []byte("i"))
	if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{Objects: []types.ObjectIdentifier{
		{Key: aws.String("index/ee")}, {Key: aws.String("locks/lock1")}}}}); err != nil {
		t.Fatal(err)
	}

	h.settled()
	type row struct{ key, op, class, sha string }
	var rows []row
	for _, e := range h.entries("laptop") {
		if e.State != state.StateDone {
			t.Errorf("row %d left %s", e.Seq, e.State)
		}
		rows = append(rows, row{e.Key, e.Op, e.Class, e.SHA256})
	}
	// A multipart object's hash is the SHA-256 of the whole object (§3.3).
	whole := sum(append(append([]byte{}, part1...), part2...))
	want := []row{
		{"data/aa/pack1", "put", "data", sum(pack)},
		{"config", "put", "metadata", sum([]byte("cfg"))},
		{"data/bb/big", "put", "data", whole},
		{"data/dd/gone", "put", "data", sum([]byte("g"))},
		{"data/dd/gone", "delete", "data", ""},
		{"index/ee", "put", "metadata", sum([]byte("i"))},
		{"index/ee", "delete", "metadata", ""},
	}
	if len(rows) != len(want) {
		t.Fatalf("queue:\n%v\nwant\n%v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("row %d = %v, want %v", i, rows[i], want[i])
		}
	}
	if es := h.entries("laptop"); es[2].Size != 5<<20+4 {
		t.Errorf("multipart entry size %d", es[2].Size)
	}
}

// Q-H: server-side copies are refused on own buckets, since their bytes
// never pass the decorator; nothing is queued.
func TestCopyRefusedOnOwnBuckets(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	c := h.client("laptop")
	ctx := context.Background()
	b := aws.String("laptop")
	c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("data/src"), Body: strings.NewReader("s")})
	if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("data/dst"),
		CopySource: aws.String("laptop/data/src")}); code(err) != "NotImplemented" {
		t.Fatalf("CopyObject: %v", err)
	}
	// The same-key metadata replace is a copy too.
	if _, err := c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: aws.String("data/src"),
		CopySource: aws.String("laptop/data/src"), MetadataDirective: types.MetadataDirectiveReplace,
		Metadata: map[string]string{"a": "b"}}); code(err) != "NotImplemented" {
		t.Fatalf("CopyObject in place: %v", err)
	}
	up, _ := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: aws.String("data/mp")})
	one := int32(1)
	if _, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: b, Key: aws.String("data/mp"), UploadId: up.UploadId,
		PartNumber: &one, CopySource: aws.String("laptop/data/src")}); code(err) != "NotImplemented" {
		t.Fatalf("UploadPartCopy: %v", err)
	}
	if n := len(h.entries("laptop")); n != 1 {
		t.Fatalf("%d entries, want the one put", n)
	}
}

// EP-3: rows a crash left pending are settled against the store before
// anything is served.
func TestPendingResolvedAtStartup(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	ctx := context.Background()
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	b := aws.String("laptop")
	rawPut := func(key, body, token string) {
		t.Helper()
		meta := map[string]string{}
		if token != "" {
			meta[MetaCapture] = token
		}
		n := int64(len(body))
		if _, err := be.PutObject(ctx, s3response.PutObjectInput{Bucket: b, Key: aws.String(key), ContentLength: &n,
			Body: strings.NewReader(body), Metadata: meta}); err != nil {
			t.Fatal(err)
		}
	}
	begin := func(c state.Capture) int64 {
		t.Helper()
		c.Bucket = "laptop"
		if c.Class == "" {
			c.Class = "data"
		}
		s, err := h.db.BeginCaptures(ctx, []state.Capture{c})
		if err != nil {
			t.Fatal(err)
		}
		return s[0]
	}

	// A put that reached the store before the crash, and one that did not.
	landed := begin(state.Capture{Key: "data/landed", Op: state.OpPut, Token: "tok1"})
	rawPut("data/landed", "landed body", "tok1")
	lost := begin(state.Capture{Key: "data/lost", Op: state.OpPut, Token: "tok2"})
	// A put over an existing object that did not reach the store: the
	// object there is the earlier write's.
	rawPut("data/older", "older", "tok-earlier")
	overwrite := begin(state.Capture{Key: "data/older", Op: state.OpPut, Token: "tok3"})
	// Deletes: one done, one not.
	delDone := begin(state.Capture{Key: "data/deleted", Op: state.OpDelete})
	rawPut("data/kept", "k", "")
	delLost := begin(state.Capture{Key: "data/kept", Op: state.OpDelete})
	// A multipart completion that took effect, and one that did not.
	mp := func(key string, complete bool) int64 {
		t.Helper()
		up, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{Bucket: b, Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		one := int32(1)
		body := "part of " + key
		n := int64(len(body))
		out, err := d.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: aws.String(key), UploadId: &up.UploadId,
			PartNumber: &one, ContentLength: &n, Body: strings.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		cps := []types.CompletedPart{{PartNumber: &one, ETag: out.ETag}}
		in := &s3.CompleteMultipartUploadInput{Bucket: b, Key: aws.String(key), UploadId: &up.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: cps}}
		etag, _ := computeETag(up.UploadId, cps)
		seq := begin(state.Capture{Key: key, Op: state.OpPut, UploadID: up.UploadId, ETag: etag})
		if complete {
			if _, _, err := be.CompleteMultipartUpload(ctx, in); err != nil {
				t.Fatal(err)
			}
		}
		return seq
	}
	mpDone := mp("data/mp-done", true)
	mpLost := mp("data/mp-lost", false)

	done, dropped, err := d.ResolvePending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if done != 3 || dropped != 4 {
		t.Errorf("done %d, dropped %d; want 3 and 4", done, dropped)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 0 {
		t.Fatalf("still pending: %+v", p)
	}
	bySeq := map[int64]state.Entry{}
	for _, e := range h.entries("laptop") {
		bySeq[e.Seq] = e
	}
	if e, ok := bySeq[landed]; !ok || e.SHA256 != sum([]byte("landed body")) || e.Size != 11 {
		t.Errorf("landed put: %+v", e)
	}
	for name, s := range map[string]int64{"lost": lost, "overwrite": overwrite, "delete not done": delLost, "multipart not done": mpLost} {
		if _, ok := bySeq[s]; ok {
			t.Errorf("%s kept", name)
		}
	}
	if _, ok := bySeq[delDone]; !ok {
		t.Error("done delete dropped")
	}
	if e := bySeq[mpDone]; e.SHA256 != sum([]byte("part of data/mp-done")) {
		t.Errorf("multipart: %+v", e)
	}
}

func computeETag(uploadID string, parts []types.CompletedPart) (string, error) {
	return backend.ComputeMultipartETagFromPartETags(uploadID, parts)
}

// Q-H: a browser-form POST upload reaches PutObject in versitygw; the
// gate refuses it before routing.
func TestBrowserPostRefused(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", h.socket)
	}}}
	body := "--x\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\ndata/p\r\n--x\r\n" +
		"Content-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nhello\r\n--x--\r\n"
	resp, err := hc.Post("http://gw/laptop", "multipart/form-data; boundary=x", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(b), "browser-form") {
		t.Fatalf("POST upload: %d %s", resp.StatusCode, b)
	}
}
