package replica

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

var quiet = slog.New(slog.DiscardHandler)

func newID(t *testing.T) *identity.Identity {
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

// request is what a lender saw.
type request struct {
	Method, Path string
	Header       http.Header
	Body         []byte
}

// tunnelLender serves h on a tailcat tunnel port with a local relay and
// returns a Friend for self that dials it: the real client, the real
// tunnel, a lender whose answers the test chooses.
func tunnelLender(t *testing.T, self *identity.Identity, h http.Handler) (*Friend, identity.NodeID) {
	t.Helper()
	d := transporttest.Start(t)
	lender := newID(t)
	srv, err := transport.NewTailcat(lender, d.Region, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.Admit(func(identity.NodeID) bool { return true })
	ln, err := srv.Listen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: h}
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close() })
	dl, err := transport.NewDialer(self, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	f, err := NewFriend(self, lender.NodeID(), srv.Address(), dl.Dial, Options{DialTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return f, lender.NodeID()
}

// recorder answers every request with 200 and remembers it.
type recorder struct {
	mu   sync.Mutex
	reqs []request
}

func (rec *recorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	rec.mu.Lock()
	rec.reqs = append(rec.reqs, request{r.Method, strings.TrimSuffix(r.URL.Path, "/"), r.Header.Clone(), b})
	rec.mu.Unlock()
	if r.Method == http.MethodPut && strings.Count(strings.Trim(r.URL.Path, "/"), "/") > 0 {
		w.Header().Set("ETag", `"0123"`)
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (rec *recorder) all() []request {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]request(nil), rec.reqs...)
}

func put(t *testing.T, f *Friend, bucket, key string, body []byte) error {
	t.Helper()
	sum := sha256.Sum256(body)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return f.Put(ctx, Object{Bucket: bucket, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:]),
		Body: strings.NewReader(string(body))})
}

// #18, NET-7a, SEC-16, REPL-10: a put to a friend goes through the
// tunnel to the bucket's sealed name, which is created first, signed
// with the node's ID as access key; it carries the capture's SHA-256 as
// metadata and as the request checksum, with an unsigned payload and no
// trailer (D12, Q-Y.2: minio-go's signed trailer is never used).
func TestPutGoesToSealedNameWithHash(t *testing.T) {
	self := newID(t)
	rec := &recorder{}
	f, _ := tunnelLender(t, self, rec)
	body := []byte(strings.Repeat("restic pack ", 5000))
	if err := put(t, f, "laptop", "data/ab/abcd", body); err != nil {
		t.Fatal(err)
	}
	if err := put(t, f, "laptop", "index/01", []byte("index")); err != nil {
		t.Fatal(err)
	}
	reqs := rec.all()
	sealed := "/" + self.SealedName("laptop")
	if len(reqs) != 3 || reqs[0].Method != http.MethodPut || reqs[0].Path != sealed {
		t.Fatalf("want bucket create then two puts, got %v", paths(reqs))
	}
	p := reqs[1]
	if p.Method != http.MethodPut || p.Path != sealed+"/data/ab/abcd" || string(p.Body) != string(body) {
		t.Fatalf("put %s %s, %d bytes", p.Method, p.Path, len(p.Body))
	}
	sum := sha256.Sum256(body)
	if got := p.Header.Get("X-Amz-Meta-Lendisk-Sha256"); got != hex.EncodeToString(sum[:]) {
		t.Errorf("metadata hash %q", got)
	}
	if got := p.Header.Get("X-Amz-Checksum-Sha256"); got != base64.StdEncoding.EncodeToString(sum[:]) {
		t.Errorf("request checksum %q", got)
	}
	if got := p.Header.Get("X-Amz-Content-Sha256"); got != "UNSIGNED-PAYLOAD" {
		t.Errorf("payload %q", got)
	}
	if p.Header.Get("X-Amz-Trailer") != "" || p.Header.Get("Content-Encoding") != "" {
		t.Errorf("trailer or chunked encoding sent: %v", p.Header)
	}
	if a := p.Header.Get("Authorization"); !strings.Contains(a, "Credential="+self.NodeID().String()+"/") {
		t.Errorf("access key is not the node ID: %s", a)
	}
	for _, r := range reqs {
		if strings.Contains(r.Path, "laptop") {
			t.Errorf("the real bucket name went to the friend: %s", r.Path)
		}
	}
	// A delete goes to the same sealed name.
	if err := f.Delete(t.Context(), "laptop", "data/ab/abcd"); err != nil {
		t.Fatal(err)
	}
	if r := rec.all()[3]; r.Method != http.MethodDelete || r.Path != sealed+"/data/ab/abcd" {
		t.Fatalf("delete %s %s", r.Method, r.Path)
	}
}

func paths(rs []request) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.Method+" "+r.Path)
	}
	return out
}

// s3Error answers with an S3 error document.
func s3Error(w http.ResponseWriter, status int, code string) {
	s3ErrorMsg(w, status, code, "test")
}

func s3ErrorMsg(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, msg)
}

// Q-Y.3, LEND-2, PEER-5, EP-11, REPL-10: a friend's quota (507),
// removal (403 saying there is no relationship, or naming an unknown
// access key) and immutability (412) refusals, and a rejected hash,
// reach the worker through the tunnel as distinct errors; anything else,
// other 403s included (#53), is none of them and is retried.
func TestFriendRefusalsAreDistinct(t *testing.T) {
	self := newID(t)
	f, _ := tunnelLender(t, self, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		path := strings.TrimSuffix(r.URL.Path, "/")
		key := path[strings.LastIndex(path, "/")+1:]
		switch {
		case strings.Count(path, "/") == 1: // bucket create
		case key == "quota":
			s3Error(w, 507, "QuotaExceeded")
		case key == "removed":
			s3ErrorMsg(w, 403, "AccessDenied", "lendisk: "+state.NoRelationship)
		case key == "unknown-key":
			s3Error(w, 403, "InvalidAccessKeyId")
		case key == "denied":
			s3Error(w, 403, "AccessDenied")
		case key == "bad-signature":
			s3Error(w, 403, "SignatureDoesNotMatch")
		case key == "skewed":
			s3Error(w, 403, "RequestTimeTooSkewed")
		case key == "immutable":
			s3Error(w, 412, "PreconditionFailed")
		case key == "rotted":
			s3Error(w, 400, "BadDigest")
		case key == "flaky":
			s3Error(w, 500, "InternalError")
		default:
			s3Error(w, 503, "SlowDown")
		}
	}))
	kinds := []error{ErrQuota, ErrRemoved, ErrImmutable, ErrRejected}
	for key, want := range map[string]error{"quota": ErrQuota, "removed": ErrRemoved, "immutable": ErrImmutable,
		"rotted": ErrRejected, "flaky": nil, "busy": nil,
		"unknown-key": ErrRemoved, "denied": nil, "bad-signature": nil, "skewed": nil} {
		err := put(t, f, "laptop", "data/"+key, []byte("x"))
		if err == nil {
			t.Fatalf("%s: accepted", key)
		}
		for _, k := range kinds {
			if errors.Is(err, k) != (k == want) {
				t.Errorf("%s: %v; errors.Is(%v) = %v", key, err, k, errors.Is(err, k))
			}
		}
		// A refusal for a skewed clock says so, and what to do.
		if key == "skewed" && (!strings.Contains(err.Error(), "clock") || !strings.Contains(err.Error(), "NTP")) {
			t.Errorf("skewed: %v", err)
		}
		if key == "bad-signature" && !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
			t.Errorf("bad-signature: %v", err)
		}
	}
}

// A friend that is not there fails within the dial timeout, as a
// retryable error.
func TestUnreachableFriendFailsRetryably(t *testing.T) {
	self, other := newID(t), newID(t)
	d := transporttest.Start(t)
	dl, err := transport.NewDialer(self, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	f, err := NewFriend(self, other.NodeID(), transport.AddressOf(other, d.Region), dl.Dial, Options{DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = put(t, f, "laptop", "data/x", []byte("x"))
	if err == nil || errors.Is(err, ErrQuota) || errors.Is(err, ErrRemoved) || errors.Is(err, ErrImmutable) || errors.Is(err, ErrRejected) {
		t.Fatalf("unreachable friend: %v", err)
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Fatalf("took %v", d)
	}
}
