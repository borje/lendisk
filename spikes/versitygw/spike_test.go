package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// One gateway per process (embedgw allows no second instance), shared by
// every test.
var (
	sockPath string
	logMu    sync.Mutex
	logLines []string
)

func logf(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	line := fmt.Sprintf(format, args...)
	logLines = append(logLines, line)
	fmt.Println("  |", line)
}

func logsContaining(sub string) []string {
	logMu.Lock()
	defer logMu.Unlock()
	var out []string
	for _, l := range logLines {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return out
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "vgw-spike-")
	if err != nil {
		panic(err)
	}
	sockPath = filepath.Join(dir, "run", "gw.sock")
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- runGateway(ctx, filepath.Join(dir, "store"), sockPath, logf) }()
	for i := 0; ; i++ {
		if c, err := net.Dial("unix", sockPath); err == nil {
			c.Close()
			break
		}
		select {
		case err := <-errc:
			panic(fmt.Sprintf("gateway exited: %v", err))
		case <-time.After(50 * time.Millisecond):
		}
		if i > 100 {
			panic("gateway did not come up")
		}
	}
	code := m.Run()
	cancel()
	<-errc
	os.RemoveAll(dir)
	os.Exit(code)
}

func unixHTTP() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sockPath)
		},
	}}
}

func client(access, secret string) *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://gw.internal"),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(access, secret, ""),
		HTTPClient:   unixHTTP(),
	})
}

// apiErr extracts what an S3 client sees: HTTP status, code, message.
func apiErr(t *testing.T, err error) (int, string, string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var re *awshttp.ResponseError
	status := 0
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	var ae smithy.APIError
	code, msg := "", ""
	if errors.As(err, &ae) {
		code, msg = ae.ErrorCode(), ae.ErrorMessage()
	}
	return status, code, msg
}

func TestAccountReachesBackend(t *testing.T) {
	ctx := context.Background()
	c := client(spikeAccess, spikeSecret)
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("acct")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("acct"), Key: aws.String("data/one"),
		Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("acct"), Key: aws.String("data/one")})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(out.Body)
	out.Body.Close()
	if string(b) != "hello" {
		t.Fatalf("got %q", b)
	}
	for _, op := range []string{"CreateBucket bucket=acct", "PutObject bucket=acct key=data/one", "GetObject bucket=acct key=data/one"} {
		lines := logsContaining(op)
		if len(lines) == 0 {
			t.Fatalf("decorator did not see %s", op)
		}
		if !strings.Contains(lines[0], "account="+spikeAccess+"(role=userplus)") {
			t.Fatalf("account missing in %q", lines[0])
		}
		t.Logf("seen: %s", lines[0])
	}
}

func TestCustomErrorsReachClient(t *testing.T) {
	ctx := context.Background()
	c := client(spikeAccess, spikeSecret)
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("errs")}); err != nil {
		t.Fatal(err)
	}
	put := func(key string) error {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("errs"), Key: aws.String(key), Body: strings.NewReader("x"),
		})
		return err
	}
	cases := []struct {
		key        string
		status     int
		code, msgs string
	}{
		{"spike-quota/a", 507, "QuotaExceeded", "lent quota exhausted"},
		{"spike-precond/a", 412, "PreconditionFailed", "conditions you specified"},
		{"spike-denied/a", 403, "AccessDenied", "lendisk:quota-exceeded"},
		{"spike-wrapped/a", 500, "InternalError", ""},
	}
	for _, tc := range cases {
		status, code, msg := apiErr(t, put(tc.key))
		t.Logf("PutObject %s -> HTTP %d %s %q", tc.key, status, code, msg)
		if status != tc.status || code != tc.code || !strings.Contains(msg, tc.msgs) {
			t.Errorf("%s: got %d %s %q", tc.key, status, code, msg)
		}
	}
	// HEAD has no body: only the status survives.
	_, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("errs"), Key: aws.String("spike-quota/a")})
	status, code, _ := apiErr(t, err)
	t.Logf("HeadObject spike-quota/a -> HTTP %d code=%q", status, code)
	if status != 507 {
		t.Errorf("HEAD status %d", status)
	}
}

// TestUnusedFeatures: what happens when a client asks for versioning or
// object lock on a posix store started without a versioning directory.
func TestUnusedFeatures(t *testing.T) {
	ctx := context.Background()
	c := client(spikeAccess, spikeSecret)
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("feat")}); err != nil {
		t.Fatal(err)
	}
	_, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String("feat"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	if err == nil {
		t.Errorf("PutBucketVersioning succeeded")
	} else {
		s, code, msg := apiErr(t, err)
		t.Logf("PutBucketVersioning Enabled -> HTTP %d %s %q", s, code, msg)
	}
	lock := func(bucket string) error {
		_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket), ObjectLockEnabledForBucket: aws.Bool(true),
		})
		return err
	}
	gateOn.Store(false)
	err = lock("feat-lock")
	gateOn.Store(true)
	if err != nil {
		t.Fatalf("without gate: %v", err)
	}
	out, err := c.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String("feat-lock")})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("without gate: CreateBucket with object lock -> OK, lock config enabled=%s",
		out.ObjectLockConfiguration.ObjectLockEnabled)
	s, code, msg := apiErr(t, lock("feat-lock2"))
	t.Logf("with gate: CreateBucket with object lock -> HTTP %d %s %q", s, code, msg)
	if s != 403 {
		t.Errorf("gate did not refuse")
	}
}

// TestAdminAPIOnS3Socket: with AdminPorts empty, embedgw mounts the
// admin API (PATCH /create-user, /list-users, ...) on the S3 socket. It
// needs the admin role, which only the root account has.
func TestAdminAPIOnS3Socket(t *testing.T) {
	try := func(access, secret string) (int, string) {
		req, _ := http.NewRequest(http.MethodPatch, "http://gw.internal/list-users", nil)
		sum := sha256.Sum256(nil)
		ph := hex.EncodeToString(sum[:])
		req.Header.Set("X-Amz-Content-Sha256", ph)
		err := v4.NewSigner().SignHTTP(context.Background(),
			aws.Credentials{AccessKeyID: access, SecretAccessKey: secret},
			req, ph, "s3", "us-east-1", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		resp, err := unixHTTP().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	gateOn.Store(false)
	s, body := try(spikeAccess, spikeSecret)
	t.Logf("without gate: PATCH /list-users as borrower -> %d %s", s, oneLine(body))
	if s != 403 {
		t.Errorf("borrower got %d", s)
	}
	s, body = try("root-not-used", "root-not-used-secret-0123456789")
	t.Logf("without gate: PATCH /list-users as root -> %d %s", s, oneLine(body))
	if s != 200 {
		t.Errorf("root got %d", s)
	}
	gateOn.Store(true)
	s, body = try("root-not-used", "root-not-used-secret-0123456789")
	t.Logf("with gate: PATCH /list-users as root -> %d %s", s, oneLine(body))
	if s != 403 {
		t.Errorf("gate let PATCH through: %d", s)
	}
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// TestListeners: the process holds exactly one listening socket, the
// unix socket, and no TCP or UDP socket at all.
func TestListeners(t *testing.T) {
	inodes := map[string]bool{}
	fds, _ := os.ReadDir("/proc/self/fd")
	for _, fd := range fds {
		l, err := os.Readlink("/proc/self/fd/" + fd.Name())
		if err == nil && strings.HasPrefix(l, "socket:[") {
			inodes[strings.TrimSuffix(strings.TrimPrefix(l, "socket:["), "]")] = true
		}
	}
	for _, f := range []string{"tcp", "tcp6", "udp", "udp6"} {
		data, err := os.ReadFile("/proc/self/net/" + f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Scan()
		for sc.Scan() {
			fs := strings.Fields(sc.Text())
			if len(fs) > 9 && inodes[fs[9]] {
				// The test's own client connections to the unix socket
				// are not TCP; any TCP/UDP socket here is versitygw's.
				t.Errorf("%s socket held by process: %s", f, sc.Text())
			}
		}
	}
	data, _ := os.ReadFile("/proc/self/net/unix")
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Scan()
	var listening []string
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		// Flags 00010000 = __SO_ACCEPTCON (listening).
		if len(fs) >= 8 && inodes[fs[6]] && fs[3] == "00010000" {
			listening = append(listening, fs[7])
		}
	}
	t.Logf("listening unix sockets: %v", listening)
	if len(listening) != 1 || listening[0] != sockPath {
		t.Errorf("want only %s, got %v", sockPath, listening)
	}
	fi, err := os.Stat(sockPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("socket mode %v", fi.Mode())
}

// TestAnonymous: an unsigned request. Which backend calls run before
// authentication fails, and with what account?
func TestAnonymous(t *testing.T) {
	ctx := context.Background()
	c := client(spikeAccess, spikeSecret)
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("anon")}); err != nil {
		t.Fatal(err)
	}
	logMu.Lock()
	n := len(logLines)
	logMu.Unlock()
	resp, err := unixHTTP().Get("http://gw.internal/anon/x")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	t.Logf("unsigned GET /anon/x -> %d %s", resp.StatusCode, oneLine(string(b)))
	logMu.Lock()
	for _, l := range logLines[n:] {
		t.Logf("  backend saw: %s", l)
	}
	logMu.Unlock()
	if resp.StatusCode != 403 {
		t.Errorf("status %d", resp.StatusCode)
	}
}
