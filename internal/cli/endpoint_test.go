package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// daemon is a running `lendisk serve` for a node, in this process.
type daemon struct {
	addr string
	stop func()
}

func (n *node) serve(t *testing.T) *daemon {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrc := make(chan string, 1)
	var out, errOut bytes.Buffer
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx,
		serving: func(addr string) { addrc <- addr }, regions: n.regions}
	done := make(chan int, 1)
	go func() {
		done <- a.run([]string{"serve", "--config", n.config, "--data-dir", n.data, "--endpoint-loopback", "127.0.0.1:0"})
	}()
	select {
	case addr := <-addrc:
		d := &daemon{addr: addr}
		d.stop = func() {
			cancel()
			if code := <-done; code != 0 {
				t.Errorf("serve exit %d: %s%s", code, out.String(), errOut.String())
			}
		}
		t.Cleanup(func() {
			if ctx.Err() == nil {
				d.stop()
			}
		})
		return d
	case code := <-done:
		cancel()
		t.Fatalf("serve exited %d before serving:\n%s%s", code, out.String(), errOut.String())
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	return nil
}

func (n *node) mustInit(t *testing.T) {
	t.Helper()
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init"); code != 0 {
		t.Fatal(e)
	}
}

type creds struct{ access, secret string }

var (
	reAccess = regexp.MustCompile(`(?:AWS_ACCESS_KEY_ID=|access key: +|--access-key=)(\S+)`)
	reSecret = regexp.MustCompile(`(?:AWS_SECRET_ACCESS_KEY=|secret key: +|--secret-access-key=)(\S+)`)
)

// addBucket runs `bucket add` and reads the credential from its output.
func (n *node) addBucket(t *testing.T, name, profile string) creds {
	t.Helper()
	code, out, e := n.run("", "", "bucket", "add", name, "--profile", profile)
	if code != 0 {
		t.Fatalf("bucket add %s: %s", name, e)
	}
	a, s := reAccess.FindStringSubmatch(out), reSecret.FindStringSubmatch(out)
	if a == nil || s == nil {
		t.Fatalf("no credential in output:\n%s", out)
	}
	return creds{a[1], s[1]}
}

func s3client(addr string, c creds) *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String("http://" + addr),
		UsePathStyle:     true,
		Credentials:      credentials.NewStaticCredentialsProvider(c.access, c.secret, ""),
		RetryMaxAttempts: 1,
	})
}

func apiCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// EP-6: one credential per bucket, scoped to it; EP-1 over the bridge.
func TestBucketCredentialsAreScoped(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	laptop := n.addBucket(t, "laptop", "restic")
	other := n.addBucket(t, "other", "generic")
	d := n.serve(t)
	ctx := context.Background()

	c := s3client(d.addr, laptop)
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String("data/ab/x"),
		Body: strings.NewReader("hello")}); err != nil {
		t.Fatalf("put with own credential: %v", err)
	}
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("laptop"), Key: aws.String("data/ab/x")})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	if string(body) != "hello" {
		t.Fatalf("got %q", body)
	}
	// One bucket's credential cannot touch another bucket.
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("other"), Key: aws.String("k"),
		Body: strings.NewReader("x")}); apiCode(err) != "AccessDenied" {
		t.Fatalf("put into other bucket: %v", err)
	}
	if _, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("other")}); apiCode(err) != "AccessDenied" {
		t.Fatalf("list other bucket: %v", err)
	}
	if _, err := s3client(d.addr, other).GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/ab/x")}); apiCode(err) != "AccessDenied" {
		t.Fatalf("get from laptop with other's key: %v", err)
	}
	lb, err := c.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lb.Buckets) != 1 || *lb.Buckets[0].Name != "laptop" {
		t.Fatalf("ListBuckets shows %d buckets", len(lb.Buckets))
	}
	// A bucket credential cannot create buckets, with or without object lock.
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("sneaky")}); err == nil {
		t.Fatal("bucket credential created a bucket")
	}
	// A wrong secret is refused.
	if _, err := s3client(d.addr, creds{laptop.access, "wrong" + laptop.secret[5:]}).GetObject(ctx,
		&s3.GetObjectInput{Bucket: aws.String("laptop"), Key: aws.String("data/ab/x")}); err == nil {
		t.Fatal("wrong secret accepted")
	}
}

// SEC-8 and §3.7: the admin API and the control API path are refused.
func TestAdminAndControlPathsRefused(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "laptop", "restic")
	d := n.serve(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodPatch, "/list-users"},
		{http.MethodPatch, "/create-user"},
		{http.MethodGet, "/_lendisk/version"},
		{http.MethodPost, "/_lendisk/redeem"},
	} {
		req, _ := http.NewRequest(tc.method, "http://"+d.addr+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || !bytes.Contains(b, []byte("AccessDenied")) {
			t.Errorf("%s %s: %d %s", tc.method, tc.path, resp.StatusCode, b)
		}
	}
}

// EP-5 and SEC-8 AC: what `serve` opens is the loopback endpoint, as its
// only TCP listener, and tailcat's UDP socket on all interfaces (NET-1),
// nothing else. Sockets that exist before serve starts (the test relay in
// this process) are not the daemon's.
func TestOnlyLoopbackListener(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	before, err := ownSockets()
	if err != nil {
		t.Skipf("cannot inspect sockets: %v", err)
	}
	d := n.serve(t)
	after, _ := ownSockets()
	old := map[sock]bool{}
	for _, s := range before {
		old[s] = true
	}
	var found bool
	udp := 0
	for _, s := range after {
		if old[s] {
			continue
		}
		switch {
		case s.proto == "tcp" && s.listen && s.local == d.addr:
			found = true
		case s.proto == "tcp" && s.listen:
			t.Errorf("unexpected TCP listener %s", s.local)
		case s.proto == "udp":
			host, _, _ := net.SplitHostPort(s.local)
			if ip := net.ParseIP(host); ip == nil || !ip.IsUnspecified() {
				t.Errorf("unexpected UDP socket %s", s.local)
			}
			udp++
		}
	}
	if !found {
		t.Errorf("endpoint %s not among the new listeners %v", d.addr, after)
	}
	if udp > 2 { // tailcat binds one socket per address family
		t.Errorf("%d new UDP sockets, want tailcat's (at most one per family)", udp)
	}
}

func TestBucketNamesRefused(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	for _, name := range []string{"lent-abc", "lendisk-manifests", "UPPER", "ab", "a..b", "-x-", "a_b", "a--b"} {
		if code, _, _ := n.run("", "", "bucket", "add", name, "--profile", "restic"); code != 2 {
			t.Errorf("bucket add %q: exit %d, want 2", name, code)
		}
	}
	if code, _, _ := n.run("", "", "bucket", "add", "good", "--profile", "borg"); code != 2 {
		t.Error("unknown profile accepted")
	}
	n.addBucket(t, "good", "kopia")
	if code, _, e := n.run("", "", "bucket", "add", "good", "--profile", "kopia"); code == 0 || !strings.Contains(e, "already exists") {
		t.Errorf("duplicate bucket: %d %s", code, e)
	}
	code, out, _ := n.run("", "", "bucket", "show", "good")
	if code != 0 || !strings.Contains(out, "kopia repository create s3") {
		t.Errorf("bucket show: %s", out)
	}
	st := n.status()
	if len(st.Buckets) != 1 || st.Buckets[0].Name != "good" || st.Buckets[0].Copies != 2 {
		t.Errorf("status buckets: %+v", st.Buckets)
	}
	if !strings.HasPrefix(st.Versions.Versitygw, "v1.") {
		t.Errorf("status versitygw version %q", st.Versions.Versitygw)
	}
}

// EP-12 AC through init: a store the filesystem cannot hold is refused,
// naming the filesystem and the property, and nothing is written.
func TestInitRefusesUnfitStore(t *testing.T) {
	n := newNode(t)
	var out, errOut bytes.Buffer
	a := &app{
		in: bufio.NewReader(strings.NewReader("abandon\nabandon\nart\n")), out: &out, errOut: &errOut,
		pick: func(int, int) []int { return []int{0, 11, 23} }, newPhrase: func() (string, error) { return testPhrase, nil },
		regions: n.regions,
	}
	a.storeCheck.Xattr = func(string) error { return errors.New("operation not supported") }
	a.storeCheck.Statfs = statfsAs("vfat")
	if code := a.run([]string{"init", "--config", n.config, "--data-dir", n.data}); code == 0 {
		t.Fatal("init accepted a store without xattrs")
	}
	if !strings.Contains(errOut.String(), "vfat") || !strings.Contains(errOut.String(), "extended attributes") {
		t.Fatalf("message: %s", errOut.String())
	}
	if strings.Contains(out.String(), "art") {
		t.Fatal("phrase shown before the store was checked")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(n.config), "identity.key")); !os.IsNotExist(err) {
		t.Fatal("key written")
	}
}

// EP-1 AC with the real tools, when installed: restic (init, backup,
// check --read-data, restore), then EP-15/P-5 with the daemon stopped.
func TestResticEndToEnd(t *testing.T) {
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "laptop", "restic")
	d := n.serve(t)

	src := t.TempDir()
	for i := 0; i < 20; i++ {
		b := bytes.Repeat([]byte(fmt.Sprintf("file %d ", i)), 1000*(i+1))
		os.WriteFile(filepath.Join(src, fmt.Sprintf("f%02d", i)), b, 0o600)
	}
	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
		"RESTIC_REPOSITORY=s3:http://"+d.addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	run := func(env []string, args ...string) string {
		t.Helper()
		cmd := exec.Command(restic, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	run(env, "init")
	run(env, "backup", src)
	os.WriteFile(filepath.Join(src, "f00"), []byte("changed"), 0o600)
	run(env, "backup", src)
	run(env, "check", "--read-data")
	dst := t.TempDir()
	run(env, "restore", "latest", "--target", dst)
	want, _ := os.ReadFile(filepath.Join(src, "f07"))
	got, err := os.ReadFile(filepath.Join(dst, src, "f07"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("restored file differs: %v", err)
	}

	// With the daemon stopped, the store is a plain restic repository.
	d.stop()
	local := append(os.Environ(), "RESTIC_REPOSITORY="+filepath.Join(n.data, "store", "laptop"),
		"RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir())
	if out := run(local, "snapshots"); strings.Count(out, src) < 2 {
		t.Fatalf("plain-file repository shows:\n%s", out)
	}
	run(local, "check")
}

// EP-1 AC: the AWS CLI and rclone, path-style, when installed.
func TestAWSCLIAndRclone(t *testing.T) {
	aws, awsErr := exec.LookPath("aws")
	rclone, rcErr := exec.LookPath("rclone")
	if awsErr != nil && rcErr != nil {
		t.Skip("neither aws nor rclone installed")
	}
	n := newNode(t)
	n.mustInit(t)
	c := n.addBucket(t, "tools", "generic")
	d := n.serve(t)
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<20) // 16 MiB: multipart for both tools
	os.WriteFile(filepath.Join(dir, "big"), payload, 0o600)
	check := func(name string, cmd *exec.Cmd) {
		t.Helper()
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
	if awsErr == nil {
		env := append(os.Environ(), "AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
			"AWS_DEFAULT_REGION=us-east-1", "AWS_CONFIG_FILE=/dev/null", "AWS_SHARED_CREDENTIALS_FILE=/dev/null")
		ep := "--endpoint-url=http://" + d.addr
		for _, args := range [][]string{
			{"s3", "cp", filepath.Join(dir, "big"), "s3://tools/aws/big", ep},
			{"s3", "ls", "s3://tools/aws/", ep},
			{"s3", "cp", "s3://tools/aws/big", filepath.Join(dir, "big.aws"), ep},
			{"s3", "rm", "s3://tools/aws/big", ep},
		} {
			cmd := exec.Command(aws, args...)
			cmd.Env = env
			check("aws "+strings.Join(args[:2], " "), cmd)
		}
		if b, _ := os.ReadFile(filepath.Join(dir, "big.aws")); !bytes.Equal(b, payload) {
			t.Error("aws round trip differs")
		}
	}
	if rcErr == nil {
		remote := fmt.Sprintf(":s3,provider=Other,access_key_id=%s,secret_access_key=%s,endpoint='http://%s',force_path_style=true,no_check_bucket=true:", c.access, c.secret, d.addr)
		check("rclone copy up", exec.Command(rclone, "copyto", filepath.Join(dir, "big"), remote+"tools/rclone/big", "--config", "/dev/null"))
		check("rclone check", exec.Command(rclone, "check", dir, remote+"tools/rclone", "--include", "big", "--config", "/dev/null"))
		check("rclone copy down", exec.Command(rclone, "copyto", remote+"tools/rclone/big", filepath.Join(dir, "big.rclone"), "--config", "/dev/null"))
		if b, _ := os.ReadFile(filepath.Join(dir, "big.rclone")); !bytes.Equal(b, payload) {
			t.Error("rclone round trip differs")
		}
	}
}
