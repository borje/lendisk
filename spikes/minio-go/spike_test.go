package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// One friend (embedgw allows one instance per process) and one Garage
// container, shared by every test; the uploader binary is built once.
var (
	targets   []Target
	uploader  string
	workDir   string
	garageErr error
)

func TestMain(m *testing.M) {
	var err error
	workDir, err = os.MkdirTemp("", "minio-go-spike-")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	// The friend: versitygw on posix over TCP.
	addr := freeAddr()
	errc := make(chan error, 1)
	go func() { errc <- runFriend(ctx, filepath.Join(workDir, "friend"), addr) }()
	waitTCP(addr, errc)
	friend := Target{Name: "friend", Endpoint: addr, Region: "us-east-1",
		Access: spikeAccess, Secret: spikeSecret, Bucket: "spike"}
	core, err := newCore(friend)
	if err != nil {
		panic(err)
	}
	if err := core.MakeBucket(ctx, "spike", minio.MakeBucketOptions{}); err != nil {
		panic(err)
	}
	targets = append(targets, friend)

	// Garage: from the environment, or started here.
	garageName := ""
	if os.Getenv("GARAGE_ENDPOINT") == "" && os.Getenv("SKIP_GARAGE") == "" {
		garageName = fmt.Sprintf("lendisk-spike-garage-%d", os.Getpid())
		out, err := exec.Command("./garage.sh", garageName, filepath.Join(workDir, "garage")).Output()
		if err != nil {
			garageErr = fmt.Errorf("garage.sh: %v", err)
		} else {
			lines := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, kv := range strings.Fields(lines[len(lines)-1]) {
				k, v, _ := strings.Cut(kv, "=")
				os.Setenv(k, v)
			}
		}
	}
	if ep := os.Getenv("GARAGE_ENDPOINT"); ep != "" {
		targets = append(targets, Target{Name: "garage", Endpoint: ep, Region: "garage",
			Access: os.Getenv("GARAGE_ACCESS"), Secret: os.Getenv("GARAGE_SECRET"), Bucket: "spike"})
	}

	uploader = filepath.Join(workDir, "uploader")
	if out, err := exec.Command("go", "build", "-o", uploader, ".").CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build: %v\n%s", err, out))
	}

	code := m.Run()
	if garageName != "" {
		exec.Command("docker", "rm", "-f", garageName).Run()
	}
	cancel()
	<-errc
	os.RemoveAll(workDir)
	os.Exit(code)
}

func freeAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitTCP(addr string, errc chan error) {
	for i := 0; i < 200; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return
		}
		select {
		case err := <-errc:
			panic(fmt.Sprintf("friend exited: %v", err))
		case <-time.After(25 * time.Millisecond):
		}
	}
	panic("friend did not come up")
}

func eachTarget(t *testing.T, f func(t *testing.T, tg Target)) {
	if garageErr != nil {
		t.Errorf("garage not started: %v", garageErr)
	}
	for _, tg := range targets {
		t.Run(tg.Name, func(t *testing.T) { f(t, tg) })
	}
}

// randomFile writes size random bytes and returns path and SHA-256 hex.
func randomFile(t *testing.T, size int64) (string, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, h), rand.Reader, size); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return p, hex.EncodeToString(h.Sum(nil))
}

func objectSHA(t *testing.T, tg Target, key string) string {
	t.Helper()
	core, _ := newCore(tg)
	o, err := core.Client.GetObject(context.Background(), tg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	h := sha256.New()
	if _, err := io.Copy(h, o); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

const MiB = 1 << 20

// ---- Q-Y.1: SIGKILL mid-part, resume at the next part ----

type proc struct {
	cmd   *exec.Cmd
	lines chan string
	all   []string
}

func startUploader(t *testing.T, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(uploader, append([]string{"upload"}, args...)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, lines: make(chan string, 100)}
	go func() {
		s := bufio.NewScanner(out)
		for s.Scan() {
			p.lines <- s.Text()
		}
		close(p.lines)
	}()
	return p
}

// next returns the next stdout line, failing on timeout or exit.
func (p *proc) next(t *testing.T) string {
	t.Helper()
	select {
	case l, ok := <-p.lines:
		if !ok {
			t.Fatalf("uploader exited; output so far: %q", p.all)
		}
		p.all = append(p.all, l)
		t.Logf("  uploader: %s", l)
		return l
	case <-time.After(60 * time.Second):
		t.Fatalf("timeout; output so far: %q", p.all)
	}
	return ""
}

func (p *proc) until(t *testing.T, prefix string) string {
	t.Helper()
	for {
		if l := p.next(t); strings.HasPrefix(l, prefix) {
			return l
		}
	}
}

func TestResumeAfterSIGKILL(t *testing.T) {
	// The two configurations Q-Y.2 leaves standing: minio-go's default
	// (no request checksum, signed chunks over plain HTTP) and lendisk's
	// (unsigned payload, per-part SHA-256 in a trailer).
	for _, mode := range []string{"", "sha256"} {
		t.Run("checksum="+map[string]string{"": "none", "sha256": "sha256-trailer"}[mode], func(t *testing.T) {
			eachTarget(t, func(t *testing.T, tg Target) { resumeAfterKill(t, tg, mode) })
		})
	}
}

func resumeAfterKill(t *testing.T, tg Target, mode string) {
	const part = 5 * MiB
	size := int64(5*part + 3*MiB) // 6 parts
	src, sum := randomFile(t, size)
	key := "resume/" + tg.Name + "/" + mode + "/obj"
	state := filepath.Join(t.TempDir(), "state.json")
	args := []string{"-endpoint", tg.Endpoint, "-region", tg.Region, "-access", tg.Access,
		"-secret", tg.Secret, "-bucket", tg.Bucket, "-source", src, "-key", key,
		"-state", state, "-sha256", sum, "-part-size", fmt.Sprint(part),
		"-rate", fmt.Sprint(8 * MiB), "-checksum", mode, fmt.Sprintf("-unsigned=%v", mode != "")}

	p := startUploader(t, args...)
	p.until(t, "part-done 3 ")
	p.until(t, "part-start 4 ")
	time.Sleep(300 * time.Millisecond) // about half of part 4 at 8 MiB/s
	if err := p.cmd.Process.Signal(os.Kill); err != nil {
		t.Fatal(err)
	}
	err := p.cmd.Wait()
	t.Logf("first run: %v", err)
	if ee, ok := err.(*exec.ExitError); !ok || ee.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("expected death by SIGKILL, got %v", err)
	}

	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	if got := partNumbers(st.Parts); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("stored parts after kill = %v, want [1 2 3]", got)
	}
	core, _ := newCore(tg)
	lp, err := core.ListObjectParts(context.Background(), tg.Bucket, key, st.UploadID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var at []string
	for _, op := range lp.ObjectParts {
		at = append(at, fmt.Sprintf("%d:%d", op.PartNumber, op.Size))
	}
	t.Logf("target lists after kill: %v (upload %s)", at, st.UploadID)

	p = startUploader(t, args...)
	first := p.next(t)
	if want := "resumed " + st.UploadID + " have=[1 2 3]"; first != want {
		t.Fatalf("first line %q, want %q", first, want)
	}
	if l := p.next(t); !strings.HasPrefix(l, "part-start 4 ") {
		t.Fatalf("resumed at %q, want part 4", l)
	}
	done := p.until(t, "complete ")
	if err := p.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	// Only parts 4-6 were read from the source in the second run.
	if want := fmt.Sprintf("source-bytes=%d", size-3*part); !strings.HasSuffix(done, want) {
		t.Fatalf("%q, want %s", done, want)
	}
	if got := objectSHA(t, tg, key); got != sum {
		t.Fatalf("object sha256 %s, want %s", got, sum)
	}
	oi, err := core.StatObject(context.Background(), tg.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if oi.UserMetadata["Lendisk-Sha256"] != sum {
		t.Errorf("metadata %v lacks lendisk-sha256", oi.UserMetadata)
	}
	t.Logf("RESULT %s checksum=%q: killed in part 4, resumed upload %s at part 4, read %d of %d bytes again, object SHA-256 matches, etag %s",
		tg.Name, mode, st.UploadID, size-3*part, size, oi.ETag)
}
