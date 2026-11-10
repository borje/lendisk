package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// forwarding is a running `peer forward` of a node, in this process.
type forwarding struct {
	addr string // the loopback address it listens on
	out  string // what it printed
	stop func()
}

// forward runs `peer forward peer 127.0.0.1:0` on n until the test ends
// or stop is called.
func (n *node) forward(t *testing.T, peer string) *forwarding {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrc := make(chan string, 1)
	var out, errOut bytes.Buffer
	a := &app{in: bufio.NewReader(strings.NewReader("")), out: &out, errOut: &errOut, ctx: ctx,
		serving: func(addr string) { addrc <- addr }, regions: n.regions, dialTimeout: 30 * time.Second}
	done := make(chan int, 1)
	go func() {
		done <- a.run([]string{"peer", "forward", peer, "127.0.0.1:0", "--config", n.config, "--data-dir", n.data,
			"--log-level", "debug"})
	}()
	select {
	case addr := <-addrc:
		f := &forwarding{addr: addr, out: out.String()}
		var once sync.Once
		f.stop = func() {
			once.Do(func() {
				cancel()
				if code := <-done; code != 0 {
					t.Errorf("peer forward exit %d: %s", code, errOut.String())
				}
				// NET-7a: the secret is printed once, on stdout, and
				// never logged, even at debug.
				_, secret, _ := strings.Cut(f.out, "export AWS_SECRET_ACCESS_KEY=")
				secret, _, _ = strings.Cut(secret, "\n")
				if secret == "" || strings.Contains(errOut.String(), secret) ||
					strings.Count(f.out, secret) != 1 {
					t.Errorf("the forward's secret %q is missing, logged or printed twice:\n%s\n%s", secret, f.out, errOut.String())
				}
			})
		}
		t.Cleanup(f.stop)
		return f
	case code := <-done:
		cancel()
		t.Fatalf("peer forward exited %d before listening:\n%s%s", code, out.String(), errOut.String())
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("peer forward did not listen")
	}
	return nil
}

// lines is what the forward printed for bucket: the export lines, and
// the bucket's rclone and restic commands ("" when none), each with the
// forward's address replaced by via.
func (f *forwarding) lines(t *testing.T, bucket, via string) (exports []string, rclone, restic string) {
	t.Helper()
	in := false
	for _, l := range strings.Split(f.out, "\n") {
		l = strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(l, "export AWS_"):
			exports = append(exports, l)
		case strings.HasPrefix(l, bucket+"  "):
			in = true
		case l == "":
		case in && strings.HasPrefix(l, "rclone copy "):
			rclone = strings.ReplaceAll(l, f.addr, via)
		case in && strings.HasPrefix(l, "restic "):
			restic = strings.ReplaceAll(l, f.addr, via)
		default:
			in = false
		}
	}
	if len(exports) != 2 || rclone == "" {
		t.Fatalf("no credential or copy line for %s in:\n%s", bucket, f.out)
	}
	return exports, rclone, restic
}

// shell runs lines as one sh script in dir with env and fails unless it
// succeeds.
func shell(t *testing.T, dir string, env []string, lines ...string) string {
	t.Helper()
	cmd := exec.Command("sh", "-ec", strings.Join(lines, "\n"))
	cmd.Dir, cmd.Env = dir, env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", strings.Join(lines, "\n"), err, out)
	}
	return string(out)
}

// exchange is one request a tap saw and the status it was answered.
type exchange struct {
	method, target string
	status         int
	code           string // the S3 error code of a 4xx answer
}

// tap is a TCP relay in front of a forward that changes no byte: it
// reads the HTTP requests and answers that pass, so a test sees the
// request shapes a tool sends through the forward and how each was
// answered.
type tap struct {
	addr string
	mu   sync.Mutex
	seen []exchange
}

func newTap(t *testing.T, to string) *tap {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tp := &tap{addr: ln.Addr().String()}
	var wg sync.WaitGroup
	t.Cleanup(func() { ln.Close(); wg.Wait() })
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { tp.relay(c, to) })
		}
	})
	return tp
}

func (tp *tap) relay(c net.Conn, to string) {
	s, err := net.Dial("tcp", to)
	if err != nil {
		c.Close()
		return
	}
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	reqs := make(chan *http.Request, 64)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(reqs)
		br := bufio.NewReader(reqR)
		for {
			r, err := http.ReadRequest(br)
			if err != nil {
				io.Copy(io.Discard, br)
				return
			}
			io.Copy(io.Discard, r.Body)
			reqs <- r
		}
	})
	wg.Go(func() {
		br := bufio.NewReader(respR)
		defer io.Copy(io.Discard, br)
		for r := range reqs {
			resp, err := http.ReadResponse(br, r)
			for err == nil && resp.StatusCode >= 100 && resp.StatusCode < 200 {
				resp, err = http.ReadResponse(br, r)
			}
			if err != nil {
				for range reqs {
				}
				return
			}
			body, _ := io.ReadAll(resp.Body)
			code := ""
			if resp.StatusCode >= 400 {
				_, code, _ = strings.Cut(string(body), "<Code>")
				code, _, _ = strings.Cut(code, "</Code>")
			}
			tp.mu.Lock()
			tp.seen = append(tp.seen, exchange{r.Method, r.URL.RequestURI(), resp.StatusCode, code})
			tp.mu.Unlock()
		}
	})
	wg.Go(func() {
		io.Copy(io.MultiWriter(s, reqW), c)
		reqW.Close()
		s.(*net.TCPConn).CloseWrite()
	})
	io.Copy(io.MultiWriter(c, respW), s)
	respW.Close()
	c.Close()
	s.Close()
	wg.Wait()
}

// exchanges is what the tap has seen so far.
func (tp *tap) exchanges() []exchange {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	return slices.Clone(tp.seen)
}

// allAdmitted fails unless the tap saw requests and none was refused by
// the gate: 403 for a shape off NET-7's list, 400 InvalidArgument for an
// object key it refuses (as Lending does, SEC-5). The tool sent only
// shapes NET-7's list admits.
func (tp *tap) allAdmitted(t *testing.T, tool string) {
	t.Helper()
	seen := tp.exchanges()
	if len(seen) == 0 {
		t.Fatalf("%s sent nothing through the forward", tool)
	}
	shapes := map[string]bool{}
	for _, e := range seen {
		shapes[e.method+" "+shape(e.target)] = true
		if e.status == http.StatusForbidden || e.status == http.StatusBadRequest && e.code == "InvalidArgument" {
			t.Errorf("%s sent %s %s through the forward, refused %d %s", tool, e.method, e.target, e.status, e.code)
		}
	}
	t.Logf("%s through the forward: %d requests, shapes %v", tool, len(seen), slices.Sorted(maps.Keys(shapes)))
}

// shape is a request target with the bucket, key and query values left
// out: the level and the query keys.
func shape(target string) string {
	p, q, _ := strings.Cut(target, "?")
	level := "/bucket/key"
	switch p = strings.Trim(p, "/"); {
	case p == "":
		level = "/"
	case !strings.Contains(p, "/"):
		level = "/bucket"
	}
	var keys []string
	for kv := range strings.SplitSeq(q, "&") {
		if k, _, _ := strings.Cut(kv, "="); k != "" && !strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			keys = append(keys, k)
		}
	}
	if len(keys) > 0 {
		return level + "?" + strings.Join(keys, "&")
	}
	return level
}

// forwardHTTP is an HTTP client whose every connection goes to addr,
// whatever the URL names: a tool on the borrower's machine pointed at
// the forward.
func forwardHTTP(addr string) *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	}}
}

// noFileHolds fails if any file under the roots contains secret.
func noFileHolds(t *testing.T, secret string, roots ...string) {
	t.Helper()
	for _, root := range roots {
		filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if b, _ := os.ReadFile(p); bytes.Contains(b, []byte(secret)) {
				t.Errorf("%s holds the derived secret (NET-7a)", p)
			}
			return nil
		})
	}
}

// NET-7b: `peer forward` takes a friend's node ID and a loopback
// address. Another address is a usage error (SEC-8: the forward serves
// this machine only), and so is a name that is not a node ID; a node ID
// that is not a friend's is refused.
func TestPeerForwardRefusesNonLoopbackAndStrangers(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	stranger := newID(t).NodeID().String()
	for _, addr := range []string{"0.0.0.0:9100", "192.168.1.10:9100", ":9100", "localhost:9100", "127.1:9100", "127.0.0.1"} {
		if code, _, errOut := n.run("", "", "peer", "forward", stranger, addr); code != 2 || !strings.Contains(errOut, "loopback") {
			t.Errorf("forward on %s: exit %d %s", addr, code, errOut)
		}
	}
	if code, _, errOut := n.run("", "", "peer", "forward", "B", "127.0.0.1:0"); code != 2 || !strings.Contains(errOut, "not a node ID") {
		t.Errorf("forward to B: exit %d %s", code, errOut)
	}
	if code, _, errOut := n.run("", "", "peer", "forward", stranger, "127.0.0.1:0"); code != 1 || !strings.Contains(errOut, "not a friend") {
		t.Errorf("forward to a stranger: exit %d %s", code, errOut)
	}
}

// NET-7b through the lender's gate (#34, #15 review). A backs up with
// restic to a bucket at two copies; with two friends B and C, each holds
// every object. Then:
//
//   - While A's daemon runs, `peer forward` is refused: both would dial
//     B with one dial key and stall each other (#69). Stopped, it runs,
//     and a daemon started meanwhile is refused instead.
//   - The printed restic line, run as printed with the printed
//     credential, lists A's snapshots from B's replica.
//   - restic forget --prune through the forward is refused: B refuses
//     its lock write, which carries no SHA-256 (LEND-4), and nothing at
//     B changes. A delete through the forward is a tombstone holding the
//     bytes (RET-1), and the RET-2 purge attacks through the forward
//     leave it as it was.
//   - restic, for snapshots and prune, sends only shapes the gate
//     admits: the gate refuses (403) no request it sends through the
//     forward.
//   - Every operation outside NET-7's list, and minio-go's
//     GetBucketLocation, ListObjectsV2 with metadata=true and GetObject
//     with response-* overrides, are refused through the forward, and
//     B's store is unchanged by them.
func TestPeerForwardRefusesPruneAndOffList(t *testing.T) {
	parallel(t, 3)
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	f := newCircleAt(t, 3, "80M")
	first := f.backup("one", 3)
	src := f.src
	for i := range 3 {
		os.Remove(filepath.Join(src, "one-0"+string(rune('0'+i))))
	}
	second := f.backup("two", 3)
	f.waitOffsite(second)
	f.drain()
	var bID string
	var b *procNode
	for id, pn := range f.friends {
		if bID == "" || id < bID {
			bID, b = id, pn
		}
	}
	sealed := f.aID.SealedName("laptop")

	// One process dials B with A's dial key at a time.
	if code, _, errOut := f.an.run("", "", "peer", "forward", bID, "127.0.0.1:0"); code != 1 || !strings.Contains(errOut, "stop it before forwarding") {
		t.Fatalf("forward while A serves: exit %d %s", code, errOut)
	}
	f.a.p.kill()
	fw := f.an.forward(t, bID)
	if code, _, errOut := f.an.run("", "", "serve"); code != 1 || !strings.Contains(errOut, "stop it before serving") {
		t.Fatalf("serve while A forwards: exit %d %s", code, errOut)
	}
	if !strings.Contains(fw.out, "laptop  "+sealed) {
		t.Fatalf("the forward does not name laptop's sealed name %s:\n%s", sealed, fw.out)
	}

	// The printed restic line lists both snapshots from B's replica.
	tp := newTap(t, fw.addr)
	exports, _, resticLine := fw.lines(t, "laptop", tp.addr)
	if resticLine == "" {
		t.Fatalf("no restic line:\n%s", fw.out)
	}
	env := append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir())
	out := shell(t, t.TempDir(), env, append(exports, resticLine)...)
	for _, id := range []string{first, second} {
		if !strings.Contains(out, id) {
			t.Fatalf("snapshot %s not listed from B:\n%s", id, out)
		}
	}
	tp.allAdmitted(t, "restic snapshots")

	// restic prune through the forward: B refuses its writes (each must
	// carry its SHA-256, LEND-4), so prune cannot take its lock and
	// removes nothing. restic retries a refused write for minutes, so it
	// is stopped once the tap has seen B refuse one.
	bDir := filepath.Join(b.data, "store", gateway.LentName(f.aID.NodeID(), sealed))
	lent := tree(t, bDir)
	tp = newTap(t, fw.addr)
	repo := "s3:http://" + tp.addr + "/" + sealed
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	prune := exec.CommandContext(ctx, "sh", "-ec", strings.Join(append(exports,
		"exec "+restic+" -o s3.region=us-east-1 -r "+repo+" forget "+first+" --prune"), "\n"))
	prune.Dir, prune.Env = t.TempDir(), env
	refused := func() bool {
		return slices.ContainsFunc(tp.exchanges(), func(e exchange) bool { return e.method == "PUT" && e.status == http.StatusBadRequest })
	}
	go func() {
		for ctx.Err() == nil && !refused() {
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(time.Second) // let restic's retry show in the tap too
		cancel()
	}()
	pruneOut, err := prune.CombinedOutput()
	cancel()
	if err == nil {
		t.Fatalf("restic prune through the forward succeeded:\n%s", pruneOut)
	}
	tp.allAdmitted(t, "restic forget --prune")
	refusedPut := false
	for _, e := range tp.exchanges() {
		refusedPut = refusedPut || e.method == "PUT" && e.status == http.StatusBadRequest
		if e.method == "DELETE" && e.status < 300 {
			t.Errorf("restic prune deleted %s through the forward", e.target)
		}
	}
	if !refusedPut {
		t.Errorf("B did not refuse restic prune's writes: %v\n%s", tp.exchanges(), pruneOut)
	}
	sameTree(t, "restic prune through the forward", lent, tree(t, bDir))

	// A delete through the forward is a tombstone holding the bytes (RET-1).
	bKey, err := identity.ParseNodeID(bID)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := f.aID.S3Secret(bKey)
	if err != nil {
		t.Fatal(err)
	}
	hc := forwardHTTP(fw.addr)
	c := s3Over(hc, credentials.NewStaticCredentialsProvider(f.aID.NodeID().String(), secret, ""))
	pack := ""
	for k := range lentObjects(t, b.node, f.aID, "laptop") {
		if strings.HasPrefix(k, "data/") {
			pack = k
		}
	}
	body, err := os.ReadFile(filepath.Join(bDir, filepath.FromSlash(pack)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: aws.String(sealed), Key: aws.String(pack)}); err != nil {
		t.Fatalf("DeleteObject through the forward: %v", err)
	}
	bDB := openDB(t, b.node)
	tombs, err := bDB.TombstonesOf(t.Context(), f.aID.NodeID().String(), sealed, pack)
	if err != nil || len(tombs) != 1 || tombs[0].SHA256 != hexSum(body) {
		t.Fatalf("%s deleted through the forward: tombstones %+v %v", pack, tombs, err)
	}
	tomb := tombs[0]

	// RET-2 through the forward: the purge attacks leave the tomb as it was.
	for name, attack := range purgeAttacks(c, sealed, pack, tomb.Tomb, "", body) {
		attack()
		if got, err := os.ReadFile(filepath.Join(bDir, filepath.FromSlash(tomb.Tomb))); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("after %q through the forward the tomb holds %d bytes (%v)", name, len(got), err)
		}
	}
	if ts, err := bDB.TombstonesOf(t.Context(), f.aID.NodeID().String(), sealed, pack); err != nil ||
		!slices.ContainsFunc(ts, func(x state.Tombstone) bool { return x.ID == tomb.ID }) {
		t.Fatalf("after the purge attacks the tombstones are %+v (%v)", ts, err)
	}

	// Everything off NET-7's list, and minio-go's extra shapes, refused;
	// B's store unchanged.
	store := filepath.Join(b.data, "store")
	snap := tree(t, store)
	h := &hostile{t: t, label: "A through the forward", hc: hc, access: f.aID.NodeID().String(), secret: secret}
	h.mustRefuse(s3Attacks(sealed, "config", isRefused))
	h.mustRefuse(s3Attacks(sealed, tomb.Tomb, isRefused))
	h.mustRefuse([]attack{
		{name: "minio-go GetBucketLocation", method: "GET", target: "/" + sealed + "?location="},
		{name: "minio-go ListObjectsV2 metadata=true", method: "GET", target: "/" + sealed + "?list-type=2&metadata=true"},
		{name: "GetObject response-content-type", method: "GET", target: "/" + sealed + "/config?response-content-type=text%2Fplain"},
		{name: "GetObject response-content-disposition", method: "GET",
			target: "/" + sealed + "/config?response-content-disposition=attachment"},
		{name: "HeadObject response-cache-control", method: "HEAD", target: "/" + sealed + "/config?response-cache-control=no-cache"},
	})
	sameTree(t, "the refused requests through the forward", snap, tree(t, store))
	// The admitted shapes still work, so the refusals are the gate's.
	if _, err := c.HeadBucket(t.Context(), &s3.HeadBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatalf("HeadBucket through the forward: %v", err)
	}
	// A client that half-closes after its request (nc -N) reads B's
	// answer: the forward passes the half-close on through the tunnel
	// instead of closing both ways (#94). Which answer is B's own:
	// net/http cancels an in-flight request's context once it reads
	// EOF, so B's bridge may answer 503 rather than the gate's 403
	// (DESIGN §3.7).
	if got := ask(t, fw.addr, "GET /"+sealed+"/config HTTP/1.1\r\nHost: lendisk\r\n\r\n"); !strings.HasPrefix(got, "HTTP/1.1 ") {
		t.Errorf("a half-closing client through the forward read %q", got)
	}
	fw.stop()
}

// flakyListener fails its first Accepts with errs, then accepts.
type flakyListener struct {
	net.Listener
	mu   sync.Mutex
	errs []error
}

func (l *flakyListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if len(l.errs) > 0 {
		err := l.errs[0]
		l.errs = l.errs[1:]
		l.mu.Unlock()
		return nil, err
	}
	l.mu.Unlock()
	return l.Listener.Accept()
}

// friendStub is a TCP server standing in for the friend: each
// connection reads its request to EOF, then answers "answer:" and the
// request, and closes. dials counts the forward's dials to it.
type friendStub struct {
	addr  string
	mu    sync.Mutex
	dials int
}

func newFriendStub(t *testing.T) *friendStub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &friendStub{addr: ln.Addr().String()}
	var wg sync.WaitGroup
	t.Cleanup(func() { ln.Close(); wg.Wait() })
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer c.Close()
				req, _ := io.ReadAll(c)
				c.Write(append([]byte("answer:"), req...))
			})
		}
	})
	return s
}

func (s *friendStub) dial(ctx context.Context) (net.Conn, error) {
	s.mu.Lock()
	s.dials++
	s.mu.Unlock()
	return (&net.Dialer{}).DialContext(ctx, "tcp", s.addr)
}

func (s *friendStub) dialled() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials
}

// runForward runs fw until the test ends; done receives what run returned.
func runForward(t *testing.T, fw *forward) (done chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done = make(chan error, 1)
	go func() { done <- fw.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the forward did not stop")
		}
	})
	return done
}

// ask sends req to addr, half-closes, and returns the answer.
func ask(t *testing.T, addr, req string) string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("reading the answer to %q: %v", req, err)
	}
	return string(got)
}

// #94: an accept error a moment may cure (out of descriptors) is
// retried, and the forward goes on serving; a client that half-closes
// (nc -N) gets its answer through it. Any other accept error ends the
// forward with that error, which the command exits non-zero on.
func TestPeerForwardRetriesTemporaryAcceptErrorsAndHalfCloses(t *testing.T) {
	parallel(t, 0)
	friend := newFriendStub(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	fl := &flakyListener{Listener: ln, errs: []error{
		&net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept4", syscall.EMFILE)},
		&net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept4", syscall.ENFILE)},
	}}
	fw := &forward{ln: fl, dial: friend.dial, dialTimeout: 10 * time.Second, max: 4,
		log: slog.New(slog.NewTextHandler(&logs, nil))}
	done := runForward(t, fw)
	if got := ask(t, ln.Addr().String(), "GET / HTTP/1.0\r\n\r\n"); got != "answer:GET / HTTP/1.0\r\n\r\n" {
		t.Fatalf("a half-closing client through the forward read %q", got)
	}
	if !strings.Contains(logs.String(), "too many open files") {
		t.Errorf("the retried accept error was not logged:\n%s", logs.String())
	}
	select {
	case err := <-done:
		t.Fatalf("the forward stopped at a temporary accept error: %v", err)
	default:
	}

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	broken := errors.New("listener broken")
	fw2 := &forward{ln: &flakyListener{Listener: ln2, errs: []error{broken}}, dial: friend.dial,
		dialTimeout: 10 * time.Second, max: 4, log: slog.New(slog.DiscardHandler)}
	done2 := make(chan error, 1)
	go func() { done2 <- fw2.run(t.Context()) }()
	select {
	case err := <-done2:
		if !errors.Is(err, broken) {
			t.Fatalf("the forward ended with %v; want the accept error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the forward went on after an accept error it cannot retry")
	}
}

// #94: the forward splices at most max connections at once, so a local
// user cannot make the node's dial key open unbounded tunnel
// connections; one more waits until one ends, then is served.
func TestPeerForwardCapsConnections(t *testing.T) {
	parallel(t, 0)
	friend := newFriendStub(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	fw := &forward{ln: ln, dial: friend.dial, dialTimeout: 10 * time.Second, max: 2,
		log: slog.New(slog.NewTextHandler(&logs, nil))}
	runForward(t, fw)
	var open []net.Conn
	for range 3 {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		open = append(open, c)
	}
	deadline := time.Now().Add(10 * time.Second)
	for friend.dialled() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if n := friend.dialled(); n != 2 {
		t.Fatalf("with a cap of 2 and 3 clients the forward dialled the friend %d times", n)
	}
	if !strings.Contains(logs.String(), "new ones wait") {
		t.Errorf("reaching the cap was not logged:\n%s", logs.String())
	}
	open[0].(*net.TCPConn).CloseWrite() // its splice ends once the friend answers
	io.ReadAll(open[0])
	for friend.dialled() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := friend.dialled(); n != 3 {
		t.Fatalf("after one connection ended the waiting one was not served: %d dials", n)
	}
}

// NET-7b (#94): the forward listens on any loopback IP literal, IPv6's
// [::1] included; 127.1 and localhost are names, not literals, refused.
func TestPeerForwardLoopbackLiterals(t *testing.T) {
	parallel(t, 0)
	for _, addr := range []string{"127.0.0.1:9100", "127.0.0.2:0", "[::1]:9100"} {
		if err := loopbackOnly(addr); err != nil {
			t.Errorf("%s refused: %v", addr, err)
		}
	}
	for _, addr := range []string{"127.1:9100", "localhost:9100", "[::]:9100", "[::ffff:10.0.0.1]:9100"} {
		if err := loopbackOnly(addr); err == nil {
			t.Errorf("%s accepted", addr)
		}
	}
}

// hexSum is the SHA-256 of b in hex.
func hexSum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// Scenario 10, restore straight from friends (NET-7b, P-5; #34): A backs
// up `laptop` at two copies to B, C and D, two snapshots. A's daemon is
// stopped; on A, `peer forward B` and `peer forward C` each print the
// name of laptop there, the credential for that friend and the command
// that copies its keys into a local directory. Run as printed, into one
// directory, the copies are A's repository: `restic -r laptop snapshots`
// lists both snapshots, with no lendisk replication code in the path.
//
//   - rclone, the copy client, sends only shapes the gate admits.
//   - NET-7a: the printed secret is in no file of A's or B's.
func TestScenario10RestoreStraightFromFriends(t *testing.T) {
	parallel(t, 4)
	if _, err := exec.LookPath("rclone"); err != nil {
		t.Skip("rclone not installed")
	}
	f := newFourFriends(t)
	first := f.backup("one", 4)
	second := f.backup("two", 4)
	f.waitOffsite(second)
	f.drain()
	want := replicable(t, f.an, "laptop")
	f.a.p.kill()

	ids := slices.Sorted(maps.Keys(f.friends))
	work := t.TempDir()
	var secrets []string
	for _, id := range ids[:2] {
		fw := f.an.forward(t, id)
		sealed := f.aID.SealedName("laptop")
		if !strings.Contains(fw.out, "laptop  "+sealed) {
			t.Fatalf("the forward to %s does not name laptop's sealed name:\n%s", id, fw.out)
		}
		tp := newTap(t, fw.addr)
		exports, rclone, _ := fw.lines(t, "laptop", tp.addr)
		secrets = append(secrets, strings.TrimPrefix(exports[1], "export AWS_SECRET_ACCESS_KEY="))
		shell(t, work, os.Environ(), append(exports, rclone)...)
		tp.allAdmitted(t, "rclone copy from "+id)
		fw.stop()
	}
	if _, err := os.Stat(filepath.Join(work, "laptop", ".lendisk")); err == nil {
		t.Error("the copy holds the lender's .lendisk/")
	}
	got := storeObjects(t, filepath.Join(work, "laptop"))
	sameObjects(t, want, got)
	held := lentObjects(t, f.friends[ids[0]].node, f.aID, "laptop")
	t.Logf("the repository has %d objects; %s holds %d of them", len(want), ids[0], len(held))

	env := append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir())
	out := shell(t, work, env, "restic -r laptop snapshots")
	for _, id := range []string{first, second} {
		if !strings.Contains(out, id) {
			t.Fatalf("snapshot %s not listed from the copied directory:\n%s", id, out)
		}
	}
	for _, s := range secrets {
		noFileHolds(t, s, filepath.Dir(filepath.Dir(f.an.config)), filepath.Dir(f.an.data), work)
		for _, pn := range f.friends {
			noFileHolds(t, s, filepath.Dir(filepath.Dir(pn.config)), filepath.Dir(pn.data))
		}
	}
}
