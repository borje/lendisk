package cli

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/transport"
)

// The hostile-peer harness (SEC-7, SEC-9, SEC-11, NET-7): a node that
// sends a target, over the real tunnel, every S3 operation versitygw
// routes and every control API call, signed with whatever credential it
// chooses. Later tickets extend it: add an attack to s3Attacks or
// controlAttacks, or a phase to TestHostilePeer.

// attack is one request a hostile node sends.
type attack struct {
	name   string
	op     gateway.Op // the S3 op it is an example of; zero for a control call
	method string
	target string // path and query, as sent
	header map[string]string
	body   []byte
}

func (a attack) control() bool { return strings.HasPrefix(a.target, control.Prefix) }

// s3Attacks is one request per S3 action of gateway.Surface that keep
// selects, aimed at bucket and, for an object op, key.
func s3Attacks(bucket, key string, keep func(gateway.Op) bool) []attack {
	var out []attack
	for _, op := range gateway.Surface {
		if keep != nil && !keep(op) {
			continue
		}
		p := map[gateway.Level]string{gateway.Service: "/", gateway.Bucket: "/" + bucket, gateway.Object: "/" + bucket + "/" + key}[op.Level]
		if op.Query != "" {
			p += "?" + op.Query
		}
		a := attack{name: op.Action, op: op, method: op.Method, target: p, header: map[string]string{}}
		if op.Header != "" {
			a.header[op.Header] = bucket + "/" + key
		}
		out = append(out, a)
	}
	return out
}

func isRefused(op gateway.Op) bool  { return op.Class == gateway.Refused }
func isAdmitted(op gateway.Op) bool { return op.Class == gateway.Admitted }

// reachesABucket selects the admitted ops that name an existing bucket:
// all but CreateBucket and ListBuckets.
func reachesABucket(op gateway.Op) bool {
	return isAdmitted(op) && op.Level != gateway.Service && op.Action != "s3_CreateBucket"
}

// controlAttacks is every control API call, named by path, with a body
// where the call takes one; own and other are sealed names for list.
func controlAttacks(own, other string) []attack {
	redeem := []byte(`{"address":"x","number":1,"retention_seconds":1,"dial_key":"x"}`)
	return []attack{
		{name: "version", method: "GET", target: control.VersionPath},
		{name: "redeem", method: "POST", target: control.RedeemPath, body: redeem},
		{name: "manifest get", method: "GET", target: control.ManifestPath},
		{name: "manifest put junk", method: "PUT", target: control.ManifestPath, body: []byte("not a sealed manifest")},
		{name: "list own", method: "GET", target: control.ListPath + "?bucket=" + own},
		{name: "list another's", method: "GET", target: control.ListPath + "?bucket=" + other},
		{name: "challenge", method: "POST", target: control.Prefix + "challenge", body: []byte("{}")},
		{name: "quota", method: "POST", target: control.Prefix + "quota", body: []byte("{}")},
		{name: "undelete", method: "POST", target: control.Prefix + "undelete", body: []byte("{}")},
		{name: "an unknown call", method: "GET", target: control.Prefix + "admin"},
	}
}

// hostile is one attacker at one target: an HTTP client over the tunnel
// dialled with one key, and the credential it signs S3 requests with.
type hostile struct {
	t      *testing.T
	label  string
	hc     *http.Client
	access string
	secret string
	// wait bounds each request; zero is 20s. An attacker the target
	// refuses at the tunnel waits for nothing but its dial to fail.
	wait time.Duration
	// close drops the attacker's tunnel client, so its key can dial
	// another node: two clients with one key displace each other.
	close func()
}

// attacker dials target at addr with key and signs S3 as access with
// secret. One per key and target: two clients with one key displace
// each other at the relay.
func attacker(t *testing.T, label string, key *identity.Identity, addr, access, secret string) *hostile {
	t.Helper()
	dl, err := transport.NewDialer(key, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	them, err := transport.KeyOf(addr)
	if err != nil {
		t.Fatal(err)
	}
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dl.Dial(ctx, them, addr) },
	}}
	return &hostile{t: t, label: label, hc: hc, access: access, secret: secret, close: func() { dl.Close() }}
}

// signingAs is h on the same connection, signing with another credential.
func (h *hostile) signingAs(label, access, secret string) *hostile {
	c := *h
	c.label, c.access, c.secret = label, access, secret
	return &c
}

// reply is what the target answered, or the error that stood for an
// answer (a refused connection).
type reply struct {
	status  int
	code    string // the S3 or control error code, if any
	message string
	err     error
}

func (r reply) String() string {
	if r.err != nil {
		return "error: " + r.err.Error()
	}
	return fmt.Sprintf("%d %s %s", r.status, r.code, r.message)
}

// refused reports whether lendisk answered a with a refusal: for a
// control call a 403; for S3 a 403 or 400 from lendisk (a 400 is
// SEC-5's answer to a key no borrower may name), not from versitygw.
func (r reply) refused(a attack) bool {
	if a.control() {
		return r.status == http.StatusForbidden
	}
	return (r.status == http.StatusForbidden || r.status == http.StatusBadRequest) && strings.Contains(r.message, "lendisk")
}

var (
	xmlCode    = regexp.MustCompile(`<Code>([^<]*)</Code>`)
	xmlMessage = regexp.MustCompile(`<Message>([^<]*)</Message>`)
)

// send sends a, SigV4-signed with h's credential unless it is a control
// call, which carries the protocol header instead.
func (h *hostile) send(a attack) reply {
	h.t.Helper()
	wait := h.wait
	if wait == 0 {
		wait = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(h.t.Context(), wait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, a.method, "http://lender:9000"+a.target, bytes.NewReader(a.body))
	if err != nil {
		h.t.Fatal(err)
	}
	for k, v := range a.header {
		req.Header.Set(k, v)
	}
	if a.control() {
		req.Header.Set(control.ProtocolHeader, control.Protocol)
	} else {
		sum := sha256.Sum256(a.body)
		payload := hex.EncodeToString(sum[:])
		req.Header.Set("X-Amz-Content-Sha256", payload)
		if a.method == "POST" && strings.HasSuffix(a.target, "?delete") {
			m := md5Sum(a.body)
			req.Header.Set("Content-Md5", m)
		}
		if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: h.access, SecretAccessKey: h.secret}, req, payload,
			"s3", "us-east-1", time.Now()); err != nil {
			h.t.Fatal(err)
		}
	}
	resp, err := h.hc.Do(req)
	if err != nil {
		return reply{err: err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	r := reply{status: resp.StatusCode}
	if m := xmlCode.FindSubmatch(body); m != nil {
		r.code = string(m[1])
	}
	if m := xmlMessage.FindSubmatch(body); m != nil {
		r.message = string(m[1])
	} else if a.control() || resp.StatusCode == http.StatusForbidden {
		r.message = string(body)
	}
	if strings.Contains(r.message, `"code"`) {
		if m := regexp.MustCompile(`"code":"([^"]*)"`).FindStringSubmatch(r.message); m != nil {
			r.code = m[1]
		}
	}
	if resp.StatusCode == http.StatusForbidden && r.message == "" {
		r.message = "lendisk (HEAD, no body)" // a refused HEAD carries no body
	}
	return r
}

// mustRefuse sends each attack and fails on any that is not refused.
func (h *hostile) mustRefuse(as []attack) {
	h.t.Helper()
	for _, a := range as {
		if r := h.send(a); !r.refused(a) {
			h.t.Errorf("%s: %s (%s %s) was not refused: %v", h.label, a.name, a.method, a.target, r)
		}
	}
}

// mustNotConnect sends each attack, each bounded by 5 seconds, and fails
// on any that is answered at all: the tunnel must refuse the connection
// before any application data (SEC-10).
func (h *hostile) mustNotConnect(as []attack) {
	h.t.Helper()
	h.wait = 5 * time.Second
	for _, a := range as {
		if r := h.send(a); r.err == nil {
			h.t.Errorf("%s: %s (%s %s) was answered: %v", h.label, a.name, a.method, a.target, r)
		}
	}
}

// mustFail sends each attack and fails on any that is not answered with
// an error status.
func (h *hostile) mustFail(as []attack) {
	h.t.Helper()
	for _, a := range as {
		if r := h.send(a); r.err != nil || r.status < 400 {
			h.t.Errorf("%s: %s (%s %s) succeeded: %v", h.label, a.name, a.method, a.target, r)
		}
	}
}

func md5Sum(b []byte) string {
	h := md5.New()
	h.Write(b)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// tree lists every file and directory under dir with its size and
// modification time, to show that nothing a refused attack sent changed
// the store.
func tree(t *testing.T, dir string) map[string]string {
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

func sameTree(t *testing.T, when string, before, after map[string]string) {
	t.Helper()
	for p, v := range before {
		if after[p] != v {
			t.Errorf("%s: %s changed: %q, then %q", when, p, v, after[p])
		}
	}
	for p := range after {
		if _, ok := before[p]; !ok {
			t.Errorf("%s: %s appeared", when, p)
		}
	}
}
