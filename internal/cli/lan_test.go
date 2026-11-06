package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/borje/lendisk/internal/bridge"
)

// lanIP is the address a "second machine" reaches the node on: the
// first non-loopback IPv4 address of an interface that is up, as on a
// NAS. A machine with none (a bare container) falls back to 127.0.0.1;
// the listener code treats it like any other address, so only the
// "another machine" part is simulated.
func lanIP(t *testing.T) string {
	t.Helper()
	ifs, _ := net.Interfaces()
	for _, ifi := range ifs {
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := ifi.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && !n.IP.IsLinkLocalUnicast() {
				if ln, err := net.Listen("tcp", net.JoinHostPort(n.IP.String(), "0")); err == nil {
					ln.Close()
					return n.IP.String()
				}
			}
		}
	}
	t.Log("no non-loopback address; using 127.0.0.1")
	return "127.0.0.1"
}

func freeAddr(t *testing.T, ip string) string {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// listenOn adds an [[endpoint.listen]] entry to the node's config file.
func (n *node) listenOn(t *testing.T, entry string) {
	t.Helper()
	f, err := os.OpenFile(n.config, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "\n[[endpoint.listen]]\n%s\n", entry)
}

// Scenario 11 and EP-17's AC: restic on another machine backs up to and
// restores from the node's LAN address, with TLS on (the generated
// certificate, trusted with --cacert) and with TLS off. A plaintext
// request to the TLS port, a request without a valid credential and the
// control API are refused; status lists the listener.
func TestBackupFromAnotherMachine(t *testing.T) {
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	ip := lanIP(t)
	for _, mode := range []string{"generate", "off"} {
		t.Run("tls="+mode, func(t *testing.T) {
			n := newNode(t)
			n.mustInit(t)
			addr := freeAddr(t, ip)
			n.listenOn(t, fmt.Sprintf("address = %q\ntls = %q", addr, mode))

			code, out, errOut := n.run("", "", "bucket", "add", "laptop", "--profile", "restic")
			if code != 0 {
				t.Fatalf("bucket add: %s", errOut)
			}
			warned := strings.Contains(errOut, "unencrypted")
			if warned != (mode == "off") {
				t.Errorf("plaintext warning shown = %v with tls = %s:\n%s", warned, mode, errOut)
			}
			c := creds{reAccess.FindStringSubmatch(out)[1], reSecret.FindStringSubmatch(out)[1]}
			scheme := map[string]string{"generate": "https", "off": "http"}[mode]
			repo := "s3:" + scheme + "://" + addr + "/laptop"
			if !strings.Contains(out, "RESTIC_REPOSITORY="+repo+"\n") {
				t.Fatalf("no lines for %s:\n%s", repo, out)
			}
			if strings.Contains(out, "--cacert") != (mode == "generate") {
				t.Errorf("--cacert printed wrongly for tls = %s:\n%s", mode, out)
			}

			before, sockErr := ownSockets()
			d := n.serve(t)

			// The second machine: its own directories, the printed lines,
			// and for TLS a copy of the certificate.
			laptop := t.TempDir()
			args := []string{}
			if mode == "generate" {
				code, cert, errOut := n.run("", "", "endpoint", "cert")
				if code != 0 {
					t.Fatalf("endpoint cert: %s", errOut)
				}
				path := regexp.MustCompile(`(?m)^Certificate: +(\S+)$`).FindStringSubmatch(cert)
				fp := regexp.MustCompile(`(?m)^SHA-256: +(\S+)$`).FindStringSubmatch(cert)
				if path == nil || fp == nil {
					t.Fatalf("endpoint cert printed:\n%s", cert)
				}
				b, err := os.ReadFile(path[1])
				if err != nil {
					t.Fatal(err)
				}
				copyPath := filepath.Join(laptop, "endpoint.crt")
				os.WriteFile(copyPath, b, 0o644)
				parsed, _ := bridge.ReadCert(laptop)
				if parsed == nil || bridge.Fingerprint(parsed.Raw) != fp[1] {
					t.Fatalf("fingerprint %s does not match the certificate", fp[1])
				}
				args = append(args, "--cacert", copyPath)
			}
			src := filepath.Join(laptop, "home")
			os.MkdirAll(src, 0o700)
			for i := 0; i < 10; i++ {
				os.WriteFile(filepath.Join(src, fmt.Sprintf("f%02d", i)), bytes.Repeat([]byte{byte(i)}, 5000*(i+1)), 0o600)
			}
			env := append(os.Environ(), "AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
				"RESTIC_REPOSITORY="+repo, "RESTIC_PASSWORD=laptop-password",
				"RESTIC_CACHE_DIR="+filepath.Join(laptop, "cache"))
			run := func(a ...string) string {
				t.Helper()
				cmd := exec.Command(restic, append(append([]string{}, args...), a...)...)
				cmd.Env = env
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("restic %s: %v\n%s", strings.Join(a, " "), err, out)
				}
				return string(out)
			}
			run("init")
			run("backup", src)
			dst := filepath.Join(laptop, "restore")
			run("restore", "latest", "--target", dst)
			for i := 0; i < 10; i++ {
				want, _ := os.ReadFile(filepath.Join(src, fmt.Sprintf("f%02d", i)))
				got, err := os.ReadFile(filepath.Join(dst, src, fmt.Sprintf("f%02d", i)))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("restored f%02d differs: %v", i, err)
				}
			}

			// Refusals on the LAN listener.
			client := &http.Client{Timeout: 10 * time.Second}
			if mode == "generate" {
				resp, err := client.Get("http://" + addr + "/laptop/")
				if err == nil {
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if resp.StatusCode != http.StatusBadRequest || bytes.Contains(b, []byte("ListBucketResult")) {
						t.Errorf("plaintext request on TLS port: %d %s", resp.StatusCode, b)
					}
				}
				client = tlsClientFor(t, filepath.Join(laptop, "endpoint.crt"))
			}
			for _, path := range []string{"/laptop/", "/laptop/config", "/_lendisk/version"} {
				resp, err := client.Get(scheme + "://" + addr + path)
				if err != nil {
					t.Fatal(err)
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusForbidden {
					t.Errorf("unsigned GET %s: %d %s", path, resp.StatusCode, b)
				}
			}
			// A signed request with a wrong secret (restic would retry it
			// for minutes, so the SDK makes it).
			wrong := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(scheme + "://" + addr),
				UsePathStyle: true, HTTPClient: client, RetryMaxAttempts: 1,
				Credentials: credentials.NewStaticCredentialsProvider(c.access, "wrong"+c.secret[5:], "")})
			if _, err := wrong.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("laptop")}); err == nil {
				t.Error("wrong secret accepted")
			}
			right := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(scheme + "://" + addr),
				UsePathStyle: true, HTTPClient: client, RetryMaxAttempts: 1,
				Credentials: credentials.NewStaticCredentialsProvider(c.access, c.secret, "")})
			if _, err := right.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("laptop")}); err != nil {
				t.Errorf("right secret refused: %v", err)
			}

			// SEC-8: the new TCP listeners are loopback and this address.
			if sockErr == nil {
				after, _ := ownSockets()
				old := map[sock]bool{}
				for _, s := range before {
					old[s] = true
				}
				for _, s := range after {
					if !old[s] && s.proto == "tcp" && s.listen && s.local != d.addr && s.local != addr {
						t.Errorf("unexpected TCP listener %s", s.local)
					}
				}
			}

			// EP-17: status lists the listener, "plaintext" when TLS is off.
			st := n.status()
			if len(st.Endpoint.Listen) != 1 || st.Endpoint.Listen[0].URL != scheme+"://"+addr ||
				st.Endpoint.Listen[0].TLS != mode || st.Endpoint.Listen[0].Plaintext != (mode == "off") {
				t.Errorf("status listeners: %+v", st.Endpoint.Listen)
			}
			_, text, _ := n.run("", "", "status")
			if strings.Contains(text, "plaintext") != (mode == "off") || !strings.Contains(text, scheme+"://"+addr) {
				t.Errorf("status text:\n%s", text)
			}
		})
	}
}

// tlsClientFor trusts certFile as a CA, as restic --cacert does.
func tlsClientFor(t *testing.T, certFile string) *http.Client {
	t.Helper()
	b, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	p := x509.NewCertPool()
	p.AppendCertsFromPEM(b)
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: p}}}
}

// EP-17: an interface entry is listed with its addresses; status lists
// one line per address, and bucket add prints lines for each.
func TestInterfaceEntryListed(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	_, port, _ := net.SplitHostPort(freeAddr(t, "127.0.0.1"))
	n.listenOn(t, "interface = \"lo\"\nport = "+port)
	st := n.status()
	found := false
	for _, l := range st.Endpoint.Listen {
		found = found || (l.URL == "https://127.0.0.1:"+port && l.Interface == "lo" && l.TLS == "generate")
	}
	if !found {
		t.Fatalf("status listeners: %+v", st.Endpoint.Listen)
	}
	_, out, _ := n.run("", "", "bucket", "add", "tools", "--profile", "generic")
	if !strings.Contains(out, "endpoint:   https://127.0.0.1:"+port) {
		t.Fatalf("bucket add lines:\n%s", out)
	}
	// A missing interface is listed without an address.
	n.listenOn(t, "interface = \"nosuch0\"\nport = "+port)
	st = n.status()
	last := st.Endpoint.Listen[len(st.Endpoint.Listen)-1]
	if last.Interface != "nosuch0" || last.URL != "" {
		t.Fatalf("missing interface: %+v", last)
	}
}

// `endpoint cert` before serve has made one says what to do.
func TestEndpointCertMissing(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	code, _, errOut := n.run("", "", "endpoint", "cert")
	if code == 0 || !strings.Contains(errOut, "lendisk serve") {
		t.Fatalf("endpoint cert without a certificate: %d %s", code, errOut)
	}
}
