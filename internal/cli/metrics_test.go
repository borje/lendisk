package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/metrics"
	"github.com/borje/lendisk/internal/version"
)

// setMetrics adds [metrics] listen = addr to n's config file.
func (n *node) setMetrics(t *testing.T, addr string) {
	t.Helper()
	f, err := os.OpenFile(n.config, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "\n[metrics]\nlisten = %q\n", addr); err != nil {
		t.Fatal(err)
	}
}

// scrape reads addr's /metrics and parses it as the text format.
func scrape(t *testing.T, addr string) map[string]float64 {
	t.Helper()
	res, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != metrics.ContentType {
		t.Fatalf("scrape: %d %s: %s", res.StatusCode, res.Header.Get("Content-Type"), body)
	}
	fams, err := metrics.Parse(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("scrape is not valid Prometheus text: %v\n%s", err, body)
	}
	v := metrics.Values(fams)
	v["#families"] = float64(len(fams))
	return v
}

// OPS-4, SEC-8 (#43): with metrics.listen set, the daemon serves
// /metrics on that loopback address and adds no other listener than
// the S3 endpoint; every TCP listener it opens is on loopback.
func TestMetricsEndpointBindsLoopbackOnly(t *testing.T) {
	// Not parallel: it reads this process's sockets, which a parallel
	// test would add to (see parallel_test.go).
	n := newNode(t)
	n.mustInit(t)
	n.setMetrics(t, "127.0.0.1:0")
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
	var listeners []string
	for _, s := range after {
		if old[s] || s.proto != "tcp" || !s.listen {
			continue
		}
		host, _, _ := net.SplitHostPort(s.local)
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			t.Errorf("TCP listener on %s, not loopback", s.local)
		}
		listeners = append(listeners, s.local)
	}
	var m string
	for _, l := range listeners {
		if l != d.addr {
			m = l
		}
	}
	if len(listeners) != 2 || m == "" {
		t.Fatalf("new TCP listeners %v; want the S3 endpoint %s and the metrics listener", listeners, d.addr)
	}
	v := scrape(t, m)
	if v[`lendisk_info{version="`+version.Version+`"}`] != 1 {
		t.Errorf("metrics at %s: %v", m, v)
	}
	for _, k := range alert.Kinds() {
		if _, ok := v[`lendisk_alerts{kind="`+k+`"}`]; !ok {
			t.Errorf("no lendisk_alerts series for kind %s", k)
		}
	}
	if res, err := http.Get("http://" + d.addr + "/metrics"); err == nil {
		res.Body.Close()
		if res.Header.Get("Content-Type") == metrics.ContentType {
			t.Error("the S3 endpoint serves metrics")
		}
	}
}

// OPS-4 (#43): A replicates to B; A's scrape shows B's counters moved
// (bytes, objects), B's queue and lag at zero once drained, the quota
// with B and its use, alerts by kind and the bucket's offsite state,
// all as valid Prometheus text. status shows where metrics are served.
func TestMetricsCountReplication(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions // one relay for both
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c := an.addBucket(t, "laptop", "generic")
	maddr := freeAddr(t, "127.0.0.1")
	an.setMetrics(t, maddr)
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	bKey := bn.identity().NodeID().String()

	before := scrape(t, maddr)
	sent := `lendisk_replication_sent_bytes_total{place="` + bKey + `"}`
	if before[sent] != 0 {
		t.Fatalf("bytes sent before any write: %v", before[sent])
	}
	cl := s3client(a.addr, c)
	buf := make([]byte, 100<<10)
	for i := range 3 {
		rand.Read(buf)
		if _, err := cl.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String(fmt.Sprintf("data-%d", i)), Body: bytes.NewReader(buf)}); err != nil {
			t.Fatal(err)
		}
	}
	an.drained(bKey, 90*time.Second)
	v := scrape(t, maddr)
	if v[sent] < 3*100<<10 {
		t.Errorf("%s = %v, want at least %d", sent, v[sent], 3*100<<10)
	}
	if n := v[`lendisk_replication_objects_total{place="`+bKey+`",op="put"}`]; n < 3 {
		t.Errorf("objects acknowledged by B: %v, want at least 3", n)
	}
	for k, want := range map[string]float64{
		`lendisk_place_queue_objects{place="` + bKey + `"}`:          0,
		`lendisk_place_lag_seconds{place="` + bKey + `"}`:            0,
		`lendisk_place_state{place="` + bKey + `",state="ok"}`:       1,
		`lendisk_place_state{place="` + bKey + `",state="retrying"}`: 0,
		`lendisk_friend_quota_bytes{friend="` + bKey + `"}`:          10 << 30,
		`lendisk_bucket_copies{bucket="laptop"}`:                     2,
	} {
		got, ok := v[k]
		if !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", k, got, ok, want)
		}
	}
	if v[`lendisk_friend_used_there_bytes{friend="`+bKey+`"}`] < 3*100<<10 {
		t.Errorf("use at B: %v", v[`lendisk_friend_used_there_bytes{friend="`+bKey+`"}`])
	}
	// One friend for two copies: below copies, so not offsite.
	if v[`lendisk_bucket_offsite_complete{bucket="laptop"}`] != 0 || v[`lendisk_bucket_below_copies_objects{bucket="laptop"}`] != 3 {
		t.Errorf("offsite %v, below copies %v", v[`lendisk_bucket_offsite_complete{bucket="laptop"}`],
			v[`lendisk_bucket_below_copies_objects{bucket="laptop"}`])
	}
	for _, k := range alert.Kinds() {
		if _, ok := v[`lendisk_alerts{kind="`+k+`"}`]; !ok {
			t.Errorf("no lendisk_alerts series for kind %s", k)
		}
	}
	st := an.status()
	if st.Endpoint.Metrics != "http://"+maddr+"/metrics" {
		t.Errorf("status endpoint.metrics %q", st.Endpoint.Metrics)
	}
	if _, text, _ := an.run("", "", "status"); !strings.Contains(text, "Metrics: http://"+maddr+"/metrics\n") {
		t.Errorf("status text has no metrics line:\n%s", text)
	}
}
