package metrics

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// OPS-4: what Write produces is the text format, read back exactly,
// with label values escaped.
func TestWriteParsesBackAsPrometheusText(t *testing.T) {
	fams := []Family{
		{Name: "lendisk_replication_sent_bytes_total", Help: "bytes sent\\ with a\nnewline", Type: Counter, Samples: []Sample{
			{Labels: []Label{{"place", "a"}}, Value: 42},
			{Labels: []Label{{"place", `quote " back \ newline` + "\n"}}, Value: 1e12},
		}},
		{Name: "lendisk_place_lag_seconds", Help: "lag", Type: Gauge, Samples: []Sample{{Value: math.Inf(1)}}},
		{Name: "lendisk_empty", Help: "no samples", Type: Gauge},
	}
	var b bytes.Buffer
	if err := Write(&b, fams); err != nil {
		t.Fatal(err)
	}
	got, err := Parse(&b)
	if err != nil {
		t.Fatalf("%v\n%s", err, b.String())
	}
	if len(got) != 3 {
		t.Fatalf("families %+v", got)
	}
	for i := range fams {
		if got[i].Name != fams[i].Name || got[i].Help != fams[i].Help || got[i].Type != fams[i].Type || len(got[i].Samples) != len(fams[i].Samples) {
			t.Errorf("family %d: %+v, want %+v", i, got[i], fams[i])
			continue
		}
		for j, s := range fams[i].Samples {
			g := got[i].Samples[j]
			if g.Value != s.Value || len(g.Labels) != len(s.Labels) {
				t.Errorf("sample %d.%d: %+v, want %+v", i, j, g, s)
				continue
			}
			for k := range s.Labels {
				if g.Labels[k] != s.Labels[k] {
					t.Errorf("label %d.%d.%d: %q, want %q", i, j, k, g.Labels[k], s.Labels[k])
				}
			}
		}
	}
}

func TestWriteRefusesWhatPrometheusCannotRead(t *testing.T) {
	for name, f := range map[string]Family{
		"bad name":        {Name: "lendisk-x", Type: Gauge},
		"bad type":        {Name: "x", Type: "histogram"},
		"counter suffix":  {Name: "x", Type: Counter},
		"bad label name":  {Name: "x", Type: Gauge, Samples: []Sample{{Labels: []Label{{"a-b", "v"}}}}},
		"reserved label":  {Name: "x", Type: Gauge, Samples: []Sample{{Labels: []Label{{"__x", "v"}}}}},
		"duplicate":       {Name: "x", Type: Gauge, Samples: []Sample{{Labels: []Label{{"a", "v"}}}, {Labels: []Label{{"a", "v"}}}}},
		"negative count":  {Name: "x_total", Type: Counter, Samples: []Sample{{Value: -1}}},
		"duplicate label": {Name: "x", Type: Gauge, Samples: []Sample{{Labels: []Label{{"a", "v"}, {"a", "w"}}}}},
	} {
		if err := Write(io.Discard, []Family{f}); err == nil {
			t.Errorf("%s: written", name)
		}
	}
	if err := Write(io.Discard, []Family{{Name: "x", Type: Gauge}, {Name: "x", Type: Gauge}}); err == nil {
		t.Error("a family twice: written")
	}
}

// The parser the tests rely on refuses what Prometheus would.
func TestParseRefusesInvalidText(t *testing.T) {
	for _, text := range []string{
		"x 1\n",                                                    // no TYPE
		"# TYPE x gauge\nx{a=\"v} 1\n",                             // unterminated value
		"# TYPE x gauge\nx{a=\"v\"} one\n",                         // bad value
		"# TYPE x gauge\nx 1\n# TYPE x gauge\n",                    // TYPE twice
		"# TYPE x gauge\n# TYPE y gauge\ny 1\nx 2\n",               // family not contiguous
		"# TYPE x gauge\nx{a=\"v\"} 1\nx{a=\"v\"} 2\n",             // duplicate series
		"# TYPE x gauge\nx{a=\"\\q\"} 1\n",                         // bad escape
		"# TYPE x_total counter\nx_total -1\n",                     // negative counter
		"# TYPE x summary\nx 1\n",                                  // a type lendisk does not write
		"# TYPE x gauge\nx 1",                                      // no final newline
		"# TYPE x gauge\nx{1a=\"v\"} 1\n",                          // bad label name
		"# TYPE 1x gauge\n",                                        // bad metric name
		"# HELP x one\n# HELP x two\n# TYPE x gauge\nx 1\n",        // HELP twice
		"# TYPE x gauge\nx{a=\"v\",a=\"w\"} 1\n",                   // label twice
		"# TYPE x gauge\nx 1 notatimestamp\n",                      // bad timestamp
		"# TYPE x gauge\ny 1\n",                                    // sample of no family
		"# TYPE x gauge\nx{a=\"v\"}1\n",                            // no space before the value
		"# TYPE x gauge\nx{a=\"v\" 1\n",                            // unterminated labels
		"# TYPE x gauge\nx 1\n# HELP x late\n",                     // HELP after samples
		"# TYPE x_total counter\n# TYPE x gauge\nx_total 1\nx 1\n", // samples out of order
	} {
		if _, err := Parse(strings.NewReader(text)); err == nil {
			t.Errorf("parsed %q", text)
		}
	}
}

// OPS-4: the counters the replication worker moves, nil-safe so a
// worker without metrics needs no check.
func TestCountersAccumulatePerPlace(t *testing.T) {
	var none *Counters
	none.Sent("a", 1)
	none.Acked("a", "put")
	none.Retry("a", "network")
	c := &Counters{}
	c.Sent("a", 10)
	c.Sent("a", 5)
	c.Sent("b", 1)
	c.Acked("a", "put")
	c.Acked("a", "put")
	c.Acked("a", "delete")
	c.Retry("b", "network")
	fams := c.Families()
	var b bytes.Buffer
	if err := Write(&b, fams); err != nil {
		t.Fatal(err)
	}
	got, err := Parse(&b)
	if err != nil {
		t.Fatal(err)
	}
	v := Values(got)
	for k, want := range map[string]float64{
		`lendisk_replication_sent_bytes_total{place="a"}`:               15,
		`lendisk_replication_sent_bytes_total{place="b"}`:               1,
		`lendisk_replication_objects_total{place="a",op="put"}`:         2,
		`lendisk_replication_objects_total{place="a",op="delete"}`:      1,
		`lendisk_replication_retries_total{place="b",reason="network"}`: 1,
	} {
		if v[k] != want {
			t.Errorf("%s = %v, want %v\n%s", k, v[k], want, b.String())
		}
	}
}

// OPS-4: the endpoint serves /metrics in the text format and nothing
// else, and a collection error is a 500.
func TestServerServesMetricsOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	s := &Server{Log: slog.New(slog.DiscardHandler), Collect: func(context.Context) ([]Family, error) {
		if fail.Load() {
			return nil, io.ErrUnexpectedEOF
		}
		return []Family{{Name: "lendisk_up", Help: "1", Type: Gauge, Samples: []Sample{{Value: 1}}}}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	base := "http://" + ln.Addr().String()
	res, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != ContentType {
		t.Fatalf("%d %s", res.StatusCode, res.Header.Get("Content-Type"))
	}
	if fs, err := Parse(bytes.NewReader(body)); err != nil || Values(fs)["lendisk_up"] != 1 {
		t.Fatalf("%v %s", err, body)
	}
	for _, path := range []string{"/", "/debug/pprof/"} {
		res, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 404 {
			t.Errorf("%s: %d", path, res.StatusCode)
		}
	}
	res, err = http.Post(base+"/metrics", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST: %d", res.StatusCode)
	}
	fail.Store(true)
	res, err = http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 500 {
		t.Errorf("failed collection: %d", res.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop")
	}
}
