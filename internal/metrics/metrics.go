// Package metrics serves the daemon's Prometheus metrics on loopback
// (OPS-4, DESIGN §3.8). It owns the replication counters the worker
// moves (Counters), the text exposition format, written by hand (Write;
// version 0.0.4, counters and gauges only), and the listener (Server).
// What a scrape reports besides the counters is read from lendisk.db by
// the caller's Collect, the same numbers `lendisk status` shows.
//
// Rules a caller must keep:
//   - The listener binds loopback only (SEC-8): config.Validate refuses
//     any other metrics.listen, and the lender-only profile has none
//     (PEER-9).
//   - Labels are bounded: places (friends), buckets, alert kinds and
//     fixed reasons, never object keys.
//   - Counter names end in _total and only ever rise while the daemon
//     runs; a restart starts them at zero, which Prometheus's rate()
//     handles.
package metrics

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Metric types this package writes.
const (
	Counter = "counter"
	Gauge   = "gauge"
)

// ContentType is the text exposition format's.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Family is one metric: its name, help, type and samples.
type Family struct {
	Name, Help, Type string
	Samples          []Sample
}

// Sample is one series of a family: its labels, in order, and value.
type Sample struct {
	Labels []Label
	Value  float64
}

// Label is a label's name and value.
type Label struct{ Name, Value string }

// L builds labels from name, value pairs.
func L(pairs ...string) []Label {
	out := make([]Label, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, Label{pairs[i], pairs[i+1]})
	}
	return out
}

func metricName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		switch {
		case c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func labelName(s string) bool {
	return metricName(s) && !strings.Contains(s, ":") && !strings.HasPrefix(s, "__")
}

// check refuses a family Prometheus could not read.
func (f Family) check() error {
	if !metricName(f.Name) {
		return fmt.Errorf("metric name %q", f.Name)
	}
	switch f.Type {
	case Counter:
		if !strings.HasSuffix(f.Name, "_total") {
			return fmt.Errorf("counter %s: the name must end in _total", f.Name)
		}
	case Gauge:
	default:
		return fmt.Errorf("%s: type %q", f.Name, f.Type)
	}
	seen := map[string]bool{}
	for _, s := range f.Samples {
		names := map[string]bool{}
		for _, l := range s.Labels {
			if !labelName(l.Name) {
				return fmt.Errorf("%s: label name %q", f.Name, l.Name)
			}
			if names[l.Name] {
				return fmt.Errorf("%s: label %s twice", f.Name, l.Name)
			}
			names[l.Name] = true
		}
		if f.Type == Counter && !(s.Value >= 0) {
			return fmt.Errorf("%s: counter value %v", f.Name, s.Value)
		}
		k := series(f.Name, s.Labels)
		if seen[k] {
			return fmt.Errorf("%s: series %s twice", f.Name, k)
		}
		seen[k] = true
	}
	return nil
}

// series is a sample's name and labels, as Values keys it.
func series(name string, ls []Label) string {
	if len(ls) == 0 {
		return name
	}
	parts := make([]string, len(ls))
	for i, l := range ls {
		parts[i] = l.Name + `="` + l.Value + `"`
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

var (
	helpEscape  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	valueEscape = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
)

func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Write writes fams in the text exposition format, in order. A family
// Prometheus could not read, or one named twice, is an error and
// nothing is written.
func Write(w io.Writer, fams []Family) error {
	names := map[string]bool{}
	for _, f := range fams {
		if err := f.check(); err != nil {
			return err
		}
		if names[f.Name] {
			return fmt.Errorf("metric %s twice", f.Name)
		}
		names[f.Name] = true
	}
	bw := bufio.NewWriter(w)
	for _, f := range fams {
		fmt.Fprintf(bw, "# HELP %s %s\n# TYPE %s %s\n", f.Name, helpEscape.Replace(f.Help), f.Name, f.Type)
		for _, s := range f.Samples {
			bw.WriteString(f.Name)
			if len(s.Labels) > 0 {
				bw.WriteByte('{')
				for i, l := range s.Labels {
					if i > 0 {
						bw.WriteByte(',')
					}
					fmt.Fprintf(bw, `%s="%s"`, l.Name, valueEscape.Replace(l.Value))
				}
				bw.WriteByte('}')
			}
			bw.WriteByte(' ')
			bw.WriteString(formatValue(s.Value))
			bw.WriteByte('\n')
		}
	}
	return bw.Flush()
}

// Parse reads the text exposition format strictly: every sample under
// a TYPE line of its family, each family's lines together, no series
// twice, values and escapes as the format defines them, and only the
// types Write writes. The tests use it to check what Write and the
// daemon serve.
func Parse(r io.Reader) ([]Family, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	text := string(b)
	if text != "" && !strings.HasSuffix(text, "\n") {
		return nil, errors.New("the text does not end in a newline")
	}
	var out []Family
	index := map[string]int{}
	helped := map[string]bool{}
	sampled := map[string]bool{}
	cur := -1
	family := func(name string) (int, error) {
		i, ok := index[name]
		if ok && i != cur {
			return 0, fmt.Errorf("metric %s: its lines are not together", name)
		}
		if !ok {
			out = append(out, Family{Name: name})
			i = len(out) - 1
			index[name] = i
		}
		cur = i
		return i, nil
	}
	for n, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		n++
		if strings.TrimSpace(line) == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "#"); ok {
			fields := strings.Fields(rest)
			if len(fields) < 2 || fields[0] != "HELP" && fields[0] != "TYPE" {
				continue // a plain comment
			}
			name := fields[1]
			if !metricName(name) {
				return nil, fmt.Errorf("line %d: metric name %q", n, name)
			}
			i, err := family(name)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", n, err)
			}
			if sampled[name] {
				return nil, fmt.Errorf("line %d: %s %s after its samples", n, fields[0], name)
			}
			if fields[0] == "HELP" {
				if helped[name] {
					return nil, fmt.Errorf("line %d: HELP %s twice", n, name)
				}
				helped[name] = true
				_, h, _ := strings.Cut(strings.TrimLeft(rest, " "), " ")
				_, h, _ = strings.Cut(h, " ")
				if out[i].Help, err = unescape(h, false); err != nil {
					return nil, fmt.Errorf("line %d: %w", n, err)
				}
				continue
			}
			if out[i].Type != "" {
				return nil, fmt.Errorf("line %d: TYPE %s twice", n, name)
			}
			if len(fields) != 3 || fields[2] != Counter && fields[2] != Gauge {
				return nil, fmt.Errorf("line %d: TYPE %s: want counter or gauge", n, name)
			}
			out[i].Type = fields[2]
			continue
		}
		name, labels, value, err := parseSample(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		i, ok := index[name]
		if !ok || out[i].Type == "" {
			return nil, fmt.Errorf("line %d: sample of %s with no TYPE line before it", n, name)
		}
		if i != cur {
			return nil, fmt.Errorf("line %d: metric %s: its lines are not together", n, name)
		}
		sampled[name] = true
		out[i].Samples = append(out[i].Samples, Sample{Labels: labels, Value: value})
	}
	for _, f := range out {
		if f.Type == "" {
			return nil, fmt.Errorf("metric %s has no TYPE", f.Name)
		}
		if err := f.check(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// parseSample reads `name{labels} value [timestamp]`.
func parseSample(line string) (string, []Label, float64, error) {
	end := strings.IndexAny(line, "{ ")
	if end < 0 {
		return "", nil, 0, fmt.Errorf("sample %q has no value", line)
	}
	name, rest := line[:end], line[end:]
	if !metricName(name) {
		return "", nil, 0, fmt.Errorf("metric name %q", name)
	}
	var labels []Label
	if strings.HasPrefix(rest, "{") {
		rest = rest[1:]
		for {
			if r, ok := strings.CutPrefix(rest, "}"); ok {
				rest = r
				break
			}
			eq := strings.Index(rest, `="`)
			if eq < 0 {
				return "", nil, 0, fmt.Errorf("%s: bad labels", name)
			}
			ln := rest[:eq]
			if !labelName(ln) {
				return "", nil, 0, fmt.Errorf("%s: label name %q", name, ln)
			}
			rest = rest[eq+2:]
			var v strings.Builder
			closed := false
			for i := 0; i < len(rest); i++ {
				c := rest[i]
				if c == '"' {
					rest, closed = rest[i+1:], true
					break
				}
				if c == '\\' && i+1 < len(rest) {
					i++
					switch rest[i] {
					case '\\', '"':
						v.WriteByte(rest[i])
					case 'n':
						v.WriteByte('\n')
					default:
						return "", nil, 0, fmt.Errorf("%s: bad escape \\%c", name, rest[i])
					}
					continue
				}
				v.WriteByte(c)
			}
			if !closed {
				return "", nil, 0, fmt.Errorf("%s: unterminated label value", name)
			}
			labels = append(labels, Label{ln, v.String()})
			if r, ok := strings.CutPrefix(rest, ","); ok {
				rest = r
			} else if !strings.HasPrefix(rest, "}") {
				return "", nil, 0, fmt.Errorf("%s: bad labels", name)
			}
		}
	}
	if !strings.HasPrefix(rest, " ") {
		return "", nil, 0, fmt.Errorf("%s: no space before the value", name)
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return "", nil, 0, fmt.Errorf("%s: want a value and an optional timestamp", name)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, fmt.Errorf("%s: value %q", name, fields[0])
	}
	if len(fields) == 2 {
		if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
			return "", nil, 0, fmt.Errorf("%s: timestamp %q", name, fields[1])
		}
	}
	return name, labels, v, nil
}

// unescape reads a HELP text (quote false) or label value.
func unescape(s string, quote bool) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch {
		case s[i] == '\\':
			b.WriteByte('\\')
		case s[i] == 'n':
			b.WriteByte('\n')
		case s[i] == '"' && quote:
			b.WriteByte('"')
		default:
			return "", fmt.Errorf("bad escape \\%c", s[i])
		}
	}
	return b.String(), nil
}

// Values maps each series, `name{label="value",…}` with its labels in
// order and unescaped, to its value.
func Values(fams []Family) map[string]float64 {
	out := map[string]float64{}
	for _, f := range fams {
		for _, s := range f.Samples {
			out[series(f.Name, s.Labels)] = s.Value
		}
	}
	return out
}

// Counters are the replication worker's counters (OPS-4), per place:
// body bytes sent, rows acknowledged by operation, and failed sends
// that will be retried, by reason. A nil *Counters counts nothing.
type Counters struct {
	mu      sync.Mutex
	sent    map[string]float64
	acked   map[[2]string]float64
	retries map[[2]string]float64
}

// Retry reasons.
const (
	RetryNetwork = "network" // the place did not answer, or failed
	RetryStore   = "store"   // this node's store could not be read
	RetryQuota   = "quota"   // the place is full for this node (507)
	RetryRemoved = "removed" // the place refuses this node
	RetryBusy    = "busy"    // the place asks to send again shortly (503 SlowDown, #92)
)

// Sent counts n body bytes sent to place.
func (c *Counters) Sent(place string, n int64) {
	if c == nil || n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sent == nil {
		c.sent = map[string]float64{}
	}
	c.sent[place] += float64(n)
}

// Acked counts a row of op ("put", "delete") place acknowledged.
func (c *Counters) Acked(place, op string) {
	if c == nil {
		return
	}
	c.add(&c.acked, place, op)
}

// Retry counts a send to place that failed and will be retried.
func (c *Counters) Retry(place, reason string) {
	if c == nil {
		return
	}
	c.add(&c.retries, place, reason)
}

func (c *Counters) add(m *map[[2]string]float64, place, what string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if *m == nil {
		*m = map[[2]string]float64{}
	}
	(*m)[[2]string{place, what}]++
}

// Families are the counters as metrics, places in order.
func (c *Counters) Families() []Family {
	sent := Family{Name: "lendisk_replication_sent_bytes_total", Type: Counter,
		Help: "Object bytes replication sent to the place, every attempt and part included."}
	acked := Family{Name: "lendisk_replication_objects_total", Type: Counter,
		Help: "Queue rows the place acknowledged, by operation (put, delete)."}
	retries := Family{Name: "lendisk_replication_retries_total", Type: Counter,
		Help: "Sends to the place that failed and wait to be retried, by reason (network, store, quota, removed, busy)."}
	if c == nil {
		return []Family{sent, acked, retries}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range slices.Sorted(maps.Keys(c.sent)) {
		sent.Samples = append(sent.Samples, Sample{Labels: L("place", p), Value: c.sent[p]})
	}
	pairs := func(m map[[2]string]float64, label string) []Sample {
		keys := slices.Collect(maps.Keys(m))
		slices.SortFunc(keys, func(a, b [2]string) int { return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1]) })
		var out []Sample
		for _, k := range keys {
			out = append(out, Sample{Labels: L("place", k[0], label, k[1]), Value: m[k]})
		}
		return out
	}
	acked.Samples = pairs(c.acked, "op")
	retries.Samples = pairs(c.retries, "reason")
	return []Family{sent, acked, retries}
}

// Server serves GET /metrics: what Collect returns, in the text format.
type Server struct {
	Collect func(ctx context.Context) ([]Family, error)
	Log     *slog.Logger
}

// ServeHTTP answers a scrape.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fams, err := s.Collect(r.Context())
	if err != nil {
		s.Log.Warn("metrics: collecting failed", "err", err)
		http.Error(w, "collecting metrics failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var b strings.Builder
	if err := Write(&b, fams); err != nil {
		s.Log.Error("metrics: writing failed", "err", err)
		http.Error(w, "writing metrics failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ContentType)
	io.WriteString(w, b.String())
}

// Serve serves on ln until ctx ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      time.Minute,
		IdleTimeout:       5 * time.Minute,
		ErrorLog:          slog.NewLogLogger(s.Log.Handler(), slog.LevelDebug),
	}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
