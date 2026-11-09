package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/pace"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func writeFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "lendisk.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func load(t *testing.T, args []string, e map[string]string) (*Loaded, error) {
	t.Helper()
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	f := RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f.load(env(e))
}

// TestEverySettingHasFlagAndEnv is OPS-6's rule, enforced on every field
// any later change adds to Config.
func TestEverySettingHasFlagAndEnv(t *testing.T) {
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	RegisterFlags(fs)
	ss := settings()
	if len(ss) == 0 {
		t.Fatal("no settings found")
	}
	seenEnv := map[string]string{}
	for _, s := range ss {
		if fs.Lookup(s.Flag()) == nil {
			t.Errorf("%s has no flag", s.key)
		}
		if s.help == "" {
			t.Errorf("%s has no help tag", s.key)
		}
		if prev, dup := seenEnv[s.Env()]; dup {
			t.Errorf("%s and %s share env var %s", prev, s.key, s.Env())
		}
		seenEnv[s.Env()] = s.key
		f := reflect.TypeOf(Config{}).FieldByIndex(s.field)
		switch {
		case f.Type.Kind() == reflect.String:
		case f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Struct:
			// An array of tables: one repeatable flag and one variable,
			// entries in the compact form parseList reads.
			if !s.list {
				t.Errorf("%s is a list but not registered as one", s.key)
			}
			for i := 0; i < f.Type.Elem().NumField(); i++ {
				ef := f.Type.Elem().Field(i)
				if k := ef.Type.Kind(); k != reflect.String && k != reflect.Int {
					t.Errorf("%s.%s is %s; the compact form handles strings and ints only, extend it", s.key, ef.Tag.Get("toml"), ef.Type)
				}
			}
		default:
			t.Errorf("%s is %s; the loader handles strings and arrays of tables only, extend it", s.key, f.Type)
		}
	}
}

func TestSettingFlagAndEnvNamesFollowTheKey(t *testing.T) {
	for _, s := range settings() {
		if s.key == "log.level" {
			if s.Flag() != "log-level" || s.Env() != "LENDISK_LOG_LEVEL" {
				t.Fatalf("got %s %s", s.Flag(), s.Env())
			}
			return
		}
	}
	t.Fatal("log.level not found")
}

func TestDefaultsWhenFileMissing(t *testing.T) {
	l, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "none.toml")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.FileExists || l.Role != "full" || l.DataDir != "/var/lib/lendisk" || l.Log.Level != "info" {
		t.Fatalf("defaults wrong: %+v", l)
	}
}

func TestFlagOverEnvOverFileOverDefault(t *testing.T) {
	p := writeFile(t, "role = \"lender\"\n[log]\nlevel = \"warn\"\nformat = \"json\"\n")
	e := map[string]string{EnvConfig: p, "LENDISK_LOG_LEVEL": "error", "LENDISK_LOG_FORMAT": "text"}
	l, err := load(t, []string{"--log-format", "json"}, e)
	if err != nil {
		t.Fatal(err)
	}
	if l.Path != p || !l.FileExists {
		t.Fatalf("config path from env not used: %s", l.Path)
	}
	if l.Role != "lender" { // file over default
		t.Errorf("role %q", l.Role)
	}
	if l.Log.Level != "error" { // env over file
		t.Errorf("level %q", l.Log.Level)
	}
	if l.Log.Format != "json" { // flag over env
		t.Errorf("format %q", l.Log.Format)
	}
	if l.ConfigDir() != filepath.Dir(p) {
		t.Errorf("config dir %s", l.ConfigDir())
	}
}

func TestInvalidSettingsAndSyntaxRefused(t *testing.T) {
	cases := []string{
		`role = "borrower"`,
		"[log]\nlevel = \"loud\"",
		`data_dir = "relative/path"`,
	}
	for _, c := range cases {
		if _, err := load(t, []string{"--config", writeFile(t, c)}, nil); err == nil {
			t.Errorf("accepted %q", c)
		}
	}
	if _, err := load(t, []string{"--config", writeFile(t, "role = ")}, nil); err == nil ||
		!strings.Contains(err.Error(), "reading") {
		t.Errorf("syntax error not reported: %v", err)
	}
}

func TestUnknownKeysReported(t *testing.T) {
	p := writeFile(t, "role = \"full\"\n[store]\nengine = \"x\"\n")
	l, err := load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Unknown) != 1 || l.Unknown[0] != "store.engine" {
		t.Fatalf("unknown = %v", l.Unknown)
	}
}

// The v1 examples must parse; keys later tickets add are only reported.
// garage-on-lan and provider-store-vps are post-v1 (EP-16, store.backend s3).
func TestExamplesParse(t *testing.T) {
	var files []string
	for _, ex := range []string{"default-nas", "lender-only-pi"} {
		files = append(files, "../../examples/"+ex+"/config/lendisk.toml")
	}
	for _, p := range files {
		l, err := load(t, []string{"--config", p}, nil)
		if err != nil {
			t.Errorf("%s: %v", p, err)
			continue
		}
		// serve refuses a hook file that does not exist (#13), so an
		// example names one only in a comment.
		if l.Alerts.Command != "" {
			t.Errorf("%s sets alerts.command = %q; serve would refuse to start until it exists", p, l.Alerts.Command)
		}
	}
}

// EP-14 (#8): the store reconciler's interval and delete limit are
// settings, a day and 10% by default.
func TestReconcileSettings(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.ReconcileEvery() != 24*time.Hour || l.Store.ReconcileDeleteLimit.Int() != 10 {
		t.Fatalf("defaults %v %v", l.ReconcileEvery(), l.Store.ReconcileDeleteLimit)
	}
	l, err = load(t, []string{"--config", writeFile(t, "[store]\nreconcile_every = \"6h\"\nreconcile_delete_limit = 25\n")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.ReconcileEvery() != 6*time.Hour || l.Store.ReconcileDeleteLimit.Int() != 25 {
		t.Fatalf("from the file %v %v", l.ReconcileEvery(), l.Store.ReconcileDeleteLimit)
	}
	for _, bad := range [][]string{{"--store-reconcile-every", "0s"}, {"--store-reconcile-every", "daily"},
		{"--store-reconcile-delete-limit", "0"}, {"--store-reconcile-delete-limit", "101"}, {"--store-reconcile-delete-limit", "5%"}} {
		if _, err := load(t, append([]string{"--config", none}, bad...), nil); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

// VER-5 (#31): the bit-rot sample is a percentage of the stored bytes a
// week, 0.1 (VER-5's least) by default and never below it, and its
// disk reads are paced, 8M a second by default; empty is no limit.
func TestBitrotSettings(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.BitrotSample() != 0.1 || l.Pace().Scrub != 8<<20 {
		t.Fatalf("defaults %v %d", l.BitrotSample(), l.Pace().Scrub)
	}
	l, err = load(t, []string{"--config", writeFile(t, "[store]\nbitrot_sample = \"2.5\"\nbitrot_read_limit = \"1M\"\n")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.BitrotSample() != 2.5 || l.Pace().Scrub != 1<<20 {
		t.Fatalf("from the file %v %d", l.BitrotSample(), l.Pace().Scrub)
	}
	l, err = load(t, []string{"--config", none, "--store-bitrot-read-limit", ""}, map[string]string{"LENDISK_STORE_BITROT_SAMPLE": "100"})
	if err != nil {
		t.Fatal(err)
	}
	if l.BitrotSample() != 100 || l.Pace().Scrub != 0 {
		t.Fatalf("from the flag and environment %v %d", l.BitrotSample(), l.Pace().Scrub)
	}
	for _, bad := range [][]string{{"--store-bitrot-sample", "0.05"}, {"--store-bitrot-sample", "0"},
		{"--store-bitrot-sample", "101"}, {"--store-bitrot-sample", "1%"}, {"--store-bitrot-sample", "NaN"},
		{"--store-bitrot-read-limit", "10K"}, {"--store-bitrot-read-limit", "fast"}} {
		if _, err := load(t, append([]string{"--config", none}, bad...), nil); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

func TestStoreAndEndpointSettings(t *testing.T) {
	l, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "none.toml"), "--data-dir", "/srv/l"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.StorePath() != "/srv/l/store" || l.Endpoint.Loopback != "127.0.0.1:9000" || l.Store.FreeSpaceFloor != "5%" {
		t.Fatalf("defaults: %s %s %s", l.StorePath(), l.Endpoint.Loopback, l.Store.FreeSpaceFloor)
	}
	// EP-5: the loopback setting can never name a network address.
	for _, bad := range []string{"0.0.0.0:9000", "192.168.1.10:9000", ":9000", "[::]:9000", "127.0.0.1"} {
		if _, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--endpoint-loopback", bad}, nil); err == nil {
			t.Errorf("accepted endpoint.loopback %q", bad)
		}
	}
	for _, good := range []string{"127.0.0.1:0", "[::1]:9000", "localhost:9001"} {
		if _, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--endpoint-loopback", good}, nil); err != nil {
			t.Errorf("refused endpoint.loopback %q: %v", good, err)
		}
	}
	if _, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--store-backend", "s3"}, nil); err == nil {
		t.Error("accepted store.backend s3 in v1")
	}
}

func TestFloorParsesPercentOrSizeAndRefusesNonsense(t *testing.T) {
	cases := map[string]Floor{"5%": {Percent: 5}, "0.5%": {Percent: 0.5}, "20G": {Bytes: 20 << 30}, "512MiB": {Bytes: 512 << 20}, "1000": {Bytes: 1000}}
	for in, want := range cases {
		got, err := ParseFloor(in)
		if err != nil || got != want {
			t.Errorf("ParseFloor(%q) = %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "x", "101%", "-1%", "5Q"} {
		if _, err := ParseFloor(bad); err == nil {
			t.Errorf("ParseFloor(%q) accepted", bad)
		}
	}
}

// EP-17 and OPS-6: [[endpoint.listen]] from the file, the environment
// and a repeatable flag, each replacing the whole list.
func TestEndpointListenSources(t *testing.T) {
	p := writeFile(t, `
[[endpoint.listen]]
address = "192.168.1.10:9000"

[[endpoint.listen]]
interface = "eth0"
port = 9001
tls = "off"

[[endpoint.listen]]
address = "[fd00::5]:9002"
tls = "files"
cert = "/etc/ssl/nas.crt"
key = "/etc/ssl/nas.key"
`)
	l, err := load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []Listen{
		{Address: "192.168.1.10:9000", TLS: "generate"},
		{Interface: "eth0", Port: 9001, TLS: "off"},
		{Address: "[fd00::5]:9002", TLS: "files", Cert: "/etc/ssl/nas.crt", Key: "/etc/ssl/nas.key"},
	}
	if !reflect.DeepEqual(l.Endpoint.Listen, want) || len(l.Unknown) != 0 {
		t.Fatalf("file: %+v (unknown %v)", l.Endpoint.Listen, l.Unknown)
	}

	e := map[string]string{"LENDISK_ENDPOINT_LISTEN": "address=192.168.1.11:9000,tls=off; interface=eth1,port=9000"}
	l, err = load(t, []string{"--config", p}, e)
	if err != nil {
		t.Fatal(err)
	}
	want = []Listen{{Address: "192.168.1.11:9000", TLS: "off"}, {Interface: "eth1", Port: 9000, TLS: "generate"}}
	if !reflect.DeepEqual(l.Endpoint.Listen, want) {
		t.Fatalf("env: %+v", l.Endpoint.Listen)
	}

	l, err = load(t, []string{"--config", p, "--endpoint-listen", "address=10.0.0.2:9000",
		"--endpoint-listen", "interface=wlan0,port=9443,tls=generate"}, e)
	if err != nil {
		t.Fatal(err)
	}
	want = []Listen{{Address: "10.0.0.2:9000", TLS: "generate"}, {Interface: "wlan0", Port: 9443, TLS: "generate"}}
	if !reflect.DeepEqual(l.Endpoint.Listen, want) {
		t.Fatalf("flag: %+v", l.Endpoint.Listen)
	}

	// An empty variable clears the file's list.
	l, err = load(t, []string{"--config", p}, map[string]string{"LENDISK_ENDPOINT_LISTEN": ""})
	if err != nil || len(l.Endpoint.Listen) != 0 {
		t.Fatalf("empty env: %+v %v", l.Endpoint.Listen, err)
	}
}

// EP-17: wildcards, empty hosts, names and malformed entries are refused
// at load, and so is any entry in the lender-only profile (PEER-9).
func TestEndpointListenRefused(t *testing.T) {
	bad := []string{
		"address=0.0.0.0:9000",
		"address=[::]:9000",
		"address=:9000",
		"address=192.168.1.10",
		"address=nas.local:9000",
		"address=192.168.1.10:0",
		"address=192.168.1.10:9000,port=9000",
		"interface=eth0",
		"interface=eth0,port=70000",
		"interface=eth0,address=192.168.1.10:9000,port=9000",
		"port=9000",
		"address=192.168.1.10:9000,tls=maybe",
		"address=192.168.1.10:9000,tls=files",
		"address=192.168.1.10:9000,tls=files,cert=rel.crt,key=/k",
		"address=192.168.1.10:9000,cert=/c,key=/k",
		"address=192.168.1.10:9000,colour=blue",
		"interface=eth0,port=nine",
		"address",
	}
	for _, b := range bad {
		if _, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--endpoint-listen", b}, nil); err == nil {
			t.Errorf("accepted %q", b)
		}
	}
	_, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--role", "lender",
		"--endpoint-listen", "address=192.168.1.10:9000"}, nil)
	if err == nil || !strings.Contains(err.Error(), "lender") {
		t.Errorf("lender-only profile accepted endpoint.listen: %v", err)
	}
	for _, good := range []string{"address=192.168.1.10:9000", "address=[fd00::1]:9000,tls=off", "interface=lo,port=9000"} {
		if _, err := load(t, []string{"--config", filepath.Join(t.TempDir(), "x.toml"), "--endpoint-listen", good}, nil); err != nil {
			t.Errorf("refused %q: %v", good, err)
		}
	}
}

// PEER-9, OPS-4 (#41, #43): the lender-only profile has no metrics
// listener, so metrics.listen is refused at load in the lender role,
// from the file, a flag or the environment (Config.Validate); a full
// node takes it. A key no version knows is only reported, in both roles.
func TestLenderOnlyRefusesMetricsSettings(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	for _, c := range []struct {
		args []string
		env  map[string]string
	}{
		{[]string{"--config", writeFile(t, "role = \"lender\"\n[metrics]\nlisten = \"127.0.0.1:9101\"\n")}, nil},
		{[]string{"--config", none, "--role", "lender", "--metrics-listen", "127.0.0.1:9101"}, nil},
		{[]string{"--config", none}, map[string]string{"LENDISK_ROLE": "lender", "LENDISK_METRICS_LISTEN": "127.0.0.1:9101"}},
	} {
		_, err := load(t, c.args, c.env)
		if err == nil || !strings.Contains(err.Error(), "metrics.listen") || !strings.Contains(err.Error(), "PEER-9") {
			t.Errorf("lender-only profile accepted metrics.listen (%v %v): %v", c.args, c.env, err)
		}
	}
	l, err := load(t, []string{"--config", writeFile(t, "role = \"full\"\n[metrics]\nlisten = \"127.0.0.1:9101\"\n")}, nil)
	if err != nil || l.Metrics.Listen != "127.0.0.1:9101" || len(l.Unknown) != 0 {
		t.Errorf("full node: %v %v", l, err)
	}
	// Empty is no listener, in either role.
	if _, err := load(t, []string{"--config", writeFile(t, "role = \"lender\"\n[metrics]\nlisten = \"\"\n")}, nil); err != nil {
		t.Errorf("lender with metrics off: %v", err)
	}
	for _, role := range []string{"full", "lender"} {
		l, err := load(t, []string{"--config", writeFile(t, "role = \""+role+"\"\n[endpoint]\nmetrics = \"127.0.0.1:9101\"\n")}, nil)
		if err != nil || len(l.Unknown) != 1 {
			t.Errorf("%s node with an unknown metrics key: %v %v", role, l, err)
		}
	}
}

// OPS-4, SEC-8 (#43): metrics are served on loopback only; off by
// default.
func TestMetricsListenLoopbackOnly(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil || l.Metrics.Listen != "" {
		t.Fatalf("default metrics.listen %q: %v", l.Metrics.Listen, err)
	}
	for _, bad := range []string{"192.168.1.10:9101", "0.0.0.0:9101", "[::]:9101", ":9101", "localhost:9101",
		"example.com:9101", "127.0.0.1", "127.0.0.1:99999", "[fd00::1]:9101"} {
		_, err := load(t, []string{"--config", none, "--metrics-listen", bad}, nil)
		if err == nil || !strings.Contains(err.Error(), "metrics.listen") || !strings.Contains(err.Error(), "loopback") {
			t.Errorf("accepted %q: %v", bad, err)
		}
	}
	for _, good := range []string{"127.0.0.1:9101", "127.0.0.2:9101", "[::1]:9101", "127.0.0.1:0"} {
		if _, err := load(t, []string{"--config", none, "--metrics-listen", good}, nil); err != nil {
			t.Errorf("refused %q: %v", good, err)
		}
	}
}

// OPS-2, OPS-3 (#13): the [alerts] section, its default threshold, and
// refusals that never echo the webhook, which may hold a secret.
func TestAlertSettings(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.Alerts.Command != "" || l.Alerts.Webhook != "" || l.Alerts.UnreachableAfter != "24h" || l.UnreachableAfter() != 24*time.Hour {
		t.Fatalf("defaults %+v", l.Alerts)
	}
	p := writeFile(t, "[alerts]\ncommand = \"/etc/lendisk/hooks/ntfy.sh\"\nwebhook = \"https://hc-ping.com/uuid\"\nunreachable_after = \"6h\"\n")
	l, err = load(t, []string{"--config", p}, map[string]string{"LENDISK_ALERTS_UNREACHABLE_AFTER": "90m"})
	if err != nil {
		t.Fatal(err)
	}
	if l.Alerts.Command != "/etc/lendisk/hooks/ntfy.sh" || l.Alerts.Webhook != "https://hc-ping.com/uuid" || l.UnreachableAfter() != 90*time.Minute {
		t.Fatalf("alerts %+v", l.Alerts)
	}
	for _, bad := range []string{"0", "-1h", "a day"} {
		if _, err := load(t, []string{"--config", none, "--alerts-unreachable-after", bad}, nil); err == nil {
			t.Errorf("accepted unreachable_after %q", bad)
		}
	}
	for _, bad := range []string{"hc-ping.com/s3cr3t", "ftp://host/s3cr3t", "https:///s3cr3t"} {
		_, err := load(t, []string{"--config", none, "--alerts-webhook", bad}, nil)
		if err == nil {
			t.Errorf("accepted webhook %q", bad)
		} else if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("the refusal echoes the webhook: %v", err)
		}
	}
}

// EP-6, D6 (#19): the [replication] section: copies for buckets created
// without --copies (a TOML integer), and the placement thresholds.
func TestReplicationSettings(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.Replication.Copies.Int() != 2 || l.RerouteAfter() != time.Hour || l.ReplaceAfter() != 7*24*time.Hour {
		t.Fatalf("defaults %+v", l.Replication)
	}
	p := writeFile(t, "[replication]\ncopies = 1\nreroute_after = \"30m\"\nreplace_after = \"72h\"\n")
	l, err = load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.Replication.Copies.Int() != 1 || l.RerouteAfter() != 30*time.Minute || l.ReplaceAfter() != 72*time.Hour {
		t.Fatalf("from the file %+v", l.Replication)
	}
	for _, bad := range [][]string{{"--replication-copies", "0"}, {"--replication-copies", "two"},
		{"--replication-reroute-after", "0"}, {"--replication-replace-after", "a week"}} {
		if _, err := load(t, append([]string{"--config", none}, bad...), nil); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}

// LEND-1, LEND-8 (#24): lending.max_total caps the sum of the node's
// numbers; unset means no cap beyond the store's floor.
func TestLendingMaxTotal(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := l.MaxTotal(); ok || n != 0 || l.Lending.MaxTotal != "" {
		t.Fatalf("default max_total %q: %d %v", l.Lending.MaxTotal, n, ok)
	}
	p := writeFile(t, "[lending]\nmax_total = \"240G\"\n")
	l, err = load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := l.MaxTotal(); !ok || n != 240<<30 {
		t.Fatalf("max_total from the file: %d %v", n, ok)
	}
	l, err = load(t, []string{"--config", p}, map[string]string{"LENDISK_LENDING_MAX_TOTAL": "1T"})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := l.MaxTotal(); n != 1<<40 {
		t.Fatalf("max_total from the environment: %d", n)
	}
	for _, bad := range []string{"lots", "-1G", "0"} {
		if _, err := load(t, []string{"--config", none, "--lending-max-total", bad}, nil); err == nil {
			t.Errorf("accepted max_total %q", bad)
		}
	}
}

// REPL-6, REPL-7, LEND-5 (#23): the part size, the schedule, the global
// and per-friend limits of this node's replication, and per-borrower
// limits on what it lends, as pace.Settings. A rate is bytes a second.
func TestPacingSettings(t *testing.T) {
	b, c := identity.NodeID{1}.String(), identity.NodeID{2}.String()
	none := filepath.Join(t.TempDir(), "none.toml")
	l, err := load(t, []string{"--config", none}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.PartSize() != 16<<20 {
		t.Fatalf("default part size %d", l.PartSize())
	}
	if s := l.Pace(); s.Up != 0 || s.Down != 0 || len(s.Peers) != 0 || len(s.Lend) != 0 || len(s.Schedule) != 0 {
		t.Fatalf("default pacing %+v", s)
	}
	p := writeFile(t, `
[replication]
part_size = "32M"
schedule = "01:00-07:00, 12:00-13:00"
upload_limit = "20M"
download_limit = "512K"

[[replication.peer_limit]]
peer = "`+b+`"
upload = "8M"

[lending]
[[lending.borrower_limit]]
peer = "`+c+`"
receive = "1M"
send = "2M"
`)
	l, err = load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := l.Pace()
	if l.PartSize() != 32<<20 || s.Up != 20<<20 || s.Down != 512<<10 || s.Schedule.String() != "01:00-07:00,12:00-13:00" ||
		s.Peers[b] != (pace.Rates{Up: 8 << 20}) || s.Lend[c] != (pace.Rates{Up: 2 << 20, Down: 1 << 20}) || len(l.Unknown) != 0 {
		t.Fatalf("from the file: part %d, %+v, unknown %v", l.PartSize(), s, l.Unknown)
	}
	l, err = load(t, []string{"--config", p, "--replication-peer-limit", "peer=" + c + ",download=64K"},
		map[string]string{"LENDISK_REPLICATION_SCHEDULE": ""})
	if err != nil {
		t.Fatal(err)
	}
	if s := l.Pace(); len(s.Schedule) != 0 || len(s.Peers) != 1 || s.Peers[c] != (pace.Rates{Down: 64 << 10}) {
		t.Fatalf("from flags and the environment: %+v", s)
	}
	for _, bad := range [][]string{
		{"--replication-part-size", "4M"}, {"--replication-part-size", "6G"}, {"--replication-part-size", "big"},
		{"--replication-schedule", "01:00"}, {"--replication-schedule", "07:00-07:00"},
		{"--replication-upload-limit", "8K"}, {"--replication-download-limit", "fast"},
		{"--replication-peer-limit", "peer=bjorn,upload=8M"}, {"--replication-peer-limit", "upload=8M"},
		{"--replication-peer-limit", "peer=" + b + ",upload=1K"},
		{"--replication-peer-limit", "peer=" + b + ",upload=1M;peer=" + b + ",download=1M"},
		{"--lending-borrower-limit", "peer=" + c + ",receive=10K"},
	} {
		if _, err := load(t, append([]string{"--config", none}, bad...), nil); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}
