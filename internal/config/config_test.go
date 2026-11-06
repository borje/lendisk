package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"
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
		if _, err := load(t, []string{"--config", p}, nil); err != nil {
			t.Errorf("%s: %v", p, err)
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
