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
		if f.Type.Kind() != reflect.String {
			t.Errorf("%s is %s; the loader handles string settings only, extend it", s.key, f.Type)
		}
	}
}

func TestNames(t *testing.T) {
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

func TestPrecedence(t *testing.T) {
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

func TestValidation(t *testing.T) {
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
	p := writeFile(t, "role = \"full\"\n[store]\npath = \"/x\"\n")
	l, err := load(t, []string{"--config", p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Unknown) != 1 || l.Unknown[0] != "store.path" {
		t.Fatalf("unknown = %v", l.Unknown)
	}
}

// The shipped examples must parse; keys later tickets add are only reported.
func TestExamplesParse(t *testing.T) {
	files, _ := filepath.Glob("../../examples/*/config/lendisk.toml")
	if len(files) == 0 {
		t.Skip("no examples found")
	}
	for _, p := range files {
		if _, err := load(t, []string{"--config", p}, nil); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}
