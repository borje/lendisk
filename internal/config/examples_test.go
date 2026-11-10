package config

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

// The examples directory against the shipped settings (OPS-6, COMPAT-2,
// #44). Each example's lendisk.toml loads without an unknown key and
// passes validation; the settings it shows commented out are real keys
// too; and together the v1 examples show every setting there is, so a
// new one cannot ship undocumented. The environment files, compose
// files and the systemd unit name only variables lendisk reads, and run
// it unprivileged.

const examplesDir = "../../examples"

// postV1Examples use the s3 store backend (EP-16), which v1 refuses;
// they are checked for keys only, and may use postV1Keys.
var postV1Examples = map[string]bool{"garage-on-lan": true, "provider-store-vps": true}

var postV1Keys = map[string]bool{"store.endpoint": true, "store.region": true, "store.allow_remote": true}

// exampleConfigs is every example's lendisk.toml, by example name.
func exampleConfigs(t *testing.T) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(examplesDir, "*", "config", "lendisk.toml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no example configurations: %v", err)
	}
	out := map[string]string{}
	for _, p := range paths {
		out[filepath.Base(filepath.Dir(filepath.Dir(p)))] = p
	}
	return out
}

// reCommentedSetting is a commented-out setting or table header:
// `# key = value` or `# [table]`, `# [[table]]`. Prose comments do not
// match.
var reCommentedSetting = regexp.MustCompile(`^\s*#\s?(\[\[?[a-z_.]+\]\]?|[a-z_]+\s*=\s*\S.*)$`)

// uncomment returns the file with its commented-out settings restored,
// in place, so they decode in the table they are written under.
func uncomment(body string) string {
	var b strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if m := reCommentedSetting.FindStringSubmatch(line); m != nil {
			line = m[1]
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

// documentedKeys decodes body into a Config and returns every key it
// holds, and those Config has no field for.
func documentedKeys(t *testing.T, name, body string) (keys, unknown []string) {
	t.Helper()
	var c Config
	md, err := toml.Decode(body, &c)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	for _, k := range md.Keys() {
		keys = append(keys, k.String())
	}
	for _, k := range md.Undecoded() {
		if typ := md.Type(k...); typ != "Hash" && typ != "ArrayHash" {
			unknown = append(unknown, k.String())
		}
	}
	return keys, unknown
}

func TestExamplesMatchTheShippedSettings(t *testing.T) {
	shown := map[string]bool{}
	for name, p := range exampleConfigs(t) {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		keys, unknown := documentedKeys(t, name, uncomment(string(raw)))
		for _, k := range unknown {
			if !postV1Examples[name] || !postV1Keys[k] {
				t.Errorf("%s: %s is not a setting (active or commented out)", name, k)
			}
		}
		if postV1Examples[name] {
			continue
		}
		for _, k := range keys {
			shown[k] = true
		}
		// The file as shipped: loads, validates, and names no hook that
		// does not exist (serve refuses one, #13).
		l, err := load(t, []string{"--config", p}, nil)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(l.Unknown) > 0 {
			t.Errorf("%s: unknown keys %v", name, l.Unknown)
		}
		if l.Alerts.Command != "" {
			t.Errorf("%s sets alerts.command = %q; serve would refuse to start until it exists", name, l.Alerts.Command)
		}
	}
	var missing []string
	for _, s := range settings() {
		if !shown[s.key] {
			missing = append(missing, s.key)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("settings no v1 example shows, set or commented out: %v", missing)
	}
}

// The detector for commented-out settings: settings and table headers
// match, prose and shell lines in comments do not.
func TestCommentedOutSettingsAreRecognised(t *testing.T) {
	for line, want := range map[string]string{
		`# command = "/etc/lendisk/hooks/ntfy.sh"`:     `command = "/etc/lendisk/hooks/ntfy.sh"`,
		`# [[relays.list]]`:                            `[[relays.list]]`,
		`  # port = 41641   # NET-3`:                   `port = 41641   # NET-3`,
		`#   lendisk invite --quota 200G`:              "",
		`#   #!/bin/sh`:                                "",
		`# S3 for backup tools on this host (EP-1).`:   "",
		`#   LENDISK_OVERFLOW_B2_ACCESS_KEY / _SECRET`: "",
	} {
		got := ""
		if m := reCommentedSetting.FindStringSubmatch(line); m != nil {
			got = m[1]
		}
		if got != want {
			t.Errorf("%q: got %q, want %q", line, got, want)
		}
	}
}

var reEnvName = regexp.MustCompile(`\bLENDISK_[A-Z0-9_]+\b`)

// The variables the deployment files name are ones lendisk reads: a
// setting's, LENDISK_CONFIG, or an overflow target's credential pair
// (SEC-3, S3-1); the post-v1 store credential only where it says so.
func TestExampleEnvironmentNamesAreRead(t *testing.T) {
	known := map[string]bool{EnvConfig: true}
	for _, s := range settings() {
		known[s.Env()] = true
	}
	overflow := regexp.MustCompile(`^LENDISK_OVERFLOW_[A-Z0-9_]+_(ACCESS|SECRET)_KEY$`)
	postV1 := map[string]bool{"LENDISK_STORE_ACCESS_KEY": true, "LENDISK_STORE_SECRET_KEY": true}
	var files []string
	for _, pat := range []string{"*/*.env.example", "*/compose.yaml", "systemd/*.service"} {
		m, _ := filepath.Glob(filepath.Join(examplesDir, pat))
		files = append(files, m...)
	}
	if len(files) < 5 {
		t.Fatalf("found only %v", files)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		later := postV1Examples[filepath.Base(filepath.Dir(f))] || strings.Contains(string(b), "post-v1")
		for _, v := range reEnvName.FindAllString(string(b), -1) {
			if strings.HasSuffix(v, "_") {
				continue // a pattern, such as LENDISK_OVERFLOW_<NAME>_ACCESS_KEY
			}
			if !known[v] && !overflow.MatchString(v) && !(postV1[v] && later) {
				t.Errorf("%s names %s, which lendisk does not read", f, v)
			}
		}
	}
}

// OPS-7, SEC-3, COMPAT-2: the systemd unit runs lendisk as an
// unprivileged user with no capability, under a read-only system,
// with third-party credentials from a root-owned environment file.
func TestSystemdUnitIsUnprivileged(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(examplesDir, "systemd", "lendisk.service"))
	if err != nil {
		t.Fatal(err)
	}
	set := map[string][]string{}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok && !strings.HasPrefix(line, "#") {
			set[k] = append(set[k], v)
		}
	}
	for k, want := range map[string]string{
		"NoNewPrivileges":       "true",
		"ProtectSystem":         "strict",
		"ProtectHome":           "true",
		"PrivateTmp":            "true",
		"CapabilityBoundingSet": "",
		"AmbientCapabilities":   "",
		"StateDirectory":        "lendisk",
		"EnvironmentFile":       "-/etc/lendisk.env",
	} {
		if v := set[k]; len(v) != 1 || v[0] != want {
			t.Errorf("%s = %q, want %q", k, v, want)
		}
	}
	if u := set["User"]; len(u) != 1 || u[0] == "" || u[0] == "root" || u[0] == "0" {
		t.Errorf("User = %q: want an unprivileged user", u)
	}
	if e := set["ExecStart"]; len(e) != 1 || !strings.Contains(e[0], "lendisk serve") {
		t.Errorf("ExecStart = %q", e)
	}
}

// OPS-7, D2: every compose service running the lendisk image is
// unprivileged, drops every capability and has a read-only root.
func TestComposeServicesAreUnprivileged(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(examplesDir, "*", "compose.yaml"))
	n := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		// Services are the two-space-indented keys under services:.
		for _, svc := range regexp.MustCompile(`(?m)^  [a-z-]+:\n((?:    .*\n|\s*\n)*)`).FindAllStringSubmatch(string(b), -1) {
			body := svc[1]
			if !strings.Contains(body, "image: ghcr.io/lendisk/lendisk:") {
				continue
			}
			n++
			for _, want := range []string{"cap_drop: [ALL]", "read_only: true", "no-new-privileges:true", "network_mode: host"} {
				if !strings.Contains(body, want) {
					t.Errorf("%s: a lendisk service without %q", f, want)
				}
			}
			if u := regexp.MustCompile(`(?m)^    user: "(\d+):`).FindStringSubmatch(body); u == nil || u[1] == "0" {
				t.Errorf("%s: a lendisk service without an unprivileged user:", f)
			}
		}
	}
	if n < 4 {
		t.Fatalf("found %d lendisk services in %v", n, files)
	}
}
