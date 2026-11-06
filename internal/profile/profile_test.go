package profile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// EP-7: the restic profile's four classes; a key no rule matches is data.
func TestResticClassifiesKeys(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	p := set.Get("restic")
	if !p.Encrypted {
		t.Error("restic is inside the encryption guarantee")
	}
	cases := map[string]Class{
		"config":                    Metadata,
		"keys/0123abcd":             Metadata,
		"index/89ab":                Metadata,
		"snapshots/cdef":            Snapshot,
		"data/01/0123456789abcdef":  Data,
		"locks/4567":                LocalOnly,
		"something/else":            Data,
		"config.bak":                Data,
		"locksmith":                 Data,
		"data":                      Data,
		"snapshots":                 Data,
		"index/sub/dir/with/levels": Metadata,
	}
	for key, want := range cases {
		if got := p.Classify(key); got != want {
			t.Errorf("Classify(%q) = %s, want %s", key, got, want)
		}
	}
}

// EP-7: generic has no snapshot, metadata or local-only keys, and is
// outside the encryption guarantee.
func TestGenericIsAllData(t *testing.T) {
	set, _ := Load("")
	p := set.Get("generic")
	if p.Encrypted {
		t.Error("generic is outside the encryption guarantee")
	}
	for _, key := range []string{"config", "locks/x", "snapshots/y", "a/b/c"} {
		if got := p.Classify(key); got != Data {
			t.Errorf("generic Classify(%q) = %s", key, got)
		}
	}
	// A profile this binary has no file for classifies everything as data.
	if got := set.Get("borg").Classify("locks/x"); got != Data {
		t.Errorf("unknown profile: %s", got)
	}
}

// EP-7: a file in <config>/profiles/ replaces the compiled-in profile of
// its name, and adds one that is not compiled in; no release needed.
func TestProfilesOverriddenFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "restic.toml"), []byte(`
encrypted = true
local_only = ["locks/*", "tmp/*"]
snapshot = ["snapshots/*"]
metadata = ["config", "snapshots/*"]
data = ["data/*"]
`), 0o600)
	os.WriteFile(filepath.Join(dir, "borg.toml"), []byte(`
encrypted = true
metadata = ["manifest"]
`), 0o600)
	os.WriteFile(filepath.Join(dir, "README"), []byte("not a profile"), 0o600)
	set, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := set.Get("restic")
	if r.Classify("tmp/x") != LocalOnly || r.Classify("index/x") != Data {
		t.Errorf("override not applied: tmp/x=%s index/x=%s", r.Classify("tmp/x"), r.Classify("index/x"))
	}
	if set.Get("borg").Classify("manifest") != Metadata {
		t.Error("added profile not loaded")
	}
	if set.Get("generic").Classify("config") != Data {
		t.Error("compiled-in generic lost")
	}
}

func TestBadProfileFileRefused(t *testing.T) {
	for name, body := range map[string]string{
		"typo":   `metdata = ["x"]`,
		"syntax": `metadata = [`,
		"empty":  `local_only = [""]`,
	} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "restic.toml"), []byte(body), 0o600)
		_, err := Load(dir)
		if err == nil || !strings.Contains(err.Error(), "restic.toml") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A missing directory is no override at all.
	if _, err := Load(filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatal(err)
	}
}

func TestGlob(t *testing.T) {
	cases := []struct {
		pattern, key string
		match        bool
	}{
		{"locks/*", "locks/abc", true},
		{"locks/*", "locks/", true},
		{"locks/*", "locks", false},
		{"data/*", "data/ab/cdef", true},
		{"n*", "n0123", true},
		{"n*", "kopia.repository", false},
		{"_log*", "_log_2026", true},
		{"config", "config", true},
		{"config", "configx", false},
		{"x?z", "xyz", true},
		{"x?z", "xz", false},
		{"*.tmp", "a/b.tmp", true},
		{"a*b*c", "aXbYc", true},
		{"a*b*c", "aXbY", false},
	}
	for _, c := range cases {
		if got := glob(c.pattern, c.key); got != c.match {
			t.Errorf("glob(%q, %q) = %v", c.pattern, c.key, got)
		}
	}
}
