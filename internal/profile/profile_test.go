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

// EP-7: the kopia profile's four classes, as REQUIREMENTS lists them.
// The index blobs n* and the epoch index blobs xn* are snapshot keys
// (Kopia keeps its manifests in pack blobs, so a snapshot exists once
// the index that references it does); the other epoch blobs, sessions
// and the three format blobs are metadata; packs are data; the logs
// are local-only.
func TestKopiaClassifiesKeys(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	p := set.Get("kopia")
	if p.Name != "kopia" || !p.Encrypted {
		t.Fatalf("kopia is a compiled-in profile inside the encryption guarantee: %+v", p)
	}
	cases := map[string]Class{
		"kopia.repository":  Metadata,
		"kopia.blobcfg":     Metadata,
		"kopia.maintenance": Metadata,
		"n0123456789abcdef0123456789abcdef-s0123456789abcdef-c1": Snapshot,
		"xn0_0123456789abcdef0123456789abcdef-s01-c1":            Snapshot,
		"xs0_0123456789abcdef0123456789abcdef":                   Metadata,
		"xr0_4_0123456789abcdef0123456789abcdef":                 Metadata,
		"xe0":                                                    Metadata,
		"xw1":                                                    Metadata,
		"s0123456789abcdef-c1":                                   Metadata,
		"p0123456789abcdef0123456789abcdef-s0123":                Data,
		"q0123456789abcdef0123456789abcdef-s0123":                Data,
		"_log_20261108000000_0123_1699999999_1699999999_1_0123456789abcdef": LocalOnly,
		"kopia.repository.f": Data,
		"other/key":          Data,
	}
	for key, want := range cases {
		if got := p.Classify(key); got != want {
			t.Errorf("Classify(%q) = %s, want %s", key, got, want)
		}
	}
	// Kopia names its blobs by a keyed hash or at random, never by the
	// plain SHA-256 of their content: recovery cannot check a name.
	if p.HashNamed("p" + strings.Repeat("ab", 32)) {
		t.Error("a kopia blob is not named by its SHA-256")
	}
	// #78: Kopia reads the union of its metadata blobs; restic's lone
	// config or key stays a conflict in recovery.
	if !p.UnionMetadata || set.Get("restic").UnionMetadata || set.Get("generic").UnionMetadata {
		t.Error("only kopia reads the union of its metadata")
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

func TestGlobStarMatchesAcrossSlashesQuestionMatchesOne(t *testing.T) {
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

// ID-5 (#29 review): restic names its packs, indexes and snapshots by
// the SHA-256 of their content, so a listed hash must be the name; its
// config and keys are not so named, nor is any generic key.
func TestResticKeysNamedByTheirHash(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("ab", 32)
	r := set.Get("restic")
	for key, want := range map[string]bool{
		"data/ab/" + h: true, "index/" + h: true, "snapshots/" + h: true,
		"config": false, "keys/" + h: false, "locks/" + h: false,
	} {
		if got := r.HashNamed(key); got != want {
			t.Errorf("restic %s: %v, want %v", key, got, want)
		}
	}
	if set.Get("generic").HashNamed("data/ab/"+h) || set.Get("none").HashNamed("index/"+h) {
		t.Error("a generic key is named by its hash")
	}
	if NameHash("data/ab/"+h) != h || NameHash("config") != "config" {
		t.Error("the name's hash is its last segment")
	}
}

// EP-7 (v0.11.3, #74): a profile names the few keys its tool rewrites in
// place in normal use. Kopia rewrites its three format blobs, restic
// its config; nothing else, and a generic bucket none. A rewritable key
// keeps its class.
func TestRewritableKeysKeepTheirClass(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	for name, cases := range map[string]map[string]bool{
		"kopia": {"kopia.repository": true, "kopia.blobcfg": true, "kopia.maintenance": true,
			"kopia.maintenance.x": false, "n0123": false, "p0123": false, "xe0": false, "_log_1": false},
		"restic":  {"config": true, "keys/0123": false, "index/ab": false, "data/01/ab": false, "config.bak": false},
		"generic": {"config": false, "kopia.maintenance": false},
		"none":    {"config": false},
	} {
		p := set.Get(name)
		for key, want := range cases {
			if got := p.Rewritable(key); got != want {
				t.Errorf("%s: Rewritable(%q) = %v, want %v", name, key, got, want)
			}
		}
	}
	for name, key := range map[string]string{"kopia": "kopia.maintenance", "restic": "config"} {
		if c := set.Get(name).Classify(key); c != Metadata {
			t.Errorf("%s %s is %s; a rewritable key keeps its class", name, key, c)
		}
	}
}

// A profile file may name rewritable keys; an empty pattern stops the
// start like any other.
func TestRewritableReadFromProfileFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "mytool.toml"), []byte("metadata = [\"meta/*\"]\nrewritable = [\"meta/head\"]\n"), 0o600)
	set, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p := set.Get("mytool"); !p.Rewritable("meta/head") || p.Rewritable("meta/other") {
		t.Fatalf("mytool: %+v", p)
	}
	os.WriteFile(filepath.Join(dir, "bad.toml"), []byte(`rewritable = [""]`), 0o600)
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "empty pattern") {
		t.Fatalf("empty rewritable pattern: %v", err)
	}
}

// REPL-11 (#20): a profile says whether its snapshot keys name the
// snapshot as the tool shows it. restic's do (snapshots/<id>); Kopia's
// are index blobs, so status names the time instead; generic has no
// snapshot keys. A profile file may say so too.
func TestSnapshotIDsFromSnapshotKeys(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := set.Get("restic").SnapshotID("snapshots/0123456789abcdef"); !ok || id != "0123456789abcdef" {
		t.Errorf("restic: SnapshotID = %q %v", id, ok)
	}
	for _, name := range []string{"kopia", "generic", "none"} {
		if id, ok := set.Get(name).SnapshotID("xn0_0123"); ok {
			t.Errorf("%s names snapshot %q", name, id)
		}
	}
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "mytool.toml"), []byte("snapshot = [\"snaps/*\"]\nnames_snapshots = true\n"), 0o600)
	if set, err = Load(dir); err != nil {
		t.Fatal(err)
	}
	if id, ok := set.Get("mytool").SnapshotID("snaps/s1"); !ok || id != "s1" {
		t.Errorf("mytool: SnapshotID = %q %v", id, ok)
	}
}

// COMPAT-1, EP-7 (#79): the kopia profile tells a format-1 repository's
// blobs (legacy index n*, compaction log m*, cleanup l*) from the
// blob the format upgrade writes (Kopia's legacy-index poison blob, an
// n* name of its own), and both from what formats 2 and 3 write. The
// classes are unchanged: n* is still a snapshot key, m* and l* data.
func TestKopiaProfileSpotsFormat1Blobs(t *testing.T) {
	set, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	p := set.Get("kopia")
	cases := map[string]Format{
		"n0123456789abcdef0123456789abcdef-s0123456789abcdef-c1":                                         Format1,
		"n0123456789abcdef0123456789abcdef":                                                              Format1,
		"m0123456789abcdef0123456789abcdef":                                                              Format1,
		"l0123456789abcdef0123456789abcdef":                                                              Format1,
		"n00000000000000000000000000000000-repository_unreadable_by_this_kopia_version_upgrade_required": Upgraded,
		"xn0_0123456789abcdef0123456789abcdef-s01-c1":                                                    Upgraded,
		"xs0_0123456789abcdef0123456789abcdef":                                                           Upgraded,
		"p0123456789abcdef0123456789abcdef-s0123":                                                        FormatOK,
		"q0123456789abcdef0123456789abcdef-s0123":                                                        FormatOK,
		"s0123456789abcdef-c1":                                                                           FormatOK,
		"kopia.repository":                                                                               FormatOK,
		"kopia.maintenance":                                                                              FormatOK,
		"kopia.repository.backup.20261108":                                                               FormatOK,
		"_log_20261108000000_0123":                                                                       FormatOK,
	}
	for key, want := range cases {
		if got := p.Format(key); got != want {
			t.Errorf("Format(%q) = %v, want %v", key, got, want)
		}
	}
	if c := p.Classify("m0123"); c != Data {
		t.Errorf("m* is %s, want data (EP-7 does not list it)", c)
	}
	for _, name := range []string{"restic", "generic"} {
		if f := set.Get(name).Format("n0123"); f != FormatOK {
			t.Errorf("%s: Format = %v; only kopia names format-1 keys", name, f)
		}
	}
}

// EP-7 (#79): format1_keys and upgraded_keys are read from a profile
// file, and an empty pattern in them is refused.
func TestFormatKeysReadFromProfileFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "old.toml"), []byte("format1_keys = [\"v1/*\"]\nupgraded_keys = [\"v1/upgraded\"]\n"), 0o600)
	set, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := set.Get("old")
	if p.Format("v1/x") != Format1 || p.Format("v1/upgraded") != Upgraded || p.Format("v2/x") != FormatOK {
		t.Fatalf("old: %+v", p)
	}
	os.WriteFile(filepath.Join(dir, "old.toml"), []byte(`upgraded_keys = [""]`), 0o600)
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "old.toml") {
		t.Fatalf("empty pattern: %v", err)
	}
}
