package repoimport

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// write creates each file under dir with its relative path as content.
func write(t *testing.T, dir string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(dir, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// keys walks repo and returns its keys, sorted, failing on a file whose
// path or size is not the file's.
func keys(t *testing.T, r *Repo) []string {
	t.Helper()
	var out []string
	err := r.Walk(func(f File) error {
		st, err := os.Stat(f.Path)
		if err != nil || st.Size() != f.Size {
			t.Errorf("%s: path %s, size %d: %v", f.Key, f.Path, f.Size, err)
		}
		out = append(out, f.Key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

const (
	h1 = "ab00000000000000000000000000000000000000000000000000000000000001"
	h2 = "cd00000000000000000000000000000000000000000000000000000000000002"
)

// COMPAT-3: a restic directory and rest-server's repositories have
// restic's default layout, which is also its S3 layout: every file's key
// is its path. Temporary files of restic's local backend (-tmp-) and of
// rest-server (.rest-server-temp) are not keys.
func TestResticLayoutKeysAreThePaths(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "config", "keys/"+h1, "index/"+h2, "snapshots/"+h1, "locks/"+h2,
		"data/ab/"+h1, "data/cd/"+h2,
		"data/ab/"+h1+"-tmp-12345", "index/"+h1+".rest-server-temp8812", "README")
	os.MkdirAll(filepath.Join(dir, "data", "ff"), 0o700)
	r, err := Open(dir, "restic")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"config", "data/ab/" + h1, "data/cd/" + h2, "index/" + h2, "keys/" + h1, "locks/" + h2, "snapshots/" + h1}
	if got := keys(t, r); !reflect.DeepEqual(got, want) {
		t.Fatalf("keys\n got %v\nwant %v", got, want)
	}
}

// COMPAT-3: rest-server keeps each repository in a directory of its
// data path, per user with --private-repos (<data>/<user>/[<repo>]). A
// path that is rest-server's data directory, not a repository, is
// refused naming the repositories under it.
func TestRestServerDataDirectoryNamesItsRepositories(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, ".htpasswd", "alice/config", "alice/keys/"+h1, "bob/laptop/config", "bob/laptop/keys/"+h1)
	_, err := Open(dir, "restic")
	if err == nil {
		t.Fatal("a rest-server data directory was opened as a repository")
	}
	for _, s := range []string{filepath.Join(dir, "alice"), filepath.Join(dir, "bob", "laptop"), "rest-server"} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error does not name %s: %v", s, err)
		}
	}
	r, err := Open(filepath.Join(dir, "bob", "laptop"), "restic")
	if err != nil {
		t.Fatal(err)
	}
	if got := keys(t, r); !reflect.DeepEqual(got, []string{"config", "keys/" + h1}) {
		t.Fatalf("keys %v", got)
	}
}

// COMPAT-3: Kopia's filesystem storage shards a blob ID into
// directories (.shards: {"default":[1,3]} for repositories Kopia 0.23
// creates, [3,3] before .shards existed) and appends ".f"; its S3
// storage uses the blob ID as the key. The key is what Kopia's own
// listing makes of a file (sharded.Storage.ListBlobs): every directory
// name and the file name joined, ".f" removed; a file without ".f" is
// not a blob (.shards, temporary *.tmp.<hex> files).
func TestKopiaShardedLayoutKeysAreBlobIDs(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, ".shards", "kopia.repository.f", "kopia.blobcfg.f", "kopia.maintenance.f",
		"p/8bf/4fa1cef4d6a8998dc599c1003abc8-s621aed3d16690270146.f",
		"x/n0_/a0d38a737c03e7b993eedf6426152022-s64488e33e9063da7146-c1.f",
		"_/log/_20261109031554_5ba5_1794190554_1794190554_1_ea88af3dfb7e257f331c95b4fabef09d.f",
		"n32/a54/1245ec7fc503409750c50aaeec5-sd99b789994f84254146.f",
		"q/841/c5371c6a8a1a3b77c52ee6592c7c4-s64488e33e9063da7146.f.tmp.1a2b")
	r, err := Open(dir, "kopia")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"_log_20261109031554_5ba5_1794190554_1794190554_1_ea88af3dfb7e257f331c95b4fabef09d",
		"kopia.blobcfg", "kopia.maintenance", "kopia.repository",
		"n32a541245ec7fc503409750c50aaeec5-sd99b789994f84254146",
		"p8bf4fa1cef4d6a8998dc599c1003abc8-s621aed3d16690270146",
		"xn0_a0d38a737c03e7b993eedf6426152022-s64488e33e9063da7146-c1",
	}
	if got := keys(t, r); !reflect.DeepEqual(got, want) {
		t.Fatalf("keys\n got %v\nwant %v", got, want)
	}
}

// A path that holds no repository of the named tool is refused, saying
// what was looked for.
func TestPathWithoutTheToolsRepositoryRefused(t *testing.T) {
	restic, kopia, empty := t.TempDir(), t.TempDir(), t.TempDir()
	write(t, restic, "config")
	write(t, kopia, "kopia.repository.f")
	for _, c := range []struct{ dir, tool, want string }{
		{restic, "kopia", "kopia.repository.f"},
		{kopia, "restic", "config"},
		{empty, "restic", "config"},
		{filepath.Join(empty, "missing"), "restic", "missing"},
	} {
		if _, err := Open(c.dir, c.tool); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Open(%s, %s) = %v, want an error naming %q", c.dir, c.tool, err, c.want)
		}
	}
	if _, err := Open(restic, "generic"); err == nil {
		t.Error("a generic import was accepted")
	}
}
