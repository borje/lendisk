package export

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/repoimport"
)

const (
	bucket = "lent-abcdefghijklmnop-sabcdefghijklmnopqrstuvwxyz"
	h1     = "ab00000000000000000000000000000000000000000000000000000000000001"
	h2     = "cd00000000000000000000000000000000000000000000000000000000000002"
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

// tree maps every file under dir, by its slash path, to its content.
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// LEND-6, COMPAT-4: a lent bucket is exported as one file per key at its
// key path, the bytes as stored. lendisk's tombs (.lendisk/) and
// versitygw's temporary and multipart files (.sgwtmp/) are left out, as
// are the store's own housekeeping and other buckets; directories are
// 0700 and files 0600 from the first byte.
func TestExportWritesEachKeyAtItsPathWithoutHousekeeping(t *testing.T) {
	store := t.TempDir()
	keys := []string{"config", "keys/" + h1, "index/" + h2, "snapshots/" + h1, "data/ab/" + h1, "data/cd/" + h2,
		"a/.sgwtmp/kept"} // a key may hold the name deeper down
	for _, k := range keys {
		write(t, filepath.Join(store, bucket), k)
	}
	write(t, filepath.Join(store, bucket), ".lendisk/tomb/data/ab/"+h2+"/1700000000000000000",
		".sgwtmp/multipart/x/1", ".sgwtmp/tmpobj", ".vgwlocks/l")
	write(t, store, ".lendisk/manifests/n", ".lendisk/store-marker", ".vgwlocks/ff/1", "laptop/config",
		"lent-other0000000000-sother/config")
	os.MkdirAll(filepath.Join(store, bucket, "data", "ff"), 0o700) // an empty directory is no key

	dst := filepath.Join(t.TempDir(), "replica-1")
	r, err := Lent(store, bucket, dst, false)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	var size int64
	for _, k := range keys {
		want[k] = k
		size += int64(len(k))
	}
	if got := tree(t, dst); !reflect.DeepEqual(got, want) {
		t.Fatalf("export holds\n%v\nwant\n%v", got, want)
	}
	if r.Objects != len(keys) || r.Bytes != size || r.Kopia {
		t.Fatalf("result %+v", r)
	}
	filepath.WalkDir(dst, func(p string, de fs.DirEntry, _ error) error {
		st, _ := os.Stat(p)
		if want := map[bool]os.FileMode{true: 0o700, false: 0o600}[de.IsDir()]; st.Mode().Perm() != want {
			t.Errorf("%s has mode %v, want %v", p, st.Mode().Perm(), want)
		}
		return nil
	})
}

// LEND-6: the export never merges into or overwrites anything: a
// destination that holds a file is refused, as is a name that is no lent
// bucket in the store, which the error lists. An empty directory is
// taken.
func TestExportRefusals(t *testing.T) {
	store := t.TempDir()
	write(t, filepath.Join(store, bucket), "config")
	write(t, store, "laptop/config")

	full := t.TempDir()
	write(t, full, "something")
	if _, err := Lent(store, bucket, full, false); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("non-empty destination: %v", err)
	}
	if got := tree(t, full); len(got) != 1 {
		t.Fatalf("destination changed: %v", got)
	}
	missing := "lent-zzzzzzzzzzzzzzzz-szzzzzzzzzzzzzzzzzzzzzzzzzz"
	if _, err := Lent(store, missing, t.TempDir(), false); err == nil || !strings.Contains(err.Error(), bucket) ||
		strings.Contains(err.Error(), "laptop") {
		t.Fatalf("unknown bucket: %v", err)
	}
	if _, err := Lent(store, "laptop", t.TempDir(), false); !errors.Is(err, ErrNotLent) {
		t.Fatalf("own bucket: %v", err)
	}
	if _, err := Lent(store, bucket, t.TempDir(), false); err != nil {
		t.Fatalf("empty destination: %v", err)
	}
}

// LEND-6, COMPAT-4: with the Kopia layout every flat blob ID is written
// where Kopia's filesystem storage keeps it, under .shards' default
// sharding ([1,3], IDs over 20 characters), with ".f" appended, and
// .shards says so. It is the reverse of `bucket import`'s mapping
// (#9): importing the export gives back the keys.
func TestKopiaLayoutIsTheReverseOfImport(t *testing.T) {
	store := t.TempDir()
	keys := []string{"kopia.repository", "kopia.blobcfg", "kopia.maintenance",
		"p183bb118fc040d0a44f316fcf3628cc7-s9dc06348b8217a6d146",
		"q3ea85b566562234244fc48d29b9b5df3-sdacb56c24bdb4c18146",
		"xn0_2da15a3e0fdc67c9daff4f5c0425c6f8-sdacb56c24bdb4c18146-c1",
		"n0123456789abcdef0123", // 21 characters: sharded
		"n0123456789abcdef012",  // 20: not
	}
	write(t, filepath.Join(store, bucket), keys...)
	write(t, filepath.Join(store, bucket), ".lendisk/tomb/qdead/1")
	dst := filepath.Join(t.TempDir(), "replica-1")
	r, err := Lent(store, bucket, dst, true)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Kopia || r.Objects != len(keys) {
		t.Fatalf("result %+v", r)
	}
	got := tree(t, dst)
	for path, key := range map[string]string{
		"kopia.repository.f": "kopia.repository",
		"p/183/bb118fc040d0a44f316fcf3628cc7-s9dc06348b8217a6d146.f":       keys[3],
		"x/n0_/2da15a3e0fdc67c9daff4f5c0425c6f8-sdacb56c24bdb4c18146-c1.f": keys[5],
		"n/012/3456789abcdef0123.f":                                        keys[6],
		"n0123456789abcdef012.f":                                           keys[7],
	} {
		if got[path] != key {
			t.Errorf("%s holds %q, want %q", path, got[path], key)
		}
	}
	if got[".shards"] != `{"default":[1,3],"maxNonShardedLength":20}` {
		t.Errorf(".shards = %q", got[".shards"])
	}
	repo, err := repoimport.Open(dst, "kopia")
	if err != nil {
		t.Fatal(err)
	}
	var back []string
	if err := repo.Walk(func(f repoimport.File) error { back = append(back, f.Key); return nil }); err != nil {
		t.Fatal(err)
	}
	sort.Strings(back)
	sort.Strings(keys)
	if !reflect.DeepEqual(back, keys) {
		t.Fatalf("imported back\n%v\nwant\n%v", back, keys)
	}
}

// LEND-6: a key that is no Kopia blob ID (a restic bucket's data/…)
// cannot take Kopia's layout, and the error says to export without it.
func TestKopiaLayoutOfAResticBucketRefused(t *testing.T) {
	store := t.TempDir()
	write(t, filepath.Join(store, bucket), "config", "data/ab/"+h1)
	_, err := Lent(store, bucket, filepath.Join(t.TempDir(), "x"), true)
	if err == nil || !strings.Contains(err.Error(), "without --kopia-layout") {
		t.Fatalf("restic bucket in Kopia's layout: %v", err)
	}
}

// LEND-6 (#35): Kopia 0.23.1 opens an export as a filesystem repository.
// A repository Kopia made on disk is laid flat as its S3 storage keeps
// it (repoimport, #9) in a lent bucket; its export in Kopia's layout
// connects, verifies every file and restores byte for byte.
func TestKopiaOpensAnExportAsAFilesystemRepository(t *testing.T) {
	path, err := exec.LookPath("kopia")
	if err != nil {
		t.Skip("kopia not installed")
	}
	home := t.TempDir()
	env := append(os.Environ(), "KOPIA_CONFIG_PATH="+filepath.Join(home, "repository.config"),
		"KOPIA_CACHE_DIRECTORY="+filepath.Join(home, "cache"), "KOPIA_LOG_DIR="+filepath.Join(home, "logs"),
		"KOPIA_PASSWORD=test-password", "KOPIA_CHECK_FOR_UPDATES=false")
	kopia := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(path, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("kopia %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	src, orig := t.TempDir(), t.TempDir()
	write(t, src, "a", "b/c")
	if err := os.WriteFile(filepath.Join(src, "big"), bytes.Repeat([]byte("lendisk "), 1<<16), 0o600); err != nil {
		t.Fatal(err)
	}
	kopia("repository", "create", "filesystem", "--path", orig)
	kopia("snapshot", "create", src)
	kopia("repository", "disconnect")

	store := t.TempDir()
	if err := os.MkdirAll(filepath.Join(store, bucket), 0o700); err != nil {
		t.Fatal(err)
	}
	repo, err := repoimport.Open(orig, "kopia")
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Walk(func(f repoimport.File) error {
		if strings.HasPrefix(f.Key, "_log") { // local-only: never at a lender
			return nil
		}
		b, err := os.ReadFile(f.Path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(store, bucket, f.Key), b, 0o600)
	}); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "replica-1")
	if _, err := Lent(store, bucket, dst, true); err != nil {
		t.Fatal(err)
	}
	kopia("repository", "connect", "filesystem", "--path", dst)
	kopia("snapshot", "verify", "--verify-files-percent=100")
	out := t.TempDir()
	kopia("snapshot", "restore", src, out)
	for _, f := range []string{"a", "b/c", "big"} {
		w, _ := os.ReadFile(filepath.Join(src, f))
		g, err := os.ReadFile(filepath.Join(out, f))
		if err != nil || !bytes.Equal(g, w) {
			t.Fatalf("restored %s differs: %v", f, err)
		}
	}
}
