package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// sourceHashes is the SHA-256 of each file under dir that keep accepts
// (given its slash path relative to dir), sorted: what an import of dir
// must put in the bucket, by content.
func sourceHashes(t *testing.T, dir string, keep func(rel string) bool) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if !keep(filepath.ToSlash(rel)) {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		s := sha256.Sum256(b)
		out = append(out, hex.EncodeToString(s[:]))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

// checkImported fails unless bucket's store holds exactly the objects
// whose hashes are want, each captured as its profile classifies it
// with a done put row carrying its SHA-256 and size, none local-only,
// no row pending, and no placement acknowledged (COMPAT-3).
func checkImported(t *testing.T, n *node, bucket, prof string, want []string) {
	t.Helper()
	set, err := profile.Load("")
	if err != nil {
		t.Fatal(err)
	}
	p := set.Get(prof)
	objs := storeObjects(t, filepath.Join(n.data, "store", bucket))
	caps := latestCaptures(t, n, bucket)
	var got []string
	for key, sum := range objs {
		got = append(got, sum)
		class := p.Classify(key)
		e, ok := caps[key]
		switch {
		case class == profile.LocalOnly:
			t.Errorf("local-only %s was imported", key)
		case !ok:
			t.Errorf("%s is in the store but not in the queue", key)
		case e.Op != state.OpPut || e.SHA256 != sum || e.Class != string(class):
			t.Errorf("%s queued as %s %s sha %s, want a %s put with sha %s", key, e.Op, e.Class, e.SHA256, class, sum)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("store holds %d objects, the repository %d; or their contents differ", len(got), len(want))
	}
	if len(caps) != len(objs) {
		t.Errorf("%d queued keys, %d objects", len(caps), len(objs))
	}
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var pending, acked int
	db.QueryRow(`SELECT count(*) FROM queue WHERE bucket = ? AND state = 'pending'`, bucket).Scan(&pending)
	db.QueryRow(`SELECT count(*) FROM placement WHERE bucket = ? AND state = 'acked'`, bucket).Scan(&acked)
	if pending != 0 || acked != 0 {
		t.Errorf("%d rows pending, %d placements acknowledged; want none", pending, acked)
	}
}

// resticSnapshotIDs lists a repository's snapshot IDs, sorted.
func resticSnapshotIDs(t *testing.T, restic string, env []string) []string {
	t.Helper()
	cmd := exec.Command(restic, "snapshots", "--json")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("restic snapshots: %v\n%s", err, out)
	}
	var snaps []struct{ ID string }
	if err := json.Unmarshal(out, &snaps); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range snaps {
		ids = append(ids, s.ID)
	}
	slices.Sort(ids)
	return ids
}

// COMPAT-3 (#9): a restic repository kept by rest-server, at
// <data>/<user> as --private-repos lays it out, is imported into a new
// bucket. Pointing at rest-server's data directory is refused, naming
// the repository, and creates nothing. The import shows its progress on
// stderr; restic then lists the original snapshots through the endpoint
// and `restic check --read-data` passes. Every object is queued with
// its hash and none is acknowledged; the stale lock and the temporary
// files of restic and rest-server are not imported.
func TestImportedResticRepositoryListsItsSnapshots(t *testing.T) {
	serving(t, 0)
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	data := t.TempDir()
	repo := filepath.Join(data, "alice")
	os.WriteFile(filepath.Join(data, ".htpasswd"), []byte("alice:x\n"), 0o600)
	local := append(os.Environ(), "RESTIC_REPOSITORY="+repo, "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	runRestic := func(env []string, args ...string) {
		t.Helper()
		cmd := exec.Command(restic, args...)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	src := t.TempDir()
	runRestic(local, "init")
	randomFiles(t, src, "f", 4, 256<<10)
	runRestic(local, "backup", "--no-scan", src)
	randomFiles(t, src, "g", 2, 128<<10)
	runRestic(local, "backup", "--no-scan", src)
	want := resticSnapshotIDs(t, restic, local)
	// A lock a killed restic left, and files restic's local backend and
	// rest-server write before their rename.
	lock := strings.Repeat("1", 64)
	os.WriteFile(filepath.Join(repo, "locks", lock), []byte("not a lock"), 0o600)
	os.WriteFile(filepath.Join(repo, "index", lock+".rest-server-temp4711"), []byte("partial"), 0o600)
	os.WriteFile(filepath.Join(repo, "data", "11", lock+"-tmp-1234"), []byte("partial"), 0o600)
	hashes := sourceHashes(t, repo, func(rel string) bool {
		return !strings.HasPrefix(rel, "locks/") && !strings.Contains(rel, "-tmp-") && !strings.Contains(rel, "rest-server-temp")
	})

	n := newNode(t)
	n.mustInit(t)
	code, _, errOut := n.run("", "", "bucket", "import", "laptop", data, "--profile", "restic")
	if code != 1 || !strings.Contains(errOut, repo) || !strings.Contains(errOut, "rest-server") {
		t.Fatalf("import of rest-server's data directory: exit %d\n%s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(n.data, "store", "laptop")); err == nil {
		t.Fatal("a refused import created the bucket")
	}
	code, out, errOut := n.run("", "", "bucket", "import", "laptop", repo, "--profile", "restic")
	if code != 0 {
		t.Fatalf("bucket import: exit %d\n%s%s", code, out, errOut)
	}
	if !strings.Contains(errOut, "100%") || !strings.Contains(out, "restic snapshots") {
		t.Errorf("progress or lines missing:\n%s\n%s", errOut, out)
	}
	checkImported(t, n, "laptop", "restic", hashes)
	if _, err := os.Stat(filepath.Join(n.data, "store", "laptop", "locks", lock)); err == nil {
		t.Error("the stale lock was imported")
	}
	cr := bucketCreds(t, n, "laptop")

	d := n.serve(t)
	s3 := append(os.Environ(), "AWS_ACCESS_KEY_ID="+cr.access, "AWS_SECRET_ACCESS_KEY="+cr.secret,
		"RESTIC_REPOSITORY=s3:http://"+d.addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	if got := resticSnapshotIDs(t, restic, s3); !slices.Equal(got, want) || len(want) != 2 {
		t.Fatalf("snapshots through the endpoint %v, imported %v", got, want)
	}
	runRestic(s3, "check", "--read-data")
}

var reKopiaConnect = regexp.MustCompile(`(?m)^  kopia repository connect s3 (.*) \\\n +(.*)$`)

// COMPAT-3 (#9): a Kopia filesystem repository, sharded as Kopia 0.23
// creates it, is imported into a new bucket under Kopia's flat S3 blob
// names. The `kopia repository connect s3` lines the import prints open
// it through the endpoint; both snapshots are listed and the latest
// restores byte for byte. Every object but Kopia's logs is queued with
// its hash, and none is acknowledged.
func TestImportedKopiaFilesystemRepositoryOpensThroughS3(t *testing.T) {
	serving(t, 0)
	k := newKopia(t)
	repo := t.TempDir()
	k.run("repository", "create", "filesystem", "--path="+repo)
	src := t.TempDir()
	randomFiles(t, src, "f", 4, 256<<10)
	k.run("snapshot", "create", src)
	randomFiles(t, src, "g", 2, 128<<10)
	k.run("snapshot", "create", src)
	k.run("repository", "disconnect")
	if _, err := os.Stat(filepath.Join(repo, ".shards")); err != nil {
		t.Fatalf("not a sharded repository: %v", err)
	}
	hashes := sourceHashes(t, repo, func(rel string) bool {
		return strings.HasSuffix(rel, ".f") && !strings.HasPrefix(strings.ReplaceAll(rel, "/", ""), "_log")
	})

	n := newNode(t)
	n.mustInit(t)
	code, out, errOut := n.run("", "", "bucket", "import", "vault", repo, "--profile", "kopia")
	if code != 0 {
		t.Fatalf("bucket import: exit %d\n%s%s", code, out, errOut)
	}
	checkImported(t, n, "vault", "kopia", hashes)
	if _, err := os.Stat(filepath.Join(n.data, "store", "vault", "kopia.repository")); err != nil {
		t.Fatalf("kopia.repository not at its S3 key: %v", err)
	}
	m := reKopiaConnect.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no Kopia connect lines in:\n%s", out)
	}

	d := n.serve(t)
	args := []string{"repository", "connect", "s3"}
	for _, a := range strings.Fields(m[1] + " " + m[2]) {
		if strings.HasPrefix(a, "--endpoint=") {
			a = "--endpoint=" + d.addr
		}
		args = append(args, a)
	}
	k2 := newKopia(t)
	k2.run(args...)
	if list := k2.run("snapshot", "list", src); strings.Count(strings.TrimSpace(list), "\n") < 2 {
		t.Fatalf("snapshots:\n%s", list)
	}
	dst := t.TempDir()
	k2.run("snapshot", "restore", src, dst)
	sameFiles(t, src, dst)
}

// COMPAT-3 (#9): an import that stopped partway is continued with
// --resume into the bucket it created: objects already captured are not
// put again, the rest are. Without --resume the bucket's name is taken.
func TestImportResumesIntoItsBucket(t *testing.T) {
	parallel(t, 0)
	repo := t.TempDir()
	os.WriteFile(filepath.Join(repo, "config"), []byte("config"), 0o600)
	os.MkdirAll(filepath.Join(repo, "keys"), 0o700)
	os.WriteFile(filepath.Join(repo, "keys", strings.Repeat("a", 64)), []byte("key"), 0o600)
	n := newNode(t)
	n.mustInit(t)
	if code, out, errOut := n.run("", "", "bucket", "import", "laptop", repo, "--profile", "restic"); code != 0 {
		t.Fatalf("bucket import: %s%s", out, errOut)
	}
	// The run stopped before this one.
	os.MkdirAll(filepath.Join(repo, "data", "bb"), 0o700)
	os.WriteFile(filepath.Join(repo, "data", "bb", strings.Repeat("b", 64)), []byte("pack"), 0o600)
	if code, _, errOut := n.run("", "", "bucket", "import", "laptop", repo, "--profile", "restic"); code != 1 || !strings.Contains(errOut, "already exists") {
		t.Fatalf("a second import into the bucket: exit %d\n%s", code, errOut)
	}
	if code, out, errOut := n.run("", "", "bucket", "import", "laptop", repo, "--profile", "restic", "--resume"); code != 0 {
		t.Fatalf("bucket import --resume: %s%s", out, errOut)
	}
	checkImported(t, n, "laptop", "restic", sourceHashes(t, repo, func(string) bool { return true }))
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if es, _ := db.QueueEntries(t.Context(), "laptop"); len(es) != 3 {
		t.Fatalf("%d queue rows, want one per object: %+v", len(es), es)
	}
}

// COMPAT-1, COMPAT-3 (#9, #79): a Kopia repository in format version 1
// is not imported: the refusal names `kopia repository set-parameters
// --upgrade`, and no bucket is created.
func TestImportOfKopiaFormat1RepositoryRefused(t *testing.T) {
	parallel(t, 0)
	k := newKopia(t)
	repo := t.TempDir()
	k.run("repository", "create", "filesystem", "--path="+repo, "--format-version=1")
	src := t.TempDir()
	randomFiles(t, src, "f", 2, 64<<10)
	k.run("snapshot", "create", src)

	n := newNode(t)
	n.mustInit(t)
	code, out, errOut := n.run("", "", "bucket", "import", "old", repo, "--profile", "kopia")
	if code != 1 || !strings.Contains(errOut, "kopia repository set-parameters --upgrade") || !strings.Contains(errOut, "format version 1") {
		t.Fatalf("import of a format-1 repository: exit %d\n%s%s", code, out, errOut)
	}
	if _, out, _ := n.run("", "", "bucket", "list"); strings.Contains(out, "old") {
		t.Fatalf("a refused import created the bucket:\n%s", out)
	}
}
