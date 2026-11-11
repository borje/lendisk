package cli

import (
	"bytes"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/seed"
	"github.com/borje/lendisk/internal/transport"
)

// writeRepo writes a restic-shaped repository: name -> contents.
func writeRepo(t *testing.T, objs map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for k, b := range objs {
		p := filepath.Join(dir, filepath.FromSlash(k))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func hexName(c byte) string { return strings.Repeat(string(c), 64) }

// REPL-8 (#36), the whole path: A exports what of its bucket is placed
// at B to a disk; B imports it through its gateway, which applies the
// friendship's quota, the hash and the existing-key rule as to any put;
// A's daemon then acknowledges exactly what B's list shows with the
// captured hash, sends B none of those objects again, and sends the rest
// over the network once the seed ends.
func TestScenarioSeedAFriendFromADisk(t *testing.T) {
	serving(t, 1) // B serves in this process, A's daemon is a process
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	stopB := bn.serveLogged()
	defer stopB()
	befriend(t, bn, an) // B lends to A
	aID, bID := an.identity(), bn.identity()
	aKey, bKey := aID.NodeID().String(), bID.NodeID().String()

	tampered, conflict := "data/11/"+hexName('1'), "data/22/"+hexName('2')
	objs := map[string][]byte{
		"config": []byte("restic config"), "keys/" + hexName('a'): []byte("key"),
		"index/" + hexName('c'): []byte("index"), "snapshots/" + hexName('d'): []byte("snapshot"),
		tampered: randomBytes(100 << 10), conflict: randomBytes(50 << 10),
	}
	for i := range 4 {
		c := byte('3' + i)
		objs["data/"+string([]byte{c, c})+"/"+hexName(c)] = randomBytes(1 << 20)
	}
	repo := writeRepo(t, objs)
	if code, out, e := an.run("", "", "bucket", "import", "laptop", repo, "--profile", "restic", "--copies", "1"); code != 0 {
		t.Fatalf("bucket import: %s%s", out, e)
	}

	// Box 1: the export holds exactly the keys placed at B, in restic's
	// layout, 0600 files in 0700 directories.
	disk := filepath.Join(t.TempDir(), "disk")
	code, out, e := an.run("", "", "export", "laptop", "--for", bKey, disk)
	if code != 0 || !strings.Contains(out, "Exported 10 objects") || !strings.Contains(out, "lendisk import "+disk) {
		_, st, _ := an.run("", "", "status")
		_, pl, _ := an.run("", "", "peer", "list", "--json")
		t.Fatalf("export --for: exit %d\n%s%s\n%s\n%s", code, out, e, st, pl)
	}
	want := replicable(t, an, "laptop")
	onDisk := storeObjects(t, disk)
	delete(onDisk, seed.ManifestPath)
	sameObjects(t, want, onDisk)
	filepath.WalkDir(disk, func(p string, d fs.DirEntry, err error) error {
		fi, _ := d.Info()
		if perm := fi.Mode().Perm(); d.IsDir() && perm != 0o700 || !d.IsDir() && perm != 0o600 {
			t.Errorf("%s is %v", p, perm)
		}
		return nil
	})
	if st := an.status(); len(st.Seeds) != 1 || st.Seeds[0].Place != bKey || st.Seeds[0].Objects != 10 || st.Seeds[0].Outcome != "" {
		t.Fatalf("status seeds after the export: %+v", st.Seeds)
	}
	if _, text, _ := an.run("", "", "status"); !strings.Contains(text, "Seeding "+bKey+" from a disk: bucket laptop, 0 of 10 objects confirmed") ||
		!strings.Contains(text, "exported 0 minutes ago; if the friend's list confirms nothing more, the rest go by the network from ") {
		t.Fatalf("status text after the export:\n%s", text)
	}
	// While the disk travels, a wait for offsite says what holds it.
	maddr := freeAddr(t, "127.0.0.1")
	an.setMetrics(t, maddr)
	a := newProcNode(t, an, envReplFast+"=1")
	a.start()
	if code, _, e := an.run("", "", "status", "--wait", "laptop", "--timeout", "1s"); code != 3 ||
		!strings.Contains(e, "a seed from a disk holds 10 objects for friend "+bKey) {
		t.Fatalf("status --wait while seeding: exit %d\n%s", code, e)
	}
	a.p.kill()

	// The disk is damaged in one object; B already holds another key
	// with other bytes, sent by A over the tunnel.
	p := filepath.Join(disk, filepath.FromSlash(tampered))
	b, _ := os.ReadFile(p)
	b[0] ^= 0xff
	os.WriteFile(p, b, 0o600)
	bAddr := bn.status().Node.Address
	bNode, err := transport.KeyOf(bAddr)
	if err != nil {
		t.Fatal(err)
	}
	creds, _ := pairCreds(t, aID, bNode)
	cb := s3Over(tunnelHTTP(t, aID, bAddr), creds)
	sealed := aws.String(aID.SealedName("laptop"))
	if _, err := cb.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: sealed}); err != nil {
		t.Fatal(err)
	}
	other := []byte("other bytes")
	if _, err := cb.PutObject(t.Context(), &s3.PutObjectInput{Bucket: sealed, Key: aws.String(conflict),
		Body: bytes.NewReader(other), Metadata: hashMeta(other)}); err != nil {
		t.Fatal(err)
	}

	// Box 2: the quota applies; an import that stopped goes on when run
	// again; the damaged object and the held key are refused.
	if code, _, e := bn.run("", "", "peer", "quota", aKey, "2M"); code != 0 {
		t.Fatal(e)
	}
	if code, _, e := bn.run("", "", "import", disk); code != 1 || !strings.Contains(e, "quota") || !strings.Contains(e, "run the same import again") {
		t.Fatalf("import over quota: exit %d\n%s", code, e)
	}
	if code, _, e := bn.run("", "", "peer", "quota", aKey, "10G"); code != 0 {
		t.Fatal(e)
	}
	// #95: data goes first; as data was refused, the 4 metadata objects
	// are left for the network, so no index lands before its pack.
	code, out, e = bn.run("", "", "import", disk)
	if code != 1 || !strings.Contains(out, "Imported 4 objects") || !strings.Contains(e, "Refused "+tampered+": its bytes do not match") ||
		!strings.Contains(e, "Refused "+conflict+": this node holds the key with other bytes") ||
		!strings.Contains(e, "Left 4 metadata objects for the network") {
		t.Fatalf("import: exit %d\n%s%s", code, out, e)
	}
	held := lentObjects(t, bn, aID, "laptop")
	for k, h := range want {
		switch {
		case !strings.HasPrefix(k, "data/"):
			if _, ok := held[k]; ok {
				t.Errorf("B stored metadata %s though data was refused", k)
			}
		case k == tampered:
			if _, ok := held[k]; ok {
				t.Errorf("B stored the damaged %s", k)
			}
		case k == conflict:
			if held[k] != sha256Hex(other) {
				t.Errorf("B's %s was replaced", k)
			}
		case held[k] != h:
			t.Errorf("B holds %s as %q, want %q", k, held[k], h)
		}
	}
	if code, _, e := an.run("", "", "import", disk); code != 1 || !strings.Contains(e, "exported for node "+bKey) {
		t.Fatalf("import on the exporting node: exit %d\n%s", code, e)
	}

	// Boxes 3 and 4: A's daemon acknowledges the 4 objects B's list shows
	// with their hash and never sends them; the other six stay queued
	// while the seed holds, then go by the network.
	a.start()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st := an.status()
		if st.Seeds[0].Confirmed == 4 {
			// The queue and the seed's outcome from one status (#110):
			// read apart, the seed can end between them and the queue
			// start to drain.
			q := int64(-1)
			for _, p := range st.Places {
				if p.Place == bKey {
					q = p.Queue.Objects
				}
			}
			if st.Seeds[0].Outcome == "" && q != 6 {
				t.Fatalf("queued at B while the seed holds: %d, want the 2 refused and 4 metadata", q)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed not confirmed: %+v", st.Seeds)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for st := an.status(); st.Seeds[0].Outcome != "quiet"; st = an.status() {
		if time.Now().After(deadline) {
			t.Fatalf("seed not ended: %+v", st.Seeds)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for an.place(bKey).Queue.Objects != 0 {
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatalf("queue for B not drained: %+v", an.place(bKey))
		}
		time.Sleep(200 * time.Millisecond)
	}
	held = lentObjects(t, bn, aID, "laptop")
	for k, h := range want {
		if k != conflict && held[k] != h {
			t.Errorf("B's %s after the seed: %q, want %q (the network's copy)", k, held[k], h)
		}
	}
	v := scrape(t, maddr)
	if n := v[`lendisk_replication_objects_total{place="`+bKey+`",op="put"}`]; n != 5 {
		t.Errorf("A's worker acknowledged %v puts at B; want 5, the damaged object and the metadata (the seeded 4 never sent)", n)
	}
	if sent := v[`lendisk_replication_sent_bytes_total{place="`+bKey+`"}`]; sent < 100<<10 || sent > 151<<10 {
		t.Errorf("A sent B %v bytes; want the damaged object's 100 KiB, the metadata, and at most the held key's 50 KiB", sent)
	}
	a.p.kill()

	// #95: a second export starts a seed that status says how to cancel;
	// the cancel ends it. B imports that newer disk, and refuses the
	// first one after it, which could bring back what A deleted since.
	disk2 := filepath.Join(t.TempDir(), "disk2")
	if code, out, e := an.run("", "", "export", "laptop", "--for", bKey, disk2); code != 0 {
		t.Fatalf("second export: exit %d\n%s%s", code, out, e)
	}
	if _, text, _ := an.run("", "", "status"); !strings.Contains(text, "lendisk export --cancel laptop --for "+bKey) {
		t.Fatalf("status text of a pending seed:\n%s", text)
	}
	if code, out, e := an.run("", "", "export", "--cancel", "laptop", "--for", bKey); code != 0 || !strings.Contains(out, "Cancelled the seed") {
		t.Fatalf("cancel: exit %d\n%s%s", code, out, e)
	}
	if st := an.status(); len(st.Seeds) != 1 || st.Seeds[0].Outcome != "cancelled" {
		t.Fatalf("seeds after the cancel: %+v", st.Seeds)
	}
	if code, _, e := an.run("", "", "export", "--cancel", "laptop", "--for", bKey); code != 1 || !strings.Contains(e, "no pending seed") {
		t.Fatalf("cancel again: exit %d\n%s", code, e)
	}
	if code, _, e := bn.run("", "", "import", disk2); strings.Contains(e, "before the export") {
		t.Fatalf("the newer disk: exit %d\n%s", code, e)
	}
	if code, _, e := bn.run("", "", "import", disk); code != 1 || !strings.Contains(e, "before the export of the same bucket imported here") {
		t.Fatalf("the older disk after the newer: exit %d\n%s", code, e)
	}
}

// REPL-8: export --for and import refuse what they cannot do, and say
// what to do instead.
func TestSeedExportAndImportRefusals(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	other := friendNode(t).identity().NodeID().String()
	if code, _, e := n.run("", "", "bucket", "add", "laptop", "--profile", "restic"); code != 0 {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "disk")
	for _, c := range []struct {
		args []string
		code int
		msg  string
	}{
		{[]string{"export", "laptop", dir}, 2, "--for <friend>"},
		{[]string{"export", "laptop", "--for", other, "--lent", "lent-x", dir}, 2, "not both"},
		{[]string{"export", "laptop", "--for", other, "--kopia-layout", dir}, 2, "follows the bucket's profile"},
		{[]string{"export", "laptop", "--for", "nobody", dir}, 2, "not a node ID"},
		{[]string{"export", "nope", "--for", other, dir}, 1, `no bucket "nope"`},
		{[]string{"export", "laptop", "--for", other, dir}, 1, "is not a friend"},
		{[]string{"import", dir}, 1, "no seed manifest"},
		{[]string{"export", "--cancel", "laptop"}, 2, "export --cancel <bucket> --for <friend>"},
		{[]string{"export", "--cancel", "laptop", "--for", other, dir}, 2, "export --cancel <bucket> --for <friend>"},
		{[]string{"export", "--cancel", "laptop", "--for", other}, 1, "no pending seed of bucket laptop"},
	} {
		if code, _, e := n.run("", "", c.args...); code != c.code || !strings.Contains(e, c.msg) {
			t.Errorf("%v: exit %d, want %d with %q\n%s", c.args, code, c.code, c.msg, e)
		}
	}
	if _, err := os.Stat(dir); err == nil {
		t.Error("a refused export wrote its directory")
	}
}

// #95: with a data_dir too long for a unix socket path, the daemon
// serves on a private socket it names beside gw.sock; import and every
// command that asks whether the daemon runs find it there.
func TestImportFindsTheSocketOfALongDataDir(t *testing.T) {
	parallel(t, 0)
	long := filepath.Join(t.TempDir(), strings.Repeat("d", 100))
	if daemonRunning(long) {
		t.Fatal("running with nothing serving")
	}
	short, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	ln, err := net.Listen("unix", filepath.Join(short, "gw.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	os.MkdirAll(filepath.Dir(gateway.SocketPath(long)), 0o700)
	if err := os.WriteFile(gateway.SocketPath(long)+".path", []byte(ln.Addr().String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !daemonRunning(long) {
		t.Fatal("the daemon's moved socket was not found")
	}
}
