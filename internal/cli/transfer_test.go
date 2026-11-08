package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// twoProcs is a borrower A with a generic bucket and its lender B, both
// daemons real processes on one relay; A's env adds aEnv.
func twoProcs(t *testing.T, aEnv ...string) (an, bn *node, a, b *procNode, c creds) {
	t.Helper()
	an = friendNode(t)
	bn = newNode(t)
	bn.regions = an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c = an.addBucket(t, "files", "generic")
	fast := envReplFast + "=1"
	a, b = newProcNode(t, an, append([]string{fast}, aEnv...)...), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	return an, bn, a, b, c
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func putObject(t *testing.T, addr string, c creds, key string, body []byte) {
	t.Helper()
	if _, err := s3client(addr, c).PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("files"), Key: aws.String(key),
		Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
}

// doneSeq is the queue row of key's latest done put in n's lendisk.db.
func doneSeq(t *testing.T, db *state.DB, key string) int64 {
	t.Helper()
	var seq int64
	if err := db.QueryRow(`SELECT max(seq) FROM queue WHERE key = ? AND op = 'put' AND state = 'done'`, key).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	return seq
}

// REPL-6 AC (#23): a 30 MiB object goes to the friend in 5 MiB parts.
// The borrower's daemon is SIGKILLed while it sends part 4; the upload
// ID and parts 1 to 3 are in its lendisk.db, and once it is started
// again the object continues at part 4: the friend holds it whole, with
// its hash, and the worker logged that it kept three parts.
func TestALargeObjectResumesAtTheNextPartAfterSIGKILL(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	an, bn, a, _, c := twoProcs(t, "LENDISK_REPLICATION_PART_SIZE=5M")
	bKey := bn.identity().NodeID().String()
	a.p.kill()
	a.start(fmt.Sprintf("%s=%d", envCrashAtByte, 18<<20)) // inside part 4
	body := randomBytes(30 << 20)
	putObject(t, a.addr, c, "big.bin", body)
	for deadline := time.Now().Add(2 * time.Minute); !a.p.exited(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the daemon never reached part 4")
		}
	}
	db := openDB(t, an)
	tr, ok, err := db.Transfer(t.Context(), bKey, doneSeq(t, db, "big.bin"))
	if err != nil || !ok || tr.UploadID == "" || tr.PartSize != 5<<20 || len(tr.Parts) != 3 {
		t.Fatalf("after the kill the row's upload is %+v %v %v; want parts 1 to 3 recorded", tr, ok, err)
	}
	db.Close()
	a.start()
	an.drained(bKey, 2*time.Minute)
	if got := lentObjects(t, bn, an.identity(), "files"); got["big.bin"] != sha256Hex(body) {
		t.Fatalf("B holds %q", got)
	}
	log, _ := os.ReadFile(a.logf)
	if !strings.Contains(string(log), "resuming an upload at its next part") || !strings.Contains(string(log), "parts_kept=3") {
		t.Fatalf("no resume at part 4 in the log:\n%s", tail(log))
	}
	if _, ok, _ := openDB(t, an).Transfer(t.Context(), bKey, tr.Seq); ok {
		t.Fatal("the upload's record outlived its acknowledgement")
	}
}

// LEND-1, PEER-6, EP-11, LEND-4 (#23): the lender's quota and the
// immutability policy hold for objects sent in parts.
//
//   - EP-11: a key acknowledged with one version and written with other
//     bytes large enough for parts is held back at capture; the friend
//     keeps the acknowledged version and no upload is begun there.
//   - EP-11, LEND-4: deleted and written again in parts, the key is a
//     multipart upload marked with the tombed version (#74): stored
//     beside the tomb, with no alert.
//   - LEND-1, PEER-6: a quota lowered while an object goes in parts at
//     the global upload limit refuses its next part (507): the borrower
//     pauses for quota, the friend publishes nothing at the key, and its
//     count of the borrower stays within what it counted before.
func TestPartsKeepTheLendersQuotaAndImmutability(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	an, bn, a, _, c := twoProcs(t, "LENDISK_REPLICATION_PART_SIZE=5M", "LENDISK_REPLICATION_UPLOAD_LIMIT=1M")
	aID := an.identity()
	aKey, bKey := aID.NodeID().String(), bn.identity().NodeID().String()
	v1, v2, v3 := randomBytes(6<<20), randomBytes(6<<20), randomBytes(7<<20)

	putObject(t, a.addr, c, "doc.bin", v1)
	an.drained(bKey, 2*time.Minute)
	putObject(t, a.addr, c, "doc.bin", v2)
	if bk := bucketOf(t, an.status(), "files"); len(bk.Changed) != 1 {
		t.Fatalf("changed keys %+v, want doc.bin held back", bk.Changed)
	}
	if got := lentObjects(t, bn, aID, "files"); got["doc.bin"] != sha256Hex(v1) {
		t.Fatalf("B holds %q", got)
	}
	if us, err := openDB(t, bn).LentUploads(t.Context(), aKey); err != nil || len(us) != 0 {
		t.Fatalf("uploads at B %+v %v", us, err)
	}

	if _, err := s3client(a.addr, c).DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: aws.String("files"), Key: aws.String("doc.bin")}); err != nil {
		t.Fatal(err)
	}
	an.drained(bKey, 2*time.Minute)
	putObject(t, a.addr, c, "doc.bin", v3)
	an.drained(bKey, 2*time.Minute)
	if got := lentObjects(t, bn, aID, "files"); got["doc.bin"] != sha256Hex(v3) {
		t.Fatalf("after delete and write again B holds %q", got)
	}
	ts, err := openDB(t, bn).TombstonesOf(t.Context(), aKey, aID.SealedName("files"), "doc.bin")
	if err != nil || len(ts) != 1 || ts[0].SHA256 != sha256Hex(v1) {
		t.Fatalf("B's tombstones of doc.bin: %+v %v", ts, err)
	}
	// The held-back change cleared with the delete (its alert clears at
	// the alert service's next pass), and nothing was refused.
	st := an.status()
	if bk := bucketOf(t, st, "files"); len(bk.Changed) != 0 {
		t.Fatalf("changed keys after delete and write again: %+v", bk.Changed)
	}
	for _, al := range st.Alerts {
		if al.Kind == alert.KindPlaceRefused {
			t.Errorf("alert %s %s: %s", al.Kind, al.Subject, al.Message)
		}
	}

	// About 30 s at 1 MiB/s: the quota falls once two parts are in.
	body := randomBytes(30 << 20)
	putObject(t, a.addr, c, "big.bin", body)
	adb := openDB(t, an)
	seq := doneSeq(t, adb, "big.bin")
	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		if tr, ok, _ := adb.Transfer(t.Context(), bKey, seq); ok && len(tr.Parts) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("two parts never went")
		}
	}
	bdb := openDB(t, bn)
	before, _, _ := bdb.LentUsage(t.Context(), aKey)
	if code, _, e := bn.run("", "", "peer", "quota", aKey, "8M"); code != 0 {
		t.Fatalf("peer quota: %s", e)
	}
	for deadline := time.Now().Add(2 * time.Minute); an.place(bKey).State != state.ConditionQuota; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("A not paused for quota: %+v", an.place(bKey))
		}
	}
	if got := lentObjects(t, bn, aID, "files"); got["big.bin"] != "" {
		t.Fatal("B published big.bin over the lowered quota")
	}
	if after, _, _ := bdb.LentUsage(t.Context(), aKey); after > before+5<<20 {
		t.Fatalf("B counts %d of A, %d before the quota fell: more than the part in flight", after, before)
	}
}

// REPL-7 (#23): the schedule and limits change without a restart. With
// a window that is closed now, a write waits; the file edited to drop
// the window is picked up by the daemon's look at it, and the object
// goes. Then, with the file's look an hour off, SIGHUP applies a new
// closed window at once.
func TestPacingChangesApplyWithoutARestart(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	now := time.Now()
	closed := fmt.Sprintf("%s-%s", now.Add(2*time.Hour).Format("15:04"), now.Add(3*time.Hour).Format("15:04"))
	an := friendNode(t)
	orig, _ := os.ReadFile(an.config)
	write := func(extra string) {
		t.Helper()
		if err := os.WriteFile(an.config, append(append([]byte{}, orig...), extra...), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("\n[replication]\nschedule = \"" + closed + "\"\n")
	bn := newNode(t)
	bn.regions = an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c := an.addBucket(t, "files", "generic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast, envReloadEvery+"=200ms"), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	bKey := bn.identity().NodeID().String()
	putObject(t, a.addr, c, "one.bin", []byte("one"))
	time.Sleep(3 * time.Second)
	if p := an.place(bKey); p.Queue.Objects != 1 {
		t.Fatalf("sent outside the schedule: %+v", p)
	}
	write("\n[replication]\nupload_limit = \"1M\"\n")
	an.drained(bKey, time.Minute)

	a.p.kill()
	a.env = []string{fast, envReloadEvery + "=1h"}
	a.start()
	write("\n[replication]\nschedule = \"" + closed + "\"\n")
	a.p.cmd.Process.Signal(syscall.SIGHUP)
	log := func() string { b, _ := os.ReadFile(a.logf); return string(b) }
	for deadline := time.Now().Add(time.Minute); !strings.Contains(log(), "why=SIGHUP"); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("SIGHUP did not reload")
		}
	}
	putObject(t, a.addr, c, "two.bin", []byte("two"))
	time.Sleep(3 * time.Second)
	if p := an.place(bKey); p.Queue.Objects != 1 || a.p.exited() {
		t.Fatalf("sent outside the schedule set by SIGHUP: %+v (exited %v)", p, a.p.exited())
	}
}
