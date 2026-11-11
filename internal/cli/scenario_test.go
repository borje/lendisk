package cli

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
)

// fourFriends is REQUIREMENTS §7's setting for scenarios 1 and 2: nodes
// A, B, C and D on one relay, each pair friends at 80 GB (six invites),
// scaled down to 80 MB, and A's restic bucket `laptop` at the default
// copies, 2. Every daemon is a real process.
type fourFriends struct {
	t       *testing.T
	an      *node
	a       *procNode
	friends map[string]*procNode // B, C and D by node ID
	aID     *identity.Identity
	env     []string // restic's environment
	creds   creds    // the bucket's credential
	src     string
}

func newFourFriends(t *testing.T, aEnv ...string) *fourFriends {
	t.Helper()
	return newFourFriendsAt(t, "80M", aEnv...)
}

// newFourFriendsAt is newFourFriends with every friendship at quota.
func newFourFriendsAt(t *testing.T, quota string, aEnv ...string) *fourFriends {
	t.Helper()
	return newCircleAt(t, 4, quota, aEnv...)
}

// newCircleAt is newFourFriendsAt with size nodes: A and size − 1
// friends, each pair friends at quota.
func newCircleAt(t *testing.T, size int, quota string, aEnv ...string) *fourFriends {
	t.Helper()
	return newCircleWith(t, size, quota, nil, aEnv...)
}

// newCircleWith is newCircleAt with extra flags for every invite and
// peer add (such as --retention, which both take).
func newCircleWith(t *testing.T, size int, quota string, peerAdd []string, aEnv ...string) *fourFriends {
	t.Helper()
	if _, err := exec.LookPath("restic"); err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	nodes := make([]*node, size)
	for i := range nodes {
		nodes[i] = newNode(t)
	}
	an := nodes[0]
	for _, n := range nodes {
		n.regions = an.regions // one relay for all
		p, _ := identity.NewPhrase()
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	c := an.addBucket(t, "laptop", "restic")
	fast := envReplFast + "=1"
	f := &fourFriends{t: t, an: an, a: newProcNode(t, an, append([]string{fast}, aEnv...)...),
		friends: map[string]*procNode{}, aID: an.identity(), src: t.TempDir()}
	for _, n := range nodes[1:] {
		pn := newProcNode(t, n, fast)
		pn.start()
		f.friends[n.identity().NodeID().String()] = pn
	}
	f.a.start()
	for i, lender := range nodes { // every pair: six invites for four
		for _, borrower := range nodes[i+1:] {
			s := lender.invite(append([]string{"--quota", quota}, peerAdd...)...)
			if code, _, errOut := borrower.run("\n", "", append([]string{"peer", "add", s}, peerAdd...)...); code != 0 {
				t.Fatalf("peer add: %d %s", code, errOut)
			}
		}
	}
	f.creds = c
	f.env = append(os.Environ(), "AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
		"RESTIC_REPOSITORY=s3:http://"+f.a.addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	f.restic("init")
	return f
}

func (f *fourFriends) restic(args ...string) string {
	f.t.Helper()
	cmd := exec.Command("restic", args...)
	cmd.Env = f.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("restic %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

var reSaved = regexp.MustCompile(`snapshot ([0-9a-f]{8}) saved`)

// backup writes count new files of 1 MiB and backs them up; it returns
// restic's short snapshot ID.
func (f *fourFriends) backup(prefix string, count int, flags ...string) string {
	f.t.Helper()
	randomFiles(f.t, f.src, prefix, count, 1<<20)
	if flags == nil {
		flags = []string{"--pack-size", "4"}
	}
	m := reSaved.FindStringSubmatch(f.restic(append(append([]string{"backup"}, flags...), "--no-scan", f.src)...))
	if m == nil {
		f.t.Fatal("restic printed no snapshot ID")
	}
	return m[1]
}

// waitOffsite runs `lendisk status --wait laptop` on A and wants
// snapshot id offsite at 2 copies.
func (f *fourFriends) waitOffsite(id string) {
	f.t.Helper()
	code, out, errOut := f.an.run("", "", "status", "--wait", "laptop", "--timeout", "3m")
	if code != 0 || out != "snapshot "+id+" is offsite at 2 copies\n" {
		_, js, _ := f.an.run("", "", "status", "--json")
		f.t.Fatalf("status --wait: exit %d\n%s%s\nstatus --json:\n%s", code, out, errOut, js)
	}
}

// drain waits until A's queue to every running friend is empty. A send
// A abandons as stalled resets its connection, so the lender gives back
// what it reserved at once, and a retry meanwhile is answered 503 (busy)
// rather than 507 (#89, #92): 90 s is enough.
func (f *fourFriends) drain() {
	f.t.Helper()
	for id, pn := range f.friends {
		if pn.p != nil && !pn.p.exited() {
			// Three minutes: on a loaded runner a send retried after a
			// busy answer (#92, #93: backoff or Retry-After) can take
			// well over the 90 s this was (TestScenarioQuota flaked).
			if p := f.an.drained(id, 3*time.Minute); p.Refused != 0 {
				f.t.Fatalf("place %+v", p)
			}
		}
	}
}

// placedAsSpecified checks scenario 1's placement: the friends hold
// exactly A's replicable objects, byte for byte, every data object at
// two of them and the metadata at all three.
func (f *fourFriends) placedAsSpecified() {
	f.t.Helper()
	want := replicable(f.t, f.an, "laptop")
	holders := map[string][]string{}
	pool := map[string]string{}
	for id, pn := range f.friends {
		for k, h := range lentObjects(f.t, pn.node, f.aID, "laptop") {
			pool[k] = h
			if want[k] == h {
				holders[k] = append(holders[k], id)
			}
		}
	}
	sameObjects(f.t, want, pool)
	var data int
	for k := range want {
		n := 3
		if strings.HasPrefix(k, "data/") {
			n = 2
			data++
		}
		if len(holders[k]) != n {
			f.t.Errorf("%s is at %d friends, want %d", k, len(holders[k]), n)
		}
	}
	if data < 2 {
		f.t.Fatalf("only %d data objects", data)
	}
}

// Scenario 1, first backup (REPL-4, REPL-5, REPL-11, REPL-12; #20): four
// nodes, six invites at 80 GB (80 MB here), a restic bucket at two
// copies. `restic backup` on A finishes against A's store;
// `status --wait laptop` returns once the snapshot is offsite at two
// copies, and `status` says so by restic's snapshot ID; every data
// object is at two of B, C and D and the metadata at all three; the
// protectable size is about 120 GB (LEND-7, #37: 80 from each of three
// friends, less the metadata each holds, at two copies).
func TestScenarioFirstBackupIsOffsiteAtTwoCopies(t *testing.T) {
	parallel(t, 4)
	f := newFourFriends(t)
	id := f.backup("one", 8)
	f.waitOffsite(id)
	f.drain()
	_, text, _ := f.an.run("", "", "status")
	if !strings.Contains(text, "    snapshot "+id+" is offsite at 2 copies\n") {
		t.Fatalf("status:\n%s", text)
	}
	b := bucketOf(t, f.an.status(), "laptop")
	if !b.Offsite.Complete || b.Offsite.Snapshot == nil || b.Offsite.Snapshot.ID[:8] != id || b.BelowCopies.Objects != 0 {
		t.Fatalf("bucket %+v, offsite %+v", b, b.Offsite)
	}
	f.placedAsSpecified()
	if c := capacityOf(t, f.an.status()); c.Protectable < 115<<20 || c.Protectable > 120<<20 || c.Alerting {
		t.Errorf("protectable %d bytes, want about 120 MB: %+v", c.Protectable, c)
	}
}

// Scenario 2, a friend offline (REPL-9, REPL-12, OPS-2; #20), on A's
// injected clock: the friend that A's next data would go to first is
// powered off for three days. restic backups on A keep working; the
// copies queued for it are re-routed once it has been unreachable for
// reroute_after, and new data goes to the other two, so each snapshot
// is offsite at two copies throughout; status shows its lag; at 24h the
// friend_unreachable alert fires, and not before. When it returns its
// queue drains: the metadata, and the objects still placed there.
func TestScenarioFriendOfflineForThreeDays(t *testing.T) {
	parallel(t, 4)
	clock := filepath.Join(t.TempDir(), "clock")
	f := newFourFriends(t, envClock+"="+clock, envAlertInterval+"=200ms", envRerouteEvery+"=200ms")
	f.an.clock = offsetClock(clock)
	ahead := func(d time.Duration) {
		t.Helper()
		if err := os.WriteFile(clock, []byte(d.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.waitOffsite(f.backup("one", 8))
	f.drain()

	// B is the friend with the least use: the first choice for new data.
	use, err := openDB(t, f.an).PlaceUse(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := slices.Sorted(maps.Keys(f.friends))
	slices.SortStableFunc(ids, func(x, y string) int { return cmp.Compare(use[x], use[y]) })
	bID := ids[0]
	others := ids[1:]
	f.friends[bID].p.kill()

	queuedData := func(place string) int {
		t.Helper()
		var n int
		if err := openDB(t, f.an).QueryRow(`SELECT count(*) FROM placement p JOIN queue q ON q.seq = p.seq
			WHERE p.place = ? AND p.state = 'queued' AND q.class = 'data'`, place).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	id2 := f.backup("two", 8) // at local speed, B or not
	if queuedData(bID) == 0 {
		t.Fatal("none of the second backup's data was placed at B")
	}
	deadline := time.Now().Add(60 * time.Second)
	for f.an.place(bID).State != "retrying" {
		if time.Now().After(deadline) {
			t.Fatalf("A does not see B fail: %+v", f.an.place(bID))
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Two hours on, B is unreachable: its unsent data moves to C and D.
	ahead(2 * time.Hour)
	deadline = time.Now().Add(60 * time.Second)
	for queuedData(bID) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("B's data still queued there: %d rows", queuedData(bID))
		}
		time.Sleep(200 * time.Millisecond)
	}
	f.waitOffsite(id2)
	var before int64
	openDB(t, f.an).QueryRow(`SELECT max(seq) FROM queue`).Scan(&before)
	id3 := f.backup("three", 8)
	f.waitOffsite(id3)
	rows, err := openDB(t, f.an).Query(`SELECT DISTINCT p.place FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE q.class = 'data' AND q.op = 'put' AND p.seq > ? AND p.state IN ('queued', 'acked') ORDER BY p.place`, before)
	if err != nil {
		t.Fatal(err)
	}
	var placed []string
	for rows.Next() {
		var p string
		rows.Scan(&p)
		placed = append(placed, p)
	}
	rows.Close()
	slices.Sort(others)
	if !slices.Equal(placed, others) {
		t.Fatalf("new data placed at %v, want C and D %v", placed, others)
	}
	p := f.an.place(bID)
	if p.Queue.Objects == 0 || p.LagSeconds < int64((2*time.Hour).Seconds()) {
		t.Fatalf("status shows B as %+v; want its queue and lag", p)
	}
	unreachable := func() bool {
		for _, al := range f.an.status().Alerts {
			if al.Kind == alert.KindFriendUnreachable && al.Subject == bID {
				return true
			}
		}
		return false
	}
	time.Sleep(time.Second) // five alert passes
	if unreachable() {
		t.Fatal("friend_unreachable before 24h")
	}

	// A day on, the alert fires.
	ahead(25 * time.Hour)
	deadline = time.Now().Add(30 * time.Second)
	for !unreachable() {
		if time.Now().After(deadline) {
			t.Fatalf("no friend_unreachable alert at 25h: %+v", f.an.status().Alerts)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Three days on, backups still go offsite; then B returns and
	// drains.
	ahead(72 * time.Hour)
	f.waitOffsite(f.backup("four", 4))
	f.friends[bID].start()
	f.drain()
	if p := f.an.place(bID); p.State != "ok" || p.Queue.Objects != 0 {
		t.Fatalf("B after its return: %+v", p)
	}
	f.placedAsSpecified()
	if b := bucketOf(t, f.an.status(), "laptop"); !b.Offsite.Complete {
		t.Fatalf("offsite after B drained: %+v", b.Offsite)
	}
}

// REPL-1, REPL-5, REPL-11 (#20 review): a pack uploaded as a multipart
// upload is answered before lendisk has hashed it and completed its
// queue row (#49); here each such hash is held back fifteen seconds
// (four were too few for a loaded CI runner to write the index and
// snapshot in, e63591c), and the tool writes them meanwhile. status --wait still
// returns only once the pack is at two friends: a pending pack holds
// the watermark and the metadata barrier. restic 0.19.1 sends no
// multipart upload (§3.3), so a stock S3 client writes the backup in
// restic's layout to the restic bucket, between real daemons.
func TestStatusWaitWaitsForAMultipartPack(t *testing.T) {
	parallel(t, 4)
	f := newFourFriends(t, envHashDelay+"=15s")
	ctx := t.Context()
	cl := s3client(f.a.addr, f.creds)
	bucket := aws.String("laptop")
	hexKey := func(prefix string) string {
		b := make([]byte, 32)
		rand.Read(b)
		return prefix + hex.EncodeToString(b)
	}
	pack := hexKey("data/7f/")
	up, err := cl.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: bucket, Key: aws.String(pack)})
	if err != nil {
		t.Fatal(err)
	}
	var parts []types.CompletedPart
	for n, size := range []int{5 << 20, 1 << 20} {
		body := make([]byte, size)
		rand.Read(body)
		r, err := cl.UploadPart(ctx, &s3.UploadPartInput{Bucket: bucket, Key: aws.String(pack), UploadId: up.UploadId,
			PartNumber: aws.Int32(int32(n + 1)), Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: r.ETag, PartNumber: aws.Int32(int32(n + 1))})
	}
	if _, err := cl.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: bucket, Key: aws.String(pack),
		UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	snap := hexKey("snapshots/")
	for _, k := range []string{hexKey("index/"), snap} {
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String(k),
			Body: strings.NewReader("encrypted " + k)}); err != nil {
			t.Fatal(err)
		}
	}
	var pending int
	if err := openDB(t, f.an).QueryRow(`SELECT count(*) FROM queue WHERE key = ? AND state = 'pending'`, pack).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatal("the pack's row is not pending while the snapshot is written: the test shows nothing")
	}
	f.waitOffsite(strings.TrimPrefix(snap, "snapshots/")[:8])
	n := 0
	h := replicable(t, f.an, "laptop")[pack]
	for _, pn := range f.friends {
		if lentObjects(t, pn.node, f.aID, "laptop")[pack] == h {
			n++
		}
	}
	if n < 2 {
		t.Fatalf("status --wait returned with the multipart pack at %d friends", n)
	}
}
