package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"maps"
	mrand "math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// procNode is a node whose daemon runs as a real process.
type procNode struct {
	*node
	t    *testing.T
	addr string // loopback S3 endpoint
	logf string
	logw *os.File
	env  []string
	p    *daemonProc
}

func newProcNode(t *testing.T, n *node, env ...string) *procNode {
	t.Helper()
	logf := filepath.Join(t.TempDir(), "daemon.log")
	logw, err := os.Create(logf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logw.Close() })
	pn := &procNode{node: n, t: t, addr: freeAddr(t, "127.0.0.1"), logf: logf, logw: logw, env: env}
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(logf)
			t.Logf("daemon log of %s:\n%s", filepath.Base(filepath.Dir(n.data)), tail(b))
		}
	})
	return pn
}

// start starts the daemon with the node's env plus extra.
func (pn *procNode) start(extra ...string) {
	pn.t.Helper()
	pn.p = pn.startProc(pn.t, pn.addr, pn.logw, 0, append(slices.Clone(pn.env), extra...)...)
	pn.p.waitListening(pn.t, pn.addr, pn.logf)
}

// place is how n's status shows the place peer.
func (n *node) place(peer string) StatusPlace {
	n.t.Helper()
	for _, p := range n.status().Places {
		if p.Place == peer {
			return p
		}
	}
	n.t.Fatalf("status has no place %s", peer)
	return StatusPlace{}
}

// lentObjects is what lender's store holds of borrower's bucket.
func lentObjects(t *testing.T, lender *node, borrower *identity.Identity, bucket string) map[string]string {
	t.Helper()
	dir := filepath.Join(lender.data, "store", gateway.LentName(borrower.NodeID(), borrower.SealedName(bucket)))
	if _, err := os.Stat(dir); err != nil {
		return map[string]string{}
	}
	return storeObjects(t, dir)
}

// replicable is what of a bucket in n's store is replicated: all but
// restic's local-only locks.
func replicable(t *testing.T, n *node, bucket string) map[string]string {
	t.Helper()
	out := storeObjects(t, filepath.Join(n.data, "store", bucket))
	maps.DeleteFunc(out, func(k, _ string) bool { return strings.HasPrefix(k, "locks/") })
	return out
}

// sameObjects fails unless the friend holds exactly the borrower's
// replicable objects, byte for byte: nothing lost, nothing extra.
func sameObjects(t *testing.T, want, got map[string]string) {
	t.Helper()
	for k, h := range want {
		if got[k] != h {
			t.Errorf("friend's copy of %s: %q, want %q", k, got[k], h)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("friend holds %s, which the borrower does not", k)
		}
	}
}

// drained waits until n's FIFO for peer is empty and the place answered.
func (n *node) drained(peer string, within time.Duration) StatusPlace {
	n.t.Helper()
	deadline := time.Now().Add(within)
	for {
		p := n.place(peer)
		if p.Queue.Objects == 0 && p.State == "ok" {
			return p
		}
		if time.Now().After(deadline) {
			n.t.Fatalf("queue for %s not drained after %v: %+v", peer, within, p)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// randomFiles writes count files of size random bytes into dir.
func randomFiles(t *testing.T, dir, prefix string, count, size int) {
	t.Helper()
	buf := make([]byte, size)
	for i := 0; i < count; i++ {
		rand.Read(buf)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s-%02d", prefix, i)), buf, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// The tracer bullet of replication (#18): A backs up with restic and
// every captured object reaches its friend B through the tunnel, in
// write order, under the sealed name, with its hash, which A records on
// acknowledgement. Then:
//
//   - REPL-9: with B powered off, restic backups on A succeed at local
//     speed; status shows the queue and lag for B; when B returns, the
//     queue drains.
//   - REPL-3: A is SIGKILLed mid-transfer again and again, and B is
//     killed mid-transfer too; afterwards B holds exactly A's objects
//     (nothing lost, nothing duplicated) and every queue row is
//     acknowledged once, with its hash.
//   - B's copy is a restic repository restic can check (NET-7b's point).
//
// A and B share one relay, as two households in one region would.
func TestReplicateResticBackupToFriend(t *testing.T) {
	parallel(t, 2)
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions // one relay for both
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c := an.addBucket(t, "laptop", "restic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an) // B lends to A
	aID, bID := an.identity(), bn.identity()
	bKey := bID.NodeID().String()

	src := t.TempDir()
	env := append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+c.access, "AWS_SECRET_ACCESS_KEY="+c.secret,
		"RESTIC_REPOSITORY=s3:http://"+a.addr+"/laptop", "RESTIC_PASSWORD=test-password",
		"RESTIC_CACHE_DIR="+t.TempDir())
	backup := func() time.Duration {
		t.Helper()
		start := time.Now()
		cmd := exec.Command(restic, "backup", "--pack-size", "4", "--no-scan", src)
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("restic backup: %v\n%s", err, out)
		}
		return time.Since(start)
	}
	if out, err := (&exec.Cmd{Path: restic, Args: []string{"restic", "init"}, Env: env}).CombinedOutput(); err != nil {
		t.Fatalf("restic init: %v\n%s", err, out)
	}
	randomFiles(t, src, "one", 6, 1<<20)
	online := backup()
	p := an.drained(bKey, 60*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
	if p.LastOK == "" || p.Refused != 0 {
		t.Fatalf("place after the first backup: %+v", p)
	}

	// REPL-9: B is powered off; A's backups do not wait for it.
	b.p.kill()
	randomFiles(t, src, "two", 6, 1<<20)
	offline := backup()
	t.Logf("backup with B online %v, with B off %v", online, offline)
	if offline > 3*online+10*time.Second {
		t.Errorf("backup with B off took %v, with B on %v", offline, online)
	}
	deadline := time.Now().Add(30 * time.Second)
	for p = an.place(bKey); p.State != "retrying" || p.Queue.Objects == 0; p = an.place(bKey) {
		if time.Now().After(deadline) {
			t.Fatalf("status with B off: %+v", p)
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1100 * time.Millisecond)
	if p = an.place(bKey); p.LagSeconds < 1 || p.Queue.Bytes == 0 {
		t.Fatalf("no lag shown with B off: %+v", p)
	}
	_, text, _ := an.run("", "", "status")
	if !strings.Contains(text, bKey+" (friend): retrying, queue ") {
		t.Fatalf("status text:\n%s", text)
	}
	b.start()
	an.drained(bKey, 60*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))

	// REPL-3: restarts mid-transfer. B is off while a third backup
	// runs, so A has a queue; then A sends slowly and kills itself
	// mid-object at random, and B is killed now and then too. At one
	// crash in fifty reads the queue seldom drains, so the crashing ends
	// after five deaths of A (or two minutes) and a plain A drains it.
	b.p.kill()
	randomFiles(t, src, "three", 8, 1<<20)
	backup()
	b.start()
	a.p.kill()
	a.env = append(a.env, envSendDelay+"=5ms", envCrashMidSend+"=0.02")
	a.start()
	rng := mrand.New(mrand.NewPCG(uint64(time.Now().UnixNano()), 0))
	selfKills, bKills := 0, 0
	for i, end := 0, time.Now().Add(2*time.Minute); time.Now().Before(end); i++ {
		if a.p.exited() {
			selfKills++
			if selfKills >= 5 && bKills == 2 {
				break
			}
			a.start()
		}
		if p := an.place(bKey); p.Queue.Objects == 0 {
			break
		}
		if i == 2 || i == 6 { // while A sends, most likely mid-object
			b.p.kill()
			bKills++
			b.start()
		}
		time.Sleep(time.Duration(100+rng.IntN(200)) * time.Millisecond)
	}
	t.Logf("A killed itself mid-transfer %d times; B was killed %d times", selfKills, bKills)
	if selfKills == 0 {
		t.Error("A never died mid-transfer; the test proved nothing about it")
	}
	a.p.kill()
	a.env = []string{fast}
	a.start()
	an.drained(bKey, 60*time.Second)
	want := replicable(t, an, "laptop")
	sameObjects(t, want, lentObjects(t, bn, aID, "laptop"))

	// Every queue row is acknowledged at B once, with its hash.
	a.p.kill()
	b.p.kill()
	db, err := state.Open(context.Background(), an.data, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries, _ := db.QueueEntries(context.Background(), "laptop")
	var acked int
	for _, e := range entries {
		var n int
		var st, h string
		db.QueryRow(`SELECT count(*), coalesce(max(state), ''), coalesce(max(sha256), '') FROM placement WHERE seq = ? AND place = ?`,
			e.Seq, bKey).Scan(&n, &st, &h)
		superseded, _ := db.Superseded(context.Background(), e.Seq)
		switch {
		case n == 0 && superseded:
		case n != 1 || st != state.PlacementAcked:
			t.Errorf("%s (seq %d): %d placement rows, state %q", e.Key, e.Seq, n, st)
		case e.Op == state.OpPut && h != e.SHA256:
			t.Errorf("%s acknowledged with %s, captured %s", e.Key, h, e.SHA256)
		default:
			acked++
		}
	}
	t.Logf("%d objects at B, %d queue rows acknowledged", len(want), acked)

	// B's copy is a restic repository in its own right.
	local := append(os.Environ(), "RESTIC_PASSWORD=test-password", "RESTIC_CACHE_DIR="+t.TempDir(),
		"RESTIC_REPOSITORY="+filepath.Join(bn.data, "store", gateway.LentName(aID.NodeID(), aID.SealedName("laptop"))))
	if out, err := (&exec.Cmd{Path: restic, Args: []string{"restic", "check", "--read-data"}, Env: local}).CombinedOutput(); err != nil {
		t.Fatalf("restic check on B's copy: %v\n%s", err, out)
	}
	// SEC-16: B's store and database never hold the bucket's name.
	assertNotIn(t, bn, "laptop")
}

// REPL-3, NET-1 (#69): two friends on one relay, each with a bucket,
// replicate to each other at the same time and both queues drain. Each
// daemon serves with its identity key and dials with its dial key for
// the other; with one key for both, a node's own client took the
// packets meant for its server at the relay, and the second node to
// dial never got through.
func TestFriendsReplicateToEachOtherAtOnce(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions // one relay for both
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	ca := an.addBucket(t, "laptop", "generic")
	cb := bn.addBucket(t, "desktop", "generic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an) // B lends to A, and A to B
	aID, bID := an.identity(), bn.identity()

	put := func(addr string, c creds, bucket, prefix string, done chan<- error) {
		cl := s3client(addr, c)
		buf := make([]byte, 256<<10)
		for i := range 8 {
			rand.Read(buf)
			if _, err := cl.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket),
				Key: aws.String(fmt.Sprintf("%s-%02d", prefix, i)), Body: bytes.NewReader(buf)}); err != nil {
				done <- fmt.Errorf("put to %s: %w", bucket, err)
				return
			}
		}
		done <- nil
	}
	both := func(prefix string) {
		t.Helper()
		done := make(chan error, 2)
		go put(a.addr, ca, "laptop", prefix, done)
		go put(b.addr, cb, "desktop", prefix, done)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		// Both daemons replicate at once; the checks merely wait in turn.
		an.drained(bID.NodeID().String(), 90*time.Second)
		bn.drained(aID.NodeID().String(), 90*time.Second)
		sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
		sameObjects(t, replicable(t, bn, "desktop"), lentObjects(t, an, bID, "desktop"))
	}
	both("one")
	if len(lentObjects(t, bn, aID, "laptop")) != 8 || len(lentObjects(t, an, bID, "desktop")) != 8 {
		t.Fatal("a friend holds fewer than the 8 objects written")
	}

	// B restarts having forgotten A, as after losing lendisk.db, and
	// refuses A, whose client keeps trying while A writes on. Then the
	// friendship is restored behind B's daemon (as a one-shot command
	// does), and both write again: each reaches the other, and A without
	// a restart.
	b.p.kill()
	db := openDB(t, bn)
	f, err := db.GetFriendship(t.Context(), aID.NodeID().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `DELETE FROM friendship WHERE peer_key = ?`, f.Peer); err != nil {
		t.Fatal(err)
	}
	b.start()
	done := make(chan error, 1)
	go put(a.addr, ca, "laptop", "refused", done)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bKey := bID.NodeID().String()
	deadline := time.Now().Add(30 * time.Second)
	for p := an.place(bKey); p.State != "retrying"; p = an.place(bKey) {
		if time.Now().After(deadline) {
			t.Fatalf("A does not retry B while B refuses it: %+v", p)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := db.AddFriendship(t.Context(), f); err != nil {
		t.Fatal(err)
	}
	both("two")
}

// REPL-3, VER-5 (#50): an object the store could not supply as captured
// shows against its bucket in `status`, not against the friend, whose
// own refusals and condition stay its own.
func TestStatusShowsStoreRefusalsAgainstTheBucket(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "laptop", "generic")
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "B", Address: "tcB", OwnNumber: 1 << 30, PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	for _, k := range []string{"data/rotted", "data/immutable"} {
		s, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: k, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, s[0], 1, "h"); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s[0])
	}
	if _, err := db.RefuseObject(ctx, seqs[0], state.CauseStore, "the store holds 0 bytes, the capture 1"); err != nil {
		t.Fatal(err)
	}
	if err := db.RefusePlacement(ctx, "B", seqs[1], "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "B", state.ConditionOK, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	db.Close()
	st := n.status()
	if len(st.Buckets) != 1 || st.Buckets[0].Refused != 1 {
		t.Fatalf("buckets %+v", st.Buckets)
	}
	if p := n.place("B"); p.Refused != 1 || p.State != state.ConditionOK {
		t.Fatalf("place %+v", p)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "1 not replicated") {
		t.Fatalf("status text:\n%s", text)
	}
}

// REPL-3 (#57): while this node's store cannot be read, each place
// waiting on it shows the store as the cause, not the friend.
func TestStatusShowsAnUnreadableStoreAsTheCause(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "B", Address: "tcB", OwnNumber: 1 << 30, PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	cause := "the store cannot be read: opening laptop/data/1: too many open files"
	if err := db.SetPlaceCondition(ctx, "B", state.ConditionStore, cause, time.Now()); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if p := n.place("B"); p.State != state.ConditionStore || p.Detail != cause {
		t.Fatalf("place %+v", p)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "B (friend): waiting for this node's store,") || !strings.Contains(text, "("+cause+")") {
		t.Fatalf("status text:\n%s", text)
	}
}
