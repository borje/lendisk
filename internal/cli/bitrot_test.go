package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/gateway"
)

// flipByte changes one byte of the file at path in place, keeping its
// inode, size and modification time: what a failing disk does.
func flipByte(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chtimes(path, fi.ModTime(), fi.ModTime()); err != nil {
			t.Fatal(err)
		}
	}()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[7] ^= 0x20
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(b[7:8], 7); err != nil {
		t.Fatal(err)
	}
}

// VER-5 AC (#31), real daemons: A borrows from B, both sampling 100% of
// their stores a week. A byte flips in an object in A's store and in
// another in B's copy of A's bucket. A week on both clocks later, A
// alerts bitrot for the bucket and replaces its object with a verified
// copy from B, fetched as the read-through fetches, so its store holds
// the right bytes, without enqueueing any write or delete; B alerts lent_bitrot for A and leaves
// A's object as it is. status shows both, in text and JSON.
func TestBitrotIsFoundOnBothSidesAndRepairedAtTheBorrower(t *testing.T) {
	parallel(t, 2)
	an, bn := friendNode(t), friendNode(t)
	bn.regions = an.regions
	// The clocks move a week on once the flips are made, so the samples
	// owe the whole store.
	clock := filepath.Join(t.TempDir(), "clock")
	jump := func(d time.Duration) {
		t.Helper()
		if err := os.WriteFile(clock, []byte(d.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := []string{envReplFast + "=1", envClock + "=" + clock, envAlertInterval + "=100ms", envBitrotEvery + "=100ms",
		"LENDISK_STORE_BITROT_SAMPLE=100"}
	a, b := newProcNode(t, an, env...), newProcNode(t, bn, env...)
	cr := an.addBucket(t, "laptop", "restic")
	if code, _, e := an.run("", "", "bucket", "set", "laptop", "--copies", "1"); code != 0 {
		t.Fatal(e)
	}
	b.start()
	a.start()
	befriend(t, bn, an)
	aKey, bKey := an.identity().NodeID().String(), bn.identity().NodeID().String()
	cl := s3client(a.addr, cr)
	bodies := map[string][]byte{}
	for i := range 5 {
		body := make([]byte, 3000+i)
		rand.Read(body)
		k := fmt.Sprintf("data/%02d", i)
		bodies[k] = body
		if _, err := cl.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(k),
			Body: bytes.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	an.drained(bKey, 60*time.Second)

	own := filepath.Join(an.data, "store", "laptop", "data", "01")
	lent := filepath.Join(bn.data, "store", gateway.LentName(an.identity().NodeID(), an.identity().SealedName("laptop")),
		"data", "03")
	flipByte(t, own)
	flipByte(t, lent)
	rotten, _ := os.ReadFile(lent)
	jump(200 * time.Hour)

	within(t, 60*time.Second, "A's bit-rot alert", func() bool { return an.hasAlert(alert.KindBitrot, "laptop") })
	within(t, 60*time.Second, "A's object fetched again from B", func() bool {
		got, err := os.ReadFile(own)
		return err == nil && bytes.Equal(got, bodies["data/01"])
	})
	within(t, 60*time.Second, "B's alert of A's rotten object", func() bool { return bn.hasAlert(alert.KindLentBitrot, aKey) })
	if got, _ := os.ReadFile(lent); !bytes.Equal(got, rotten) {
		t.Fatal("B changed A's object")
	}

	st := an.status()
	if st.Bitrot == nil || len(st.Bitrot.Rotten) != 1 || st.Bitrot.Rotten[0].Key != "data/01" ||
		st.Bitrot.Rotten[0].State != "repaired" || st.Bitrot.Cycles == 0 && st.Bitrot.Objects == 0 {
		t.Fatalf("A's status bitrot %+v", st.Bitrot)
	}
	if q := bucketOf(t, st, "laptop").Queue; q.Objects != 5 {
		t.Fatalf("A's queue %+v; want the 5 puts and nothing the repair enqueued", q)
	}
	if r := bucketOf(t, st, "laptop").Reconciled; r.Writes != 0 || r.Deletes != 0 {
		t.Fatalf("A reconciled %+v", r)
	}
	_, text, _ := an.run("", "", "status")
	if !strings.Contains(text, "bit-rot in laptop/data/01") || !strings.Contains(text, "Bit-rot sample:") {
		t.Fatalf("A's status:\n%s", text)
	}
	bst := bn.status()
	if bst.Bitrot == nil || len(bst.Bitrot.Rotten) != 1 || bst.Bitrot.Rotten[0].Friend != aKey ||
		bst.Bitrot.Rotten[0].Key != "data/03" || bst.Bitrot.Rotten[0].State != "lent" {
		t.Fatalf("B's status bitrot %+v", bst.Bitrot)
	}
	_, text, _ = bn.run("", "", "status")
	if !strings.Contains(text, "cannot repair") {
		t.Fatalf("B's status:\n%s", text)
	}
}
