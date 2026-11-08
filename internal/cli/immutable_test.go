package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// EP-11 (#22): status shows a changed key under its bucket, with the
// acknowledged hash and the new one, and status --json carries them in
// buckets[].changed. The key is stuck (reason immutable), so the bucket
// is not claimed offsite with the new bytes and status --wait exits 4.
func TestStatusShowsAChangedAcknowledgedKey(t *testing.T) {
	o := newOffsiteNode(t, "files", "generic")
	o.daemonUp()
	if b := bucketOf(t, o.status(), "files"); b.Changed == nil || len(b.Changed) != 0 {
		t.Fatalf("changed before any write: %#v", b.Changed)
	}
	first := o.put("files", "doc.txt", "data", 100)
	o.ack(first)
	seqs, err := o.db.BeginCaptures(t.Context(), []state.Capture{{Bucket: "files", Key: "doc.txt", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.db.FinishPut(t.Context(), seqs[0], 100, "h-new"); err != nil {
		t.Fatal(err)
	}
	b := bucketOf(t, o.status(), "files")
	if len(b.Changed) != 1 {
		t.Fatalf("changed %+v", b.Changed)
	}
	c := b.Changed[0]
	if c.Key != "doc.txt" || c.Seq != seqs[0] || c.Acknowledged != "h-doc.txt" || c.SHA256 != "h-new" ||
		len(c.Places) != 2 || c.CapturedAt != o.now.UTC().Format(time.RFC3339) {
		t.Fatalf("changed %+v", c)
	}
	if b.Offsite.Complete || b.Offsite.Stuck.Immutable != 1 || b.Offsite.Watermark != first {
		t.Fatalf("offsite %+v", b.Offsite)
	}
	_, text, _ := o.run("", "", "status")
	for _, want := range []string{
		"    ! 1 key written with other bytes than a friend acknowledged or holds; the local write stands but is not replicated (EP-11):\n",
		"      doc.txt: acknowledged h-doc.txt, now h-new\n",
		"1 written with other bytes than a friend acknowledged or holds (EP-11",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status lacks %q:\n%s", want, text)
		}
	}
	o.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited for what cannot come"); return nil }
	if code, out, errOut := o.run("", "", "status", "--wait", "files"); code != 4 || out != "" ||
		!strings.Contains(errOut, "written with other bytes than a friend acknowledged or holds") {
		t.Fatalf("status --wait: exit %d\n%s%s", code, out, errOut)
	}

	// A friend's 412 (#22 review): its word, named, not an
	// acknowledgement; status --json says who reported it.
	other := o.put("files", "pic.jpg", "data", 100)
	at := o.queuedAt(other)
	if err := o.db.RefuseChanged(t.Context(), at[0], other, "h-theirs", "PreconditionFailed"); err != nil {
		t.Fatal(err)
	}
	b = bucketOf(t, o.status(), "files")
	if len(b.Changed) != 2 || b.Changed[1].Key != "pic.jpg" || strings.Join(b.Changed[1].ReportedBy, ",") != at[0] ||
		b.Changed[1].Places[at[0]] != "h-theirs" || len(b.Changed[0].ReportedBy) != 0 {
		t.Fatalf("changed %+v", b.Changed)
	}
	_, text, _ = o.run("", "", "status")
	if want := "      pic.jpg: friend " + at[0] + " reports holding other bytes, h-theirs (as reported by " + at[0] + "), now h-pic.jpg\n"; !strings.Contains(text, want) {
		t.Errorf("status lacks %q:\n%s", want, text)
	}
}

// EP-11, LEND-4, RET-1 (#22 review): the way to keep new bytes of a key
// that is not rewritable is to delete it and write it again, and that
// must work at a real lender, which keeps the deleted version in a tomb
// and refuses other bytes over a tomb unless marked as replacing it. A
// generic bucket's object is put, replicated, deleted (B tombs it), and
// written again with other bytes: B stores them and keeps the old
// version as a tomb, and A shows no changed key, no key_changed alert
// and no refusal.
func TestDeleteAndWriteAgainReplacesTheTombedVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions
	phrase, _ := identity.NewPhrase()
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	c := an.addBucket(t, "files", "generic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an)
	aID := an.identity()
	bKey := bn.identity().NodeID().String()
	cl := s3client(a.addr, c)
	ctx := t.Context()
	put := func(body string) {
		t.Helper()
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("files"), Key: aws.String("doc.txt"),
			Body: strings.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	put("first version")
	an.drained(bKey, 60*time.Second)
	if _, err := cl.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("files"), Key: aws.String("doc.txt")}); err != nil {
		t.Fatal(err)
	}
	an.drained(bKey, 60*time.Second)
	put("second version")
	p := an.drained(bKey, 60*time.Second)
	if got := lentObjects(t, bn, aID, "files"); got["doc.txt"] != sha256Hex([]byte("second version")) {
		t.Fatalf("B holds %q", got)
	}
	ts, err := openDB(t, bn).TombstonesOf(ctx, aID.NodeID().String(), aID.SealedName("files"), "doc.txt")
	if err != nil || len(ts) != 1 || ts[0].SHA256 != sha256Hex([]byte("first version")) {
		t.Fatalf("B's tombstones of doc.txt: %+v %v", ts, err)
	}
	st := an.status()
	for _, al := range st.Alerts {
		if al.Kind == alert.KindKeyChanged || al.Kind == alert.KindPlaceRefused {
			t.Errorf("alert %s %s: %s", al.Kind, al.Subject, al.Message)
		}
	}
	// One friend for copies 2: the object is short of a copy, but not
	// stuck as a changed key.
	if bk := bucketOf(t, st, "files"); len(bk.Changed) != 0 || bk.Offsite.Stuck.Immutable != 0 || p.Refused != 0 {
		t.Fatalf("changed %+v, offsite %+v, place %+v", bk.Changed, bk.Offsite, p)
	}
}
