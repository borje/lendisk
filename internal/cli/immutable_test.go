package cli

import (
	"context"
	"strings"
	"testing"
	"time"

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
		"    ! 1 key changed after a friend acknowledged an earlier version; the local write stands but is not replicated (EP-11):\n",
		"      doc.txt: acknowledged h-doc.txt, now h-new\n",
		"1 changed after a friend acknowledged them (EP-11)",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status lacks %q:\n%s", want, text)
		}
	}
	o.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited for what cannot come"); return nil }
	if code, out, errOut := o.run("", "", "status", "--wait", "files"); code != 4 || out != "" ||
		!strings.Contains(errOut, "changed after a friend acknowledged them") {
		t.Fatalf("status --wait: exit %d\n%s%s", code, out, errOut)
	}
}
