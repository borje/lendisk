package gateway

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// EP-11 (#22): the local store is the user's. A put to a key a friend
// acknowledged with other bytes succeeds and the store serves the new
// bytes, but nothing of it is placed: it is reported as a changed key.
// A key the bucket's profile names rewritable (Kopia's
// kopia.maintenance) is placed as a rewrite, and reported nowhere.
func TestAChangedKeyIsStoredButNotPlaced(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic", "vault": "kopia"})
	ctx := context.Background()
	if err := h.db.AddFriendship(ctx, state.Friendship{Peer: "B", Address: "tcB", OwnNumber: 1 << 40, PeerNumber: 1 << 40,
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	h.serve(false)
	put := func(bucket, key, body string) {
		t.Helper()
		if _, err := h.client(bucket).PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: strings.NewReader(body)}); err != nil {
			t.Fatalf("put %s/%s: %v", bucket, key, err)
		}
	}
	latest := func(bucket, key string) state.Entry {
		t.Helper()
		var e state.Entry
		for _, x := range h.entries(bucket) {
			if x.Key == key {
				e = x
			}
		}
		return e
	}
	ackAtB := func(bucket, key string) {
		t.Helper()
		e := latest(bucket, key)
		if err := h.db.AckPlacement(ctx, "B", e.Seq, e.SHA256, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	placed := func(seq int64) int {
		t.Helper()
		var n int
		if err := h.db.QueryRow(`SELECT count(*) FROM placement WHERE seq = ?`, seq).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	put("files", "doc.txt", "first")
	ackAtB("files", "doc.txt")
	put("files", "doc.txt", "second")
	out, err := h.client("files").GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("files"), Key: aws.String("doc.txt")})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(out.Body)
	out.Body.Close()
	if string(body) != "second" {
		t.Fatalf("the store serves %q", body)
	}
	e := latest("files", "doc.txt")
	if e.SHA256 != sum([]byte("second")) || placed(e.Seq) != 0 {
		t.Fatalf("the changed write %+v placed at %d places", e, placed(e.Seq))
	}
	cs, err := h.db.ChangedKeys(ctx)
	if err != nil || len(cs) != 1 || cs[0].Key != "doc.txt" || cs[0].Acknowledged != sum([]byte("first")) ||
		cs[0].SHA256 != sum([]byte("second")) {
		t.Fatalf("changed keys %+v %v", cs, err)
	}

	put("vault", "kopia.maintenance", "schedule one")
	ackAtB("vault", "kopia.maintenance")
	put("vault", "kopia.maintenance", "schedule two")
	if e := latest("vault", "kopia.maintenance"); !e.Rewritable || placed(e.Seq) != 1 {
		t.Fatalf("the rewrite %+v placed at %d places", e, placed(e.Seq))
	}
	if cs, _ := h.db.ChangedKeys(ctx); len(cs) != 1 {
		t.Fatalf("changed keys %+v", cs)
	}
}
