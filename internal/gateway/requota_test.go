package gateway

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// lower sets this node's number for l's borrower to n, as `peer quota`
// does, and tells the lending layer, as the daemon does (#24).
func (l *lent) lower(n int64) {
	l.h.t.Helper()
	if _, _, err := l.h.db.SetOwnNumber(context.Background(), l.peer(), n, 0); err != nil {
		l.h.t.Fatal(err)
	}
	l.h.gw.Requota(l.peer())
}

// PEER-6, SEC-15 (#24): a quota lowered below what a borrower stores
// applies to its writes in flight, not only to the next request: a put
// whose body is still streaming fails its next read with 507, nothing
// of it is stored, and the borrower's count is unchanged. A put that
// still fits under the lowered quota goes on.
func TestLoweredQuotaEndsAPutInFlight(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	first := bytes.Repeat([]byte("a"), 3000)
	if err := l.put("data/a", first, hashMeta(first)); err != nil {
		t.Fatal(err)
	}
	used := l.usage()

	stream := func(key string, body []byte) (*io.PipeWriter, chan error) {
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() { done <- l.putDirect(key, pr, int64(len(body)), hashMeta(body)) }()
		if _, err := pw.Write(body[:10]); err != nil { // the store reads: the put is admitted and streaming
			t.Fatal(err)
		}
		return pw, done
	}

	// Lowered below use: the put in flight is refused at its next read.
	big := bytes.Repeat([]byte("b"), 2000)
	pw, done := stream("data/b", big)
	l.lower(used + 100)
	go pw.Write(big[10:])
	err := within(t, "the put in flight", done)
	if err != errQuotaExceeded {
		t.Fatalf("a put in flight after the quota was lowered below it: %v", err)
	}
	pw.CloseWithError(io.ErrClosedPipe)
	if _, ok := l.stored("data/b"); ok {
		t.Fatal("the refused put was stored")
	}
	l.wantUsage(used)
	// So is the next request.
	if err := l.put("data/c", []byte("c"), hashMeta([]byte("c"))); code(err) != "QuotaExceeded" {
		t.Fatalf("a put after the lowering: %v", err)
	}
	if _, err := l.c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: l.bucket, Key: aws.String("data/a")}); err != nil {
		t.Fatalf("what was stored is still served: %v", err)
	}

	// Lowered, but the put still fits: it is stored.
	small := bytes.Repeat([]byte("s"), 50)
	l.lower(1 << 30)
	pw, done = stream("data/s", small)
	l.lower(used + ov + 50)
	go func() { pw.Write(small[10:]); pw.Close() }()
	if err := within(t, "the put that fits", done); err != nil {
		t.Fatalf("a put that fits under the lowered quota: %v", err)
	}
	l.wantUsage(used + ov + 50)
}

// #24 review: the re-check counts the borrower's other writes in flight:
// two puts that each fit under the lowered quota alone, but not
// together, are not both stored.
func TestLoweredQuotaCountsOtherWritesInFlight(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	used := l.usage()
	type put struct {
		pw   *io.PipeWriter
		done chan error
		body []byte
	}
	var puts []put
	for i, c := range []byte("xy") {
		body := bytes.Repeat([]byte{c}, 2000)
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		key := "data/" + string(rune('a'+i))
		go func() { done <- l.putDirect(key, pr, int64(len(body)), hashMeta(body)) }()
		if _, err := pw.Write(body[:10]); err != nil {
			t.Fatal(err)
		}
		puts = append(puts, put{pw, done, body})
	}
	quota := used + 2*(2000+ov) - 1
	l.lower(quota)
	for _, p := range puts {
		go func() { p.pw.Write(p.body[10:]); p.pw.Close() }()
	}
	stored := 0
	for _, p := range puts {
		if err := within(t, "a put in flight", p.done); err == nil {
			stored++
		} else if err != errQuotaExceeded {
			t.Fatalf("a put in flight: %v", err)
		}
	}
	if stored > 1 || l.usage() > quota {
		t.Fatalf("%d puts stored, usage %d over the lowered quota %d", stored, l.usage(), quota)
	}
}
