package gateway

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// trickleFetch serves each place's bytes; a place in slow sends one
// byte every gap, so the stall watchdog never fires, until the fetch's
// context ends.
type trickleFetch struct {
	fakeFetch
	slow map[string]bool
	gap  time.Duration
}

func (f *trickleFetch) Fetch(ctx context.Context, place, bucket, key string) (io.ReadCloser, error) {
	rc, err := f.fakeFetch.Fetch(ctx, place, bucket, key)
	if err != nil || !f.slow[place] {
		return rc, err
	}
	return io.NopCloser(&trickle{ctx: ctx, r: rc, gap: f.gap}), nil
}

type trickle struct {
	ctx context.Context
	r   io.Reader
	gap time.Duration
}

func (t *trickle) Read(p []byte) (int, error) {
	select {
	case <-t.ctx.Done():
		return 0, t.ctx.Err()
	case <-time.After(t.gap):
	}
	return t.r.Read(p[:min(len(p), 1)])
}

// EP-10 (#73): a fetch has a deadline from the object's size and a
// minimum throughput. A friend trickling a byte at a time, never long
// enough apart for the stall watchdog, is given up on at the deadline,
// logged, and the next friend serves the object: the key's lock and
// the reader are not held for as long as the trickle lasts.
func TestATricklingFriendIsGivenUpOnAtTheFetchDeadline(t *testing.T) {
	pack := bytes.Repeat([]byte("p"), 400)
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {"data/aa/1": pack}, "C": {"data/aa/1": pack}})
	ctx := context.Background()
	now := time.Now()
	h.db.SetPlaceCondition(ctx, "B", state.ConditionOK, "", now)
	h.db.SetPlaceCondition(ctx, "C", state.ConditionOK, "", now.Add(-time.Hour))
	// B, tried first, would take 400 × 20 ms = 8 s; the bound is
	// 200 ms + 400 B at 1 KiB/s, about 0.6 s.
	f := &trickleFetch{fakeFetch: fakeFetch{held: map[string][]byte{"B data/aa/1": pack, "C data/aa/1": pack}},
		slow: map[string]bool{"B": true}, gap: 20 * time.Millisecond}
	logs := &syncBuffer{}
	h.fetch, h.log = f, slog.New(slog.NewTextHandler(logs, nil))
	h.fetchBound = FetchBound{Base: 200 * time.Millisecond, Rate: 1 << 10}
	h.serve(false)
	start := time.Now()
	got, err := getBody(t, h.client("laptop"), "data/aa/1", "")
	if err != nil || !bytes.Equal(got, pack) {
		t.Fatalf("read-through: %d bytes, %v", len(got), err)
	}
	if el := time.Since(start); el > 4*time.Second {
		t.Fatalf("the read waited %v for a trickling friend", el)
	}
	if calls := f.fetched(); !slices.Equal(calls, []string{"B data/aa/1", "C data/aa/1"}) {
		t.Fatalf("fetched %v, want B then C", calls)
	}
	if l := logs.String(); !strings.Contains(l, "place=B") || !strings.Contains(l, "longer than") {
		t.Fatalf("B's slow fetch not logged:\n%s", l)
	}
}

// The default bound is the stall timeout plus the size at the lender's
// floor rate.
func TestTheDefaultFetchBoundIsTheStallPlusTheSizeAtTheFloorRate(t *testing.T) {
	var b FetchBound
	if got := b.of(0); got != 2*time.Minute {
		t.Fatalf("an empty object: %v", got)
	}
	if got := b.of(16 << 20); got != 2*time.Minute+2048*time.Second {
		t.Fatalf("a 16 MiB pack: %v", got)
	}
}

// EP-10 (#73): a recorded size of 0 with the hash of other bytes is no
// version any place could serve; it is refused before anything is
// fetched or stored, so no empty object is ever visible under the key.
func TestAnEmptyObjectRecordedWithAnotherHashIsRefusedBeforeStoring(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	ctx := context.Background()
	if err := h.db.AddRelists(ctx, []string{"B"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.RecordListed(ctx, "B", "laptop", []state.Listed{{Key: "index/1", Size: 0,
		SHA256: sum([]byte("not empty")), Class: "metadata"}}, 1, "", true); err != nil {
		t.Fatal(err)
	}
	if err := h.db.SettleFinished(ctx); err != nil {
		t.Fatal(err)
	}
	f := &fakeFetch{held: map[string][]byte{"B index/1": {}}}
	h.fetch = f
	h.serve(false)
	if _, err := getBody(t, h.client("laptop"), "index/1", ""); code(err) != "ServiceUnavailable" {
		t.Fatalf("got %v, want ServiceUnavailable", err)
	}
	if calls := f.fetched(); len(calls) != 0 {
		t.Fatalf("fetched %v", calls)
	}
	if _, ok := h.stored("index/1"); ok {
		t.Fatal("an empty object was stored")
	}
}

// shortPages is a store that truncates every page after one entry,
// whatever MaxKeys asks.
type shortPages struct{ *Store }

func (s shortPages) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	in2 := *in
	in2.MaxKeys = aws.Int32(1)
	return s.Store.ListObjectsV2(ctx, &in2)
}

// EP-10 (#73): when the store's page is truncated, the merged index keys
// are cut at its last name, so a store that ends a page before MaxKeys
// loses none of its keys to an index key further on.
func TestAShortStorePageLosesNoKeysToTheIndex(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {"b": []byte("b"), "d": []byte("d"), "f": []byte("f")}})
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, k := range []string{"a", "c", "e"} {
		n := int64(1)
		if _, err := be.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(k),
			ContentLength: &n, Body: strings.NewReader(k)}); err != nil {
			t.Fatal(err)
		}
	}
	set, _ := profile.Load("")
	d := &Decorator{Backend: shortPages{be}, Uploads: be, DB: h.db, Profiles: set, Log: quiet}
	in := &s3.ListObjectsV2Input{Bucket: aws.String("laptop"), MaxKeys: aws.Int32(10), StartAfter: aws.String("")}
	var keys []string
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("the list does not end")
		}
		out, err := d.ListObjectsV2(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		in.ContinuationToken = out.NextContinuationToken
	}
	if want := []string{"a", "b", "c", "d", "e", "f"}; !slices.Equal(keys, want) {
		t.Fatalf("listed %v, want %v", keys, want)
	}
}
