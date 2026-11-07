package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// fakeFetch serves each place's copy of each key, as the test chose
// them, and records every fetch.
type fakeFetch struct {
	mu    sync.Mutex
	held  map[string][]byte // place + " " + key → bytes
	calls []string          // place + " " + key
	// delay holds every fetch this long before its body.
	delay time.Duration
}

func (f *fakeFetch) Fetch(ctx context.Context, place, bucket, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.calls = append(f.calls, place+" "+key)
	b, ok := f.held[place+" "+key]
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	if !ok {
		return nil, fmt.Errorf("%s is offline", place)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (f *fakeFetch) fetched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// recovered records, as recovery does, that each place listed the keys
// given for it (key → bytes) in laptop.
func (h *harness) recovered(lists map[string]map[string][]byte) {
	h.t.Helper()
	ctx := context.Background()
	var places []string
	for p := range lists {
		places = append(places, p)
	}
	if err := h.db.AddRelists(ctx, places, []string{"laptop"}); err != nil {
		h.t.Fatal(err)
	}
	for p, objs := range lists {
		var ls []state.Listed
		for k, b := range objs {
			class := "data"
			if !strings.HasPrefix(k, "data/") {
				class = "metadata"
			}
			ls = append(ls, state.Listed{Key: k, Size: int64(len(b)), SHA256: sum(b), Class: class})
		}
		if _, err := h.db.RecordListed(ctx, p, "laptop", ls, len(ls), "", true); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.db.SettleFinished(ctx); err != nil {
		h.t.Fatal(err)
	}
}

// queueRows counts laptop's queue rows and queued placements: what a
// capture would add.
func (h *harness) queueRows() (rows, queued int) {
	h.t.Helper()
	if err := h.db.QueryRow(`SELECT count(*) FROM queue`).Scan(&rows); err != nil {
		h.t.Fatal(err)
	}
	if err := h.db.QueryRow(`SELECT count(*) FROM placement WHERE state = 'queued'`).Scan(&queued); err != nil {
		h.t.Fatal(err)
	}
	return rows, queued
}

func (h *harness) stored(key string) ([]byte, bool) {
	b, err := os.ReadFile(filepath.Join(h.store, "laptop", filepath.FromSlash(key)))
	return b, err == nil
}

func getBody(t *testing.T, c *s3.Client, key string, rng string) ([]byte, error) {
	t.Helper()
	in := &s3.GetObjectInput{Bucket: aws.String("laptop"), Key: aws.String(key)}
	if rng != "" {
		in.Range = aws.String(rng)
	}
	out, err := c.GetObject(context.Background(), in)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// syncBuffer is a log destination tests read while the gateway writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// EP-10, ID-6 (#30): a GET of a recovered key the store lacks fetches it
// from the friend most recently reachable that holds the version that
// counts; a friend serving other bytes (hostile or rotten) is not
// believed: its bytes are not stored, it is logged as its fault, and the
// next friend is used. The object is then local, served from the store
// on the next read, and neither captured nor replicated again.
func TestMissFetchesFromAFriendAndRejectsWrongBytes(t *testing.T) {
	pack := bytes.Repeat([]byte("restic pack "), 50_000)
	forged := bytes.Repeat([]byte("forged pack "), 50_000)
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{
		"B": {"data/aa/1": pack},
		"C": {"data/aa/1": pack},
	})
	ctx := context.Background()
	now := time.Now()
	h.db.SetPlaceCondition(ctx, "B", state.ConditionOK, "", now)
	h.db.SetPlaceCondition(ctx, "C", state.ConditionOK, "", now.Add(-time.Hour))
	// B, reached last, serves forged bytes of the right size.
	f := &fakeFetch{held: map[string][]byte{"B data/aa/1": forged, "C data/aa/1": pack}}
	logs := &syncBuffer{}
	h.fetch, h.log = f, slog.New(slog.NewTextHandler(logs, nil))
	rows, _ := h.queueRows()
	h.serve(false)
	c := h.client("laptop")

	got, err := getBody(t, c, "data/aa/1", "")
	if err != nil || !bytes.Equal(got, pack) {
		t.Fatalf("read-through: %d bytes, %v", len(got), err)
	}
	if calls := f.fetched(); !slices.Equal(calls, []string{"B data/aa/1", "C data/aa/1"}) {
		t.Fatalf("fetched %v, want B (most recently reachable) then C", calls)
	}
	if !strings.Contains(logs.String(), "place=B") || !strings.Contains(logs.String(), "not the version it acknowledged") {
		t.Fatalf("B's wrong bytes not logged as its fault:\n%s", logs.String())
	}
	if b, ok := h.stored("data/aa/1"); !ok || !bytes.Equal(b, pack) {
		t.Fatalf("the store holds %d bytes (%v), want the verified pack", len(b), ok)
	}
	// Served from the store now; nothing was captured or queued.
	if got, err := getBody(t, c, "data/aa/1", ""); err != nil || !bytes.Equal(got, pack) {
		t.Fatalf("second read: %v", err)
	}
	if n := len(f.fetched()); n != 2 {
		t.Fatalf("the second read fetched again: %v", f.fetched())
	}
	if r, q := h.queueRows(); r != rows || q != 0 {
		t.Fatalf("the fetch was captured: %d queue rows (had %d), %d queued placements", r, rows, q)
	}
}

// EP-10 (#30): when every friend holding a key serves wrong bytes or is
// offline, the read fails with a retryable error and nothing is stored;
// the friend that failed last time is tried after the others next time.
func TestNoFriendServingTheVersionStoresNothing(t *testing.T) {
	pack := []byte("the real pack")
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{
		"B": {"data/aa/1": pack, "data/aa/2": pack},
		"C": {"data/aa/1": pack, "data/aa/2": pack},
	})
	// B is offline; C serves a truncated copy of 1, which it holds.
	f := &fakeFetch{held: map[string][]byte{"C data/aa/1": pack[:4], "C data/aa/2": pack}}
	h.fetch = f
	h.serve(false)
	c := h.client("laptop")
	_, err := getBody(t, c, "data/aa/1", "")
	if code(err) != "ServiceUnavailable" {
		t.Fatalf("no friend could serve it: %v", err)
	}
	if _, ok := h.stored("data/aa/1"); ok {
		t.Fatal("unverified bytes were stored")
	}
	if calls := f.fetched(); !slices.Equal(calls, []string{"B data/aa/1", "C data/aa/1"}) {
		t.Fatalf("fetched %v", calls)
	}
	// C serves another key, so it is reachable again and goes first;
	// offline B is tried after it.
	if got, err := getBody(t, c, "data/aa/2", ""); err != nil || !bytes.Equal(got, pack) {
		t.Fatalf("data/aa/2: %v", err)
	}
	if _, err := getBody(t, c, "data/aa/1", ""); code(err) != "ServiceUnavailable" {
		t.Fatalf("data/aa/1 again: %v", err)
	}
	calls := f.fetched()
	if !slices.Equal(calls[len(calls)-2:], []string{"C data/aa/1", "B data/aa/1"}) {
		t.Fatalf("after C served, it is tried first: %v", calls)
	}
}

// EP-10 (#30): a HEAD and a ranged GET of a key the store lacks fetch the
// whole object, verify and store it, and then answer from the store.
func TestHeadAndRangedReadFetchTheWholeObject(t *testing.T) {
	pack := make([]byte, 3<<20)
	for i := range pack {
		pack[i] = byte(i * 7)
	}
	idx := []byte("an index")
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {"data/aa/1": pack, "index/1": idx}})
	h.fetch = &fakeFetch{held: map[string][]byte{"B data/aa/1": pack, "B index/1": idx}}
	h.serve(false)
	c := h.client("laptop")
	got, err := getBody(t, c, "data/aa/1", "bytes=1048576-1048675")
	if err != nil || !bytes.Equal(got, pack[1<<20:1<<20+100]) {
		t.Fatalf("ranged read: %d bytes %v", len(got), err)
	}
	if b, ok := h.stored("data/aa/1"); !ok || !bytes.Equal(b, pack) {
		t.Fatalf("a ranged read stored %d bytes", len(b))
	}
	out, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("laptop"), Key: aws.String("index/1")})
	if err != nil || aws.ToInt64(out.ContentLength) != int64(len(idx)) {
		t.Fatalf("head: %+v %v", out, err)
	}
	if b, ok := h.stored("index/1"); !ok || !bytes.Equal(b, idx) {
		t.Fatal("a head did not store the object")
	}
}

// ID-5, ID-6 (#30): a key whose friends hold different versions is
// refused with what to run; a key nobody knows is not found; a key
// deleted since recovery is not found and not fetched.
func TestConflictRefusedAndUnknownNotFound(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{
		"B": {"keys/k": []byte("key one"), "data/aa/1": []byte("p")},
		"C": {"keys/k": []byte("key two")},
	})
	f := &fakeFetch{held: map[string][]byte{"B keys/k": []byte("key one"), "C keys/k": []byte("key two"),
		"B data/aa/1": []byte("p")}}
	h.fetch = f
	h.serve(false)
	c := h.client("laptop")
	_, err := getBody(t, c, "keys/k", "")
	if code(err) != "KeyConflict" || !strings.Contains(err.Error(), "lendisk bucket resolve laptop keys/k") {
		t.Fatalf("conflict: %v", err)
	}
	_, err = c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("laptop"), Key: aws.String("keys/k")})
	var re interface{ HTTPStatusCode() int }
	if !errors.As(err, &re) || re.HTTPStatusCode() != 409 {
		t.Fatalf("head of a conflict: %v", err)
	}
	if _, err := getBody(t, c, "data/zz/none", ""); code(err) != "NoSuchKey" {
		t.Fatalf("unknown key: %v", err)
	}
	if _, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/aa/1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := getBody(t, c, "data/aa/1", ""); code(err) != "NoSuchKey" {
		t.Fatalf("deleted key: %v", err)
	}
	if calls := f.fetched(); len(calls) != 0 {
		t.Fatalf("fetched %v", calls)
	}
}

// EP-10 (#30): concurrent reads of one missing key fetch it once.
func TestConcurrentMissesFetchOnce(t *testing.T) {
	pack := bytes.Repeat([]byte("p"), 1<<20)
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {"data/aa/1": pack}})
	f := &fakeFetch{held: map[string][]byte{"B data/aa/1": pack}, delay: 200 * time.Millisecond}
	h.fetch = f
	h.serve(false)
	c := h.client("laptop")
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := getBody(t, c, "data/aa/1", "")
			if err == nil && !bytes.Equal(got, pack) {
				err = errors.New("wrong bytes")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls := f.fetched(); len(calls) != 1 {
		t.Fatalf("eight concurrent misses fetched %d times", len(calls))
	}
}

// ID-6 (#30): listing a recovered bucket shows the keys its friends hold
// before any is read, merged with what the store holds, in key order,
// by prefix, a page at a time, with common prefixes under a delimiter.
func TestListShowsRecoveredKeysBeforeTheyAreRead(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.recovered(map[string]map[string][]byte{"B": {
		"config": []byte("c"), "data/aa/1": []byte("p1"), "data/bb/2": []byte("p2"),
		"snapshots/s1": []byte("s1"), "snapshots/s3": []byte("s3"),
	}})
	h.fetch = &fakeFetch{}
	h.serve(false)
	c := h.client("laptop")
	ctx := context.Background()
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String("snapshots/s2"),
		Body: strings.NewReader("s2")}); err != nil {
		t.Fatal(err)
	}
	list := func(prefix, delim string, max int32) ([]string, []string) {
		t.Helper()
		var keys, prefixes []string
		in := &s3.ListObjectsV2Input{Bucket: aws.String("laptop"), Prefix: aws.String(prefix), MaxKeys: aws.Int32(max)}
		if delim != "" {
			in.Delimiter = aws.String(delim)
		}
		for pages := 0; ; pages++ {
			if pages > 20 {
				t.Fatal("the list does not end")
			}
			out, err := c.ListObjectsV2(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range out.Contents {
				keys = append(keys, aws.ToString(o.Key))
			}
			for _, p := range out.CommonPrefixes {
				prefixes = append(prefixes, aws.ToString(p.Prefix))
			}
			if !aws.ToBool(out.IsTruncated) {
				return keys, prefixes
			}
			in.ContinuationToken = out.NextContinuationToken
		}
	}
	all := []string{"config", "data/aa/1", "data/bb/2", "snapshots/s1", "snapshots/s2", "snapshots/s3"}
	if keys, _ := list("", "", 1000); !slices.Equal(keys, all) {
		t.Fatalf("list %v", keys)
	}
	if keys, _ := list("", "", 1); !slices.Equal(keys, all) {
		t.Fatalf("one key a page %v", keys)
	}
	if keys, _ := list("snapshots/", "", 2); !slices.Equal(keys, all[3:]) {
		t.Fatalf("snapshots %v", keys)
	}
	if keys, prefixes := list("", "/", 1); !slices.Equal(keys, []string{"config"}) ||
		!slices.Equal(prefixes, []string{"data/", "snapshots/"}) {
		t.Fatalf("with a delimiter %v %v", keys, prefixes)
	}
	if keys, prefixes := list("data/", "/", 1000); len(keys) != 0 || !slices.Equal(prefixes, []string{"data/aa/", "data/bb/"}) {
		t.Fatalf("data/ with a delimiter %v %v", keys, prefixes)
	}
}
