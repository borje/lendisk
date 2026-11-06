package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/time/rate"
)

// ---- Q-Y.4: limiter and timetable ----

const burst = 64 << 10

type partTiming struct {
	n          int
	start, end time.Time
	size       int64
}

// timedUpload runs Upload in-process and records each part's timing.
func timedUpload(t *testing.T, tg Target, key, src, sum string, part int64, l *Limits, w Window) ([]partTiming, error) {
	t.Helper()
	core, err := newCore(tg)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var ps []partTiming
	_, err = Upload(context.Background(), core, tg, Job{
		Source: src, Key: key, PartSize: part, SHA256Hex: sum,
		StatePath: filepath.Join(t.TempDir(), "state.json"),
	}, l, w, Events{
		PartStart: func(n int, off, size int64) {
			mu.Lock()
			ps = append(ps, partTiming{n: n, start: time.Now(), size: size})
			mu.Unlock()
		},
		PartDone: func(n int, etag string) {
			mu.Lock()
			ps[len(ps)-1].end = time.Now()
			mu.Unlock()
		},
	})
	return ps, err
}

func mibps(bytes int64, d time.Duration) float64 { return float64(bytes) / MiB / d.Seconds() }

// TestLimiterHoldsRate: 4 parts of 5 MiB at 4 MiB/s per place, upload
// and download. Each part must take size/rate within a few percent, and
// the whole transfer must not run ahead of the bucket by more than a
// burst.
func TestLimiterHoldsRate(t *testing.T) {
	eachTarget(t, func(t *testing.T, tg Target) {
		const part = 5 * MiB
		const lim = 4 * MiB
		src, sum := randomFile(t, 4*part)
		l := &Limits{Peer: rate.NewLimiter(lim, burst)}
		t0 := time.Now()
		ps, err := timedUpload(t, tg, "limit/obj", src, sum, part, l, Window{})
		if err != nil {
			t.Fatal(err)
		}
		total := time.Since(t0)
		for _, p := range ps {
			d := p.end.Sub(p.start)
			r := mibps(p.size, d)
			t.Logf("RESULT %s upload part %d: %d bytes in %v = %.3f MiB/s (limit 4)", tg.Name, p.n, p.size, d.Round(time.Millisecond), r)
			if math.Abs(r-4)/4 > 0.05 {
				t.Errorf("part %d at %.3f MiB/s, more than 5%% off 4", p.n, r)
			}
		}
		ideal := time.Duration(float64(4*part) / lim * float64(time.Second))
		t.Logf("RESULT %s upload total: 20 MiB in %v (ideal %v, +%v incl. create/complete)", tg.Name, total.Round(time.Millisecond), ideal, (total - ideal).Round(time.Millisecond))

		// Download through the same wrapper (read-through, VER-2 samples).
		core, _ := newCore(tg)
		dl := &Limits{Peer: rate.NewLimiter(lim, burst)}
		o, err := core.Client.GetObject(context.Background(), tg.Bucket, "limit/obj", minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t1 := time.Now()
		n, err := io.Copy(io.Discard, dl.Reader(context.Background(), o))
		o.Close()
		if err != nil {
			t.Fatal(err)
		}
		d := time.Since(t1)
		t.Logf("RESULT %s download: %d bytes in %v = %.3f MiB/s (limit 4)", tg.Name, n, d.Round(time.Millisecond), mibps(n, d))
		if r := mibps(n, d); math.Abs(r-4)/4 > 0.05 {
			t.Errorf("download at %.3f MiB/s", r)
		}
	})
}

// sampler records Limits.Bytes every 50 ms.
type sample struct {
	at    time.Time
	bytes int64
}

func sampleBytes(ctx context.Context, ls ...*Limits) <-chan []sample {
	out := make(chan []sample, 1)
	go func() {
		var s []sample
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		for {
			var b int64
			for _, l := range ls {
				b += l.Bytes.Load()
			}
			s = append(s, sample{time.Now(), b})
			select {
			case <-ctx.Done():
				out <- s
				return
			case <-tk.C:
			}
		}
	}()
	return out
}

// rateBetween is the mean rate over [from, to) from samples.
func rateBetween(s []sample, from, to time.Time) float64 {
	var a, b *sample
	for i := range s {
		if a == nil && !s[i].at.Before(from) {
			a = &s[i]
		}
		if !s[i].at.After(to) {
			b = &s[i]
		}
	}
	if a == nil || b == nil || !b.at.After(a.at) {
		return 0
	}
	return mibps(b.bytes-a.bytes, b.at.Sub(a.at))
}

// TestLimitChangesAtRuntime: 2 MiB/s, raised to 8 MiB/s 3 s into the
// transfer (mid-part); the new rate applies from the next read.
func TestLimitChangesAtRuntime(t *testing.T) {
	tg := targets[0]
	src, sum := randomFile(t, 30*MiB)
	l := &Limits{Peer: rate.NewLimiter(2*MiB, burst)}
	ctx, cancel := context.WithCancel(context.Background())
	samples := sampleBytes(ctx, l)
	t0 := time.Now()
	var tChange time.Time
	time.AfterFunc(3*time.Second, func() { tChange = time.Now(); l.Peer.SetLimit(8 * MiB) })
	ps, err := timedUpload(t, tg, "limit/runtime", src, sum, 5*MiB, l, Window{})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	s := <-samples
	before := rateBetween(s, t0.Add(500*time.Millisecond), tChange)
	after := rateBetween(s, tChange.Add(100*time.Millisecond), ps[len(ps)-1].start)
	firstAfter := rateBetween(s, tChange, tChange.Add(250*time.Millisecond))
	t.Logf("RESULT runtime change: %.3f MiB/s before (limit 2), %.3f MiB/s after (limit 8), %.3f MiB/s in the first 250 ms after SetLimit; change landed in part %d", before, after, firstAfter, partAt(ps, tChange))
	if math.Abs(before-2)/2 > 0.05 || math.Abs(after-8)/8 > 0.05 {
		t.Errorf("rates %.3f / %.3f", before, after)
	}
}

func partAt(ps []partTiming, at time.Time) int {
	for _, p := range ps {
		if !at.Before(p.start) && at.Before(p.end) {
			return p.n
		}
	}
	return 0
}

// TestGlobalLimitAcrossPlaces: two places in parallel, each with a 4 MiB/s
// peer bucket, sharing a 6 MiB/s global bucket.
func TestGlobalLimitAcrossPlaces(t *testing.T) {
	global := rate.NewLimiter(6*MiB, burst)
	src, sum := randomFile(t, 20*MiB)
	places := []Target{targets[0], targets[len(targets)-1]}
	ls := []*Limits{
		{Peer: rate.NewLimiter(4*MiB, burst), Global: global},
		{Peer: rate.NewLimiter(4*MiB, burst), Global: global},
	}
	ctx, cancel := context.WithCancel(context.Background())
	samples := sampleBytes(ctx, ls...)
	var wg sync.WaitGroup
	durs := make([]time.Duration, 2)
	t0 := time.Now()
	for i := range places {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := timedUpload(t, places[i], fmt.Sprintf("limit/global-%d", i), src, sum, 5*MiB, ls[i], Window{})
			if err != nil {
				t.Error(err)
			}
			durs[i] = time.Since(t0)
		}(i)
	}
	wg.Wait()
	cancel()
	s := <-samples
	both := min(durs[0], durs[1])
	r := rateBetween(s, t0.Add(500*time.Millisecond), t0.Add(both))
	t.Logf("RESULT global 6 MiB/s over %s and %s (4 MiB/s each): combined %.3f MiB/s while both ran; each 20 MiB took %v and %v",
		places[0].Name, places[1].Name, r, durs[0].Round(time.Millisecond), durs[1].Round(time.Millisecond))
	if math.Abs(r-6)/6 > 0.05 {
		t.Errorf("combined %.3f MiB/s", r)
	}
}

// TestWindowStopsWithinOnePart: the window closes at various points in
// part 2 (5 MiB at 4 MiB/s = 1.25 s per part); the transfer must stop
// at the end of that part, then resume at the next part when it opens.
func TestWindowStopsWithinOnePart(t *testing.T) {
	eachTarget(t, func(t *testing.T, tg Target) {
		const part = 5 * MiB
		for _, frac := range []float64{0.1, 0.5, 0.9} {
			src, sum := randomFile(t, 4*part)
			key := fmt.Sprintf("window/%v", frac)
			state := filepath.Join(t.TempDir(), "state.json")
			l := &Limits{Peer: rate.NewLimiter(4*MiB, burst)}
			core, _ := newCore(tg)
			job := Job{Source: src, Key: key, PartSize: part, SHA256Hex: sum, StatePath: state}

			t0 := time.Now()
			// Part 2 runs from about 1.25 s to 2.5 s.
			closeAt := t0.Add(time.Duration((1.25 + frac*1.25) * float64(time.Second)))
			tod := func(x time.Time) time.Duration {
				y, m, d := x.Date()
				return x.Sub(time.Date(y, m, d, 0, 0, 0, 0, x.Location()))
			}
			w := Window{Start: tod(t0.Add(-time.Hour)), End: tod(closeAt)}
			var atClose int64
			time.AfterFunc(time.Until(closeAt), func() { atClose = l.Bytes.Load() })
			var started []int
			_, err := Upload(context.Background(), core, tg, job, l, w, Events{
				PartStart: func(n int, _, _ int64) { started = append(started, n) },
			})
			stopped := time.Now()
			if !errors.Is(err, ErrWindowClosed) {
				t.Fatalf("got %v, want ErrWindowClosed", err)
			}
			over := l.Bytes.Load() - atClose
			late := stopped.Sub(closeAt)
			t.Logf("RESULT %s window closed %.0f%% into part 2: stopped %v later, %d bytes (%.2f parts) sent after close, parts started %v",
				tg.Name, frac*100, late.Round(time.Millisecond), over, float64(over)/part, started)
			if over > part || late > 1250*time.Millisecond+250*time.Millisecond {
				t.Errorf("overshoot %d bytes / %v exceeds one part", over, late)
			}

			// The window opens again: resume at part 3.
			started = nil
			_, err = Upload(context.Background(), core, tg, job, l, Window{}, Events{
				PartStart: func(n int, _, _ int64) { started = append(started, n) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(started) != "[3 4]" {
				t.Errorf("after reopening sent parts %v, want [3 4]", started)
			}
			if got := objectSHA(t, tg, key); got != sum {
				t.Errorf("object sha256 mismatch")
			}
		}
	})
}

func TestWindowOpen(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 11, 6, h, m, 0, 0, time.Local) }
	for _, c := range []struct {
		win  string
		t    time.Time
		open bool
	}{
		{"01:00-07:00", at(0, 59), false},
		{"01:00-07:00", at(1, 0), true},
		{"01:00-07:00", at(6, 59), true},
		{"01:00-07:00", at(7, 0), false},
		{"23:00-07:00", at(23, 30), true},
		{"23:00-07:00", at(3, 0), true},
		{"23:00-07:00", at(12, 0), false},
		{"", at(12, 0), true},
	} {
		w, err := ParseWindow(c.win)
		if err != nil {
			t.Fatal(err)
		}
		if w.Open(c.t) != c.open {
			t.Errorf("%q at %s: open=%v", c.win, c.t.Format("15:04"), !c.open)
		}
	}
}
