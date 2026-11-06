package main

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Limits are the token buckets one transfer waits on (REPL-7): the
// place's own and the global one. Either may be nil. Both can be
// changed while a transfer runs (SetLimit).
type Limits struct {
	Peer, Global *rate.Limiter
	Now          func() time.Time
	// Bytes counts what passed through, for the measurements.
	Bytes atomic.Int64
}

func (l *Limits) now() time.Time {
	if l == nil || l.Now == nil {
		return time.Now()
	}
	return l.Now()
}

// Reader wraps one part's (or one download's) reader. Each Read is cut to
// the smallest burst so WaitN never asks for more than a bucket holds.
func (l *Limits) Reader(ctx context.Context, r io.Reader) io.Reader {
	if l == nil {
		return r
	}
	return &limitedReader{ctx: ctx, r: r, l: l}
}

type limitedReader struct {
	ctx context.Context
	r   io.Reader
	l   *Limits
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	max := len(p)
	for _, lim := range []*rate.Limiter{lr.l.Peer, lr.l.Global} {
		if lim != nil && lim.Limit() != rate.Inf && lim.Burst() < max {
			max = lim.Burst()
		}
	}
	n, err := lr.r.Read(p[:max])
	if n > 0 {
		for _, lim := range []*rate.Limiter{lr.l.Peer, lr.l.Global} {
			if lim == nil {
				continue
			}
			if werr := lim.WaitN(lr.ctx, n); werr != nil {
				return 0, werr
			}
		}
		lr.l.Bytes.Add(int64(n))
	}
	return n, err
}

// Window is the time-of-day timetable: open from Start to End (local
// time, offset from midnight); End before Start wraps past midnight
// (23:00-07:00). Zero Start and End means always open.
type Window struct {
	Start, End time.Duration
}

func (w Window) Open(t time.Time) bool {
	if w.Start == w.End {
		return true
	}
	y, m, d := t.Date()
	tod := t.Sub(time.Date(y, m, d, 0, 0, 0, 0, t.Location()))
	if w.Start < w.End {
		return tod >= w.Start && tod < w.End
	}
	return tod >= w.Start || tod < w.End
}

// ParseWindow reads "HH:MM-HH:MM" or "HH:MM:SS-HH:MM:SS"; "" is always open.
func ParseWindow(s string) (Window, error) {
	if s == "" {
		return Window{}, nil
	}
	var a, b string
	for i := range s {
		if s[i] == '-' {
			a, b = s[:i], s[i+1:]
		}
	}
	pa, err := parseTOD(a)
	if err != nil {
		return Window{}, err
	}
	pb, err := parseTOD(b)
	if err != nil {
		return Window{}, err
	}
	return Window{pa, pb}, nil
}

func parseTOD(s string) (time.Duration, error) {
	for _, layout := range []string{"15:04:05", "15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second, nil
		}
	}
	return 0, fmt.Errorf("bad time of day %q", s)
}
