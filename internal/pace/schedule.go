package pace

import (
	"fmt"
	"strings"
	"time"
)

// Window is one time-of-day window, local time: open from Start up to
// End, as offsets from midnight. End before Start wraps past midnight.
type Window struct{ Start, End time.Duration }

// Schedule is when replication sends (REPL-7): open inside any of its
// windows; empty is always open.
type Schedule []Window

// ParseSchedule reads comma-separated windows such as "01:00-07:00" or
// "23:00-02:00, 12:00-13:30"; "" is always open.
func ParseSchedule(s string) (Schedule, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out Schedule
	for _, part := range strings.Split(s, ",") {
		a, b, ok := strings.Cut(strings.TrimSpace(part), "-")
		if !ok {
			return nil, fmt.Errorf("%q: want windows such as 01:00-07:00, separated by commas", s)
		}
		start, err := parseTOD(a)
		if err != nil {
			return nil, err
		}
		end, err := parseTOD(b)
		if err != nil {
			return nil, err
		}
		if start == end {
			return nil, fmt.Errorf("%q: the window %s is empty; leave the schedule empty to replicate at any time", s, part)
		}
		out = append(out, Window{start, end})
	}
	return out, nil
}

func parseTOD(s string) (time.Duration, error) {
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("%q: want a time of day such as 01:00", s)
	}
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute, nil
}

func sinceMidnight(t time.Time) time.Duration {
	y, m, d := t.Date()
	return t.Sub(time.Date(y, m, d, 0, 0, 0, 0, t.Location()))
}

// Open reports whether t falls inside a window.
func (s Schedule) Open(t time.Time) bool {
	if len(s) == 0 {
		return true
	}
	tod := sinceMidnight(t)
	for _, w := range s {
		if w.Start < w.End && tod >= w.Start && tod < w.End ||
			w.Start > w.End && (tod >= w.Start || tod < w.End) {
			return true
		}
	}
	return false
}

// Next returns when the schedule next opens after t (t itself if open).
func (s Schedule) Next(t time.Time) time.Time {
	if s.Open(t) {
		return t
	}
	y, m, d := t.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, t.Location())
	var best time.Time
	for _, w := range s {
		at := midnight.Add(w.Start)
		if !at.After(t) {
			at = time.Date(y, m, d+1, 0, 0, 0, 0, t.Location()).Add(w.Start)
		}
		if best.IsZero() || at.Before(best) {
			best = at
		}
	}
	return best
}

// String is the schedule as ParseSchedule reads it.
func (s Schedule) String() string {
	var parts []string
	for _, w := range s {
		parts = append(parts, fmt.Sprintf("%02d:%02d-%02d:%02d", int(w.Start.Hours()), int(w.Start.Minutes())%60,
			int(w.End.Hours()), int(w.End.Minutes())%60))
	}
	return strings.Join(parts, ",")
}
