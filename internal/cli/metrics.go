package cli

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/metrics"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/version"
)

// placeStateNames are the values of lendisk_place_state's state label:
// StatusPlace.State, "none" before the place's first answer.
var placeStateNames = []string{state.ConditionOK, state.ConditionRetrying, state.ConditionQuota,
	state.ConditionRemoved, state.ConditionStore, "none"}

// bitrotStates are StatusRot.State's values.
var bitrotStates = []string{"kept", "refetching", "repaired", "lent"}

// metricsFamilies is what a scrape reports (OPS-4, DESIGN §3.8): the
// replication worker's counters and, read from lendisk.db at the
// scrape, the numbers `status` shows, by the same functions. Labels are
// places, friends, buckets, alert kinds and fixed states only.
func (a *app) metricsFamilies(ctx context.Context, db *state.DB, profiles profile.Set, copies int,
	counters *metrics.Counters) ([]metrics.Family, error) {
	now := a.now()
	gauge := func(name, help string) metrics.Family {
		return metrics.Family{Name: name, Help: help, Type: metrics.Gauge}
	}
	add := func(f *metrics.Family, v float64, labels ...string) {
		f.Samples = append(f.Samples, metrics.Sample{Labels: metrics.L(labels...), Value: v})
	}
	info := gauge("lendisk_info", "Always 1; the version label is this binary's.")
	add(&info, 1, "version", version.Version)
	out := []metrics.Family{info}
	out = append(out, counters.Families()...)

	// Places (REPL-1, REPL-4, REPL-11), as status's Places.
	ps, err := places(ctx, db, now)
	if err != nil {
		return nil, err
	}
	pq := gauge("lendisk_place_queue_objects", "Objects waiting to be sent to the place.")
	pqb := gauge("lendisk_place_queue_bytes", "Bytes waiting to be sent to the place.")
	lag := gauge("lendisk_place_lag_seconds", "Age of the oldest object waiting to be sent to the place (REPL-11).")
	ref := gauge("lendisk_place_refused_objects", "Objects the place refused for good and not written again since.")
	drop := gauge("lendisk_place_dropping_copies", "Copies re-placement is deleting at the place (REPL-13).")
	pst := gauge("lendisk_place_state", "1 for the place's condition as it answered last (ok, retrying, quota, removed, store, none), 0 for the others.")
	lastOK := gauge("lendisk_place_last_ok_timestamp_seconds", "When the place last acknowledged anything or answered a probe (Unix time).")
	for _, p := range ps {
		add(&pq, float64(p.Queue.Objects), "place", p.Place)
		add(&pqb, float64(p.Queue.Bytes), "place", p.Place)
		add(&lag, float64(p.LagSeconds), "place", p.Place)
		add(&ref, float64(p.Refused), "place", p.Place)
		add(&drop, float64(p.Dropping), "place", p.Place)
		cur := p.State
		if cur == "" {
			cur = "none"
		}
		for _, s := range placeStateNames {
			v := 0.0
			if s == cur {
				v = 1
			}
			add(&pst, v, "place", p.Place, "state", s)
		}
		if t, ok := parseTime(p.LastOK); ok {
			add(&lastOK, unixTime(t), "place", p.Place)
		}
	}
	out = append(out, pq, pqb, lag, ref, drop, pst, lastOK)

	// Quotas and capacity (LEND-7, LEND-8, P-8), as status's capacity.
	c, err := a.capacityStatus(ctx, db, copies, now)
	if err != nil {
		return nil, err
	}
	quota := gauge("lendisk_friend_quota_bytes", "The quota in force with the friend, each way (LEND-8).")
	there := gauge("lendisk_friend_used_there_bytes", "What this node stores at the friend, of the quota.")
	here := gauge("lendisk_friend_used_here_bytes", "What the friend stores at this node, of the quota.")
	for _, f := range c.Friends {
		add(&quota, float64(f.Quota), "friend", f.Peer)
		add(&there, float64(f.UseThere), "friend", f.Peer)
		add(&here, float64(f.UseHere), "friend", f.Peer)
	}
	repo := gauge("lendisk_repository_bytes", "The repository as friends count it (LEND-7).")
	add(&repo, float64(c.RepositorySize))
	prot := gauge("lendisk_protectable_bytes", "The protectable size: the most the pool's quotas can hold at copies (LEND-7).")
	add(&prot, float64(c.Protectable))
	out = append(out, quota, there, here, repo, prot)

	// Pending alerts by kind (OPS-2); every kind, 0 when none.
	as, err := db.ActiveAlerts(ctx)
	if err != nil {
		return nil, err
	}
	count := map[string]int{}
	for _, al := range as {
		count[al.Kind]++
	}
	alerts := gauge("lendisk_alerts", "Pending alerts by kind (OPS-2).")
	for _, k := range alert.Kinds() {
		add(&alerts, float64(count[k]), "kind", k)
		delete(count, k)
	}
	for _, k := range slices.Sorted(maps.Keys(count)) { // a kind a later version raised
		add(&alerts, float64(count[k]), "kind", k)
	}
	out = append(out, alerts)

	// Buckets (EP-2, REPL-11, OPS-1), as status's Buckets.
	bs, err := db.Buckets(ctx)
	if err != nil {
		return nil, err
	}
	queues, err := db.QueueStats(ctx)
	if err != nil {
		return nil, err
	}
	refused, err := db.RefusedObjects(ctx)
	if err != nil {
		return nil, err
	}
	below, err := db.BelowCopies(ctx)
	if err != nil {
		return nil, err
	}
	bc := gauge("lendisk_bucket_copies", "The bucket's copies: how many friends hold each object (EP-6).")
	bq := gauge("lendisk_bucket_captured_objects", "Writes and deletes the bucket captured (EP-2).")
	bqb := gauge("lendisk_bucket_captured_bytes", "Bytes of the writes the bucket captured (EP-2).")
	bref := gauge("lendisk_bucket_refused_objects", "The bucket's objects not replicated: the store's copy changed or cannot be sent.")
	bbo := gauge("lendisk_bucket_below_copies_objects", "Live objects held at fewer than copies places.")
	bbb := gauge("lendisk_bucket_below_copies_bytes", "Bytes of live objects held at fewer than copies places.")
	oc := gauge("lendisk_bucket_offsite_complete", "1 when everything the bucket captured is held at copies places (REPL-11).")
	behind := gauge("lendisk_bucket_offsite_behind_seconds", "Age of the first write not yet offsite, 0 when everything is (the offsite watermark's lag).")
	through := gauge("lendisk_bucket_offsite_through_timestamp_seconds", "Capture time of the last write up to which everything is offsite (Unix time); absent before one is.")
	for _, b := range bs {
		q, bl := queues[b.Name], below[b.Name]
		add(&bc, float64(b.Copies), "bucket", b.Name)
		add(&bq, float64(q.Entries), "bucket", b.Name)
		add(&bqb, float64(q.Bytes), "bucket", b.Name)
		add(&bref, float64(refused[b.Name]), "bucket", b.Name)
		add(&bbo, float64(bl.Objects), "bucket", b.Name)
		add(&bbb, float64(bl.Bytes), "bucket", b.Name)
		off, err := offsiteStatus(ctx, db, b, profiles.Get(b.Profile))
		if err != nil {
			return nil, err
		}
		complete, age := 0.0, 0.0
		if off.Complete {
			complete = 1
		} else if t, ok := parseTime(off.Before); ok {
			age = max(now.Sub(t).Seconds(), 0)
		}
		add(&oc, complete, "bucket", b.Name)
		add(&behind, age, "bucket", b.Name)
		if t, ok := parseTime(off.Through); ok {
			add(&through, unixTime(t), "bucket", b.Name)
		}
	}
	out = append(out, bc, bq, bqb, bref, bbo, bbb, oc, behind, through)

	// The bit-rot sample (VER-5), as status's bitrot.
	br, err := bitrotStatus(ctx, db, now)
	if err != nil {
		return nil, err
	}
	finds := gauge("lendisk_bitrot_findings", "Objects found rotten that status shows, by state: kept (no friend holds it), refetching, repaired (shown 7 days), lent (a friend's object here).")
	cycles := metrics.Family{Name: "lendisk_bitrot_cycles_total", Type: metrics.Counter,
		Help: "Cycles of the bit-rot sample finished over the whole store."}
	checked := gauge("lendisk_bitrot_last_checked_timestamp_seconds", "When the bit-rot sample last checked an object (Unix time); absent before one.")
	n := map[string]int{}
	if br != nil {
		for _, r := range br.Rotten {
			n[r.State]++
		}
		add(&cycles, float64(br.Cycles))
		if t, ok := parseTime(br.LastChecked); ok {
			add(&checked, unixTime(t))
		}
	} else {
		add(&cycles, 0)
	}
	for _, s := range bitrotStates {
		add(&finds, float64(n[s]), "state", s)
	}
	out = append(out, finds, cycles, checked)
	return out, nil
}

// parseTime reads status's RFC 3339 times; "" is none.
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

func unixTime(t time.Time) float64 { return float64(t.Unix()) }
