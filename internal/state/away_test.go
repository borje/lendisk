package state

import (
	"context"
	"testing"
	"time"
)

// answer records place's answer at the pool's clock, as the worker does.
func (p *pool) answer(place, cond string) {
	p.t.Helper()
	if err := p.db.SetPlaceCondition(context.Background(), place, cond, "x", p.now); err != nil {
		p.t.Fatal(err)
	}
}

// §3.4 (#65, from the #21 review): a friend is away (unreachable or full
// for reroute_after) once it has not answered ok for that long, whatever
// mix of failures it gave meanwhile: one alternating between network
// errors and 507s no longer holds the delete and metadata barriers, nor
// its unsent data, for ever. Its first ok ends the run, and a failure
// after it counts from then.
func TestAwaySurvivesAlternatingNetworkErrorsAnd507s(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.friend("C", 10)
	p.friend("D", 10)
	p.bucket("laptop", 1)
	d := p.put("laptop", "data/1", "data", 1)
	for _, f := range p.places(d) {
		p.ack(f, d)
	}
	m := p.put("laptop", "index/1", "metadata", 0.1) // B, C and D
	p.ack("C", m)
	p.ack("D", m)
	del := p.del("laptop", "data/1", "data")
	start := p.now
	away := func() bool { return !p.held(del) }
	// B alternates every 20 minutes: a network error, a 507, a
	// network error, a 507, …
	conds := []string{ConditionRetrying, ConditionQuota}
	for i := 0; p.now.Sub(start) < time.Hour; i++ {
		p.answer("B", conds[i%2])
		if away() {
			t.Fatalf("away after %v of failures", p.now.Sub(start))
		}
		p.now = p.now.Add(20 * time.Minute)
	}
	for i := 0; i < 4; i++ {
		p.answer("B", conds[i%2])
		if !away() {
			t.Fatalf("not away after %v of failures, last %s", p.now.Sub(start), conds[i%2])
		}
		c, err := p.db.PlaceConditions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := c["B"].FailingSince; !got.Equal(start) {
			t.Fatalf("failing since %v, want %v", got, start)
		}
		p.now = p.now.Add(20 * time.Minute)
	}
	// Its unsent data moves elsewhere while it alternates.
	p.answer("B", ConditionRetrying)
	d2 := p.put("laptop", "data/2", "data", 1)
	p.db.Exec(`UPDATE placement SET place = 'B' WHERE seq = ? AND state = 'queued'`, d2)
	p.wantPlaces("data/2", d2, "B")
	p.answer("B", ConditionQuota)
	p.answer("B", ConditionRetrying) // the latest answer a network error
	if moved, _, err := p.db.Reroute(ctx, nil, nil); err != nil || moved != 1 {
		t.Fatalf("moved %d: %v", moved, err)
	}

	// Recovery: one ok ends the run; failures after it count afresh.
	p.answer("B", ConditionOK)
	if away() {
		t.Fatal("away after it answered ok")
	}
	p.now = p.now.Add(time.Minute)
	restart := p.now
	p.answer("B", ConditionQuota)
	p.now = p.now.Add(40 * time.Minute)
	p.answer("B", ConditionRetrying)
	if away() {
		t.Fatalf("away %v after its last ok", p.now.Sub(restart))
	}
	p.now = restart.Add(time.Hour)
	if !away() {
		t.Fatal("not away an hour after its last ok")
	}
	// This node's store failing is not the friend's answer: it neither
	// ends the run nor starts one.
	p.answer("B", ConditionStore)
	p.answer("B", ConditionRetrying)
	if c, _ := p.db.PlaceConditions(ctx); !c["B"].FailingSince.Equal(restart) {
		t.Fatalf("failing since %v after a store failure, want %v", c["B"].FailingSince, restart)
	}
}

// §3.4 (#65): a probe's answer is a sign of life, not an
// acknowledgement of work: it advances last_ok and ends a network
// failure or a refusal, but a friend full (507) stays full, and away
// once full for reroute_after, and a wait on this node's store stands.
func TestAProbeAnswerRefreshesLastOKButKeepsFullAndStore(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	cond := func() PlaceCondition {
		t.Helper()
		c, err := p.db.PlaceConditions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return c["B"]
	}
	probed := func() {
		t.Helper()
		if err := p.db.ProbeAnswered(ctx, "B", p.now); err != nil {
			t.Fatal(err)
		}
	}
	// No condition yet: the probe makes it ok.
	probed()
	if c := cond(); c.Condition != ConditionOK || !c.LastOK.Equal(p.now) {
		t.Fatalf("first probe: %+v", c)
	}
	for _, failed := range []string{ConditionRetrying, ConditionRemoved} {
		p.now = p.now.Add(time.Hour)
		p.answer("B", failed)
		p.now = p.now.Add(time.Minute)
		probed()
		if c := cond(); c.Condition != ConditionOK || c.Detail != "" || !c.LastOK.Equal(p.now) ||
			!c.Since.Equal(p.now) || !c.FailingSince.IsZero() {
			t.Fatalf("probe after %s: %+v", failed, c)
		}
	}
	for _, kept := range []string{ConditionQuota, ConditionStore} {
		p.now = p.now.Add(time.Hour)
		p.answer("B", kept)
		since := p.now
		p.now = p.now.Add(time.Minute)
		probed()
		if c := cond(); c.Condition != kept || c.Detail != "x" || !c.LastOK.Equal(p.now) || !c.Since.Equal(since) {
			t.Fatalf("probe of %s: %+v", kept, c)
		}
	}
	// Full and answering probes for two hours: full for long.
	p.answer("B", ConditionOK)
	p.answer("B", ConditionQuota)
	full := p.now
	p.now = p.now.Add(2 * time.Hour)
	probed()
	if c := cond(); c.Condition != ConditionQuota || !c.FailingSince.Equal(full) {
		t.Fatalf("full and probed: %+v", c)
	}
	ps, err := poolPlaces(ctx, p.db)
	if err != nil {
		t.Fatal(err)
	}
	if !ps[0].fullForLong(p.db, p.now) || ps[0].gone(p.db, p.now) {
		t.Fatalf("full for two hours while answering probes: full for long %t, gone %t",
			ps[0].fullForLong(p.db, p.now), ps[0].gone(p.db, p.now))
	}
}

// #65 review: a friend full for longer than replace_after that answers
// every probe, then misses one, is neither unreachable nor gone: its
// silence counts from its last answer, not from its first 507, so a
// 15-minute blip does not move every copy it holds.
func TestAFullFriendThatAnswersProbesIsNotGoneAfterOneMissedProbe(t *testing.T) {
	ctx := context.Background()
	p := newPool(t)
	p.friend("B", 10)
	p.answer("B", ConditionOK)
	p.answer("B", ConditionQuota)
	end := p.now.Add(8 * 24 * time.Hour)
	for p.now.Before(end) {
		p.now = p.now.Add(15 * time.Minute)
		if err := p.db.ProbeAnswered(ctx, "B", p.now); err != nil {
			t.Fatal(err)
		}
	}
	p.now = p.now.Add(15 * time.Minute)
	p.answer("B", ConditionRetrying)
	ps, err := poolPlaces(ctx, p.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, pl := range ps {
		if pl.peer != "B" {
			continue
		}
		if pl.gone(p.db, p.now) || pl.unreachable(p.db, p.now) {
			t.Fatalf("one missed probe after a week full and answering: gone %v, unreachable %v",
				pl.gone(p.db, p.now), pl.unreachable(p.db, p.now))
		}
		return
	}
	t.Fatal("B not in the pool")
}
