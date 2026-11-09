package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
)

// OPS-2, D6 (#65): an idle friend is probed through the tunnel, by its
// real lendisk, without a single object to send: A's place B is ok with
// a last answer. Days pass on A's clock while B answers its probes, and
// no alert is raised. Then B is switched off: A judges it from the
// probes alone, retrying, and alerts it as unreachable once
// alerts.unreachable_after (24h) has passed since its last answer, not
// before.
func TestAnIdleFriendIsProbedAndAlertedOnlyOnceAway(t *testing.T) {
	parallel(t, 2)
	a, b := friendNode(t), friendNode(t)
	clock := filepath.Join(t.TempDir(), "clock")
	jump := func(d time.Duration) {
		t.Helper()
		if err := os.WriteFile(clock, []byte(d.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pa := newProcNode(t, a, envReplFast+"=1", envClock+"="+clock, envAlertInterval+"=100ms")
	pb := newProcNode(t, b, envReplFast+"=1")
	pb.start()
	pa.start()
	befriend(t, b, a) // B lends to A; A has nothing to send it
	bKey := b.identity().NodeID().String()
	lastOK := func() time.Time {
		t.Helper()
		p := a.place(bKey)
		if p.LastOK == "" {
			return time.Time{}
		}
		at, err := time.Parse(time.RFC3339, p.LastOK)
		if err != nil {
			t.Fatal(err)
		}
		return at
	}
	within(t, 60*time.Second, "A's probe of B answered", func() bool {
		p := a.place(bKey)
		return p.State == "ok" && p.LastOK != "" && p.Queue.Objects == 0
	})
	unreachable := func() []StatusAlert {
		var out []StatusAlert
		for _, al := range a.status().Alerts {
			if al.Kind == alert.KindFriendUnreachable {
				out = append(out, al)
			}
		}
		return out
	}

	// Two days on A's clock, B answering each probe.
	for _, d := range []time.Duration{25 * time.Hour, 50 * time.Hour} {
		before := lastOK()
		jump(d)
		within(t, 60*time.Second, "B probed after the clock moved", func() bool {
			return lastOK().Sub(before) >= 24*time.Hour
		})
		time.Sleep(time.Second) // ten alert passes
		if as := unreachable(); len(as) != 0 || a.place(bKey).State != "ok" {
			t.Fatalf("B answering its probes: %+v, place %+v", as, a.place(bKey))
		}
	}
	last := lastOK()

	// B is switched off.
	pb.p.kill()
	jump(52 * time.Hour)
	within(t, 60*time.Second, "B retrying from a failed probe", func() bool { return a.place(bKey).State == "retrying" })
	jump(73 * time.Hour)    // 23h after its last answer, retrying for 21h
	time.Sleep(time.Second) // ten alert passes, and a probe
	if as := unreachable(); len(as) != 0 {
		t.Fatalf("B alerted within 24h of its last answer (%v): %+v", last, as)
	}
	jump(74*time.Hour + 5*time.Minute)
	within(t, 60*time.Second, "B alerted as unreachable", func() bool { return len(unreachable()) == 1 })
	al := unreachable()[0]
	if al.Subject != bKey || !strings.Contains(al.Message, last.UTC().Format(time.RFC3339)) {
		t.Fatalf("alert %+v, want B, not answering since %v", al, last)
	}
}
