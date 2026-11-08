package replica

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// #74 (second review): a rewritable key the owner deleted and then wrote
// again with other bytes is sent as a rewrite marked with the version
// the place last acknowledged, the deleted one, which its lender keeps
// in a tomb: the lender stores it beside the tomb rather than refusing
// it until the purge.
func TestRewritableKeyWrittenAgainAfterADeleteIsMarked(t *testing.T) {
	fx := newFixture(t, "B")
	b := &immutablePlace{place: fx.places["B"]}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	fx.run()
	fx.writeAs("vault", "kopia.repository", "repository one", "metadata", true)
	fx.drained("B")
	fx.remove("vault", "kopia.repository")
	fx.drained("B")
	fx.writeAs("vault", "kopia.repository", "repository two", "metadata", true)
	fx.drained("B")
	marks := b.sentMarks()
	if last := marks[len(marks)-1]; last != "kopia.repository="+hashOf("repository one") {
		t.Fatalf("marks %v: the re-put should name the deleted version", marks)
	}
	if got := b.held("vault/kopia.repository"); got != "repository two" {
		t.Fatalf("B holds %q", got)
	}
}

// #74 (second review): a rewrite the place keeps refusing although its
// Head gives no other version to name (it holds nothing live at the key,
// or holds the very version the rewrite named, as a lender too old to
// read the mark does) is retried for Worker.RewriteRetry and then refused
// for good, as place_refused with its alert, so the place's FIFO moves
// on: a later object reaches the place.
func TestRewriteRefusedPastTheRetryWindowIsRefused(t *testing.T) {
	for name, heads := range map[string]func(marked string) (string, bool){
		"absent":                 func(string) (string, bool) { return "", false },
		"a lender ignoring mark": func(marked string) (string, bool) { return marked, true },
	} {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t, "B")
			fx.w.RewriteRetry = 300 * time.Millisecond
			b := &headless{place: fx.places["B"]}
			b.heads = func() (string, bool) { return heads(hashOf("config one")) }
			fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
			fx.writeAs("vault", "config", "config one", "metadata", true)
			fx.run()
			fx.drained("B")
			fx.writeAs("vault", "config", "config two", "metadata", true)
			fx.writeAs("vault", "index/1", "an index", "metadata", false)
			fx.drained("B")
			if n := fx.refusedAt("B"); n != 1 || fx.alerted.Load() == 0 {
				t.Fatalf("refused %d, alerts %d", n, fx.alerted.Load())
			}
			if got := b.place.objsOf("vault/index/1"); got != "an index" {
				t.Fatalf("the FIFO stalled: B holds %q", got)
			}
			st, err := fx.db.PlaceStats(t.Context(), time.Now())
			if err != nil || st["B"].Refused != 1 {
				t.Fatalf("stats %+v %v", st, err)
			}
			var detail string
			fx.db.QueryRow(`SELECT coalesce(detail, '') FROM placement WHERE state = 'refused'`).Scan(&detail)
			if !strings.Contains(detail, "rewrite") {
				t.Fatalf("refusal detail %q", detail)
			}
		})
	}
}
