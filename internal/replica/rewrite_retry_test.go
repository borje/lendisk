package replica

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// headless is a place that refuses a rewritable key's rewrite (412) and
// answers its Head with what heads says: the key is absent there (a
// deleted object's tomb), or holds the very version the rewrite marked
// (a Head landing inside the lender's own rewrite window). Once fixed is
// set it stores the put.
type headless struct {
	*place
	mu    sync.Mutex
	heads func() (string, bool)
	fixed bool
	tries int
}

func (p *headless) Put(ctx context.Context, o Object) error {
	p.mu.Lock()
	p.tries++
	fixed := p.fixed
	p.mu.Unlock()
	if o.Key == "config" && !fixed {
		p.place.mu.Lock()
		_, ok := p.objs[o.Bucket+"/"+o.Key]
		p.place.mu.Unlock()
		if ok {
			return ErrImmutable
		}
	}
	return p.place.Put(ctx, o)
}

func (p *headless) Head(ctx context.Context, bucket, key string) (string, bool, error) {
	h, ok := p.heads()
	return h, ok, nil
}

func (p *headless) attempts() int { p.mu.Lock(); defer p.mu.Unlock(); return p.tries }

// #74 review (M2): a rewrite refused while the place's Head says it holds
// nothing at the key, or holds exactly the marked version, is a passing
// condition, not a refusal for good: the row waits with backoff, the
// place shows retrying, no place_refused is recorded or alerted, and the
// rewrite lands once the place takes it.
func TestRefusedRewriteWithNoNewsFromTheHeadIsRetried(t *testing.T) {
	for name, heads := range map[string]func(marked string) (string, bool){
		"absent":         func(string) (string, bool) { return "", false },
		"marked version": func(marked string) (string, bool) { return marked, true },
	} {
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t, "B")
			b := &headless{place: fx.places["B"]}
			b.heads = func() (string, bool) { return heads(hashOf("config one")) }
			fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
			fx.writeAs("vault", "config", "config one", "metadata", true)
			fx.run()
			fx.drained("B")
			fx.writeAs("vault", "config", "config two", "metadata", true)
			waitFor(t, "a few attempts", func() bool { return b.attempts() >= 4 })
			if n := fx.refusedAt("B"); n != 0 || fx.alerted.Load() != 0 {
				t.Fatalf("refused %d, alerts %d", n, fx.alerted.Load())
			}
			if c := fx.condition("B"); c.Condition != state.ConditionRetrying {
				t.Fatalf("condition %+v", c)
			}
			b.mu.Lock()
			b.fixed = true
			b.mu.Unlock()
			fx.drained("B")
			if got := b.place.objsOf("vault/config"); got != "config two" {
				t.Fatalf("B holds %q", got)
			}
			time.Sleep(10 * time.Millisecond)
		})
	}
}

func (p *place) objsOf(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return string(p.objs[key])
}
