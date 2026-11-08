package replica

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// LEND-8 AC (#24): a place paused for quota waits PauseRetry (an hour by
// default) before it tries again, unless the quota rises: Resume wakes
// it at once, and the FIFO drains with no other action. Each 507 is
// passed to Full, so the daemon asks the friend for its count (#68).
func TestARaisedQuotaResumesAPausedPlaceAtOnce(t *testing.T) {
	fx := newFixture(t, "B")
	fx.w.PauseRetry = time.Hour
	var full atomic.Int32
	fx.w.Full = func(place string) {
		if place == "B" {
			full.Add(1)
		}
	}
	var mu sync.Mutex
	refuse := true
	fx.places["B"].fail = func(string, string) error {
		mu.Lock()
		defer mu.Unlock()
		if refuse {
			return ErrQuota
		}
		return nil
	}
	fx.write("laptop", "data/1", "x")
	fx.run()
	waitFor(t, "the pause", func() bool { return fx.condition("B").Condition == state.ConditionQuota })
	waitFor(t, "Full", func() bool { return full.Load() == 1 })
	mu.Lock()
	refuse = false
	mu.Unlock()
	fx.w.Resume("C") // another place: nothing
	fx.w.Resume("B")
	fx.drained("B")
	waitFor(t, "ok", func() bool { return fx.condition("B").Condition == state.ConditionOK })
}
