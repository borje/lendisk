package replica

import (
	"io"
	"sync/atomic"
	"time"
)

// watchdog is a request body that cancels its request when the
// transport takes no bytes from it for a while: a friend switched off
// mid-transfer stops the tunnel's TCP window, and nothing else would end
// the write.
type watchdog struct {
	r     io.Reader
	t     *time.Timer
	d     time.Duration
	fired atomic.Bool
}

func newWatchdog(r io.Reader, d time.Duration, cancel func()) *watchdog {
	w := &watchdog{r: r, d: d}
	w.t = time.AfterFunc(d, func() {
		w.fired.Store(true)
		cancel()
	})
	return w
}

func (w *watchdog) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if err != nil {
		w.t.Stop() // the response wait is the transport's header timeout
	} else {
		w.t.Reset(w.d)
	}
	return n, err
}

func (w *watchdog) stop()         { w.t.Stop() }
func (w *watchdog) stalled() bool { return w.fired.Load() }
