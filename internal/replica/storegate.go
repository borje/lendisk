package replica

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// storeGate keeps the worker from piling calls onto a wedged store
// (REPL-3, OPS-1, #67). A store call that does not return within
// Worker.StoreStall is left behind on its goroutine, holding an OS
// thread in its syscall until the disk answers (storeCall,
// storeReader). While one is left behind the store is wedged: no new
// call into it is started. An open or stat waits for the stuck calls to
// return, up to StoreStall or until its context ends, and then fails as
// the store's failure without touching the disk; a read fails at once.
// So a disk hung for weeks holds at most the calls that were under way
// when it hung (one per sender), not one per retry.
//
// The gate is the worker's only: its senders, re-hashes and fills all
// go through it. Nothing else shares it (the gateway, verify and status
// call the store on their own goroutines and wait, so they never leave
// a call behind).
type storeGate struct {
	mu    sync.Mutex
	stuck int           // calls left behind that have not returned
	what  string        // the first of them, for the error
	clear chan struct{} // closed when the last returns; nil while none is stuck
}

// stuckCall records a call left behind while it blocks. The function it
// returns is called when the call returns; it may be called more than
// once.
func (g *storeGate) stuckCall(what string) (returned func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stuck == 0 {
		g.clear, g.what = make(chan struct{}), what
	}
	g.stuck++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.stuck--; g.stuck == 0 {
				close(g.clear)
				g.clear, g.what = nil, ""
			}
		})
	}
}

// wedged is the error a call meets while an earlier one is stuck, or nil.
func (g *storeGate) wedged() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.clear == nil {
		return nil
	}
	return g.wedgedErr()
}

func (g *storeGate) wedgedErr() error {
	return fmt.Errorf("%s did not return and still has not; the store is not called again until it does", g.what)
}

// pass waits for the store's stuck calls to return, up to stall or until
// ctx ends: nil when none is stuck (any more), else why not.
func (g *storeGate) pass(ctx context.Context, stall time.Duration) error {
	g.mu.Lock()
	clear := g.clear
	g.mu.Unlock()
	if clear == nil {
		return nil
	}
	t := time.NewTimer(stall)
	defer t.Stop()
	select {
	case <-clear:
		return nil
	case <-t.C:
		return g.wedged()
	case <-ctx.Done():
		return ctx.Err()
	}
}
