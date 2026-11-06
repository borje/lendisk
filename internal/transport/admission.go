package transport

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/identity"
)

// Lookup reports whether lendisk.db holds a relationship or a pending
// invite for a key.
type Lookup func(ctx context.Context, peer identity.NodeID) (bool, error)

// lookupTimeout bounds one lookup; the hook has no context of its own.
const lookupTimeout = 5 * time.Second

// maxDenied bounds the negative cache. Keys are free to make, so a
// stranger could otherwise grow it without limit; when it is full it is
// emptied, which costs only lookups.
const maxDenied = 4096

// DeniedFor is how long a rejection is remembered without a Clear. A
// one-shot command (`peer add`) writes a friendship from another process
// and cannot call Clear on the daemon's Admission, so a key it adds is
// admitted at most this long after the commit (SEC-15's 60 seconds).
const DeniedFor = 30 * time.Second

// Admission is the admission function (SEC-10). It looks every key up
// with its Lookup, and admits only on a positive answer without error.
// A rejected client retries about once a second, so rejections are
// remembered until Clear or for DeniedFor; admissions and errors are not
// remembered.
type Admission struct {
	lookup Lookup
	log    *slog.Logger
	now    func() time.Time

	mu     sync.Mutex
	denied map[identity.NodeID]time.Time // when the rejection is forgotten
	// gen counts Clear calls, so a rejection looked up before a Clear
	// is not cached after it.
	gen uint64
}

// NewAdmission returns an admission function over lookup.
func NewAdmission(lookup Lookup, log *slog.Logger) *Admission {
	return &Admission{lookup: lookup, log: log, now: time.Now, denied: map[identity.NodeID]time.Time{}}
}

// Admit reports whether peer may connect. Pass it to Transport.Admit.
func (a *Admission) Admit(peer identity.NodeID) bool {
	now := a.now()
	a.mu.Lock()
	until, denied := a.denied[peer]
	if denied && !now.Before(until) {
		delete(a.denied, peer)
		denied = false
	}
	gen := a.gen
	a.mu.Unlock()
	if denied {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	ok, err := a.lookup(ctx, peer)
	if err != nil {
		a.log.Error("tunnel admission lookup failed; refusing", "peer", peer.String(), "err", err)
		return false
	}
	if ok {
		a.log.Debug("tunnel admitted peer", "peer", peer.String())
		return true
	}
	a.log.Debug("tunnel refused unknown key", "peer", peer.String())
	a.mu.Lock()
	if gen == a.gen {
		if len(a.denied) >= maxDenied {
			clear(a.denied)
		}
		a.denied[peer] = now.Add(DeniedFor)
	}
	a.mu.Unlock()
	return false
}

// Clear forgets every rejection. Call it after committing any change to
// a friendship or an invite (SEC-15).
func (a *Admission) Clear() {
	a.mu.Lock()
	clear(a.denied)
	a.gen++
	a.mu.Unlock()
}
