package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// farewell is the daemon's side of ending friendships (PEER-5, PEER-12,
// SEC-15, #25). `peer remove` commits a removal from another process,
// and a friend's remove call commits one inside the daemon (Kick); each
// pass of the farewell reads the former friends and:
//
//   - revokes each one not revoked by this process yet, in spike #3's
//     order: the removal is committed (the friendship row is gone, so
//     admission and the per-request middleware refuse both keys), then
//     Clear empties admission's cache, Cut cancels the in-flight
//     requests of every connection of the friend's node ID and dial key
//     and closes them, and only then Disconnect drops each key's tunnel:
//     a connection closed after the disconnect never tells the client;
//   - tells a friend that this node ended it (Tell, the remove call),
//     retried with a backoff from a minute to an hour until it answers;
//   - at the end of the retention window purges the friend's replica
//     here (Purge: its lent buckets and its manifest) and forgets it.
//
// A pass is at most Interval apart (10 s), so a removal reaches open
// connections well within SEC-15's 60 seconds.
type farewell struct {
	DB  *state.DB
	Log *slog.Logger
	Now func() time.Time
	// Interval is the time between passes; zero means 10 s.
	Interval time.Duration

	Clear      func()
	Cut        func(keys ...identity.NodeID) int
	Disconnect func(identity.NodeID) bool
	// Tell calls the former friend's remove call.
	Tell func(ctx context.Context, f state.FormerFriend) error
	// Purge deletes what the former friend stored here.
	Purge func(ctx context.Context, peer string) error
	// Alerted asks the alert service to look again; nil means nothing.
	Alerted func()

	once    sync.Once
	wake    chan struct{}
	mu      sync.Mutex
	cut     map[string]time.Time     // peer -> the removal revoked in this process
	next    map[string]time.Time     // peer -> when to tell it next
	backoff map[string]time.Duration // peer -> the wait after its next failure
	telling map[string]bool
	wg      sync.WaitGroup
}

const (
	tellMinBackoff = time.Minute
	tellMaxBackoff = time.Hour
	tellTimeout    = 30 * time.Second
)

func (f *farewell) init() {
	f.once.Do(func() { f.wake = make(chan struct{}, 1) })
}

// Kick asks for a pass now (a remove call committed in this process).
func (f *farewell) Kick() {
	f.init()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *farewell) now() time.Time {
	if f.Now == nil {
		return time.Now()
	}
	return f.Now()
}

// Run makes a pass at once and then every Interval or Kick until ctx
// ends.
func (f *farewell) Run(ctx context.Context) {
	f.init()
	defer f.wait()
	every := f.Interval
	if every <= 0 {
		every = 10 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		f.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-f.wake:
		}
	}
}

// wait waits for the tells in flight.
func (f *farewell) wait() { f.wg.Wait() }

// pass makes one pass over the former friends (type comment).
func (f *farewell) pass(ctx context.Context) {
	fs, err := f.DB.FormerFriends(ctx)
	if err != nil {
		if ctx.Err() == nil {
			f.Log.Error("ending friendships: reading the former friends failed; trying again", "err", err)
		}
		return
	}
	f.mu.Lock()
	if f.cut == nil {
		f.cut, f.next, f.backoff, f.telling = map[string]time.Time{}, map[string]time.Time{}, map[string]time.Duration{}, map[string]bool{}
	}
	seen := map[string]bool{}
	var revoke []state.FormerFriend
	for _, ff := range fs {
		seen[ff.Peer] = true
		if at, ok := f.cut[ff.Peer]; !ok || !at.Equal(ff.RemovedAt) {
			f.cut[ff.Peer] = ff.RemovedAt
			revoke = append(revoke, ff)
		}
	}
	for p := range f.cut {
		if !seen[p] { // purged, or friends again
			delete(f.cut, p)
			delete(f.next, p)
			delete(f.backoff, p)
		}
	}
	f.mu.Unlock()
	if len(revoke) > 0 {
		f.revoke(revoke)
		if f.Alerted != nil {
			f.Alerted()
		}
	}
	now := f.now()
	for _, ff := range fs {
		if !ff.ByPeer && ff.NotifiedAt.IsZero() && ff.Address != "" {
			f.tell(ctx, ff, now)
		}
	}
	for _, ff := range fs {
		if now.Before(ff.PurgeAfter) {
			continue
		}
		if err := f.purge(ctx, ff.Peer); err != nil {
			if ctx.Err() == nil {
				f.Log.Error("ending friendships: purging a former friend's replica failed; trying again at the next pass",
					"peer", ff.Peer, "err", err)
			}
			continue
		}
		f.Log.Info("ended friendship purged: the former friend's replica here is deleted and it is forgotten", "peer", ff.Peer)
		if f.Alerted != nil {
			f.Alerted()
		}
	}
}

// revoke applies removals already committed, in spike #3's order.
func (f *farewell) revoke(fs []state.FormerFriend) {
	var keys []identity.NodeID
	for _, ff := range fs {
		for _, k := range []string{ff.Peer, ff.DialKey} {
			if k == "" {
				continue
			}
			id, err := identity.ParseNodeID(k)
			if err != nil {
				f.Log.Error("ending friendships: a former friend's key is malformed", "peer", ff.Peer, "key", k, "err", err)
				continue
			}
			keys = append(keys, id)
		}
	}
	f.Clear()
	n := f.Cut(keys...)
	for _, k := range keys {
		connected := f.Disconnect(k)
		f.Log.Debug("ending friendships: disconnected a former friend's key", "key", k.String(), "was_connected", connected)
	}
	// A connection opened inside the tunnel between the cut and the
	// disconnect was refused request by request; close it too.
	n += f.Cut(keys...)
	for _, ff := range fs {
		f.Log.Info("friendship ended: both keys refused, open connections closed", "peer", ff.Peer, "by_peer", ff.ByPeer,
			"purge_after", ff.PurgeAfter.UTC().Format(time.RFC3339))
	}
	f.Log.Debug("ending friendships: connections cut", "count", n)
}

// tell starts telling ff, unless a try is in flight or its backoff has
// not passed.
func (f *farewell) tell(ctx context.Context, ff state.FormerFriend, now time.Time) {
	f.mu.Lock()
	if f.telling[ff.Peer] || now.Before(f.next[ff.Peer]) {
		f.mu.Unlock()
		return
	}
	f.telling[ff.Peer] = true
	f.mu.Unlock()
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		tctx, cancel := context.WithTimeout(ctx, tellTimeout)
		err := f.Tell(tctx, ff)
		cancel()
		var ce *control.CallError
		if errors.As(err, &ce) && ce.Status == http.StatusForbidden {
			err = nil // it holds no friendship with this node either
		}
		f.mu.Lock()
		delete(f.telling, ff.Peer)
		if err != nil {
			b := f.backoff[ff.Peer]
			if b == 0 {
				b = tellMinBackoff
			}
			f.next[ff.Peer] = f.now().Add(b)
			f.backoff[ff.Peer] = min(2*b, tellMaxBackoff)
		}
		f.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		msg := ""
		if err != nil {
			msg = err.Error()
			f.Log.Warn("ending friendships: telling the former friend failed; trying again later", "peer", ff.Peer, "err", err)
		} else {
			f.Log.Info("ending friendships: the former friend was told", "peer", ff.Peer)
		}
		if err := f.DB.NoteFarewell(ctx, ff.Peer, msg, f.now()); err != nil && ctx.Err() == nil {
			f.Log.Error("ending friendships: recording the farewell failed", "peer", ff.Peer, "err", err)
		}
	}()
}

// purge deletes the former friend's replica here and forgets it.
func (f *farewell) purge(ctx context.Context, peer string) error {
	if err := f.Purge(ctx, peer); err != nil {
		return err
	}
	return f.DB.ForgetFormerFriend(ctx, peer)
}

// formerLines prints status's ended friendships.
func formerLines(tw *tabwriter.Writer, fs []StatusFormer) {
	for _, f := range fs {
		by := "you"
		if f.ByPeer {
			by = "them"
		}
		told := "told"
		switch {
		case f.NotifiedAt == "" && f.NotifyError != "":
			told = "not told yet (" + f.NotifyError + ")"
		case f.NotifiedAt == "":
			told = "not told yet"
		}
		fmt.Fprintf(tw, "  %s\tended %s by %s, %s;\treplicas purged at %s\n", f.Peer, f.RemovedAt, by, told, f.PurgeAfter)
	}
}
