package manifest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// DialFunc opens a connection to a friend's tunnel port
// (transport.Transport.Dial).
type DialFunc func(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error)

// Pusher keeps every friend holding the node's current manifest (ID-4).
// Each round builds the manifest from lendisk.db, counts its version up
// when the content changed, and pushes it to every friend that has not
// acknowledged that version, all at once; what each acknowledged, and
// the last failure, is recorded in lendisk.db. A friend that missed a
// push (offline, or this node was) gets it at the first round it
// answers. Rounds run at start, every Poll, and on Kick.
type Pusher struct {
	DB   *state.DB
	Self *identity.Identity
	// Dial reaches a friend's tunnel port; Push uses it when nil.
	Dial DialFunc
	// Push sends a sealed manifest to f; nil means the control API
	// manifest call over Dial. Tests set it.
	Push func(ctx context.Context, f state.Friendship, blob []byte) error
	// Now and Random are the clock and the nonce source; nil means
	// time.Now and crypto/rand.
	Now    func() time.Time
	Random io.Reader
	// Poll is the time between rounds (default 15 s): changes made by
	// another process (`bucket add`, `peer quota`) are seen within it.
	Poll time.Duration
	// Timeout bounds one push (default 30 s).
	Timeout time.Duration
	Log     *slog.Logger

	once sync.Once
	kick chan struct{}
}

func (p *Pusher) init() { p.once.Do(func() { p.kick = make(chan struct{}, 1) }) }

func (p *Pusher) now() time.Time {
	if p.Now == nil {
		return time.Now()
	}
	return p.Now()
}

// Kick asks for a round now: call it after a change this process made
// (a friendship committed by redeem). It never blocks.
func (p *Pusher) Kick() {
	p.init()
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// Run makes rounds until ctx ends.
func (p *Pusher) Run(ctx context.Context) {
	p.init()
	poll := p.Poll
	if poll <= 0 {
		poll = 15 * time.Second
	}
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		p.Round(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-p.kick:
		}
	}
}

// Round makes one round: build, version, push to every friend lacking
// the current version, record.
func (p *Pusher) Round(ctx context.Context) {
	c, err := Build(ctx, p.DB, p.Self)
	if err != nil {
		p.logErr(ctx, "manifest: reading state failed", err)
		return
	}
	v, err := p.DB.NoteManifest(ctx, Hash(c), p.now())
	if err != nil {
		p.logErr(ctx, "manifest: recording its version failed", err)
		return
	}
	pushes, err := p.DB.ManifestPushes(ctx)
	if err != nil {
		p.logErr(ctx, "manifest: reading deliveries failed", err)
		return
	}
	fs, err := p.DB.Friendships(ctx)
	if err != nil {
		p.logErr(ctx, "manifest: reading friendships failed", err)
		return
	}
	var todo []state.Friendship
	for _, f := range fs {
		// Not "below": a node rebuilt without its version history
		// counts again from 1, and its friends must still get it.
		if pushes[f.Peer].Seq != v.Seq {
			todo = append(todo, f)
		}
	}
	if len(todo) == 0 {
		return
	}
	blob, err := Seal(p.Self, &Manifest{Seq: v.Seq, UpdatedAt: v.UpdatedAt.UTC(), Content: c}, p.Random)
	if err != nil {
		p.logErr(ctx, "manifest: sealing failed", err)
		return
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	var wg sync.WaitGroup
	for _, f := range todo {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, timeout)
			err := p.push(pctx, f, blob)
			cancel()
			if ctx.Err() != nil {
				return // stopping: not the friend's failure
			}
			if err != nil {
				if pushes[f.Peer].Error == "" {
					p.Log.Warn("manifest push failed; retrying until the friend answers", "peer", f.Peer, "version", v.Seq, "err", err)
				}
				if err := p.DB.ManifestPushFailed(ctx, f.Peer, err.Error(), p.now()); err != nil {
					p.logErr(ctx, "manifest: recording a failed push failed", err)
				}
				return
			}
			if err := p.DB.ManifestDelivered(ctx, f.Peer, v.Seq, p.now()); err != nil {
				p.logErr(ctx, "manifest: recording a delivery failed", err)
				return
			}
			p.Log.Info("manifest delivered", "peer", f.Peer, "version", v.Seq, "size", len(blob))
		}()
	}
	wg.Wait()
}

func (p *Pusher) push(ctx context.Context, f state.Friendship, blob []byte) error {
	if p.Push != nil {
		return p.Push(ctx, f, blob)
	}
	peer, err := identity.ParseNodeID(f.Peer)
	if err != nil {
		return err
	}
	c, done := client(p.Dial, peer, f.Address)
	defer done()
	_, err = c.PutManifest(ctx, blob)
	return err
}

func (p *Pusher) logErr(ctx context.Context, msg string, err error) {
	if ctx.Err() == nil {
		p.Log.Error(msg, "err", err)
	}
}

// client is a control API client for the node at addr over dial; done
// closes its connections.
func client(dial DialFunc, peer identity.NodeID, addr string) (*control.Client, func()) {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dial(ctx, peer, addr)
	}}
	return &control.Client{HTTP: &http.Client{Transport: tr}}, tr.CloseIdleConnections
}

// Fetch fetches self's manifest from the lender at addr and opens it
// (ID-5): nothing but the identity is needed, no bucket, no S3
// credential and no lendisk.db. dial must dial with self's own key,
// since the lender answers the manifest call for the connection's key
// alone (SEC-11, SEC-12); use one transport for the whole recovery, as
// two clients of one key stall each other at the lender's relay (#52).
func Fetch(ctx context.Context, self *identity.Identity, addr string, dial DialFunc) (*Manifest, error) {
	peer, err := transport.KeyOf(addr)
	if err != nil {
		return nil, err
	}
	c, done := client(dial, peer, addr)
	defer done()
	b, err := c.GetManifest(ctx)
	var ce *control.CallError
	switch {
	case errors.As(err, &ce) && ce.Status == http.StatusNotFound:
		return nil, fmt.Errorf("the lender %s keeps no manifest for this node: try another lender", peer)
	case errors.As(err, &ce) && ce.Status == http.StatusForbidden:
		return nil, fmt.Errorf("the lender %s does not know this node (%s): check the phrase, or try another lender", peer, ce.Message)
	case err != nil:
		return nil, fmt.Errorf("fetching the manifest from %s: %w", peer, err)
	}
	return Open(self, b)
}
