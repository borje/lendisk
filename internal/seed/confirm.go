package seed

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
)

// Confirmer runs in the borrower's daemon: for each pending seed it
// lists the friend's copy of the bucket and acknowledges the held rows
// the list shows with their captured hash (REPL-8, REPL-10), then ends
// the seed as the state package's seed rules say (seed.go there).
type Confirmer struct {
	DB   *state.DB
	Self *identity.Identity
	Dial manifest.DialFunc
	// Page reads one page of the list of sealed at friend f after the
	// key after; nil means the control API list call over Dial. Tests
	// set it.
	Page func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error)
	Now  func() time.Time
	// Poll is the time between rounds (default 1 min); Timeout bounds one
	// page (default 1 min); after failures a seed is listed again only
	// Poll doubled per failure later, at most an hour.
	Poll, Timeout time.Duration
	// Quiet is how long a seed that confirmed some objects waits for more
	// (default state.SeedQuietAfter). Tests shorten it.
	Quiet time.Duration
	Log   *slog.Logger
}

func (c *Confirmer) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func or(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

func (c *Confirmer) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.New(slog.DiscardHandler)
}

// Round lists each pending seed that is due once.
func (c *Confirmer) Round(ctx context.Context) error {
	seeds, err := c.DB.Seeds(ctx)
	if err != nil {
		return err
	}
	for _, s := range seeds {
		if !s.DoneAt.IsZero() || ctx.Err() != nil {
			continue
		}
		if s.Failures > 0 {
			wait := or(c.Poll, time.Minute) << min(s.Failures, 12)
			if c.now().Sub(s.CheckedAt) < min(wait, time.Hour) {
				continue
			}
		}
		if err := c.one(ctx, s); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// one lists s's friend once and records what it found; an error is the
// database's.
func (c *Confirmer) one(ctx context.Context, s state.Seed) error {
	f, err := c.DB.GetFriendship(ctx, s.Place)
	if errors.Is(err, state.ErrNotFound) {
		c.log().Info("seeding: the friend is no longer a friend; the seed ends", "place", s.Place, "bucket", s.Bucket)
		return c.DB.EndSeed(ctx, s.Place, s.Bucket, state.SeedRemoved, c.now())
	} else if err != nil {
		return err
	}
	page, done := c.page(f)
	defer done()
	sealed := c.Self.SealedName(s.Bucket)
	var confirmed int64
	after := ""
	for {
		pctx, cancel := context.WithTimeout(ctx, or(c.Timeout, time.Minute))
		p, err := page(pctx, f, sealed, after)
		cancel()
		var ce *control.CallError
		if errors.As(err, &ce) && ce.Code == control.CodeNoSuchBucket {
			break // nothing imported yet
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.log().Info("seeding: the friend's list could not be read; trying again later",
				"place", s.Place, "bucket", s.Bucket, "err", err)
			return c.DB.SeedChecked(ctx, s.Place, s.Bucket, 0, err.Error(), c.now())
		}
		var live []state.SeedListed
		for _, o := range p.Objects {
			if o.TombstonedAt == nil {
				live = append(live, state.SeedListed{Key: o.Key, SHA256: o.SHA256})
			}
		}
		n, err := c.DB.ConfirmSeeded(ctx, s.Place, s.Bucket, s.Through, live, c.now())
		confirmed += n
		if err != nil {
			return err
		}
		if p.Next == "" {
			break
		}
		after = p.Next
	}
	now := c.now()
	if err := c.DB.SeedChecked(ctx, s.Place, s.Bucket, confirmed, "", now); err != nil {
		return err
	}
	left, err := c.DB.SeedLeft(ctx, s.Place, s.Bucket, s.Through)
	if err != nil {
		return err
	}
	total := s.Confirmed + confirmed
	outcome := ""
	switch {
	case left == 0:
		outcome = state.SeedConfirmed
	case confirmed == 0 && total > 0 && now.Sub(s.ProgressAt) >= or(c.Quiet, state.SeedQuietAfter):
		outcome = state.SeedQuiet
	case total == 0 && now.Sub(s.ExportedAt) >= state.SeedExpiry:
		outcome = state.SeedExpired
	}
	if confirmed > 0 || outcome != "" {
		c.log().Info("seeding: the friend's list confirmed seeded objects", "place", s.Place, "bucket", s.Bucket,
			"confirmed", confirmed, "confirmed_total", total, "left", left, "outcome", outcome)
	}
	if outcome == "" {
		return nil
	}
	return c.DB.EndSeed(ctx, s.Place, s.Bucket, outcome, now)
}

// page is the list call at f over Dial, or Page; done closes its
// connections.
func (c *Confirmer) page(f state.Friendship) (func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error), func()) {
	if c.Page != nil {
		return c.Page, func() {}
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		peer, err := identity.ParseNodeID(f.Peer)
		if err != nil {
			return nil, err
		}
		return c.Dial(ctx, peer, f.Address)
	}}
	cl := &control.Client{HTTP: &http.Client{Transport: tr}}
	return func(ctx context.Context, _ state.Friendship, sealed, after string) (control.ListPage, error) {
		return cl.ListPage(ctx, sealed, after, 0)
	}, tr.CloseIdleConnections
}

// Run makes a round every Poll until ctx ends.
func (c *Confirmer) Run(ctx context.Context) {
	tick := time.NewTicker(or(c.Poll, time.Minute))
	defer tick.Stop()
	for {
		if err := c.Round(ctx); err != nil && ctx.Err() == nil {
			c.log().Error("seeding: confirming seeded objects failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
