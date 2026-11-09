package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/borje/lendisk/internal/capacity"
	"github.com/borje/lendisk/internal/state"
)

// Capacity planning in status (LEND-7, LEND-8, OPS-1, PEER-9; #37).

// LowUseFor is how long one side of a friendship must have used less
// than half of the quota before status says so (LEND-7).
const LowUseFor = 30 * 24 * time.Hour

// StatusCapacity is capacity planning in `status --json` (LEND-7):
// fields are added, never renamed or removed. Sizes are bytes.
type StatusCapacity struct {
	// RepositorySize is the node's repository as friends count it (each
	// live object plus state.ObjectOverhead), Metadata its metadata part,
	// held at every friend; LocalCopy the live bytes in the local store.
	RepositorySize int64 `json:"repository_size"`
	Metadata       int64 `json:"metadata"`
	LocalCopy      int64 `json:"local_copy"`
	// Copies is c: the largest copies of a bucket holding data.
	Copies int `json:"copies"`
	// Protectable is the protectable size; FairProtectable the most fair
	// quotas could protect, each quota replaced by the friend's
	// repository size (DESIGN §4).
	Protectable     int64 `json:"protectable"`
	FairProtectable int64 `json:"fair_protectable"`
	// Alerting is true while the repository is above 90% of the
	// protectable size (OPS-2); Deficit is how far it is above it, 0
	// when not. Coverable is false when no quotas can cover it (fewer
	// friends than copies): a friend or an overflow target is needed.
	Alerting  bool  `json:"alerting"`
	Deficit   int64 `json:"deficit"`
	Coverable bool  `json:"coverable"`
	// Friends is each friendship's quota and use both ways.
	Friends []StatusCapacityFriend `json:"friends"`
	// Overflow is each overflow target's room and use (S3-7); none until
	// overflow targets exist.
	Overflow []StatusCapacityOverflow `json:"overflow"`
	// Lent is the quota lent to all friends, LentUsed what they store
	// here; Received the quota they lend this node (the same, as every
	// friendship is symmetric), ReceivedUnused what of it this node does
	// not use (PEER-9: all of it on a lender-only node).
	Lent           int64 `json:"lent"`
	LentUsed       int64 `json:"lent_used"`
	Received       int64 `json:"received"`
	ReceivedUnused int64 `json:"received_unused"`
}

// StatusCapacityFriend is one friendship in capacity planning.
type StatusCapacityFriend struct {
	Peer  string `json:"peer"`
	Quota int64  `json:"quota"`
	// UseThere is what this node stores at the friend (its count,
	// reconciled from the friend's); UseHere what the friend stores here.
	UseThere int64 `json:"use_there"`
	UseHere  int64 `json:"use_here"`
	// RepositorySize is the friend's, as it reported it; absent before
	// its first report.
	RepositorySize *int64 `json:"repository_size,omitempty"`
	// Cover is the quota the two should agree on to cover the deficit
	// (LEND-8), while the repository is above 90% of the protectable
	// size; absent otherwise or when this friendship need not change.
	Cover int64 `json:"cover,omitempty"`
	// LowThereSince and LowHereSince are set when this node (there) or
	// the friend (here) has used less than half of the quota for
	// LowUseFor: since when (RFC 3339, UTC).
	LowThereSince string `json:"low_there_since,omitempty"`
	LowHereSince  string `json:"low_here_since,omitempty"`
}

// StatusCapacityOverflow is one overflow target's room (max_bytes) and
// what it holds (S3-7).
type StatusCapacityOverflow struct {
	Name     string `json:"name"`
	MaxBytes int64  `json:"max_bytes"`
	Use      int64  `json:"use"`
}

// capacityStatus reads capacity planning at now.
func (a *app) capacityStatus(ctx context.Context, db *state.DB, defaultCopies int, now time.Time) (*StatusCapacity, error) {
	plan, err := capacity.Read(ctx, db, defaultCopies)
	if err != nil {
		return nil, err
	}
	local, err := db.RepositorySize(ctx)
	if err != nil {
		return nil, err
	}
	fs, err := db.Friendships(ctx)
	if err != nil {
		return nil, err
	}
	use, err := db.PlaceUse(ctx)
	if err != nil {
		return nil, err
	}
	c := &StatusCapacity{RepositorySize: plan.Size(), Metadata: plan.Metadata, LocalCopy: local, Copies: plan.Copies,
		Protectable: plan.Protectable(), FairProtectable: plan.FairProtectable(), Alerting: plan.Alerting(),
		Friends: []StatusCapacityFriend{}, Overflow: []StatusCapacityOverflow{}}
	c.Deficit = max(c.RepositorySize-c.Protectable, 0)
	cover, ok := plan.Cover()
	c.Coverable = ok
	low := func(since time.Time) string {
		if since.IsZero() || now.Sub(since) < LowUseFor {
			return ""
		}
		return rfc3339(since)
	}
	for _, f := range fs {
		here, _, err := db.LentUsage(ctx, f.Peer)
		if err != nil {
			return nil, err
		}
		sf := StatusCapacityFriend{Peer: f.Peer, Quota: f.Quota(), UseThere: max(use[f.Peer], 0), UseHere: here,
			Cover: cover[f.Peer], LowThereSince: low(f.LowThereSince), LowHereSince: low(f.LowHereSince)}
		if !f.SeenAt.IsZero() {
			size := f.PeerRepositorySize
			sf.RepositorySize = &size
		}
		c.Friends = append(c.Friends, sf)
		c.Lent += sf.Quota
		c.LentUsed += sf.UseHere
		c.Received += sf.Quota
		c.ReceivedUnused += max(sf.Quota-sf.UseThere, 0)
	}
	return c, nil
}

// capacityLines are status's lines on capacity planning.
func capacityLines(c *StatusCapacity, role string) []string {
	if c == nil {
		return nil
	}
	h := capacity.Human
	var out []string
	if role == "lender" {
		out = append(out, fmt.Sprintf("Lending only: lends %s to %s, who use %s of it; received %s from them, unused until this node has a bucket",
			h(c.Lent), plural(int64(len(c.Friends)), "friend", "friends"), h(c.LentUsed), h(c.Received)))
	} else {
		out = append(out, fmt.Sprintf("Capacity: repository %s at %d copies as friends count it (local copy %s); protectable %s; with fair quotas (each friend's repository size) %s",
			h(c.RepositorySize), c.Copies, h(c.LocalCopy), h(c.Protectable), h(c.FairProtectable)))
		out = append(out, fmt.Sprintf("  lends %s to %s, who use %s of it; received %s, of which %s unused",
			h(c.Lent), plural(int64(len(c.Friends)), "friend", "friends"), h(c.LentUsed), h(c.Received), h(c.ReceivedUnused)))
		if c.Alerting {
			l := fmt.Sprintf("  ! the repository is above %d%% of the protectable size", capacity.AlertPercent)
			if c.Deficit > 0 {
				l += fmt.Sprintf(", by %s: copies beyond it are paused for quota", h(c.Deficit))
			}
			out = append(out, l)
			if !c.Coverable {
				out = append(out, fmt.Sprintf("    with %s at %d copies no quota covers it: add another friend or an overflow target",
					plural(int64(len(c.Friends)), "friend", "friends"), c.Copies))
			} else {
				out = append(out, "    to cover it, agree these quotas, each side running `lendisk peer quota <friend> <size>`:")
				for _, f := range c.Friends {
					if f.Cover > 0 {
						out = append(out, fmt.Sprintf("      %s: %s (now %s)", f.Peer, capacity.Format(f.Cover), capacity.Format(f.Quota)))
					}
				}
			}
		}
	}
	for _, f := range c.Friends {
		l := fmt.Sprintf("  %s: quota %s; you use %s there, it uses %s here", f.Peer, capacity.Format(f.Quota), h(f.UseThere), h(f.UseHere))
		if f.RepositorySize != nil {
			l += "; its repository " + h(*f.RepositorySize)
		}
		out = append(out, l)
		if f.LowThereSince != "" && role != "lender" {
			out = append(out, fmt.Sprintf("    this node has used under half of the quota there since %s; you could both lower the number", f.LowThereSince))
		}
		if f.LowHereSince != "" {
			out = append(out, fmt.Sprintf("    friend %s has used under half of the quota here since %s; you could both lower the number", f.Peer, f.LowHereSince))
		}
	}
	if role != "lender" {
		if len(c.Overflow) == 0 {
			out = append(out, "  Overflow targets: none")
		}
		for _, o := range c.Overflow {
			out = append(out, fmt.Sprintf("    %s: %s of %s", o.Name, h(o.Use), h(o.MaxBytes)))
		}
	}
	return out
}
