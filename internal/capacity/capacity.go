// Package capacity is the node's capacity planning (LEND-7, LEND-8,
// OPS-2; DESIGN §4, §3.5): the protectable size of its pool, the most
// fair quotas could protect, the per-friendship numbers that would cover
// a shortfall, and init's suggestion for lending.max_total.
//
// Everything here is arithmetic on a Plan, which Read takes from the
// state database; nothing is stored. Sizes are bytes as a place counts
// them (each object plus state.ObjectOverhead), so they compare with
// quotas.
//
// Rules a caller must keep:
//   - A Plan's Friends are the friendships that are room for copies:
//     Read leaves out none, and the guidance names each by node ID.
//   - The numbers Cover names are guidance for two people to agree on
//     (LEND-8): nothing here changes a number.
package capacity

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/borje/lendisk/internal/state"
)

// AlertPercent is the share of the protectable size above which the
// repository is alerted (OPS-2) and Cover names numbers.
const AlertPercent = 90

// TombstoneMargin is the share of c × S that §4 plans for tombstones
// held for the retention window (RET-1), in percent.
const TombstoneMargin = 20

// DefaultMetadataShare is the metadata share assumed while the
// repository holds no data yet (§4: about 1% for restic and Kopia).
const DefaultMetadataShare = 0.01

// Friend is one friendship as capacity planning sees it.
type Friend struct {
	Peer  string
	Quota int64 // the friendship's quota (LEND-8)
	// Size is the friend's repository size as it reported it (LEND-7),
	// which is the most of this node's disk it has use for.
	Size int64
}

// Plan is a node's repository and its pool.
type Plan struct {
	// Data and Metadata are the repository's live data objects and its
	// metadata (index, snapshots, keys…), as places count them.
	Data, Metadata int64
	// Copies is c: the largest copies of the node's buckets.
	Copies   int
	Friends  []Friend
	Overflow []int64 // each overflow target's max_bytes (S3-7)
}

// Size is the repository size S as places count it.
func (p Plan) Size() int64 { return p.Data + p.Metadata }

// share is the metadata's share of the repository: m = share × S.
func (p Plan) share() float64 {
	if p.Data <= 0 {
		return DefaultMetadataShare
	}
	return float64(p.Metadata) / float64(p.Data+p.Metadata)
}

// feasible reports whether the pool holds a repository of size s at
// Copies copies (DESIGN §4): with m its metadata, held at every place,
// and D = s − m, Σ over friends of min(D, quota − m) plus each overflow
// target's room less m is at least c × D. quota gives each friend's
// quota.
func (p Plan) feasible(s float64, quota func(Friend) int64) bool {
	m := p.share() * s
	d := s - m
	var room float64
	for _, f := range p.Friends {
		room += math.Min(d, math.Max(float64(quota(f))-m, 0))
	}
	for _, o := range p.Overflow {
		room += math.Max(float64(o)-m, 0)
	}
	return room >= float64(max(p.Copies, 1))*d
}

// largest is the largest whole s for which feasible holds: the set is
// an interval from 0 (once fewer than c friends hold a full copy, the
// sum grows slower than c × D), so bisection finds its end.
func (p Plan) largest(quota func(Friend) int64) int64 {
	var total float64
	for _, f := range p.Friends {
		total += float64(max(quota(f), 0))
	}
	for _, o := range p.Overflow {
		total += float64(max(o, 0))
	}
	// c × D ≤ total, so S ≤ total ÷ (1 − share), whatever c.
	hi := min(total/(1-math.Min(p.share(), 0.5))+1, float64(state.MaxQuota))
	lo := 0.0
	if p.feasible(hi, quota) {
		return int64(hi)
	}
	for range 200 {
		mid := (lo + hi) / 2
		if p.feasible(mid, quota) {
			lo = mid
		} else {
			hi = mid
		}
		if hi-lo < 1 {
			break
		}
	}
	return int64(lo)
}

// Protectable is the protectable size (LEND-7): the largest repository
// the pool holds at Copies copies with the quotas as they are.
func (p Plan) Protectable() int64 { return p.largest(func(f Friend) int64 { return f.Quota }) }

// FairProtectable is the most the circle could protect with fair
// quotas (LEND-7): each quota replaced by the friend's repository size,
// since a friend has no use for more of this node's disk than that.
func (p Plan) FairProtectable() int64 { return p.largest(func(f Friend) int64 { return f.Size }) }

// Alerting reports whether the repository is above AlertPercent of the
// protectable size (OPS-2).
func (p Plan) Alerting() bool {
	s := p.Size()
	return s > 0 && float64(s)*100 > float64(p.Protectable())*AlertPercent
}

// Target is the protectable size Cover aims for: the repository size
// over AlertPercent, so the numbers it names clear the alert and cover
// any deficit with that margin.
func (p Plan) Target() int64 {
	return int64(math.Ceil(float64(p.Size()) * 100 / AlertPercent))
}

// PlacementRoom is the room for data placement leaves at a friend of
// quota holding metadata m (§3.4): the quota less the metadata and less
// the metadata reserve, the larger of state.MetadataReserveFactor × m
// and state.MetadataReservePercent of the quota. It is at most the
// room §4's formula counts (quota − m).
func PlacementRoom(quota, m int64) int64 {
	reserve := max(state.MetadataReserveFactor*m, quota*state.MetadataReservePercent/100)
	return quota - m - reserve
}

// Cover names, per friendship, the quota that would bring the
// protectable size to Target (LEND-8): the friendships with the least
// room are raised to one common level, the lowest that is enough, so
// the shortfall is shared and no friend is asked for more than another.
// Each number leaves placement's metadata reserve on top (PlacementRoom)
// and is rounded up (RoundUp). Friendships already above the level are
// not named. ok is false when no quotas can cover it: fewer friends
// than copies, and not enough overflow; a new friend or an overflow
// target is needed. Nothing is named while the repository is within
// AlertPercent of the protectable size.
func (p Plan) Cover() (numbers map[string]int64, ok bool) {
	numbers = map[string]int64{}
	if !p.Alerting() {
		return numbers, true
	}
	t := float64(p.Target())
	m := p.share() * t
	d := t - m
	need := float64(max(p.Copies, 1)) * d
	for _, o := range p.Overflow {
		need -= math.Max(float64(o)-m, 0)
	}
	if need <= 0 {
		return numbers, true // overflow alone holds it
	}
	if float64(len(p.Friends))*d < need {
		return numbers, false
	}
	room := func(f Friend) float64 { return math.Max(float64(PlacementRoom(f.Quota, int64(math.Ceil(m)))), 0) }
	at := func(level float64) float64 {
		var sum float64
		for _, f := range p.Friends {
			sum += math.Min(d, math.Max(room(f), level))
		}
		return sum
	}
	lo, hi := 0.0, d
	for range 200 {
		if hi-lo < 1 {
			break
		}
		mid := (lo + hi) / 2
		if at(mid) >= need {
			hi = mid
		} else {
			lo = mid
		}
	}
	level := hi
	mi := math.Ceil(m)
	for _, f := range p.Friends {
		if room(f) >= level {
			continue
		}
		// The smallest quota whose PlacementRoom is level: q − m − reserve.
		q := math.Max(level+(1+state.MetadataReserveFactor)*mi, (level+mi)*100/(100-state.MetadataReservePercent))
		numbers[f.Peer] = RoundUp(int64(math.Ceil(q)) + 1)
	}
	return numbers, true
}

// RoundUp rounds a number to name up to a whole MiB, or a whole GiB from
// 10 GiB, so that it is easy to type and agree on.
func RoundUp(n int64) int64 {
	if n <= 0 {
		return 0
	}
	unit := int64(1) << 20
	if n > 10<<30 {
		unit = 1 << 30
	}
	return (n + unit - 1) / unit * unit
}

// SuggestMaxTotal is init's suggestion for lending.max_total (LEND-7,
// §4): c × S plus TombstoneMargin, rounded up.
func SuggestMaxTotal(size int64, copies int) int64 {
	return RoundUp(int64(math.Ceil(float64(size) * float64(copies) * (100 + TombstoneMargin) / 100)))
}

// Read is the node's plan from db: its repository and every friendship
// with an address (the pool, D6), with defaultCopies for a node with no
// bucket.
func Read(ctx context.Context, db *state.DB, defaultCopies int) (Plan, error) {
	use, err := db.RepositoryUse(ctx)
	if err != nil {
		return Plan{}, err
	}
	p := Plan{Data: use.Data, Metadata: use.Metadata, Copies: use.Copies}
	if p.Copies == 0 {
		p.Copies = defaultCopies
	}
	fs, err := db.Friendships(ctx)
	if err != nil {
		return Plan{}, err
	}
	for _, f := range fs {
		if f.Address == "" {
			continue
		}
		p.Friends = append(p.Friends, Friend{Peer: f.Peer, Quota: f.Quota(), Size: f.PeerRepositorySize})
	}
	return p, nil
}

// Format writes a number for `lendisk peer quota`: whole TiB, GiB, MiB
// or KiB as 2T, 68G, 28M or 4K, else bytes.
func Format(n int64) string {
	for _, u := range []struct {
		s string
		b int64
	}{{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}} {
		if n >= u.b && n%u.b == 0 {
			return strconv.FormatInt(n/u.b, 10) + u.s
		}
	}
	return strconv.FormatInt(n, 10)
}

// Human writes bytes for a person: B, KiB, MiB, GiB or TiB with one
// decimal.
func Human(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}
