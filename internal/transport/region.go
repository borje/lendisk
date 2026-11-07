package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tailscale/tailcat"
	"tailscale.com/tailcfg"
)

// regionKey is the node-table key holding the saved DERP region as JSON.
const regionKey = "tunnel.region"

// NodeStore is the part of lendisk.db the region is saved in.
type NodeStore interface {
	GetNode(ctx context.Context, key string) (string, bool, error)
	SetNode(ctx context.Context, key, value string) error
}

// RegionSource offers DERP regions to choose the bootstrap region from.
type RegionSource interface {
	// DERPMap returns the regions on offer.
	DERPMap(ctx context.Context) (*tailcfg.DERPMap, error)
	// Pick chooses the nearest region of m.
	Pick(ctx context.Context, m *tailcfg.DERPMap) (tailcfg.DERPRegionID, error)
}

// PublicRelays is tailcat's published DERP map, picked by measured
// latency. It needs the internet.
var PublicRelays RegionSource = publicRelays{}

type publicRelays struct{}

func (publicRelays) DERPMap(ctx context.Context) (*tailcfg.DERPMap, error) {
	return tailcat.FetchDERPMap(ctx, tailcat.ExpandForServer)
}

func (publicRelays) Pick(ctx context.Context, m *tailcfg.DERPMap) (tailcfg.DERPRegionID, error) {
	id, err := tailcat.PickBestRegion(ctx, m)
	if err == nil && id == 0 {
		err = fmt.Errorf("no relay region answered a latency probe")
	}
	return id, err
}

// RestoreRegion saves raw, a region as SavedRegion stores it, taken
// from the node manifest of a node being rebuilt (ID-4, ID-5), so its
// address stays what its friends hold (SEC-14).
func RestoreRegion(ctx context.Context, st NodeStore, raw json.RawMessage) error {
	var r tailcfg.DERPRegion
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("the manifest's tunnel region is not valid: %w", err)
	}
	if len(r.Nodes) == 0 {
		return fmt.Errorf("the manifest's tunnel region %d has no relay", r.RegionID)
	}
	b, err := json.Marshal(&r)
	if err != nil {
		return err
	}
	return st.SetNode(ctx, regionKey, string(b))
}

// SavedRegion returns the region saved in st, or nil if none is.
func SavedRegion(ctx context.Context, st NodeStore) (*tailcfg.DERPRegion, error) {
	v, ok, err := st.GetNode(ctx, regionKey)
	if err != nil || !ok {
		return nil, err
	}
	var r tailcfg.DERPRegion
	if err := json.Unmarshal([]byte(v), &r); err != nil {
		return nil, fmt.Errorf("saved tunnel region is not valid JSON: %w", err)
	}
	return &r, nil
}

// EnsureRegion returns the bootstrap region, choosing and saving it the
// first time (SEC-14). want is the tunnel.region setting: a region ID
// or code, or empty to pick the nearest. The saved region is kept while
// want is empty or names it, so the address does not move when the map
// or the latencies change; a setting naming another region chooses that
// one, which changes the address. chosen reports a new choice.
func EnsureRegion(ctx context.Context, st NodeStore, want string, src RegionSource) (r *tailcfg.DERPRegion, chosen bool, err error) {
	saved, err := SavedRegion(ctx, st)
	if err != nil {
		return nil, false, err
	}
	if saved != nil && (want == "" || regionMatches(saved, want)) {
		return saved, false, nil
	}
	m, err := src.DERPMap(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("fetching the DERP map to choose a tunnel region: %w", err)
	}
	var id tailcfg.DERPRegionID
	if want == "" {
		if id, err = src.Pick(ctx, m); err != nil {
			return nil, false, fmt.Errorf("choosing the nearest tunnel region: %w", err)
		}
	} else {
		for rid, reg := range m.Regions {
			if regionMatches(reg, want) {
				id = rid
			}
		}
	}
	r = m.Regions[id]
	if r == nil {
		return nil, false, fmt.Errorf("tunnel.region = %q: no such region in the DERP map; use a region ID or code from it", want)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, false, err
	}
	if err := st.SetNode(ctx, regionKey, string(b)); err != nil {
		return nil, false, err
	}
	return r, true, nil
}

func regionMatches(r *tailcfg.DERPRegion, want string) bool {
	if id, err := strconv.Atoi(want); err == nil {
		return int(r.RegionID) == id
	}
	return strings.EqualFold(r.RegionCode, want)
}
