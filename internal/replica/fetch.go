package replica

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Fetcher opens objects at friends for the gateway's read-through
// (EP-10, ID-6; gateway.Fetcher): one Friend client per friend, built on
// first use with the friend's address from lendisk.db and kept while the
// address stays the same.
type Fetcher struct {
	DB     *state.DB
	Self   *identity.Identity
	Dial   DialFunc
	Client Options // a zero DialTimeout is DefaultFetchDial

	mu      sync.Mutex
	clients map[string]fetchClient
}

type fetchClient struct {
	addr string
	f    *Friend
}

// DefaultFetchDial bounds opening the tunnel to a friend for a fetch: a
// reader waits for it, so an offline friend is given up on sooner than
// replication's default.
const DefaultFetchDial = 10 * time.Second

// Fetch opens key of bucket at place, a friend. The caller verifies the
// bytes and closes the body.
func (x *Fetcher) Fetch(ctx context.Context, place, bucket, key string) (io.ReadCloser, error) {
	fr, err := x.DB.GetFriendship(ctx, place)
	if err != nil {
		return nil, fmt.Errorf("%s is not a friend: %w", place, err)
	}
	if fr.Address == "" {
		return nil, fmt.Errorf("no address for %s", place)
	}
	c, err := x.client(place, fr.Address)
	if err != nil {
		return nil, err
	}
	return c.Get(ctx, bucket, key)
}

func (x *Fetcher) client(place, addr string) (*Friend, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if c, ok := x.clients[place]; ok && c.addr == addr {
		return c.f, nil
	}
	peer, err := identity.ParseNodeID(place)
	if err != nil {
		return nil, err
	}
	o := x.Client
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultFetchDial
	}
	f, err := NewFriend(x.Self, peer, addr, x.Dial, o)
	if err != nil {
		return nil, err
	}
	if x.clients == nil {
		x.clients = map[string]fetchClient{}
	}
	x.clients[place] = fetchClient{addr: addr, f: f}
	return f, nil
}
