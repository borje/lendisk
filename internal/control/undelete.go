package control

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// UndeletePath is the undelete call (RET-3, #32): the borrower names one
// of its buckets at the lender and a time; the lender moves the bucket's
// tombs deleted since then back to their keys and drops their records,
// a page of keys at a time, and answers what it restored and what it
// left as it was.
const UndeletePath = Prefix + "undelete"

const (
	// DefaultUndeleteRate is how many keys per second a friend's undelete
	// calls may make this node look at, over time (Server.UndeleteRate),
	// and DefaultUndeleteBurst how many at once (Server.UndeleteBurst):
	// two full pages. A restic repository of 100 GB has about 6,000
	// packs: undeleting all of them takes under a minute.
	DefaultUndeleteRate  = 100
	DefaultUndeleteBurst = 2 * gateway.MaxListPage
	// UndeleteCallCost is the least an undelete call counts against the
	// pace, however few keys it looks at: it bounds the calls a friend
	// makes, not only their keys.
	UndeleteCallCost = 10
)

// UndeleteRequest is one page of an undelete: the caller's bucket by
// sealed name, the time since which its tombs come back (Unix seconds; 0
// for every tomb in the window), whether rewrites' tombs (#74) count
// too, the key the page starts after, and at most how many keys it
// covers (0 for gateway.MaxListPage).
type UndeleteRequest struct {
	Bucket   string `json:"bucket"`
	Since    int64  `json:"since"`
	Rewrites bool   `json:"rewrites,omitempty"`
	After    string `json:"after,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

func (r UndeleteRequest) check() error {
	switch {
	case r.Bucket == "":
		return errors.New("undelete: name the bucket")
	case r.Since < 0:
		return errors.New("undelete: since must be 0 or a time after 1970, in Unix seconds")
	case r.Limit < 0 || r.Limit > gateway.MaxListPage:
		return fmt.Errorf("undelete: limit must be 1 to %d", gateway.MaxListPage)
	}
	return nil
}

// UndeleteResponse is the lender's answer: the keys it restored, each
// with the version's size, hash and when it was tombstoned; the keys it
// left as they were; and the key the next page starts after, "" after
// the last.
type UndeleteResponse struct {
	Restored []ListObject   `json:"restored"`
	Skipped  []UndeleteSkip `json:"skipped,omitempty"`
	Next     string         `json:"next,omitempty"`
}

// UndeleteSkip is a key the lender left as it was: its version that would
// have come back, what the key holds, and why (gateway.SkipConflict:
// other bytes; gateway.SkipLive: the same).
type UndeleteSkip struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Live   string `json:"live"`
	Reason string `json:"reason"`
}

// Undeleter brings a borrower's tombs back (gateway.Gateway.Undelete).
// It must answer for borrower alone, which the control API takes from
// the caller, never from the request.
type Undeleter interface {
	Undelete(ctx context.Context, borrower identity.NodeID, sealed string, since time.Time, rewrites bool,
		after string, limit int) (gateway.Undeleted, error)
}

// maxUndeleteAnswer bounds one page of the undelete call, as a list
// page's.
const maxUndeleteAnswer = maxListAnswer

// Undelete makes the undelete call (RET-3) for one page; a *CallError
// with status 404 and CodeNoSuchBucket means the peer holds no bucket of
// this node by that name, and 503 to retry after RetryAfter.
func (c *Client) Undelete(ctx context.Context, req UndeleteRequest) (UndeleteResponse, error) {
	var resp UndeleteResponse
	b, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	body, err := c.send(ctx, http.MethodPost, UndeletePath, bytes.NewReader(b), "application/json", maxUndeleteAnswer)
	if err != nil {
		return resp, err
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return resp, fmt.Errorf("the peer's answer to %s is malformed: %w", UndeletePath, err)
	}
	// A page covers at most a page of keys, each past after, and the
	// next starts past them, so an undelete ends however the peer answers.
	if len(resp.Restored)+len(resp.Skipped) > gateway.MaxListPage {
		return resp, fmt.Errorf("the peer's undelete page holds %d keys, more than %d", len(resp.Restored)+len(resp.Skipped), gateway.MaxListPage)
	}
	last := req.After
	for _, o := range resp.Restored {
		if o.Key <= req.After {
			return resp, fmt.Errorf("the peer restored %q, not after %q", o.Key, req.After)
		}
		last = max(last, o.Key)
	}
	for _, s := range resp.Skipped {
		if s.Key <= req.After {
			return resp, fmt.Errorf("the peer skipped %q, not after %q", s.Key, req.After)
		}
		last = max(last, s.Key)
	}
	if resp.Next != "" && (resp.Next <= req.After || resp.Next < last) {
		return resp, fmt.Errorf("the peer's undelete does not move on from %q", req.After)
	}
	return resp, nil
}

// undelete is the undelete call (RET-3): it brings the caller's own tombs
// of one bucket back through Undeleter, keyed by the resolved caller
// alone. Each friend's calls run one at a time and are paced in keys
// (undeleteLimits).
func (s *Server) undelete(w http.ResponseWriter, r *http.Request, c Caller) {
	if c.Role != state.RolePeer {
		Error(w, http.StatusForbidden, "the undelete call is only for a friend")
		return
	}
	if s.Undeleter == nil {
		Error(w, http.StatusNotFound, "this node lends nothing")
		return
	}
	var req UndeleteRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
		Error(w, http.StatusBadRequest, "undelete: malformed request")
		return
	}
	if err := req.check(); err != nil {
		Error(w, http.StatusBadRequest, err.Error())
		return
	}
	peer := c.Peer.String()
	wait, ok := s.admitUndelete(peer, s.now())
	if !ok {
		w.Header().Set("Retry-After", strconv.FormatInt(max(int64(math.Ceil(wait.Seconds())), 1), 10))
		Error(w, http.StatusServiceUnavailable, "undelete: this node runs one undelete of yours at a time, at a bounded pace; retry later")
		return
	}
	var looked int
	defer func() { s.undeleteDone(peer, looked, s.now()) }()
	limit := cmp.Or(req.Limit, gateway.MaxListPage)
	u, err := s.Undeleter.Undelete(r.Context(), c.Peer, req.Bucket, time.Unix(req.Since, 0), req.Rewrites, req.After, limit)
	looked = len(u.Restored) + len(u.Skipped)
	switch {
	case errors.Is(err, gateway.ErrNoLentBucket):
		writeJSON(w, http.StatusNotFound, errorBody{Error: err.Error(), Code: CodeNoSuchBucket})
		return
	case errors.Is(err, gateway.ErrNotServing):
		w.Header().Set("Retry-After", "5")
		Error(w, http.StatusServiceUnavailable, "undelete: this node is starting; retry shortly")
		return
	case err != nil:
		s.Log.Error("undelete: restoring a friend's tombs failed", "peer", peer, "err", err)
		Error(w, http.StatusInternalServerError, "undelete failed on the lender; try again")
		return
	}
	resp := UndeleteResponse{Restored: make([]ListObject, 0, len(u.Restored)), Next: u.Next}
	for _, o := range u.Restored {
		at := o.TombstonedAt.UTC()
		resp.Restored = append(resp.Restored, ListObject{Key: o.Key, Size: o.Size, SHA256: o.SHA256, TombstonedAt: &at})
	}
	for _, k := range u.Skipped {
		resp.Skipped = append(resp.Skipped, UndeleteSkip{Key: k.Key, SHA256: k.SHA256, Live: k.Live, Reason: k.Reason})
	}
	if len(u.Restored) > 0 {
		s.Log.Info("undelete: restored a friend's deleted objects", "peer", peer, "restored", len(u.Restored),
			"skipped", len(u.Skipped))
	}
	writeJSON(w, http.StatusOK, resp)
}

// undeleteLimits pace each friend's undelete calls (LEND-5, #32): per
// friend, a token bucket of keys looked at (UndeleteBurst deep, refilled
// at UndeleteRate). A call may enter only when the bucket is not in
// debt and no other undelete of the friend's runs; it then takes the
// keys it looked at, at least UndeleteCallCost. A full page may thus
// overdraw once, and the friend waits for the debt.
type undeleteLimits struct {
	mu    sync.Mutex
	peers map[string]*undeletePeer
}

type undeletePeer struct {
	b       bucket
	running bool
}

func (s *Server) undeletePace() (rate, burst float64) {
	return float64(cmp.Or(s.UndeleteRate, DefaultUndeleteRate)), float64(cmp.Or(s.UndeleteBurst, DefaultUndeleteBurst))
}

func (s *Server) refillUndelete(b *bucket, now time.Time) {
	rate, burst := s.undeletePace()
	if d := now.Sub(b.at); d > 0 {
		b.tokens = min(b.tokens+d.Seconds()*rate, burst)
	}
	b.at = now
}

// admitUndelete reports whether peer's undelete at now may run; if not,
// how long until it may.
func (s *Server) admitUndelete(peer string, now time.Time) (time.Duration, bool) {
	l := &s.undeleteLimits
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.peers == nil {
		l.peers = map[string]*undeletePeer{}
	}
	rate, burst := s.undeletePace()
	p := l.peers[peer]
	if p == nil {
		p = &undeletePeer{b: bucket{tokens: burst, at: now}}
		l.peers[peer] = p
	}
	s.refillUndelete(&p.b, now)
	switch {
	case p.running:
		return time.Second, false
	case p.b.tokens < 0:
		return time.Duration(-p.b.tokens / rate * float64(time.Second)), false
	}
	p.running = true
	// Friends back at a full burst need no entry: forget them.
	for k, o := range l.peers {
		if k != peer && !o.running {
			s.refillUndelete(&o.b, now)
			if o.b.tokens >= burst {
				delete(l.peers, k)
			}
		}
	}
	return 0, true
}

// undeleteDone takes the keys peer's undelete looked at off its bucket.
func (s *Server) undeleteDone(peer string, looked int, now time.Time) {
	l := &s.undeleteLimits
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.peers[peer]
	if p == nil {
		return
	}
	s.refillUndelete(&p.b, now)
	p.b.tokens -= float64(max(looked, UndeleteCallCost))
	p.running = false
}
