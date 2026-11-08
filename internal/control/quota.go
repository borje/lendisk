package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/borje/lendisk/internal/state"
)

// QuotaPath is the quota call (LEND-7, LEND-8, PEER-6, #24): each side's
// number for the friendship, its use of the other's space and its
// repository size, exchanged both ways in one call.
const QuotaPath = Prefix + "quota"

// MaxNumber is the largest number, use or repository size the quota call
// carries.
const MaxNumber = 1 << 62

// GiB is the unit the repository size crosses the tunnel in (LEND-7).
const GiB = 1 << 30

// RoundSize rounds a repository size up to a whole GiB: the friend
// learns how big this node's repository is to the GiB, no closer.
func RoundSize(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return min((n+GiB-1)/GiB*GiB, MaxNumber/GiB*GiB)
}

// QuotaRequest is the caller's side: its number for the friendship, its
// count of what it stores at the callee, and its repository size
// (RoundSize).
type QuotaRequest struct {
	Number         int64 `json:"number"`
	Use            int64 `json:"use"`
	RepositorySize int64 `json:"repository_size"`
}

// QuotaResponse is the callee's side, after it applied the caller's
// number: its own number, the quota (the smaller), its count of what it
// stores at the caller, its count as lender of what the caller stores
// with it (LEND-1; the borrower reconciles its own count from it, #68),
// and its repository size. Protocol is the callee's protocol version,
// from the answer's header.
type QuotaResponse struct {
	Number         int64  `json:"number"`
	Quota          int64  `json:"quota"`
	Use            int64  `json:"use"`
	Lent           int64  `json:"lent"`
	RepositorySize int64  `json:"repository_size"`
	Protocol       string `json:"-"`
}

// check refuses figures no node sends.
func (r QuotaRequest) check() error {
	switch {
	case r.Number <= 0 || r.Number > MaxNumber:
		return errors.New("quota: the number must be above zero and at most 2^62")
	case r.Use < 0 || r.Use >= MaxNumber:
		return errors.New("quota: the use must be at least 0 and below 2^62")
	case r.RepositorySize < 0 || r.RepositorySize >= MaxNumber || r.RepositorySize%GiB != 0:
		return errors.New("quota: the repository size must be whole GiB, at least 0 and below 2^62")
	}
	return nil
}

// Check refuses an answer no lendisk node sends.
func (r QuotaResponse) Check() error {
	q := QuotaRequest{Number: r.Number, Use: r.Use, RepositorySize: r.RepositorySize}
	if err := q.check(); err != nil {
		return fmt.Errorf("the peer's answer: %w", err)
	}
	if r.Lent < 0 || r.Lent >= MaxNumber || r.Quota < 0 || r.Quota > r.Number {
		return errors.New("the peer's answer: its lent count or quota is out of range")
	}
	return nil
}

// Quota makes the quota call (LEND-8): it sends this node's side and
// returns the peer's.
func (c *Client) Quota(ctx context.Context, req QuotaRequest) (QuotaResponse, error) {
	var resp QuotaResponse
	b, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	body, proto, err := c.sendProto(ctx, http.MethodPost, QuotaPath, bytes.NewReader(b), "application/json", maxBody)
	if err != nil {
		return resp, err
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return resp, fmt.Errorf("the peer's answer to %s is malformed: %w", QuotaPath, err)
	}
	resp.Protocol = proto
	return resp, resp.Check()
}

// quota is the quota call (LEND-7, LEND-8): the caller's number is in
// force here once the answer is written, so a lowering holds on both
// sides after one call. It is keyed by the resolved caller alone.
func (s *Server) quota(w http.ResponseWriter, r *http.Request, c Caller) {
	if c.Role != state.RolePeer {
		Error(w, http.StatusForbidden, "the quota call is only for a friend")
		return
	}
	var req QuotaRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
		Error(w, http.StatusBadRequest, "quota: malformed request")
		return
	}
	if err := req.check(); err != nil {
		Error(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, peer := r.Context(), c.Peer.String()
	f, err := s.DB.QuotaReported(ctx, peer, state.QuotaReport{Number: req.Number, Use: req.Use,
		RepositorySize: req.RepositorySize, Protocol: r.Header.Get(ProtocolHeader)}, s.now())
	if errors.Is(err, state.ErrNotFound) {
		Error(w, http.StatusForbidden, state.NoRelationship)
		return
	}
	var lent, size int64
	var use map[string]int64
	if err == nil {
		lent, _, err = s.DB.LentUsage(ctx, peer)
	}
	if err == nil {
		use, err = s.DB.PlaceUse(ctx)
	}
	if err == nil {
		size, err = s.DB.RepositorySize(ctx)
	}
	if err != nil {
		s.Log.Error("quota: applying a friend's number failed", "peer", peer, "err", err)
		Error(w, http.StatusInternalServerError, "the quota call failed on this node; try again")
		return
	}
	if s.Reported != nil {
		s.Reported(peer)
	}
	s.Log.Debug("quota call answered", "peer", peer, "their_number", req.Number, "quota", f.Quota())
	writeJSON(w, http.StatusOK, QuotaResponse{Number: f.OwnNumber, Quota: f.Quota(),
		Use: min(max(use[peer], 0), MaxNumber-1), Lent: min(lent, MaxNumber-1), RepositorySize: RoundSize(size)})
}
