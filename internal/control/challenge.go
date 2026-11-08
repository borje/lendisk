package control

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
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

// ChallengePath is the challenge call (VER-1, VER-3, #27): the borrower
// names one of its objects at the lender and a byte range of it, or the
// whole object, with a fresh nonce; the lender answers HMAC-SHA256 keyed
// with the nonce over those bytes as it holds them, which it cannot
// compute without them.
const ChallengePath = Prefix + "challenge"

const (
	// MaxChallengeRange is the longest byte range one ranged challenge
	// reads.
	MaxChallengeRange = 1 << 20
	// MinNonce and MaxNonce bound the nonce's length in bytes.
	MinNonce = 16
	MaxNonce = 64
	// DefaultChallengeRate is how many bytes per second a friend's
	// challenges may make this node read, over time
	// (Server.ChallengeRate), and DefaultChallengeBurst how many at once
	// (Server.ChallengeBurst). A full check of a large replica is paced
	// to the rate; the daily ranged challenges stay far inside the
	// burst.
	DefaultChallengeRate  = 16 << 20
	DefaultChallengeBurst = 64 << 20
	// ChallengeCallCost is the least a challenge counts against the
	// pace, however few bytes it reads: it bounds the calls a friend
	// makes, not only their bytes.
	ChallengeCallCost = 4 << 10
)

// Codes of a challenge the lender cannot answer from what it holds: the
// borrower counts each as a failed proof (FailedProof).
const (
	CodeNoSuchKey    = "NoSuchKey"
	CodeInvalidRange = "InvalidRange"
)

// ChallengeRequest is one challenge: the caller's bucket by sealed name,
// a key, and Length bytes from Offset, or the whole object (Whole, with
// Offset and Length zero), with a fresh Nonce.
type ChallengeRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Nonce  []byte `json:"nonce"`
	Offset int64  `json:"offset,omitempty"`
	Length int64  `json:"length,omitempty"`
	Whole  bool   `json:"whole,omitempty"`
}

func (r ChallengeRequest) check() error {
	switch {
	case len(r.Nonce) < MinNonce || len(r.Nonce) > MaxNonce:
		return fmt.Errorf("challenge: the nonce must be %d to %d bytes", MinNonce, MaxNonce)
	case r.Key == "" || gateway.CheckKey(r.Key) != nil:
		return errors.New("challenge: not a key a borrower may name")
	case r.Whole && (r.Offset != 0 || r.Length != 0):
		return errors.New("challenge: a whole-object challenge names no range")
	case !r.Whole && (r.Offset < 0 || r.Offset >= gateway.MaxObjectSize || r.Length < 1 || r.Length > MaxChallengeRange):
		return fmt.Errorf("challenge: the range must start at 0 or later and be 1 to %d bytes", MaxChallengeRange)
	}
	return nil
}

// ChallengeResponse is the lender's answer: the HMAC and how many bytes
// it read.
type ChallengeResponse struct {
	HMAC []byte `json:"hmac"`
	Size int64  `json:"size"`
}

// ChallengeMAC is HMAC-SHA256 keyed with nonce over b, as the lender
// answers it.
func ChallengeMAC(nonce, b []byte) []byte {
	m := hmac.New(sha256.New, nonce)
	m.Write(b)
	return m.Sum(nil)
}

// LentReader reads what this node holds of a borrower's objects, as the
// borrower would through the lender's S3 surface (gateway.LentIndex.Read,
// through Lending): only the borrower's own buckets, never a tomb or a
// reserved key. A negative length reads the whole object. It returns
// gateway.ErrNoLentBucket, gateway.ErrNoLentKey or gateway.ErrBadRange
// when it holds no such bytes. Read answers for borrower alone, which
// the control API takes from the caller, never from the request.
type LentReader interface {
	Read(ctx context.Context, borrower identity.NodeID, sealed, key string, offset, length int64) (io.ReadCloser, error)
}

// FailedProof reports whether err, from Client.Challenge, is the
// lender's answer that it does not hold the bytes: no such bucket or
// key, or a range past the object's end. Any other error (no answer, a
// refusal, a 404 without a code from a node without the call) says
// nothing about the replica.
func FailedProof(err error) bool {
	var ce *CallError
	if !errors.As(err, &ce) {
		return false
	}
	return ce.Status == http.StatusNotFound && (ce.Code == CodeNoSuchBucket || ce.Code == CodeNoSuchKey) ||
		ce.Status == http.StatusRequestedRangeNotSatisfiable && ce.Code == CodeInvalidRange
}

// Challenge makes the challenge call (VER-1) and returns the lender's
// answer; checking it is the caller's.
func (c *Client) Challenge(ctx context.Context, req ChallengeRequest) (ChallengeResponse, error) {
	var resp ChallengeResponse
	b, err := json.Marshal(req)
	if err != nil {
		return resp, err
	}
	body, err := c.send(ctx, http.MethodPost, ChallengePath, bytes.NewReader(b), "application/json", maxBody)
	if err != nil {
		return resp, err
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return resp, fmt.Errorf("the peer's answer to %s is malformed: %w", ChallengePath, err)
	}
	if len(resp.HMAC) != sha256.Size || resp.Size < 0 {
		return resp, fmt.Errorf("the peer's answer to %s is not an HMAC-SHA256", ChallengePath)
	}
	return resp, nil
}

// challenge is the challenge call (VER-1): it reads the range, or the
// whole object, of the caller's own object through Held, keyed by the
// resolved caller alone, and answers the HMAC. Each friend's challenges
// are read one at a time and paced (challengeLimits).
func (s *Server) challenge(w http.ResponseWriter, r *http.Request, c Caller) {
	if c.Role != state.RolePeer {
		Error(w, http.StatusForbidden, "the challenge call is only for a friend")
		return
	}
	if s.Held == nil {
		Error(w, http.StatusNotFound, "this node lends nothing")
		return
	}
	var req ChallengeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
		Error(w, http.StatusBadRequest, "challenge: malformed request")
		return
	}
	if err := req.check(); err != nil {
		Error(w, http.StatusBadRequest, err.Error())
		return
	}
	peer := c.Peer.String()
	wait, ok := s.admitChallenge(peer, s.now())
	if !ok {
		w.Header().Set("Retry-After", strconv.FormatInt(int64(math.Ceil(wait.Seconds())), 10))
		Error(w, http.StatusServiceUnavailable, "challenge: this node reads one challenge of yours at a time, at a bounded pace; retry later")
		return
	}
	var read int64
	defer func() { s.challengeDone(peer, read, s.now()) }()
	offset, length := req.Offset, req.Length
	if req.Whole {
		offset, length = 0, -1
	}
	body, err := s.Held.Read(r.Context(), c.Peer, req.Bucket, req.Key, offset, length)
	switch {
	case errors.Is(err, gateway.ErrNoLentBucket):
		writeJSON(w, http.StatusNotFound, errorBody{Error: err.Error(), Code: CodeNoSuchBucket})
		return
	case errors.Is(err, gateway.ErrNoLentKey):
		writeJSON(w, http.StatusNotFound, errorBody{Error: err.Error(), Code: CodeNoSuchKey})
		return
	case errors.Is(err, gateway.ErrBadRange):
		writeJSON(w, http.StatusRequestedRangeNotSatisfiable, errorBody{Error: err.Error(), Code: CodeInvalidRange})
		return
	case err != nil:
		s.Log.Error("challenge: reading a friend's object failed", "peer", peer, "err", err)
		Error(w, http.StatusInternalServerError, "reading the object failed on the lender; try again")
		return
	}
	defer body.Close()
	m := hmac.New(sha256.New, req.Nonce)
	src := io.Reader(body)
	if !req.Whole {
		src = io.LimitReader(body, length)
	}
	read, err = io.Copy(m, src)
	if err != nil {
		s.Log.Error("challenge: reading a friend's object failed", "peer", peer, "err", err)
		Error(w, http.StatusInternalServerError, "reading the object failed on the lender; try again")
		return
	}
	writeJSON(w, http.StatusOK, ChallengeResponse{HMAC: m.Sum(nil), Size: read})
}

// challengeLimits pace each friend's challenges (LEND-5, #27): a token
// bucket of bytes per friend, ChallengeBurst deep and refilled at
// ChallengeRate, which a call may enter only when it is not in debt and
// no other call of the friend's is reading; the call then takes what it
// read, at least ChallengeCallCost. A whole-object challenge can thus
// overdraw once, and the friend waits for the debt to refill.
type challengeLimits struct {
	mu    sync.Mutex
	peers map[string]*challengePeer
}

type challengePeer struct {
	tokens  float64
	at      time.Time
	reading bool
}

func (s *Server) challengeRate() float64 {
	if s.ChallengeRate > 0 {
		return float64(s.ChallengeRate)
	}
	return DefaultChallengeRate
}

func (s *Server) challengeBurst() float64 {
	if s.ChallengeBurst > 0 {
		return float64(s.ChallengeBurst)
	}
	return DefaultChallengeBurst
}

// refill brings p's tokens to now.
func (s *Server) refill(p *challengePeer, now time.Time) {
	if d := now.Sub(p.at); d > 0 {
		p.tokens = min(p.tokens+d.Seconds()*s.challengeRate(), s.challengeBurst())
	}
	p.at = now
}

// admitChallenge reports whether peer's challenge at now may read; if
// not, how long until it may.
func (s *Server) admitChallenge(peer string, now time.Time) (time.Duration, bool) {
	l := &s.challengeLimits
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.peers == nil {
		l.peers = map[string]*challengePeer{}
	}
	p := l.peers[peer]
	if p == nil {
		p = &challengePeer{tokens: s.challengeBurst(), at: now}
		l.peers[peer] = p
	}
	s.refill(p, now)
	switch {
	case p.reading:
		return time.Second, false
	case p.tokens < 0:
		return time.Duration(-p.tokens / s.challengeRate() * float64(time.Second)), false
	}
	p.reading = true
	// Friends back at a full burst need no entry: forget them.
	for k, o := range l.peers {
		if k != peer && !o.reading {
			if s.refill(o, now); o.tokens >= s.challengeBurst() {
				delete(l.peers, k)
			}
		}
	}
	return 0, true
}

// challengeDone takes what peer's challenge read off its tokens.
func (s *Server) challengeDone(peer string, read int64, now time.Time) {
	l := &s.challengeLimits
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.peers[peer]
	if p == nil {
		return
	}
	s.refill(p, now)
	p.tokens -= float64(max(read, ChallengeCallCost))
	p.reading = false
}
