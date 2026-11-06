package control

import (
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/version"
)

// maxBody bounds a control API request body.
const maxBody = 64 << 10

// Server answers the control API for the node Self.
type Server struct {
	DB   *state.DB
	Self *identity.Identity
	// Changed is called after a friendship or invite is committed, so
	// admission forgets its rejections (Admission.Clear, SEC-15).
	Changed func()
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	Log *slog.Logger
}

func (s *Server) now() time.Time {
	if s.Now == nil {
		return time.Now()
	}
	return s.Now()
}

// ServeHTTP routes one control API request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(ProtocolHeader, Protocol)
	c, ok := CallerFrom(r.Context())
	if !ok || c.Role == state.RoleNone {
		// The middleware never lets this through; refuse anyway.
		Error(w, http.StatusForbidden, state.NoRelationship)
		return
	}
	if r.URL.Path == VersionPath {
		// Answered whatever the caller speaks, so it can tell why it is
		// refused elsewhere.
		if !s.method(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, http.StatusOK, VersionResponse{Protocol: Protocol, Version: version.Version})
		return
	}
	if err := Compatible(Protocol, r.Header.Get(ProtocolHeader)); err != nil {
		Error(w, http.StatusBadRequest, err.Error())
		return
	}
	switch r.URL.Path {
	case RedeemPath:
		if s.method(w, r, http.MethodPost) {
			s.redeem(w, r, c)
		}
	default:
		Error(w, http.StatusNotFound, "no such control API call on this node")
	}
}

func (s *Server) method(w http.ResponseWriter, r *http.Request, m string) bool {
	if r.Method != m {
		w.Header().Set("Allow", m)
		Error(w, http.StatusMethodNotAllowed, r.URL.Path+" takes "+m)
		return false
	}
	return true
}

// redeem is the invite handshake (PEER-2). The caller is the invite key;
// the redeeming node's own key is the one its address names, proven by
// RedeemProof. The invite is marked used by that key in the transaction
// that stores the friendship, and the invite key's connection is closed
// after the answer (SEC-13).
//
// The redeemer's dial key (#69) is bound by its proof and proven by
// DialProof, and stored on the friendship; the answer carries this
// node's dial key with its own DialProof.
//
// The proven key may have lost the answer (#51, #56): if it is a friend
// already, any unexpired invite answers it with that friendship's terms
// (state.RedeemInvite). The proof binds the invite key of this
// connection, so one seen elsewhere does not carry over. Every other
// key is refused.
func (s *Server) redeem(w http.ResponseWriter, r *http.Request, c Caller) {
	w.Header().Set("Connection", "close")
	if c.Role != state.RoleInvite {
		Error(w, http.StatusForbidden, "redeem is only for a pending invite's key")
		return
	}
	var req RedeemRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
		Error(w, http.StatusBadRequest, "redeem: malformed request")
		return
	}
	friend, err := transport.KeyOf(req.Address)
	switch {
	case err != nil:
		Error(w, http.StatusBadRequest, "redeem: "+err.Error())
		return
	case friend == s.Self.NodeID():
		Error(w, http.StatusBadRequest, "redeem: this is the inviting node's own address")
		return
	case req.Number <= 0 || req.RetentionSeconds <= 0:
		Error(w, http.StatusBadRequest, "redeem: number and retention must be positive")
		return
	}
	if req.DialKey == "" {
		Error(w, http.StatusBadRequest, "redeem: no dial key: upgrade lendisk on the redeeming node")
		return
	}
	dial, err := identity.ParseNodeID(req.DialKey)
	switch {
	case err != nil:
		Error(w, http.StatusBadRequest, "redeem: dial key: "+err.Error())
		return
	case dial == friend || dial == s.Self.NodeID() || dial == s.Self.DialKey().NodeID():
		Error(w, http.StatusBadRequest, "redeem: the dial key is a node ID or this node's dial key")
		return
	}
	want, err := RedeemProof(s.Self, friend, c.Peer, dial)
	if err != nil || !hmac.Equal(want, req.Proof) {
		Error(w, http.StatusForbidden, "redeem: the proof does not match the address's key and the dial key")
		return
	}
	if !checkDialProof(s.Self, dial, c.Peer, req.DialProof) {
		Error(w, http.StatusForbidden, "redeem: the dial key's proof does not match the dial key")
		return
	}
	ownDial := s.Self.DialKey()
	dialProof, err := DialProof(ownDial, friend, c.Peer)
	if err != nil {
		Error(w, http.StatusInternalServerError, "redeem failed on the inviting node; try again")
		return
	}
	f, err := s.DB.RedeemInvite(r.Context(), c.Peer.String(), state.Friendship{
		Peer:          friend.String(),
		Address:       req.Address,
		PeerNumber:    req.Number,
		PeerRetention: time.Duration(req.RetentionSeconds) * time.Second,
		DialKey:       dial.String(),
	}, s.now())
	switch {
	case errors.Is(err, state.ErrInviteSpent):
		Error(w, http.StatusConflict, "this invite was already used or has expired; ask for a new one")
		return
	case errors.Is(err, state.ErrExists):
		Error(w, http.StatusConflict, "you are already friends with this node")
		return
	case errors.Is(err, state.ErrDialKeyTaken):
		Error(w, http.StatusConflict, "redeem: the dial key already names another friend of this node")
		return
	case err != nil:
		s.Log.Error("redeem: storing the friendship failed", "err", err)
		Error(w, http.StatusInternalServerError, "redeem failed on the inviting node; try again")
		return
	}
	if s.Changed != nil {
		s.Changed()
	}
	s.Log.Info("invite redeemed", "peer", f.Peer, "quota", f.Quota())
	writeJSON(w, http.StatusOK, RedeemResponse{Number: f.OwnNumber, RetentionSeconds: int64(f.OwnRetention / time.Second),
		DialKey: ownDial.NodeID().String(), DialProof: dialProof})
}

// Error writes a control API error.
func Error(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorBody{Error: msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
