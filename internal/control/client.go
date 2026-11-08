package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/borje/lendisk/internal/gateway"
)

// Client calls a peer's control API over HTTP whose transport dials the
// peer's tunnel port.
type Client struct {
	HTTP *http.Client
	// Protocol is the version claimed; empty means this build's. Tests
	// set it to stand in for another release.
	Protocol string
}

func (c *Client) protocol() string {
	if c.Protocol == "" {
		return Protocol
	}
	return c.Protocol
}

// CallError is a refusal from the peer.
type CallError struct {
	Status  int
	Message string
	Code    string // the refusal's code, if the peer named one
	// RetryAfter is how long the peer asked to wait (Retry-After, in
	// whole seconds); zero if it did not say.
	RetryAfter time.Duration
}

func (e *CallError) Error() string {
	return fmt.Sprintf("the peer refused (%d): %s", e.Status, e.Message)
}

// ErrNotConnected is in the chain of a call's error when no connection
// to the peer's tunnel port was made: the peer is unreachable, or its
// tunnel refuses this node's key. tailcat ignores a refused key's
// handshake, so the two look alike to the caller (#84).
var ErrNotConnected = errors.New("no connection to the peer")

// ErrNoAnswer is in the chain of a call's error when the request was
// written whole on a connection to the peer and no answer came: the
// peer received it, or the connection broke on the way (#84).
var ErrNoAnswer = errors.New("the request was sent and no answer came")

// stageError is err, marked with how far the call got (ErrNotConnected
// or ErrNoAnswer); its message is err's.
type stageError struct{ stage, err error }

func (e *stageError) Error() string   { return e.err.Error() }
func (e *stageError) Unwrap() []error { return []error{e.stage, e.err} }

// Redeem completes the invite handshake (PEER-2) on a connection dialled
// with the invite key.
func (c *Client) Redeem(ctx context.Context, req RedeemRequest) (RedeemResponse, error) {
	var resp RedeemResponse
	return resp, c.call(ctx, http.MethodPost, RedeemPath, req, &resp)
}

// Remove tells the peer that this node ended the friendship with it
// (PEER-5, #25), so it ends it too.
func (c *Client) Remove(ctx context.Context) error {
	var resp struct{}
	return c.call(ctx, http.MethodPost, RemovePath, struct{}{}, &resp)
}

// Version asks the peer's protocol version (NET-6).
func (c *Client) Version(ctx context.Context) (VersionResponse, error) {
	var resp VersionResponse
	return resp, c.call(ctx, http.MethodGet, VersionPath, nil, &resp)
}

// Do sends one request to path with this client's protocol header and
// returns the raw response; for tests of calls not yet served.
func (c *Client) Do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, "http://lendisk"+path, body)
	if err != nil {
		return nil, err
	}
	r.Header.Set(ProtocolHeader, c.protocol())
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	return c.HTTP.Do(r)
}

// PutManifest stores this node's sealed manifest at the peer (ID-4) and
// checks that the peer's receipt names exactly those bytes.
func (c *Client) PutManifest(ctx context.Context, blob []byte) (ManifestReceipt, error) {
	var rc ManifestReceipt
	b, err := c.send(ctx, http.MethodPut, ManifestPath, bytes.NewReader(blob), "application/octet-stream", maxBody)
	if err != nil {
		return rc, err
	}
	if err := json.Unmarshal(b, &rc); err != nil {
		return rc, fmt.Errorf("the peer's answer to %s is malformed: %w", ManifestPath, err)
	}
	sum := sha256.Sum256(blob)
	if rc.Size != int64(len(blob)) || rc.SHA256 != hex.EncodeToString(sum[:]) {
		return rc, fmt.Errorf("the peer stored %d bytes (sha256 %s), not the %d bytes sent", rc.Size, rc.SHA256, len(blob))
	}
	return rc, nil
}

// GetManifest fetches the manifest the peer keeps for this node's key
// (ID-4); a *CallError with status 404 means it keeps none.
func (c *Client) GetManifest(ctx context.Context) ([]byte, error) {
	return c.send(ctx, http.MethodGet, ManifestPath, nil, "", MaxManifest)
}

// maxListAnswer bounds one page of the list call: MaxListPage keys of at
// most 1024 bytes each, with their sizes and hashes.
const maxListAnswer = 4 << 20

// ListPage reads one page of the list of this node's bucket sealed at
// the peer (NET-7), after the key after, at most limit keys (0 means
// the most the peer serves); a *CallError with status 404 means the
// peer holds no bucket of this node by that name.
func (c *Client) ListPage(ctx context.Context, sealed, after string, limit int) (ListPage, error) {
	q := url.Values{"bucket": {sealed}}
	if after != "" {
		q.Set("after", after)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var p ListPage
	b, err := c.send(ctx, http.MethodGet, ListPath+"?"+q.Encode(), nil, "", maxListAnswer)
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return p, fmt.Errorf("the peer's answer to %s is malformed: %w", ListPath, err)
	}
	// A page holds at most MaxListPage keys, rising strictly from after,
	// and the next page starts past them, so a list ends however the peer
	// answers (#29 review).
	if len(p.Objects) > gateway.MaxListPage {
		return p, fmt.Errorf("the peer's page holds %d keys, more than %d", len(p.Objects), gateway.MaxListPage)
	}
	last := after
	for _, o := range p.Objects {
		if o.Key <= last {
			return p, fmt.Errorf("the peer's list does not rise past %q (it sent %q)", last, o.Key)
		}
		last = o.Key
	}
	if p.Next != "" && p.Next <= after || p.Next != "" && len(p.Objects) > 0 && p.Next < last {
		return p, fmt.Errorf("the peer's list does not move on from %q", after)
	}
	return p, nil
}

func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	b, err := c.send(ctx, method, path, body, "application/json", maxBody)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("the peer's answer to %s is malformed: %w", path, err)
	}
	return nil
}

// send makes one call and returns the body of a 200 answer, at most max
// bytes; any other answer is a *CallError.
func (c *Client) send(ctx context.Context, method, path string, body io.Reader, contentType string, max int64) ([]byte, error) {
	b, _, err := c.sendProto(ctx, method, path, body, contentType, max)
	return b, err
}

// sendProto is send, also returning the protocol version the peer's
// answer carries (NET-6).
func (c *Client) sendProto(ctx context.Context, method, path string, body io.Reader, contentType string, max int64) ([]byte, string, error) {
	r, err := http.NewRequestWithContext(ctx, method, "http://lendisk"+path, body)
	if err != nil {
		return nil, "", err
	}
	r.Header.Set(ProtocolHeader, c.protocol())
	if body != nil {
		r.Header.Set("Content-Type", contentType)
	}
	var connected, wrote atomic.Bool
	r = r.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn:      func(httptrace.GotConnInfo) { connected.Store(true) },
		WroteRequest: func(w httptrace.WroteRequestInfo) { wrote.Store(w.Err == nil) },
	}))
	resp, err := c.HTTP.Do(r)
	if err != nil {
		switch {
		case !connected.Load():
			err = &stageError{ErrNotConnected, err}
		case wrote.Load():
			err = &stageError{ErrNoAnswer, err}
		}
		return nil, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, "", err
	}
	// Every answer from a lendisk control API, refusals included,
	// carries the protocol header (NET-6); one without it is from
	// something else on that port and is not trusted.
	v := resp.Header.Get(ProtocolHeader)
	if v == "" {
		return nil, "", fmt.Errorf("the peer's answer to %s (%d) has no %s header: whatever answered is not a lendisk control API",
			path, resp.StatusCode, ProtocolHeader)
	}
	if err := Compatible(c.protocol(), v); err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		var e errorBody
		if json.Unmarshal(b, &e) != nil || e.Error == "" {
			e.Error = http.StatusText(resp.StatusCode)
		}
		ce := &CallError{Status: resp.StatusCode, Message: e.Error, Code: e.Code}
		if n, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); err == nil && n > 0 {
			ce.RetryAfter = time.Duration(n) * time.Second
		}
		return nil, v, ce
	}
	if int64(len(b)) > max {
		return nil, "", fmt.Errorf("the peer's answer to %s is over %d bytes", path, max)
	}
	return b, v, nil
}
