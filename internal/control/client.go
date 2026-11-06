package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
}

func (e *CallError) Error() string {
	return fmt.Sprintf("the peer refused (%d): %s", e.Status, e.Message)
}

// Redeem completes the invite handshake (PEER-2) on a connection dialled
// with the invite key.
func (c *Client) Redeem(ctx context.Context, req RedeemRequest) (RedeemResponse, error) {
	var resp RedeemResponse
	return resp, c.call(ctx, http.MethodPost, RedeemPath, req, &resp)
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

func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	resp, err := c.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return err
	}
	if v := resp.Header.Get(ProtocolHeader); v != "" {
		if err := Compatible(c.protocol(), v); err != nil {
			return err
		}
	}
	if resp.StatusCode != http.StatusOK {
		var e errorBody
		if json.Unmarshal(b, &e) != nil || e.Error == "" {
			e.Error = http.StatusText(resp.StatusCode)
		}
		return &CallError{Status: resp.StatusCode, Message: e.Error}
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("the peer's answer to %s is malformed: %w", path, err)
	}
	return nil
}
