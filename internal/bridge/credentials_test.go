package bridge

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// #15 review: versitygw removes every whitespace character from the
// Authorization header after its scheme before reading the key, so a
// friend's node ID spelled with spaces or tabs is still the node ID to
// it. The loopback and LAN listeners read it the same way and refuse
// it, and refuse a credential they cannot read at all; on the tunnel
// the key is read the same way, so it must still equal the connection's.
func TestListenersReadSpacedCredentials(t *testing.T) {
	sock, g := startFakeGateway(t)
	h := New(sock, quiet).LoopbackHandler()
	friend := newID(t).NodeID().String()
	half := len(friend) / 2
	rest := "/20261106/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=00ff"
	for name, auth := range map[string]string{
		"space before =":   "AWS4-HMAC-SHA256 Credential =" + friend + rest,
		"space in the key": "AWS4-HMAC-SHA256 Credential=" + friend[:half] + " " + friend[half:] + rest,
		"tab in the key":   "AWS4-HMAC-SHA256 Credential=" + friend[:half] + "\t" + friend[half:] + rest,
		"two spaces":       "AWS4-HMAC-SHA256  Credential=" + friend + rest,
		"unreadable":       "AWS4-HMAC-SHA256 Credential" + friend,
		"another scheme":   "Bearer x",
	} {
		r := httptest.NewRequest("PUT", "/sbucket/key?tagging", nil)
		r.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s on loopback: %d, want 403", name, w.Code)
		}
	}
	if n := g.count(); n != 0 {
		t.Fatalf("%d reached the gateway", n)
	}
	r := httptest.NewRequest("GET", "/sbucket/key", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential ="+friend+rest)
	if k, ok := AccessKeyOf(r); !ok || k != friend {
		t.Fatalf("the tunnel reads %q %v; want the friend's node ID, as versitygw reads it", k, ok)
	}
}
