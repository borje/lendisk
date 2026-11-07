package gateway

import (
	"slices"
	"strings"
	"testing"
)

// spacedCredentials are Authorization headers naming access the way
// versitygw v1.10.0 reads them once it has removed every whitespace
// character after the scheme (sigv4auth.ParseAuthorization), though the
// literal "Credential=<access>" appears nowhere in them (#15 review).
func spacedCredentials(access string) map[string]string {
	half := len(access) / 2
	rest := "/20261107/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=00ff"
	return map[string]string{
		"space before =":   "AWS4-HMAC-SHA256 Credential =" + access + rest,
		"space after =":    "AWS4-HMAC-SHA256 Credential= " + access + rest,
		"space in the key": "AWS4-HMAC-SHA256 Credential=" + access[:half] + " " + access[half:] + rest,
		"tab in the key":   "AWS4-HMAC-SHA256 Credential=" + access[:half] + "\t" + access[half:] + rest,
		"no space at all":  "AWS4-HMAC-SHA256 Credential=" + access + strings.ReplaceAll(rest, " ", ""),
		"two spaces":       "AWS4-HMAC-SHA256  Credential=" + access + rest,
		"vertical tab":     "AWS4-HMAC-SHA256 Credential=\v" + access + rest,
		"in the middle":    "AWS4-HMAC-SHA256 SignedHeaders=host, Credential=" + access + "/20261107/us-east-1/s3/aws4_request, Signature=00",
	}
}

// #15 review: a friend's node ID is found in every spelling versitygw
// reads as that access key, and a credential that cannot be read is
// unreadable, never "not a friend".
func TestCredentialsReadAsVersitygwReadsThem(t *testing.T) {
	friend := newIdentity(t).NodeID().String()
	for name, h := range spacedCredentials(friend) {
		c := ReadCredentials([]string{h}, nil)
		if !c.Friend && c.Readable {
			t.Errorf("%s: read as %+v", name, c)
		}
	}
	for name, c := range map[string]Credentials{
		"none":             ReadCredentials(nil, nil),
		"an own key":       ReadCredentials([]string{"AWS4-HMAC-SHA256 Credential=LDKAAAAAAAAAAAAAAAAA/20261107/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"}, nil),
		"an own presigned": ReadCredentials(nil, [][2]string{{"X-Amz-Credential", "LDKAAAAAAAAAAAAAAAAA/20261107/us-east-1/s3/aws4_request"}}),
	} {
		if c.Friend || !c.Readable {
			t.Errorf("%s: read as %+v", name, c)
		}
	}
	if c := ReadCredentials([]string{"AWS4-HMAC-SHA256 Credential=LDKAAAAAAAAAAAAAAAAA/20261107/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"}, nil); !slices.Equal(c.Keys, []string{"LDKAAAAAAAAAAAAAAAAA"}) {
		t.Errorf("an own key's keys: %v", c.Keys)
	}
	for name, c := range map[string]Credentials{
		"two headers":           ReadCredentials([]string{"AWS4-HMAC-SHA256 Credential=a/b/c/s3/aws4_request, SignedHeaders=host, Signature=00", "x"}, nil),
		"a malformed header":    ReadCredentials([]string{"AWS4-HMAC-SHA256 Credential" + friend}, nil),
		"another scheme":        ReadCredentials([]string{"Bearer " + friend}, nil),
		"SigV2":                 ReadCredentials([]string{"AWS " + friend + ":c2ln"}, nil),
		"a short presigned key": ReadCredentials(nil, [][2]string{{"X-Amz-Credential", friend}}),
		"a presigned v2 friend": ReadCredentials(nil, [][2]string{{"AWSAccessKeyId", friend}}),
		"a spaced presigned":    ReadCredentials(nil, [][2]string{{"X-Amz-Credential", " " + friend + "/20261107/us-east-1/s3/aws4_request"}}),
	} {
		if !c.Friend && c.Readable {
			t.Errorf("%s: read as a stranger's readable credential: %+v", name, c)
		}
	}
}

// #15 review, NET-7: the spellings above, sent through versitygw's router
// behind the gate as a borrower's refused calls, are refused before
// routing.
func TestGateRefusesSpacedFriendCredentials(t *testing.T) {
	r := startRouter(t, true)
	friend := newIdentity(t).NodeID().String()
	for name, h := range spacedCredentials(friend) {
		for _, c := range []struct{ method, target string }{
			{"PUT", "/sbucket/k?tagging"},
			{"PUT", "/sbucket?object-lock"},
			{"GET", "/sbucket?versions"},
		} {
			a := r.send(t, c.method, c.target, map[string]string{"Authorization": h, "X-Amz-Date": "20261107T000000Z",
				"X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD"})
			if a.status < 400 || len(a.actions) != 0 { // fasthttp itself answers 400 to some
				t.Errorf("%s: %s %s: %d, routed %v: %s", name, c.method, c.target, a.status, a.actions, a.body)
			}
		}
	}
}
