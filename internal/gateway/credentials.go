package gateway

import (
	"strings"
	"unicode"
)

// Credentials is what a request's credentials name, read without
// verifying anything, the way versitygw v1.10.0 reads them
// (internal/sigv4auth: ParseAuthorization, ParseCredentials,
// ParseQueryAuthorization). Re-check on every versitygw upgrade.
type Credentials struct {
	// Keys are the access keys versitygw would look up: from the
	// Authorization header and from each X-Amz-Credential in the query.
	Keys []string
	// Friend is set when any key, or any candidate that differs from
	// one only by whitespace, is a node ID: a friend's access key
	// (NET-7a).
	Friend bool
	// Readable is false when a credential is present that cannot be
	// read as versitygw reads it: more than one Authorization header,
	// another scheme (SigV2 included, which versitygw refuses), a
	// malformed header, a malformed X-Amz-Credential, or an
	// AWSAccessKeyId. Such a request is refused: versitygw would refuse
	// it too, and a reading that differs from versitygw's must never
	// pass as "no friend".
	Readable bool
}

// ReadCredentials reads the credentials of a request with the given
// Authorization header values and query.
func ReadCredentials(authorization []string, query [][2]string) Credentials {
	c := Credentials{Readable: true}
	var near []string // whitespace-free spellings, checked for node IDs too
	switch len(authorization) {
	case 0:
	case 1:
		if authorization[0] == "" {
			break
		}
		k, ok := headerAccess(authorization[0])
		if ok {
			c.Keys = append(c.Keys, k)
		} else {
			c.Readable = false
		}
		near = append(near, credentialsIn(removeSpace(authorization[0]))...)
	default:
		c.Readable = false
		for _, h := range authorization {
			near = append(near, credentialsIn(removeSpace(h))...)
		}
	}
	for _, kv := range query {
		switch {
		case kv[0] == "X-Amz-Credential":
			parts := strings.Split(kv[1], "/")
			if len(parts) != 5 {
				c.Readable = false
			} else {
				c.Keys = append(c.Keys, parts[0])
			}
			near = append(near, strings.Split(removeSpace(kv[1]), "/")[0])
		case strings.EqualFold(kv[0], "X-Amz-Credential"), strings.EqualFold(kv[0], "AWSAccessKeyId"):
			c.Readable = false
			near = append(near, strings.Split(removeSpace(kv[1]), "/")[0])
		}
	}
	for _, k := range append(near, c.Keys...) {
		if _, ok := borrowerOf(k); ok {
			c.Friend = true
		}
	}
	return c
}

// headerAccess is the access key of a SigV4 Authorization header exactly
// as sigv4auth.ParseAuthorization finds it: split at the first space,
// each part stripped of every whitespace character if it holds a space,
// the scheme AWS4-HMAC-SHA256, then three comma-separated key=value
// pairs, Credential first, whose value has five slash-separated parts.
func headerAccess(h string) (string, bool) {
	parts := strings.SplitN(h, " ", 2)
	for i, p := range parts {
		if strings.Contains(p, " ") {
			parts[i] = removeSpace(p)
		}
	}
	if len(parts) < 2 || parts[0] != "AWS4-HMAC-SHA256" {
		return "", false
	}
	kvs := strings.Split(parts[1], ",")
	if len(kvs) != 3 {
		return "", false
	}
	kv := strings.Split(kvs[0], "=")
	if len(kv) != 2 || kv[0] != "Credential" {
		return "", false
	}
	scope := strings.Split(kv[1], "/")
	if len(scope) != 5 {
		return "", false
	}
	return scope[0], true
}

// credentialsIn returns every value after "Credential=" in s, up to the
// next slash or comma, ignoring case: where a key could be, however the
// header is laid out.
func credentialsIn(s string) []string {
	var out []string
	low := strings.ToLower(s)
	for i := strings.Index(low, "credential="); i >= 0; {
		rest := s[i+len("credential="):]
		end := strings.IndexAny(rest, "/,")
		if end < 0 {
			end = len(rest)
		}
		out = append(out, rest[:end])
		next := strings.Index(low[i+1:], "credential=")
		if next < 0 {
			break
		}
		i += 1 + next
	}
	// SigV2's "AWS key:sig".
	if k, ok := strings.CutPrefix(s, "AWS"); ok && !strings.HasPrefix(k, "4") {
		key, _, _ := strings.Cut(k, ":")
		out = append(out, key)
	}
	return out
}

// removeSpace drops every whitespace character, as versitygw's does.
func removeSpace(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
}
