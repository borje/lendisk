package gateway

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3err"
)

// Gate is lendisk's fiber middleware in front of versitygw's routing and
// authentication (D4, NET-7, SEC-8, SEC-9). It refuses by the shape of
// the request, before versitygw does anything with it. For every
// account:
//
//   - every PATCH: S3 has none, and every route of the admin API embedgw
//     mounts on the socket is one (SEC-8);
//   - object lock: CreateBucket asking for it, PutObjectLockConfiguration
//     (posix turns lock on for an existing bucket), retention and legal
//     hold writes, and any X-Amz-Object-Lock-* header (D15, Q-H);
//   - a credential it cannot read as versitygw reads it (ReadCredentials);
//   - the control API path, however spelled, raw or decoded the way
//     versitygw decodes it; it belongs to the bridge on the tunnel and to
//     nothing behind it;
//   - a browser-form POST upload (a POST to a bucket that is not a
//     multi-delete): it reaches the backend's PutObject, but no backup tool
//     uses it and its policy-signed form is a second way in (Q-H, §3.3).
//
// For a borrower's account, a request whose access key (read unverified,
// as the bridge does; versitygw verifies it next) is a node ID, it
// admits exactly the Admitted ops of Surface in their exact shape and
// refuses everything else (NET-7, SEC-9): see borrowerRefusal.
//
// Server-side copies are refused on own buckets by the decorator, which
// knows the bucket map (Q-H).
func Gate() s3api.Option {
	return s3api.WithMiddleware("/", func(c fiber.Ctx) error {
		if msg := refuse(requestOf(c)); msg != "" {
			return deny(c, msg)
		}
		return c.Next()
	})
}

// ControlPrefix is the control API's path on the tunnel (§3.7).
const ControlPrefix = "/_lendisk/"

// IsControlPath reports whether p names the control API once cleaned:
// /_lendisk itself, or anything under it however spelled (//_lendisk/x,
// /b/../_lendisk/x). The request is not rewritten; this only decides
// whether to refuse it (#53).
func IsControlPath(p string) bool {
	c := path.Clean("/" + p)
	prefix := strings.TrimSuffix(ControlPrefix, "/")
	return c == prefix || strings.HasPrefix(c, prefix+"/")
}

// request is what the gate looks at.
type request struct {
	method string
	path   string      // as sent, before any decoding (fasthttp's PathOriginal)
	query  [][2]string // the query's keys and values, in order, as fasthttp parses them
	header [][2]string // the headers' names and values
}

func requestOf(c fiber.Ctx) request {
	r := request{method: c.Method(), path: string(c.Request().URI().PathOriginal())}
	for k, v := range c.Request().URI().QueryArgs().All() {
		r.query = append(r.query, [2]string{string(k), string(v)})
	}
	for k, v := range c.Request().Header.All() {
		r.header = append(r.header, [2]string{string(k), string(v)})
	}
	return r
}

// get is the first value of header h, ignoring case.
func (r request) get(h string) string {
	for _, kv := range r.header {
		if strings.EqualFold(kv[0], h) {
			return kv[1]
		}
	}
	return ""
}

func (r request) has(key string) bool {
	for _, kv := range r.query {
		if kv[0] == key {
			return true
		}
	}
	return false
}

// value is the query's first value for key, as fiber's Query reads it.
func (r request) value(key string) string {
	for _, kv := range r.query {
		if kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

// refuse returns why a request is refused, or "" to let it through.
func refuse(r request) string {
	decoded, err := url.PathUnescape(r.path)
	switch {
	case r.method == http.MethodPatch:
		return "lendisk: the admin API is not served"
	case IsControlPath(r.path) || err == nil && IsControlPath(decoded):
		return "lendisk: control API not served here"
	case r.method == http.MethodPut && strings.EqualFold(strings.TrimSpace(r.get("X-Amz-Bucket-Object-Lock-Enabled")), "true"),
		r.method == http.MethodPut && (r.has("object-lock") || r.has("retention") || r.has("legal-hold")),
		r.hasHeaderPrefix("X-Amz-Object-Lock-"):
		// posix grants lock to a new bucket that asks, turns it on for an
		// existing one (PutObjectLockConfiguration), and then binds
		// objects by retention, legal hold or these headers (D15, Q-H).
		return "lendisk: object lock is not offered"
	case r.method == http.MethodPost && !r.has("delete") && isBucketPath(decoded):
		return "lendisk: browser-form uploads are not offered; use PUT"
	}
	creds := ReadCredentials(r.all("Authorization"), r.query)
	if !creds.Readable {
		return "lendisk: the request's credential cannot be read; sign it with SigV4"
	}
	if creds.Friend {
		if err != nil {
			return "lendisk: the request path is malformed"
		}
		return borrowerRefusal(r, decoded)
	}
	return ""
}

// all is every value of header h, ignoring case.
func (r request) all(h string) []string {
	var out []string
	for _, kv := range r.header {
		if strings.EqualFold(kv[0], h) {
			out = append(out, kv[1])
		}
	}
	return out
}

// refusedHeaders are headers that would make an admitted op do something
// NET-7 does not offer: copy, lock, retain, tag, grant, or encrypt with
// a key the lender never sees (Q-H, RET-2). A borrower's request with
// any header beginning with one of these is refused.
var refusedHeaders = []string{
	"X-Amz-Copy-Source",
	"X-Amz-Object-Lock-",
	"X-Amz-Bypass-Governance-Retention",
	"X-Amz-Tagging",
	"X-Amz-Acl",
	"X-Amz-Grant-",
	"X-Amz-Server-Side-Encryption",
	"X-Amz-Bucket-Object-Lock-Enabled",
}

// errNotOffered is the gate's answer to a borrower outside the list.
const errNotOffered = "lendisk: this operation is not offered to a friend's node (NET-7)"

// borrowerRefusal admits a borrower's request only when it has the exact
// shape of one Admitted op in Surface: its method and level, that op's
// query keys present, no query key beyond those, the op's Allow and the
// signature's, and no refused header. An object's key must be one a
// borrower may name (checkKey: nothing under .lendisk/, no empty, "." or
// ".." segment, SEC-5). decoded is the path as versitygw decodes it.
func borrowerRefusal(r request, decoded string) string {
	for _, h := range refusedHeaders {
		if r.hasHeaderPrefix(h) {
			return errNotOffered
		}
	}
	level, key := levelOf(decoded)
	for _, op := range Surface {
		if op.Class != Admitted || op.Method != r.method || op.Level != level || !r.shaped(op) {
			continue
		}
		if level == Object && checkKey(key) != nil {
			return errBadKey.Description // answered as Lending answers it (SEC-5)
		}
		return ""
	}
	return errNotOffered
}

// hasHeaderPrefix reports whether the request carries a header whose
// name begins with prefix, ignoring case.
func (r request) hasHeaderPrefix(prefix string) bool {
	for _, kv := range r.header {
		if len(kv[0]) >= len(prefix) && strings.EqualFold(kv[0][:len(prefix)], prefix) {
			return true
		}
	}
	return false
}

// shaped reports whether r's query is exactly op's: each of op.Query's
// keys present (with its value where versitygw routes on the value), and
// every other key one of op.Allow or a signature or SDK key.
func (r request) shaped(op Op) bool {
	want, _ := url.ParseQuery(op.Query)
	for k := range want {
		if !r.has(k) {
			return false
		}
		if k == "list-type" && r.value(k) != "2" {
			return false
		}
	}
	for _, kv := range r.query {
		k := kv[0]
		_, routed := want[k]
		switch {
		case routed:
		case k == "x-id", len(k) > 6 && strings.EqualFold(k[:6], "x-amz-"):
		default:
			allowed := false
			for _, a := range op.Allow {
				allowed = allowed || a == k
			}
			if !allowed {
				return false
			}
		}
	}
	return true
}

// levelOf splits a decoded path-style path into its level and key, as
// versitygw's routes do: "/" is the service, "/b" and "/b/" the bucket,
// anything longer an object.
func levelOf(p string) (Level, string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return Service, ""
	}
	_, key, ok := strings.Cut(p, "/")
	if !ok || key == "" {
		return Bucket, ""
	}
	return Object, key
}

// isBucketPath reports whether a path-style path names a bucket only.
func isBucketPath(path string) bool {
	p := strings.Trim(path, "/")
	return p != "" && !strings.Contains(p, "/")
}

func deny(c fiber.Ctx, msg string) error {
	e := s3err.APIError{Code: "AccessDenied", Description: msg, HTTPStatusCode: http.StatusForbidden}
	if msg == errBadKey.Description {
		e = errBadKey
	}
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationXML)
	return c.Status(e.HTTPStatusCode).Send(e.XMLBody("", ""))
}
