package gateway

import (
	"net/http"
	"path"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3err"
)

// Gate is lendisk's fiber middleware in front of versitygw's routing and
// authentication (D4, NET-7, SEC-8). It refuses by the shape of the
// request, before versitygw does anything with it:
//
//   - every PATCH: S3 has none, and every route of the admin API embedgw
//     mounts on the socket is one (SEC-8);
//   - CreateBucket asking for object lock, which posix would otherwise
//     grant (D15);
//   - the control API path, which belongs to the bridge on the tunnel and
//     to nothing behind it;
//   - a browser-form POST upload (a POST to a bucket that is not a
//     multi-delete): it reaches the backend's PutObject, but no backup tool
//     uses it and its policy-signed form is a second way in (Q-H, §3.3).
//
// Server-side copies are refused on own buckets by the decorator, which
// knows the bucket map (Q-H).
//
// The borrower allow-list (NET-7, #15) grows here.
func Gate() s3api.Option {
	return s3api.WithMiddleware("/", func(c fiber.Ctx) error {
		if msg := refuse(request{method: c.Method(), path: c.Path(), objectLock: c.Get("X-Amz-Bucket-Object-Lock-Enabled"),
			multiDelete: c.Request().URI().QueryArgs().Has("delete")}); msg != "" {
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
	method, path string
	objectLock   string // the x-amz-bucket-object-lock-enabled header
	multiDelete  bool   // the query has "delete"
}

// refuse returns why a request is refused, or "" to let it through.
func refuse(r request) string {
	switch {
	case r.method == http.MethodPatch:
		return "lendisk: the admin API is not served"
	case IsControlPath(r.path):
		return "lendisk: control API not served here"
	case r.method == http.MethodPut && strings.EqualFold(strings.TrimSpace(r.objectLock), "true"):
		return "lendisk: object lock is not offered"
	case r.method == http.MethodPost && !r.multiDelete && isBucketPath(r.path):
		return "lendisk: browser-form uploads are not offered; use PUT"
	}
	return ""
}

// isBucketPath reports whether a path-style path names a bucket only.
func isBucketPath(path string) bool {
	p := strings.Trim(path, "/")
	return p != "" && !strings.Contains(p, "/")
}

func deny(c fiber.Ctx, msg string) error {
	e := s3err.APIError{Code: "AccessDenied", Description: msg, HTTPStatusCode: http.StatusForbidden}
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationXML)
	return c.Status(e.HTTPStatusCode).Send(e.XMLBody("", ""))
}
