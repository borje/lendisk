package gateway

import (
	"net/http"
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
//     to nothing behind it.
//
// The borrower allow-list (NET-7, #15) grows here.
func Gate() s3api.Option {
	return s3api.WithMiddleware("/", func(c fiber.Ctx) error {
		if msg := refuse(c.Method(), c.Path(), c.Get("X-Amz-Bucket-Object-Lock-Enabled")); msg != "" {
			return deny(c, msg)
		}
		return c.Next()
	})
}

// ControlPrefix is the control API's path on the tunnel (§3.7).
const ControlPrefix = "/_lendisk/"

// refuse returns why a request is refused, or "" to let it through.
func refuse(method, path, objectLock string) string {
	switch {
	case method == http.MethodPatch:
		return "lendisk: the admin API is not served"
	case strings.HasPrefix(path, ControlPrefix):
		return "lendisk: control API not served here"
	case method == http.MethodPut && strings.EqualFold(strings.TrimSpace(objectLock), "true"):
		return "lendisk: object lock is not offered"
	}
	return ""
}

func deny(c fiber.Ctx, msg string) error {
	e := s3err.APIError{Code: "AccessDenied", Description: msg, HTTPStatusCode: http.StatusForbidden}
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationXML)
	return c.Status(e.HTTPStatusCode).Send(e.XMLBody("", ""))
}
