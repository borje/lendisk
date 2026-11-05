package main

import (
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3err"
)

// gateOn lets the test show the admin API with and without the gate.
var gateOn atomic.Bool

func init() { gateOn.Store(true) }

// gate is a fiber middleware mounted through Config.S3Options
// (s3api.WithMiddleware). It runs after the rate limiter and before every
// S3 and admin route and before authentication, so it can refuse by
// request shape what the decorator cannot tell apart: versitygw itself
// calls GetBucketAcl and GetBucketPolicy on the backend for every
// request's access check, so a decorator that refused those methods
// would refuse everything.
func gate() s3api.Option {
	return s3api.WithMiddleware("/", func(c fiber.Ctx) error {
		if !gateOn.Load() {
			return c.Next()
		}
		// The admin API is mounted on the S3 socket when AdminPorts is
		// empty; all its routes are PATCH, which S3 never uses.
		if c.Method() == http.MethodPatch {
			return deny(c, "lendisk: admin API not served")
		}
		// CreateBucket with object lock succeeds on posix without a
		// versioning directory.
		if c.Method() == http.MethodPut && strings.EqualFold(c.Get("X-Amz-Bucket-Object-Lock-Enabled"), "true") {
			return deny(c, "lendisk: object lock not offered")
		}
		return c.Next()
	})
}

func deny(c fiber.Ctx, msg string) error {
	e := s3err.APIError{Code: "AccessDenied", Description: msg, HTTPStatusCode: http.StatusForbidden}
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationXML)
	return c.Status(e.HTTPStatusCode).Send(e.XMLBody("", ""))
}
