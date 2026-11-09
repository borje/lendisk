package replica

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/state"
)

// The overflow target's client (S3-1, S3-2, S3-3, #38): the same Client
// as a friend's, so the same checksums, multipart resume and stall
// bounds, with three differences: it reaches the provider over the
// internet with the credential from the environment (SEC-3); each of
// the node's buckets is a sub-prefix <prefix>/<sealed name>/ of the
// one bucket the user named, which it never creates; and the provider's
// refusals are classified as a provider's (classifyOverflow), never as
// a friend ending the friendship.

// OverflowKey is where key of the node's bucket sealed (its sealed
// name) is at an overflow target with prefix: <prefix>/<sealed>/<key>,
// the backup tool's own layout below it (S3-2).
func OverflowKey(prefix, sealed, key string) string {
	if p := strings.Trim(prefix, "/"); p != "" {
		return p + "/" + sealed + "/" + key
	}
	return sealed + "/" + key
}

// NewOverflow builds the client for overflow target o with credential c,
// sealing the node's bucket names with seal (identity.SealedName). The
// credential is kept in memory only.
func NewOverflow(o state.Overflow, c overflow.Credential, seal func(bucket string) string, opts Options) (*Client, error) {
	u, err := url.Parse(o.Endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("overflow target %s: endpoint %q is not an http:// or https:// URL", o.Name, o.Endpoint)
	}
	dialer := &net.Dialer{Timeout: opts.dialTimeout(), KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   opts.dialTimeout(),
		ResponseHeaderTimeout: opts.stallTimeout(),
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure:       u.Scheme == "https",
		Region:       o.Region,
		BucketLookup: minio.BucketLookupAuto,
		Transport:    tr,
		// The worker owns retries, and no signed trailer (as a friend's).
		MaxRetries: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("overflow target %s: %w", o.Name, err)
	}
	where := func(bucket, key string) (string, string) { return o.Bucket, OverflowKey(o.Prefix, seal(bucket), key) }
	return &Client{core: core, where: where, overflow: true, cred: c, root: root{o.Bucket, strings.Trim(o.Prefix, "/")}, stall: opts.stallTimeout(), created: map[string]bool{}}, nil
}

// root is an overflow target's bucket and prefix (no slashes at its
// ends).
type root struct{ bucket, prefix string }

func (f *Client) classify(err error) error {
	if f.overflow {
		return redacted{classifyOverflow(err), f.cred}
	}
	return classify(err)
}

// redact takes an overflow target's credential out of err's message; a
// friend's error is returned as it is.
func (f *Client) redact(err error) error {
	if f.overflow {
		return redacted{err, f.cred}
	}
	return err
}

// redacted is an overflow target's error with the credential's keys
// taken out of its message (SEC-3): a provider's 403 may name the access
// key (Garage does), and the message reaches place.detail, the log,
// alerts and webhooks. errors.Is and As still see the error inside.
type redacted struct {
	err  error
	cred overflow.Credential
}

func (r redacted) Error() string { return r.cred.Redact(r.err.Error()) }
func (r redacted) Unwrap() error { return r.err }

// classifyOverflow marks a provider's refusals with the kind the worker
// acts on: a checksum mismatch is ErrRejected (the worker re-hashes its
// copy, #57), SlowDown is ErrBusy. A 403 is the credential's or the
// bucket policy's, retried and reported: a provider never ends a
// friendship, so nothing is ErrRemoved, and nothing is ErrQuota, as
// max_bytes is the borrower's own bound (S3-7).
func classifyOverflow(err error) error {
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		return err
	}
	switch {
	case er.Code == "BadDigest" || er.Code == "InvalidDigest" || er.Code == "XAmzContentSHA256Mismatch" ||
		er.Code == "InvalidChecksum" || strings.Contains(er.Message, "checksum") && er.StatusCode == http.StatusBadRequest:
		return fmt.Errorf("%w: %w", ErrRejected, err)
	case er.Code == "SlowDown" || er.StatusCode == http.StatusServiceUnavailable:
		return fmt.Errorf("%w: %w", ErrBusy, err)
	case er.Code == "RequestTimeTooSkewed":
		return fmt.Errorf("the provider refuses the request because this node's clock is more than 15 minutes off; enable NTP (%s): %w", er.Code, err)
	case er.StatusCode == http.StatusForbidden || er.Code == "InvalidAccessKeyId" || er.Code == "SignatureDoesNotMatch":
		return fmt.Errorf("the provider refused the credential (%d %s): check the access key, secret key and its permissions: %w",
			er.StatusCode, er.Code, err)
	case er.Code == "NoSuchBucket":
		return fmt.Errorf("the provider has no bucket %s: create it, or add the target again with the right --bucket: %w", er.BucketName, err)
	}
	return err
}

// probeOverflow is Probe for an overflow target: one list of the bucket
// under the prefix, at most one key, which the recommended credential
// may do (S3-5), where a friend is asked for its bucket list.
func (f *Client) probeOverflow(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	bucket, prefix := f.root.bucket, f.root.prefix
	if err := f.listOne(ctx, bucket, strings.TrimSuffix(prefix, "/")); err != nil {
		return f.classify(fmt.Errorf("probing the overflow target: %w", err))
	}
	return nil
}

// listOne lists at most one key of bucket under prefix.
func (f *Client) listOne(ctx context.Context, bucket, prefix string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range f.core.Client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: strings.TrimPrefix(prefix+"/", "/"), MaxKeys: 1}) {
		return o.Err
	}
	return nil
}

// Check is what overflow add found at a target (S3-1, S3-4, S3-5).
type Check struct {
	Versioning     string // state.Versioning…
	Lifecycle      string // state.Lifecycle…
	VersionRemoval string // state.VersionRemoval…
	// Notes are what to tell the user, one line each: warnings and
	// recommendations.
	Notes []string
}

// CheckOverflow tests the credential at an overflow target before it is
// accepted (S3-1): it lists under the prefix, puts a small object there
// with a SHA-256 checksum the provider verifies (S3-3), reads it back,
// and deletes it. On the way it reads the bucket's versioning and
// lifecycle (S3-4), and tests whether the credential can remove an
// object version (S3-5) by removing the test object's own version; where
// it cannot, the test object stays behind as a non-current version until
// the lifecycle rule purges it. With lifecycleDays above 0 and no rule
// for the prefix, it adds one expiring non-current versions after that
// many days to the bucket's rules. An error means the credential does
// not work, and the target must not be added.
func (f *Client) CheckOverflow(ctx context.Context, name string, lifecycleDays int) (Check, error) {
	var c Check
	bucket, prefix := f.root.bucket, f.root.prefix
	prefix = strings.TrimSuffix(prefix, "/")
	if err := f.listOne(ctx, bucket, prefix); err != nil {
		return c, f.classify(fmt.Errorf("listing bucket %s: %w", bucket, err))
	}
	// The test object: a random name, so two checks never meet.
	var r [8]byte
	rand.Read(r[:])
	key := strings.TrimPrefix(prefix+"/lendisk-check-"+hex.EncodeToString(r[:]), "/")
	body := []byte("lendisk overflow check " + name + "\n")
	sum := sha256.Sum256(body)
	info, err := f.core.PutObject(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), "", "", minio.PutObjectOptions{
		UserMetadata: map[string]string{
			MetaSHA256:              hex.EncodeToString(sum[:]),
			"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(sum[:]),
		},
		DisableContentSha256: true,
	})
	if err != nil {
		return c, f.classify(fmt.Errorf("putting a test object at %s/%s: %w", bucket, key, err))
	}
	rd, _, _, err := f.core.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return c, f.classify(fmt.Errorf("reading the test object back: %w", err))
	}
	got, err := io.ReadAll(io.LimitReader(rd, 1<<20))
	rd.Close()
	if err != nil {
		return c, f.classify(fmt.Errorf("reading the test object back: %w", err))
	}
	if !bytes.Equal(got, body) {
		return c, fmt.Errorf("the test object read back differs from what was put at %s/%s", bucket, key)
	}

	v, err := f.core.GetBucketVersioning(ctx, bucket)
	switch {
	case err != nil:
		c.Versioning = state.VersioningOff
		c.Notes = append(c.Notes, fmt.Sprintf("versioning: cannot be read (%v); counted as off", f.classify(err)))
	case v.Enabled():
		c.Versioning = state.VersioningEnabled
	case v.Suspended():
		c.Versioning = state.VersioningSuspended
	default:
		c.Versioning = state.VersioningOff
	}
	if c.Versioning != state.VersioningEnabled {
		c.Notes = append(c.Notes, fmt.Sprintf("warning: versioning is %s on bucket %s: a delete there is final: neither the provider nor lendisk keeps a deleted copy, and a compromised node could remove these copies (S3-4); enable versioning if the provider has it",
			c.Versioning, bucket))
	}

	// Version removal (S3-5): remove the test object's own version.
	c.VersionRemoval = state.VersionRemovalUnknown
	if c.Versioning == state.VersioningEnabled && info.VersionID != "" {
		err := f.core.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{VersionID: info.VersionID})
		var er minio.ErrorResponse
		switch {
		case err == nil:
			c.VersionRemoval = state.VersionRemovalAllowed
			c.Notes = append(c.Notes, "warning: the credential can remove object versions, so a compromised node could destroy these copies; "+
				"create one that can put, get (earlier versions too), list and delete objects but not remove versions (s3:DeleteObjectVersion), and set it in the environment (S3-5)")
		case errors.As(err, &er) && (er.StatusCode == http.StatusForbidden || er.Code == "AccessDenied"):
			c.VersionRemoval = state.VersionRemovalDenied
		default:
			c.Notes = append(c.Notes, fmt.Sprintf("version removal: could not be tested (%v)", f.classify(err)))
		}
	}
	if c.VersionRemoval != state.VersionRemovalAllowed {
		// A delete marker, or with versioning off the delete itself.
		if err := f.core.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
			return c, f.classify(fmt.Errorf("deleting the test object: the credential must be able to delete objects (S3-5): %w", err))
		}
	}

	c.Lifecycle = state.LifecycleAbsent
	cfg, err := f.core.GetBucketLifecycle(ctx, bucket)
	var er minio.ErrorResponse
	switch {
	case err == nil && coversPrefix(cfg, prefix):
		c.Lifecycle = state.LifecyclePresent
	case err == nil:
	case errors.As(err, &er) && er.Code == "NoSuchLifecycleConfiguration":
		cfg = lifecycle.NewConfiguration()
	case errors.As(err, &er) && (er.Code == "NotImplemented" || er.StatusCode == http.StatusNotImplemented):
		c.Lifecycle = state.LifecycleUnsupported
	default:
		c.Notes = append(c.Notes, fmt.Sprintf("lifecycle: cannot be read (%v)", f.classify(err)))
		cfg = nil
	}
	if c.Lifecycle == state.LifecycleAbsent && c.Versioning == state.VersioningEnabled {
		if lifecycleDays > 0 && cfg != nil {
			cfg.Rules = append(cfg.Rules, lifecycle.Rule{
				ID:                          "lendisk-" + name,
				Status:                      "Enabled",
				RuleFilter:                  lifecycle.Filter{Prefix: strings.TrimPrefix(prefix+"/", "/")},
				NoncurrentVersionExpiration: lifecycle.NoncurrentVersionExpiration{NoncurrentDays: lifecycle.ExpirationDays(lifecycleDays)},
			})
			if err := f.core.SetBucketLifecycle(ctx, bucket, cfg); err != nil {
				c.Notes = append(c.Notes, fmt.Sprintf("lifecycle: the rule could not be created (%v); create one that expires non-current versions after %d days under %q", f.classify(err), lifecycleDays, prefix))
			} else {
				c.Lifecycle = state.LifecycleCreated
			}
		} else {
			c.Notes = append(c.Notes, fmt.Sprintf("offer: bucket %s has no rule expiring non-current versions under %q, so deleted copies are kept for ever; "+
				"add the target with --lifecycle to create one for the retention window, or create it at the provider (S3-4)", bucket, prefix))
		}
	}
	if c.Lifecycle == state.LifecycleUnsupported {
		c.Notes = append(c.Notes, "lifecycle: the provider has no lifecycle rules; non-current versions are kept until removed by hand")
	}
	return c, nil
}

// coversPrefix: an enabled rule of cfg expires non-current versions of
// every key under prefix.
func coversPrefix(cfg *lifecycle.Configuration, prefix string) bool {
	if cfg == nil {
		return false
	}
	for _, r := range cfg.Rules {
		p := r.RuleFilter.Prefix
		if p == "" {
			p = r.RuleFilter.And.Prefix
		}
		if p == "" {
			p = r.Prefix
		}
		if r.Status == "Enabled" && !r.NoncurrentVersionExpiration.IsDaysNull() && strings.HasPrefix(prefix+"/", p) {
			return true
		}
	}
	return false
}
