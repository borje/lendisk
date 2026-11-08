package replica

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// DialFunc opens a connection to a friend's tunnel port
// (transport.Transport.Dial).
type DialFunc func(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error)

// Options tune a client. Zero values mean the defaults.
type Options struct {
	// DialTimeout bounds opening the tunnel connection (default 30 s).
	DialTimeout time.Duration
	// StallTimeout ends a request that moves no bytes for this long
	// (default 2 minutes): a friend switched off mid-transfer.
	StallTimeout time.Duration
}

func (o Options) dialTimeout() time.Duration {
	if o.DialTimeout > 0 {
		return o.DialTimeout
	}
	return 30 * time.Second
}

func (o Options) stallTimeout() time.Duration {
	if o.StallTimeout > 0 {
		return o.StallTimeout
	}
	return 2 * time.Minute
}

// region is the one SigV4 region every lendisk S3 surface uses; setting
// it keeps minio-go from asking the bucket's location.
const region = "us-east-1"

// friendHost is the Host a friend's client sends. The tunnel dial
// ignores it; the lender's versitygw checks the signature over it.
const friendHost = "lendisk-friend:9000"

// Friend is the replica client for a friend's node (D12, §3.4): S3
// through the tailcat tunnel, signed with the derived per-pair
// credential (NET-7a), each bucket under its sealed name (SEC-16).
type Friend struct {
	core    *minio.Core
	self    *identity.Identity
	stall   time.Duration
	mu      sync.Mutex
	created map[string]bool // sealed names created at the friend by this client
}

// NewFriend builds the client for peer at its tunnel address addr,
// dialling with dial. The secret is derived here and kept in memory only.
func NewFriend(self *identity.Identity, peer identity.NodeID, addr string, dial DialFunc, o Options) (*Friend, error) {
	secret, err := self.S3Secret(peer)
	if err != nil {
		return nil, err
	}
	dt := o.dialTimeout()
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, dt)
			defer cancel()
			return dial(ctx, peer, addr)
		},
		ResponseHeaderTimeout: o.stallTimeout(),
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
	}
	core, err := minio.NewCore(friendHost, &minio.Options{
		Creds:        credentials.NewStaticV4(self.NodeID().String(), secret, ""),
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
		Transport:    tr,
		// minio-go retries only bodies it can seek; the worker owns
		// retries (D12, Q-Y.1). TrailingHeaders stays off: minio-go's
		// signed trailer is refused by versitygw and Garage (Q-Y.2).
		MaxRetries: 1,
	})
	if err != nil {
		return nil, err
	}
	return &Friend{core: core, self: self, stall: o.stallTimeout(), created: map[string]bool{}}, nil
}

// ensureBucket creates the sealed bucket at the friend once per client;
// a bucket already there is fine.
func (f *Friend) ensureBucket(ctx context.Context, sealed string) error {
	f.mu.Lock()
	done := f.created[sealed]
	f.mu.Unlock()
	if done {
		return nil
	}
	err := f.core.MakeBucket(ctx, sealed, minio.MakeBucketOptions{Region: region})
	if err != nil {
		var er minio.ErrorResponse
		if !errors.As(err, &er) || (er.Code != "BucketAlreadyOwnedByYou" && er.Code != "BucketAlreadyExists") {
			return classify(fmt.Errorf("creating the bucket at the friend: %w", err))
		}
	}
	f.mu.Lock()
	f.created[sealed] = true
	f.mu.Unlock()
	return nil
}

// Put sends the object in one PUT with an unsigned payload, carrying its
// SHA-256 as x-amz-meta-lendisk-sha256 and as the request checksum, so
// the friend verifies the bytes it stores (REPL-10, S3-3). The body is
// read once. A put that moves no bytes for the stall timeout fails.
func (f *Friend) Put(ctx context.Context, o Object) error {
	sum, err := hex.DecodeString(o.SHA256)
	if err != nil || len(sum) != 32 {
		return fmt.Errorf("object %s has no SHA-256 to send (%q)", o.Key, o.SHA256)
	}
	sealed := f.self.SealedName(o.Bucket)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := f.ensureBucket(ctx, sealed); err != nil {
		return err
	}
	body := newWatchdog(o.Body, f.stall, cancel)
	defer body.stop()
	meta := map[string]string{
		MetaSHA256:              o.SHA256,
		"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(sum),
	}
	if o.Replaces != "" {
		meta[MetaReplaces] = o.Replaces
	}
	_, err = f.core.PutObject(ctx, sealed, o.Key, body, o.Size, "", "", minio.PutObjectOptions{
		UserMetadata:         meta,
		DisableContentSha256: true,
	})
	if err != nil {
		if body.stalled() {
			return fmt.Errorf("sending %s: no progress for %v: %w", o.Key, f.stall, err)
		}
		return classify(fmt.Errorf("sending %s: %w", o.Key, err))
	}
	return nil
}

// Head returns the hash the friend stores key with (its
// x-amz-meta-lendisk-sha256, REPL-10); ok is false when it holds no
// object there (a tombstoned key included).
func (f *Friend) Head(ctx context.Context, bucket, key string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	info, err := f.core.StatObject(ctx, f.self.SealedName(bucket), key, minio.StatObjectOptions{})
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchKey" || er.Code == "NoSuchBucket" || er.StatusCode == http.StatusNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, classify(fmt.Errorf("asking for %s: %w", key, err))
	}
	return strings.ToLower(info.Metadata.Get("X-Amz-Meta-" + MetaSHA256)), true, nil
}

// Delete removes the key from the sealed bucket. A key or bucket the
// friend does not have is already deleted.
func (f *Friend) Delete(ctx context.Context, bucket, key string) error {
	err := f.core.RemoveObject(ctx, f.self.SealedName(bucket), key, minio.RemoveObjectOptions{})
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchKey" || er.Code == "NoSuchBucket") {
		return nil
	}
	if err != nil {
		return classify(fmt.Errorf("deleting %s: %w", key, err))
	}
	return nil
}

// Get opens key of the bucket at the friend for the read-through
// (EP-10, ID-6): the whole object, from its sealed name. The caller
// verifies the bytes against the hash it expects; the friend's word is
// not taken for it. A body that moves no bytes for the stall timeout
// fails, so a friend switched off mid-transfer cannot hang the read.
func (f *Friend) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	body, _, _, err := f.core.GetObject(ctx, f.self.SealedName(bucket), key, minio.GetObjectOptions{})
	if err != nil {
		cancel()
		return nil, classify(fmt.Errorf("fetching %s: %w", key, err))
	}
	return &fetched{watchdog: newWatchdog(body, f.stall, cancel), body: body, cancel: cancel}, nil
}

// fetched is a friend's object body being read; Close ends its request.
type fetched struct {
	*watchdog
	body   io.Closer
	cancel context.CancelFunc
}

func (b *fetched) Read(p []byte) (int, error) {
	n, err := b.watchdog.Read(p)
	if err != nil && err != io.EOF && b.stalled() {
		err = fmt.Errorf("no progress for %v: %w", b.d, err)
	}
	return n, err
}

func (b *fetched) Close() error {
	b.stop()
	b.cancel()
	return b.body.Close()
}

// classify marks a place's refusals with the kind the worker acts on
// (§3.4 Errors); anything else is left to retry. Only a 403 saying this
// node is not a friend is ErrRemoved: the tunnel's "no relationship"
// refusal, or versitygw not knowing the access key. Other 403s (a skewed
// clock, a bad signature) are retried and reported as what they are
// (#53).
func classify(err error) error {
	var er minio.ErrorResponse
	if !errors.As(err, &er) {
		return err
	}
	switch {
	case er.StatusCode == 507 || er.Code == "QuotaExceeded":
		return fmt.Errorf("%w: %w", ErrQuota, err)
	case er.StatusCode == http.StatusForbidden && (er.Code == "InvalidAccessKeyId" ||
		er.Code == "AccessDenied" && strings.Contains(er.Message, state.NoRelationship)):
		return fmt.Errorf("%w: %w", ErrRemoved, err)
	case er.Code == "RequestTimeTooSkewed":
		return fmt.Errorf("the friend refuses the request because this node's clock and the friend's differ by more than 15 minutes; "+
			"set the wrong one right (enable NTP) (%s): %w", er.Code, err)
	case er.StatusCode == http.StatusForbidden:
		return fmt.Errorf("the friend refused the request (%d %s): %w", er.StatusCode, er.Code, err)
	case er.StatusCode == http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %w", ErrImmutable, err)
	case er.Code == "BadDigest" || er.Code == "InvalidDigest" || er.Code == "XAmzContentSHA256Mismatch":
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return err
}
