package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/state"
)

// ResolvePending settles the queue rows a crash left pending (EP-3),
// against the store, before the gateway serves anything:
//
//   - a put is done if the object on disk carries the row's capture token
//     (PutObject) or the ETag its completion was to give it (multipart);
//     else it never took effect and the row is removed. A put that took
//     effect is hashed by reading the object once;
//   - a delete is done if the key is gone, else removed.
//
// It returns how many rows it marked done and removed.
func (d *Decorator) ResolvePending(ctx context.Context) (done, dropped int, err error) {
	if d.DB == nil {
		return 0, 0, nil
	}
	pending, err := d.DB.PendingCaptures(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range pending {
		landed, err := d.landed(ctx, e)
		if err != nil {
			return done, dropped, fmt.Errorf("resolving queue row %d (%s/%s): %w", e.Seq, e.Bucket, e.Key, err)
		}
		if !landed {
			if err := d.DB.DropCaptures(ctx, e.Seq); err != nil {
				return done, dropped, err
			}
			dropped++
			continue
		}
		switch {
		case e.Op == state.OpDelete:
			err = d.DB.FinishDelete(ctx, e.Seq)
		default:
			var size int64
			var sum string
			if size, sum, err = d.hashStored(ctx, e.Bucket, e.Key); err == nil {
				_, err = d.DB.FinishPut(ctx, e.Seq, size, sum)
			}
		}
		if err != nil {
			return done, dropped, fmt.Errorf("resolving queue row %d (%s/%s): %w", e.Seq, e.Bucket, e.Key, err)
		}
		done++
	}
	return done, dropped, nil
}

// landed reports whether a pending row's backend call took effect.
func (d *Decorator) landed(ctx context.Context, e state.Entry) (bool, error) {
	head, err := d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.Bucket, Key: &e.Key})
	if notFound(err) {
		return e.Op == state.OpDelete, nil
	}
	if err != nil {
		return false, err
	}
	switch {
	case e.Op == state.OpDelete:
		return false, nil
	case e.Upload != "":
		return head.ETag != nil && strings.Trim(*head.ETag, `"`) == strings.Trim(e.ETag, `"`), nil
	default:
		return head.Metadata[MetaCapture] == e.Token, nil
	}
}

// hashStored reads a stored object once and returns its size and SHA-256.
func (d *Decorator) hashStored(ctx context.Context, bucket, key string) (int64, string, error) {
	out, err := d.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	h := sha256.New()
	n, err := io.Copy(h, out.Body)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func notFound(err error) bool {
	var ae s3err.APIError
	if errors.As(err, &ae) {
		return ae.Code == "NoSuchKey" || ae.Code == "NoSuchBucket" || ae.HTTPStatusCode == 404
	}
	return false
}
