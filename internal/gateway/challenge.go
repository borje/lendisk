package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/identity"
)

var (
	// ErrNoLentKey means the borrower has no object by that key in that
	// bucket here (VER-1: a challenge the lender cannot answer).
	ErrNoLentKey = errors.New("no object of yours by that key here")
	// ErrBadRange means a challenged range starts past the object's end.
	ErrBadRange = errors.New("the range starts past the end of the object here")
)

// Read reads length bytes from offset of borrower's object key in its
// bucket sealed, or the whole object when length is negative, for the
// challenge call (VER-1). It reads through Lending in borrower's
// account, as the borrower's own GetObject would be served: the sealed
// name maps to that borrower's bucket alone (PEER-8), and a key under a
// reserved prefix, a tomb's included, is refused (SEC-5, RET-2). The
// caller must pass the borrower the request is answered for, never one
// a request names.
func (x *LentIndex) Read(ctx context.Context, borrower identity.NodeID, sealed, key string, offset, length int64) (io.ReadCloser, error) {
	if !identity.IsSealedName(sealed) {
		return nil, ErrNoLentBucket
	}
	if checkKey(key) != nil {
		return nil, ErrNoLentKey
	}
	l := &Lending{Next: x.Store, DB: x.DB}
	bctx := context.WithValue(ctx, "account", auth.Account{Access: borrower.String(), Role: auth.RoleUser}) //nolint:staticcheck // versitygw's key
	in := &s3.GetObjectInput{Bucket: &sealed, Key: &key}
	if length >= 0 {
		r := fmt.Sprintf("bytes=%d-%d", offset, offset+max(length, 1)-1)
		in.Range = &r
	}
	out, err := l.GetObject(bctx, in)
	var code string
	var ae s3err.APIError
	var re s3err.InvalidRangeError
	switch {
	case errors.As(err, &ae):
		code = ae.Code
	case errors.As(err, &re):
		code = re.Code
	}
	switch {
	case code == "NoSuchBucket":
		return nil, ErrNoLentBucket
	case code == "NoSuchKey":
		return nil, ErrNoLentKey
	case code == "InvalidRange":
		return nil, ErrBadRange
	case err != nil:
		return nil, err
	case out.Body == nil:
		return io.NopCloser(eofReader{}), nil
	}
	return out.Body, nil
}
