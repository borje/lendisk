package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

// LentPrefix begins the store name of every bucket a borrower keeps here.
const LentPrefix = "lent-"

// LentName is the store name of a borrower's bucket (DESIGN §3, bucket
// naming): lent-<id16>-<sealed>.
func LentName(borrower identity.NodeID, sealed string) string {
	return LentPrefix + borrower.Short() + "-" + sealed
}

// borrowerOf returns the node ID an access key names, exactly in display
// form; anything else (a bucket key, the root key) is not a borrower.
func borrowerOf(access string) (identity.NodeID, bool) {
	id, err := identity.ParseNodeID(access)
	if err != nil || id.String() != access {
		return identity.NodeID{}, false
	}
	return id, true
}

// Lending is the decorator's outer layer (D4, PEER-8, SEC-16): it maps
// every bucket name a borrower's account sends, which is the bucket's
// sealed name, to the bucket's store name, through the borrower's rows
// in lendisk.db's mapping table. A borrower can therefore name only its
// own buckets; versitygw's ownership check, on the store name, is the
// second wall. The account is the authenticated one versitygw puts in
// the request context (Q-I.2). Requests from any other account (an own
// bucket's key, root) pass through unchanged.
//
// Every backend.Backend method is written out here, none embedded, so a
// method added by a versitygw upgrade fails to compile instead of
// passing a borrower's sealed name through unmapped. For a borrower's
// authenticated account, every method BackendMethods classifies Refused
// or Unreachable is refused (AccessDenied) before anything else, so the
// gate's allow-list (Gate, Surface), which reads the access key
// unverified, is not the only wall (SEC-9).
//
// For a borrower it also enforces quota, the free-space floor, object
// integrity and key names on every write (quota.go: LEND-1, LEND-2,
// LEND-4, SEC-5), and turns its deletes into tombstones kept for the
// retention window (tomb.go: RET-1, RET-2).
type Lending struct {
	Next backend.Backend
	DB   *state.DB
	Now  func() time.Time
	// Uploads reads which upload assembled a stored object (#62), to
	// tell a retried completion.
	Uploads UploadIDs
	// StoreDir, Floor and Statfs give the free-space floor (LEND-2): a
	// nil Floor is DefaultFloor, a nil Statfs the real filesystem's.
	StoreDir string
	Floor    *config.Floor
	Statfs   func(dir string) (store.Space, error)
	Log      *slog.Logger
	// AbandonAfter is how long a borrower's multipart upload may go
	// without a part before the reconcile aborts it (default
	// DefaultAbandonAfter).
	AbandonAfter time.Duration
	// Tests only: beforeComplete runs just before a borrower's upload is
	// completed in the store; afterList when a reconcile has listed a
	// borrower and not yet set its count; afterPartStored when a part is
	// stored and not yet counted.
	beforeComplete   func()
	afterList        func()
	afterPartStored  func(part int32, size int64)
	afterPurgeBucket func() // purgeFormer removed a former friend's bucket
	beforeTombMove   func() // a tombstone is recorded and its object not yet moved
	// afterRewriteMove runs when a rewrite has moved the replaced version
	// into its tomb and the new one is not yet published; an error fails
	// the rewrite there, as the store failing would.
	afterRewriteMove func() error

	tombDirs sync.Mutex // creating and removing directories for tombs and revivals

	keys     keyLocks // per borrower's key: deletes, multipart creates and completions, puts (from a verified body until the store answers), rewrites, revivals and purges
	uploads  keyLocks // one accounting step per borrower's upload at a time
	mu       sync.Mutex
	peers    map[string]*borrowerUse
	reserved int64 // admitted bytes of every borrower's writes in flight
}

var (
	errNoSuchBucket = s3err.GetAPIError(s3err.ErrNoSuchBucket)
	errNotSealed    = s3err.APIError{Code: "InvalidBucketName", HTTPStatusCode: 400,
		Description: "lendisk: a borrower names its buckets by their sealed names (s and 26 base32 characters)"}
	errNotForBorrower = s3err.APIError{Code: "AccessDenied", HTTPStatusCode: 403,
		Description: "lendisk: this operation is not offered to a borrower"}
)

// borrower returns the borrower whose account made the request, if any.
func borrower(ctx context.Context) (identity.NodeID, bool) {
	acct, ok := ctx.Value("account").(auth.Account)
	if !ok {
		return identity.NodeID{}, false
	}
	return borrowerOf(acct.Access)
}

// name maps a bucket name sent by the request's account. For a borrower
// it is the store name of that borrower's bucket, or NoSuchBucket when
// the borrower has no bucket by that sealed name.
func (l *Lending) name(ctx context.Context, bucket string) (string, error) {
	peer, ok := borrower(ctx)
	if !ok {
		return bucket, nil
	}
	if !identity.IsSealedName(bucket) {
		return "", errNoSuchBucket
	}
	has, err := l.DB.HasLentBucket(context.Background(), peer.String(), bucket)
	if err != nil {
		return "", errInternal
	}
	if !has {
		return "", errNoSuchBucket
	}
	return LentName(peer, bucket), nil
}

// ptr maps *bucket the same way, returning a new pointer.
func (l *Lending) ptr(ctx context.Context, bucket *string) (*string, error) {
	if bucket == nil {
		return nil, nil
	}
	n, err := l.name(ctx, *bucket)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// refused reports whether the request's account is a borrower, for the
// calls a borrower may not make.
func refused(ctx context.Context) bool {
	_, ok := borrower(ctx)
	return ok
}

// sealedOf returns the wire name for a store name the borrower owns, so
// answers name the bucket as the borrower does.
func sealedOf(ctx context.Context, stored string) string {
	peer, ok := borrower(ctx)
	if !ok {
		return stored
	}
	return strings.TrimPrefix(stored, LentPrefix+peer.Short()+"-")
}

func sealedPtr(ctx context.Context, s *string) *string {
	if s == nil {
		return nil
	}
	n := sealedOf(ctx, *s)
	return &n
}

func (l *Lending) String() string { return l.Next.String() }
func (l *Lending) Shutdown()      { l.Next.Shutdown() }
func (l *Lending) NormalizeObjectKey(bucket, object string) string {
	return l.Next.NormalizeObjectKey(bucket, object)
}
func (l *Lending) StoresDotSegmentKeys() bool             { return l.Next.StoresDotSegmentKeys() }
func (l *Lending) ValidateUploadID(uploadID string) error { return l.Next.ValidateUploadID(uploadID) }

// ListBuckets answers a borrower from its rows in the mapping table alone
// (PEER-8): its sealed names, never another borrower's or the lender's.
func (l *Lending) ListBuckets(ctx context.Context, in s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	peer, ok := borrower(ctx)
	if !ok {
		return l.Next.ListBuckets(ctx, in)
	}
	rows, err := l.DB.LentBuckets(context.Background(), peer.String())
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, errInternal
	}
	res := s3response.ListAllMyBucketsResult{Owner: s3response.CanonicalUser{ID: peer.String()}, Prefix: in.Prefix}
	for _, r := range rows {
		if !strings.HasPrefix(r.Sealed, in.Prefix) {
			continue
		}
		res.Buckets.Bucket = append(res.Buckets.Bucket, s3response.ListAllMyBucketsEntry{Name: r.Sealed, CreationDate: r.CreatedAt})
	}
	return res, nil
}

func (l *Lending) HeadBucket(ctx context.Context, in *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	c := *in
	c.Bucket = b
	return l.Next.HeadBucket(ctx, &c)
}

func (l *Lending) GetBucketAcl(ctx context.Context, in *s3.GetBucketAclInput) ([]byte, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	c := *in
	c.Bucket = b
	return l.Next.GetBucketAcl(ctx, &c)
}

// CreateBucket creates a borrower's bucket under its store name, private
// and owned by the borrower's account alone, and records it in the
// mapping table (PEER-2, PEER-8). The name sent must be a sealed name.
// A new bucket is admitted like a write (createBucketLent). A retry finds
// the bucket already its own and records it.
func (l *Lending) CreateBucket(ctx context.Context, in *s3.CreateBucketInput, acl []byte) error {
	peer, ok := borrower(ctx)
	if !ok {
		return l.Next.CreateBucket(ctx, in, acl)
	}
	if in.Bucket == nil || !identity.IsSealedName(*in.Bucket) {
		return errNotSealed
	}
	sealed, stored := *in.Bucket, LentName(peer, *in.Bucket)
	own, err := json.Marshal(auth.ACL{Owner: peer.String()})
	if err != nil {
		return errInternal
	}
	c := *in
	c.Bucket = &stored
	c.ObjectOwnership = types.ObjectOwnershipBucketOwnerEnforced
	return l.createBucketLent(ctx, peer.String(), sealed, func() error {
		err := l.Next.CreateBucket(ctx, &c, own)
		if err != nil && !isCode(err, "BucketAlreadyOwnedByYou") {
			return err
		}
		if rerr := l.DB.AddLentBucket(context.Background(), peer.String(), sealed, l.now()); rerr != nil {
			return errInternal
		}
		return err
	})
}

func (l *Lending) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

func isCode(err error, code string) bool {
	ae, ok := err.(s3err.APIError)
	return ok && ae.Code == code
}

func (l *Lending) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.PutBucketAcl(ctx, bucket, data)
}

// DeleteBucket is refused to a borrower: its row in the mapping table and
// its bytes in the quota count would have to go with it, which is not
// built.
func (l *Lending) DeleteBucket(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.DeleteBucket(ctx, bucket)
}

func (l *Lending) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.PutBucketVersioning(ctx, b, status)
}

func (l *Lending) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return s3response.GetBucketVersioningOutput{}, err
	}
	return l.Next.GetBucketVersioning(ctx, b)
}

func (l *Lending) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.PutBucketPolicy(ctx, bucket, policy)
}

func (l *Lending) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return l.Next.GetBucketPolicy(ctx, b)
}

func (l *Lending) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.DeleteBucketPolicy(ctx, bucket)
}

func (l *Lending) PutBucketOwnershipControls(ctx context.Context, bucket string, o types.ObjectOwnership) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.PutBucketOwnershipControls(ctx, bucket, o)
}

func (l *Lending) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	if refused(ctx) {
		return "", errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return "", err
	}
	return l.Next.GetBucketOwnershipControls(ctx, b)
}

func (l *Lending) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.DeleteBucketOwnershipControls(ctx, bucket)
}

func (l *Lending) PutBucketCors(ctx context.Context, bucket string, cors []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.PutBucketCors(ctx, b, cors)
}

func (l *Lending) GetBucketCors(ctx context.Context, bucket string) ([]byte, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return l.Next.GetBucketCors(ctx, b)
}

func (l *Lending) DeleteBucketCors(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.DeleteBucketCors(ctx, b)
}

func (l *Lending) PutBucketWebsite(ctx context.Context, bucket string, website []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.PutBucketWebsite(ctx, b, website)
}

func (l *Lending) GetBucketWebsite(ctx context.Context, bucket string) ([]byte, error) {
	if refused(ctx) {
		return nil, errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return l.Next.GetBucketWebsite(ctx, b)
}

func (l *Lending) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.DeleteBucketWebsite(ctx, b)
}

func (l *Lending) CreateMultipartUpload(ctx context.Context, in s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	sealed := deref(in.Bucket)
	in.Bucket = b
	var res s3response.InitiateMultipartUploadResult
	if peer, ok := borrower(ctx); ok {
		res, err = l.createUploadLent(ctx, peer.String(), sealed, in)
	} else {
		res, err = l.Next.CreateMultipartUpload(ctx, in)
	}
	res.Bucket = sealedOf(ctx, res.Bucket)
	return res, err
}

func (l *Lending) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	c := *in
	c.Bucket = b
	var res s3response.CompleteMultipartUploadResult
	var ver string
	if peer, ok := borrower(ctx); ok {
		res, ver, err = l.completeLent(ctx, peer.String(), deref(in.Bucket), &c)
	} else {
		res, ver, err = l.Next.CompleteMultipartUpload(ctx, &c)
	}
	res.Bucket = sealedPtr(ctx, res.Bucket)
	return res, ver, err
}

func (l *Lending) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput) error {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return err
	}
	c := *in
	c.Bucket = b
	if peer, ok := borrower(ctx); ok {
		return l.abortLent(ctx, peer.String(), deref(in.Bucket), &c)
	}
	return l.Next.AbortMultipartUpload(ctx, &c)
}

func (l *Lending) ListMultipartUploads(ctx context.Context, in *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	if refused(ctx) {
		return s3response.ListMultipartUploadsResult{}, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}
	c := *in
	c.Bucket = b
	res, err := l.Next.ListMultipartUploads(ctx, &c)
	res.Bucket = sealedOf(ctx, res.Bucket)
	return res, err
}

func (l *Lending) ListParts(ctx context.Context, in *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return s3response.ListPartsResult{}, kerr
	}
	c := *in
	c.Bucket = b
	res, err := l.Next.ListParts(ctx, &c)
	res.Bucket = sealedOf(ctx, res.Bucket)
	return res, err
}

func (l *Lending) UploadPart(ctx context.Context, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return nil, kerr
	}
	c := *in
	c.Bucket = b
	if peer, ok := borrower(ctx); ok {
		return l.uploadPartLent(ctx, peer.String(), deref(in.Bucket), &c)
	}
	return l.Next.UploadPart(ctx, &c)
}

// UploadPartCopy is refused to a borrower: its source names a bucket
// inside a string, outside the mapping.
func (l *Lending) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	if refused(ctx) {
		return s3response.CopyPartResult{}, errNotForBorrower
	}
	return l.Next.UploadPartCopy(ctx, in)
}

func (l *Lending) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	sealed := deref(in.Bucket)
	in.Bucket = b
	if peer, ok := borrower(ctx); ok {
		return l.putLent(ctx, peer.String(), sealed, in)
	}
	return l.Next.PutObject(ctx, in)
}

func (l *Lending) HeadObject(ctx context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return nil, kerr
	}
	c := *in
	c.Bucket = b
	return l.Next.HeadObject(ctx, &c)
}

func (l *Lending) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return nil, kerr
	}
	c := *in
	c.Bucket = b
	return l.Next.GetObject(ctx, &c)
}

func (l *Lending) GetObjectAcl(ctx context.Context, in *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	if refused(ctx) {
		return nil, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return nil, kerr
	}
	c := *in
	c.Bucket = b
	return l.Next.GetObjectAcl(ctx, &c)
}

func (l *Lending) GetObjectAttributes(ctx context.Context, in *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	if refused(ctx) {
		return s3response.GetObjectAttributesResponse{}, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.GetObjectAttributesResponse{}, err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return s3response.GetObjectAttributesResponse{}, kerr
	}
	c := *in
	c.Bucket = b
	return l.Next.GetObjectAttributes(ctx, &c)
}

// CopyObject is refused to a borrower, as UploadPartCopy is.
func (l *Lending) CopyObject(ctx context.Context, in s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	if refused(ctx) {
		return s3response.CopyObjectOutput{}, errNotForBorrower
	}
	return l.Next.CopyObject(ctx, in)
}

func (l *Lending) ListObjects(ctx context.Context, in *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if refused(ctx) {
		return s3response.ListObjectsResult{}, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}
	c := *in
	c.Bucket = b
	res, err := l.Next.ListObjects(ctx, &c)
	res.Name = sealedPtr(ctx, res.Name)
	return res, err
}

func (l *Lending) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.ListObjectsV2Result{}, err
	}
	c := *in
	c.Bucket = b
	res, err := l.Next.ListObjectsV2(ctx, &c)
	res.Name = sealedPtr(ctx, res.Name)
	return res, err
}

func (l *Lending) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return nil, err
	}
	c := *in
	c.Bucket = b
	if peer, ok := borrower(ctx); ok {
		return l.deleteLent(ctx, peer.String(), deref(in.Bucket), &c)
	}
	return l.Next.DeleteObject(ctx, &c)
}

// DeleteObjects is refused to a borrower (NET-7): each delete is one
// DeleteObject, so the counter and tombstones see each (tomb.go).
func (l *Lending) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if refused(ctx) {
		return s3response.DeleteResult{}, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.DeleteResult{}, err
	}
	c := *in
	c.Bucket = b
	return l.Next.DeleteObjects(ctx, &c)
}

func (l *Lending) PutObjectAcl(ctx context.Context, in *s3.PutObjectAclInput) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.PutObjectAcl(ctx, in)
}

func (l *Lending) ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	if refused(ctx) {
		return s3response.ListVersionsResult{}, errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return s3response.ListVersionsResult{}, err
	}
	c := *in
	c.Bucket = b
	res, err := l.Next.ListObjectVersions(ctx, &c)
	res.Name = sealedPtr(ctx, res.Name)
	return res, err
}

func (l *Lending) RestoreObject(ctx context.Context, in *s3.RestoreObjectInput) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil {
		return err
	}
	if kerr := borrowerKey(ctx, in.Key); kerr != nil {
		return kerr
	}
	c := *in
	c.Bucket = b
	return l.Next.RestoreObject(ctx, &c)
}

func (l *Lending) SelectObjectContent(ctx context.Context, in *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	if refused(ctx) {
		return func(*bufio.Writer) {}
	}
	b, err := l.ptr(ctx, in.Bucket)
	if err != nil || borrowerKey(ctx, in.Key) != nil {
		return func(*bufio.Writer) {} // not on posix anyway; answer nothing
	}
	c := *in
	c.Bucket = b
	return l.Next.SelectObjectContent(ctx, &c)
}

func (l *Lending) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	if refused(ctx) {
		return nil, errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return l.Next.GetBucketTagging(ctx, b)
}

func (l *Lending) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.PutBucketTagging(ctx, b, tags)
}

func (l *Lending) DeleteBucketTagging(ctx context.Context, bucket string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.DeleteBucketTagging(ctx, b)
}

func (l *Lending) GetObjectTagging(ctx context.Context, bucket, object, versionID string) (map[string]string, error) {
	if refused(ctx) {
		return nil, errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return nil, kerr
	}
	return l.Next.GetObjectTagging(ctx, b, object, versionID)
}

func (l *Lending) PutObjectTagging(ctx context.Context, bucket, object, versionID string, tags map[string]string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return kerr
	}
	return l.Next.PutObjectTagging(ctx, b, object, versionID, tags)
}

func (l *Lending) DeleteObjectTagging(ctx context.Context, bucket, object, versionID string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return kerr
	}
	return l.Next.DeleteObjectTagging(ctx, b, object, versionID)
}

func (l *Lending) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	return l.Next.PutObjectLockConfiguration(ctx, b, config)
}

func (l *Lending) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return l.Next.GetObjectLockConfiguration(ctx, b)
}

func (l *Lending) PutObjectRetention(ctx context.Context, bucket, object, versionID string, retention []byte) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return kerr
	}
	return l.Next.PutObjectRetention(ctx, b, object, versionID, retention)
}

func (l *Lending) GetObjectRetention(ctx context.Context, bucket, object, versionID string) ([]byte, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return nil, kerr
	}
	return l.Next.GetObjectRetention(ctx, b, object, versionID)
}

func (l *Lending) PutObjectLegalHold(ctx context.Context, bucket, object, versionID string, status bool) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	b, err := l.name(ctx, bucket)
	if err != nil {
		return err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return kerr
	}
	return l.Next.PutObjectLegalHold(ctx, b, object, versionID, status)
}

func (l *Lending) GetObjectLegalHold(ctx context.Context, bucket, object, versionID string) (*bool, error) {
	b, err := l.name(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if kerr := borrowerKey(ctx, &object); kerr != nil {
		return nil, kerr
	}
	return l.Next.GetObjectLegalHold(ctx, b, object, versionID)
}

func (l *Lending) ChangeBucketOwner(ctx context.Context, bucket, owner string) error {
	if refused(ctx) {
		return errNotForBorrower
	}
	return l.Next.ChangeBucketOwner(ctx, bucket, owner)
}

func (l *Lending) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	if refused(ctx) {
		return nil, errNotForBorrower
	}
	return l.Next.ListBucketsAndOwners(ctx)
}

var _ backend.Backend = (*Lending)(nil)
