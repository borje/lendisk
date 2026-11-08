package gateway

// Class is what the lender's surface does with an operation when a
// borrower's account (a friend's node ID as the access key) sends it
// (SEC-9, NET-7).
type Class string

const (
	// Admitted is on NET-7's list: the gate lets it through to
	// versitygw, which authenticates it, and Lending maps and polices it.
	Admitted Class = "admitted"
	// Refused is answered AccessDenied by the gate before versitygw
	// routes or authenticates it, so the store is never touched.
	Refused Class = "refused"
	// Internal is a backend method versitygw calls for itself while it
	// serves an admitted request (its access, CORS and object-lock
	// checks) or for plumbing; no request names it.
	Internal Class = "internal"
	// Unreachable is a backend method no S3 route reaches: only the
	// admin API calls it, which the gate refuses for everyone (SEC-8).
	Unreachable Class = "unreachable"
)

// Level is the part of the path an S3 route matches on.
type Level int

const (
	Service Level = iota // /
	Bucket               // /{bucket}
	Object               // /{bucket}/{key}
)

// Op is one S3 action versitygw's router names at the pinned version
// (v1.10.0, s3api/router.go), by its metrics action name, with a request
// that routes to it and its class for a borrower. The gate admits a
// borrower's request only when it has the exact shape of an Admitted
// op: that method and level, that op's query keys present (Query), no
// other query keys but Allow's and the signature's, and none of the
// headers in refusedHeaders. Everything else is refused, whatever
// versitygw would route it to.
type Op struct {
	Action string // versitygw's metrics action name, e.g. s3_PutObject
	Method string
	Level  Level
	Query  string // the query that routes to it, e.g. "uploadId=u&partNumber=1"
	Header string // a header that routes to it (X-Amz-Copy-Source), if any
	Class  Class  // Admitted or Refused
	Allow  []string
	Why    string
}

// Surface classifies every S3 action versitygw v1.10.0 routes (SEC-9).
// TestSurfaceCoversEveryRoutedAction fails when an upgrade routes an
// action this table does not name, and TestSurfaceExamplesRouteAsNamed
// sends each example through versitygw's own router.
var Surface = []Op{
	// NET-7's list.
	{Action: "s3_ListAllMyBuckets", Method: "GET", Level: Service, Class: Admitted,
		Allow: []string{"max-buckets", "continuation-token", "prefix", "bucket-region"},
		Why:   "list the borrower's own buckets: Lending answers from its rows in the mapping table"},
	{Action: "s3_CreateBucket", Method: "PUT", Level: Bucket, Class: Admitted,
		Why: "create the borrower's bucket under its sealed name, counted against quota"},
	{Action: "s3_HeadBucket", Method: "HEAD", Level: Bucket, Class: Admitted, Why: "check the borrower's bucket exists"},
	{Action: "s3_ListObjectsV2", Method: "GET", Level: Bucket, Query: "list-type=2", Class: Admitted,
		Allow: []string{"prefix", "delimiter", "continuation-token", "start-after", "max-keys", "encoding-type", "fetch-owner"},
		Why:   "list objects"},
	{Action: "s3_PutObject", Method: "PUT", Level: Object, Class: Admitted,
		Why: "store an object; or rewrite one, marked with x-amz-meta-lendisk-replaces, whose replaced version becomes a tombstone (LEND-4, RET-1)"},
	{Action: "s3_CreateMultipartUpload", Method: "POST", Level: Object, Query: "uploads", Class: Admitted, Why: "store a large object in parts"},
	{Action: "s3_UploadPart", Method: "PUT", Level: Object, Query: "uploadId=u&partNumber=1", Class: Admitted, Why: "store a part"},
	{Action: "s3_CompleteMultipartUpload", Method: "POST", Level: Object, Query: "uploadId=u", Class: Admitted, Why: "finish a large object"},
	{Action: "s3_AbortMultipartUpload", Method: "DELETE", Level: Object, Query: "uploadId=u", Class: Admitted, Why: "abandon an upload"},
	{Action: "s3_ListParts", Method: "GET", Level: Object, Query: "uploadId=u", Class: Admitted,
		Allow: []string{"max-parts", "part-number-marker"}, Why: "resume an upload"},
	{Action: "s3_GetObject", Method: "GET", Level: Object, Class: Admitted, Why: "read an object or a byte range of it"},
	{Action: "s3_HeadObject", Method: "HEAD", Level: Object, Class: Admitted, Why: "check an object's existence and size"},
	{Action: "s3_DeleteObject", Method: "DELETE", Level: Object, Class: Admitted,
		Why: "delete without a version id: a tombstone kept for the retention window (RET-1)"},

	// Content changes outside NET-7 (Q-H): copies, bulk and versioned deletes, uploads by form.
	{Action: "s3_CopyObject", Method: "PUT", Level: Object, Header: "X-Amz-Copy-Source", Class: Refused,
		Why: "a copy: its source names a bucket in a string (PEER-8); not captured (Q-H)"},
	{Action: "s3_UploadPartCopy", Method: "PUT", Level: Object, Query: "uploadId=u&partNumber=1", Header: "X-Amz-Copy-Source", Class: Refused,
		Why: "a copy into a part"},
	{Action: "s3_DeleteObjects", Method: "POST", Level: Bucket, Query: "delete", Class: Refused,
		Why: "bulk delete (RET-2): each delete is one DeleteObject"},
	{Action: "s3_PostObject", Method: "POST", Level: Bucket, Class: Refused,
		Why: "browser-form upload: refused for every account (Q-H, §3.3)"},
	{Action: "s3_RenameObject", Method: "PUT", Level: Object, Query: "renameObject", Class: Refused, Why: "not NET-7; not implemented"},
	{Action: "s3_RestoreObject", Method: "POST", Level: Object, Query: "restore", Class: Refused, Why: "not NET-7; not on posix"},
	{Action: "s3_SelectObjectContent", Method: "POST", Level: Object, Query: "select&select-type=2", Class: Refused, Why: "not NET-7; not on posix"},

	// Listings outside NET-7: versions, v1, uploads.
	{Action: "s3_ListObjects", Method: "GET", Level: Bucket, Class: Refused, Why: "list v1: NET-7 lists with v2"},
	{Action: "s3_ListObjectVersions", Method: "GET", Level: Bucket, Query: "versions", Class: Refused, Why: "a versioned call (RET-2)"},
	{Action: "s3_ListMultipartUploads", Method: "GET", Level: Bucket, Query: "uploads", Class: Refused, Why: "not NET-7"},
	{Action: "s3_GetObjectAttributes", Method: "GET", Level: Object, Query: "attributes", Class: Refused, Why: "not NET-7: HEAD gives size"},
	{Action: "s3_GetBucketLocation", Method: "GET", Level: Bucket, Query: "location", Class: Refused,
		Why: "not NET-7: lendisk's clients set the region"},
	{Action: "s3_GetObjectTorrent", Method: "GET", Level: Object, Query: "torrent", Class: Refused, Why: "not NET-7; not implemented"},

	// Metadata-only writes and reads (Q-H): ACLs, tagging, object lock, retention, legal hold.
	{Action: "s3_PutBucketAcl", Method: "PUT", Level: Bucket, Query: "acl", Class: Refused, Why: "would weaken the ownership wall (PEER-8)"},
	{Action: "s3_GetBucketAcl", Method: "GET", Level: Bucket, Query: "acl", Class: Refused, Why: "ACL"},
	{Action: "s3_PutObjectAcl", Method: "PUT", Level: Object, Query: "acl", Class: Refused, Why: "would weaken the ownership wall (PEER-8)"},
	{Action: "s3_GetObjectAcl", Method: "GET", Level: Object, Query: "acl", Class: Refused, Why: "ACL"},
	{Action: "s3_PutBucketTagging", Method: "PUT", Level: Bucket, Query: "tagging", Class: Refused, Why: "tagging"},
	{Action: "s3_GetBucketTagging", Method: "GET", Level: Bucket, Query: "tagging", Class: Refused, Why: "tagging"},
	{Action: "s3_DeleteBucketTagging", Method: "DELETE", Level: Bucket, Query: "tagging", Class: Refused, Why: "tagging"},
	{Action: "s3_PutObjectTagging", Method: "PUT", Level: Object, Query: "tagging", Class: Refused, Why: "tagging: metadata a replica does not carry"},
	{Action: "s3_GetObjectTagging", Method: "GET", Level: Object, Query: "tagging", Class: Refused, Why: "tagging"},
	{Action: "s3_DeleteObjectTagging", Method: "DELETE", Level: Object, Query: "tagging", Class: Refused, Why: "tagging"},
	{Action: "s3_PutObjectLockConfiguration", Method: "PUT", Level: Bucket, Query: "object-lock", Class: Refused, Why: "object lock (RET-2)"},
	{Action: "s3_GetObjectLockConfiguration", Method: "GET", Level: Bucket, Query: "object-lock", Class: Refused, Why: "object lock"},
	{Action: "s3_PutObjectRetention", Method: "PUT", Level: Object, Query: "retention", Class: Refused, Why: "retention (RET-2)"},
	{Action: "s3_GetObjectRetention", Method: "GET", Level: Object, Query: "retention", Class: Refused, Why: "retention"},
	{Action: "s3_PutObjectLegalHold", Method: "PUT", Level: Object, Query: "legal-hold", Class: Refused, Why: "legal hold (RET-2)"},
	{Action: "s3_GetObjectLegalHold", Method: "GET", Level: Object, Query: "legal-hold", Class: Refused, Why: "legal hold"},
	{Action: "s3_PutObjectAnnotation", Method: "PUT", Level: Object, Query: "annotation", Class: Refused, Why: "not NET-7; not implemented"},
	{Action: "s3_GetObjectAnnotation", Method: "GET", Level: Object, Query: "annotation&annotationName=a", Class: Refused, Why: "not NET-7"},
	{Action: "s3_ListObjectAnnotations", Method: "GET", Level: Object, Query: "annotation", Class: Refused, Why: "not NET-7"},
	{Action: "s3_DeleteObjectAnnotation", Method: "DELETE", Level: Object, Query: "annotation", Class: Refused, Why: "not NET-7"},

	// Bucket configuration and deletion.
	{Action: "s3_DeleteBucket", Method: "DELETE", Level: Bucket, Class: Refused,
		Why: "bucket deletion (RET-2); its mapping row and quota count would go stale"},
	{Action: "s3_PutBucketVersioning", Method: "PUT", Level: Bucket, Query: "versioning", Class: Refused, Why: "versioning (RET-2)"},
	{Action: "s3_GetBucketVersioning", Method: "GET", Level: Bucket, Query: "versioning", Class: Refused, Why: "versioning"},
	{Action: "s3_PutBucketPolicy", Method: "PUT", Level: Bucket, Query: "policy", Class: Refused, Why: "would weaken the ownership wall (PEER-8)"},
	{Action: "s3_GetBucketPolicy", Method: "GET", Level: Bucket, Query: "policy", Class: Refused, Why: "policy"},
	{Action: "s3_DeleteBucketPolicy", Method: "DELETE", Level: Bucket, Query: "policy", Class: Refused, Why: "policy"},
	{Action: "s3_GetBucketPolicyStatus", Method: "GET", Level: Bucket, Query: "policyStatus", Class: Refused, Why: "policy"},
	{Action: "s3_PutBucketOwnershipControls", Method: "PUT", Level: Bucket, Query: "ownershipControls", Class: Refused, Why: "ownership (PEER-8)"},
	{Action: "s3_GetBucketOwnershipControls", Method: "GET", Level: Bucket, Query: "ownershipControls", Class: Refused, Why: "ownership"},
	{Action: "s3_DeleteBucketOwnershipControls", Method: "DELETE", Level: Bucket, Query: "ownershipControls", Class: Refused, Why: "ownership"},
	{Action: "s3_PutBucketCors", Method: "PUT", Level: Bucket, Query: "cors", Class: Refused, Why: "CORS"},
	{Action: "s3_GetBucketCors", Method: "GET", Level: Bucket, Query: "cors", Class: Refused, Why: "CORS"},
	{Action: "s3_DeleteBucketCors", Method: "DELETE", Level: Bucket, Query: "cors", Class: Refused, Why: "CORS"},
	{Action: "s3_PutBucketWebsite", Method: "PUT", Level: Bucket, Query: "website", Class: Refused, Why: "website"},
	{Action: "s3_GetBucketWebsite", Method: "GET", Level: Bucket, Query: "website", Class: Refused, Why: "website"},
	{Action: "s3_DeleteBucketWebsite", Method: "DELETE", Level: Bucket, Query: "website", Class: Refused, Why: "website"},
	{Action: "s3_PutBucketLifecycleConfiguration", Method: "PUT", Level: Bucket, Query: "lifecycle", Class: Refused, Why: "lifecycle (RET-2)"},
	{Action: "s3_GetBucketLifecycleConfiguration", Method: "GET", Level: Bucket, Query: "lifecycle", Class: Refused, Why: "lifecycle"},
	{Action: "s3_DeleteBucketLifecycle", Method: "DELETE", Level: Bucket, Query: "lifecycle", Class: Refused, Why: "lifecycle"},
	{Action: "s3_PutBucketAnalyticsConfiguration", Method: "PUT", Level: Bucket, Query: "analytics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketAnalyticsConfiguration", Method: "GET", Level: Bucket, Query: "analytics&id=x", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_ListBucketAnalyticsConfigurations", Method: "GET", Level: Bucket, Query: "analytics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketAnalyticsConfiguration", Method: "DELETE", Level: Bucket, Query: "analytics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketEncryption", Method: "PUT", Level: Bucket, Query: "encryption", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketEncryption", Method: "GET", Level: Bucket, Query: "encryption", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketEncryption", Method: "DELETE", Level: Bucket, Query: "encryption", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketIntelligentTieringConfiguration", Method: "PUT", Level: Bucket, Query: "intelligent-tiering", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketIntelligentTieringConfiguration", Method: "GET", Level: Bucket, Query: "intelligent-tiering&id=x", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_ListBucketIntelligentTieringConfigurations", Method: "GET", Level: Bucket, Query: "intelligent-tiering", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketIntelligentTieringConfiguration", Method: "DELETE", Level: Bucket, Query: "intelligent-tiering", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketInventoryConfiguration", Method: "PUT", Level: Bucket, Query: "inventory", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketInventoryConfiguration", Method: "GET", Level: Bucket, Query: "inventory&id=x", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_ListBucketInventoryConfigurations", Method: "GET", Level: Bucket, Query: "inventory", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketInventoryConfiguration", Method: "DELETE", Level: Bucket, Query: "inventory", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketMetricsConfiguration", Method: "PUT", Level: Bucket, Query: "metrics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketMetricsConfiguration", Method: "GET", Level: Bucket, Query: "metrics&id=x", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_ListBucketMetricsConfigurations", Method: "GET", Level: Bucket, Query: "metrics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketMetricsConfiguration", Method: "DELETE", Level: Bucket, Query: "metrics", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketReplication", Method: "PUT", Level: Bucket, Query: "replication", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketReplication", Method: "GET", Level: Bucket, Query: "replication", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeleteBucketReplication", Method: "DELETE", Level: Bucket, Query: "replication", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutPublicAccessBlock", Method: "PUT", Level: Bucket, Query: "publicAccessBlock", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetPublicAccessBlock", Method: "GET", Level: Bucket, Query: "publicAccessBlock", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_DeletePublicAccessBlock", Method: "DELETE", Level: Bucket, Query: "publicAccessBlock", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketLogging", Method: "PUT", Level: Bucket, Query: "logging", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketLogging", Method: "GET", Level: Bucket, Query: "logging", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketRequestPayment", Method: "PUT", Level: Bucket, Query: "requestPayment", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketRequestPayment", Method: "GET", Level: Bucket, Query: "requestPayment", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketNotificationConfiguration", Method: "PUT", Level: Bucket, Query: "notification", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketNotificationConfiguration", Method: "GET", Level: Bucket, Query: "notification", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_PutBucketAccelerateConfiguration", Method: "PUT", Level: Bucket, Query: "accelerate", Class: Refused, Why: "bucket configuration"},
	{Action: "s3_GetBucketAccelerateConfiguration", Method: "GET", Level: Bucket, Query: "accelerate", Class: Refused, Why: "bucket configuration"},

	// versitygw's own error routes and CORS preflight.
	{Action: "s3_Options", Method: "OPTIONS", Level: Bucket, Class: Refused, Why: "CORS preflight: reads the bucket's CORS, which NET-7 does not offer"},
	{Action: "ActionUnDetected", Method: "GET", Level: Service, Header: "X-Amz-Copy-Source", Class: Refused,
		Why: "versitygw's error routes (a copy source where none is allowed, uploads with uploadId, torrent writes, unmatched methods)"},
}

// BackendMethod classifies one method of versitygw's backend.Backend
// interface for a borrower's account (SEC-9).
type BackendMethod struct {
	Class Class
	Why   string
}

// BackendMethods classifies every backend.Backend method at the pinned
// version; TestBackendMethodsAllClassified fails when an upgrade adds
// one. Admitted methods are reached by an admitted op; Refused ones only
// by refused ops; Internal ones by versitygw itself.
var BackendMethods = map[string]BackendMethod{
	"String":               {Internal, "plumbing"},
	"Shutdown":             {Internal, "plumbing"},
	"NormalizeObjectKey":   {Internal, "plumbing: the router normalises keys"},
	"StoresDotSegmentKeys": {Internal, "plumbing"},
	"ValidateUploadID":     {Internal, "plumbing: the multipart routes check the id's form"},

	"ListBuckets":             {Admitted, "s3_ListAllMyBuckets"},
	"HeadBucket":              {Admitted, "s3_HeadBucket"},
	"CreateBucket":            {Admitted, "s3_CreateBucket"},
	"CreateMultipartUpload":   {Admitted, "s3_CreateMultipartUpload"},
	"CompleteMultipartUpload": {Admitted, "s3_CompleteMultipartUpload: captured (Q-H)"},
	"AbortMultipartUpload":    {Admitted, "s3_AbortMultipartUpload"},
	"ListParts":               {Admitted, "s3_ListParts"},
	"UploadPart":              {Admitted, "s3_UploadPart"},
	"PutObject":               {Admitted, "s3_PutObject: captured (Q-H)"},
	"HeadObject":              {Admitted, "s3_HeadObject"},
	"GetObject":               {Admitted, "s3_GetObject"},
	"ListObjectsV2":           {Admitted, "s3_ListObjectsV2"},
	"DeleteObject":            {Admitted, "s3_DeleteObject: captured; a tombstone for a borrower (RET-1)"},

	"GetBucketAcl":               {Internal, "versitygw's access check on every request (Q-I)"},
	"GetBucketPolicy":            {Internal, "versitygw's access check on every request (Q-I)"},
	"GetBucketCors":              {Internal, "versitygw's CORS check when a request carries Origin"},
	"GetBucketVersioning":        {Internal, "versitygw's object-lock check on writes and deletes; versioning is off"},
	"GetObjectLockConfiguration": {Internal, "versitygw's object-lock check on writes and deletes; lock is never on"},
	"GetObjectRetention":         {Internal, "versitygw's object-lock check on deletes"},
	"GetObjectLegalHold":         {Internal, "versitygw's object-lock check on deletes"},

	"PutBucketAcl":                  {Refused, "s3_PutBucketAcl; Lending refuses it too"},
	"DeleteBucket":                  {Refused, "s3_DeleteBucket; Lending refuses it too"},
	"PutBucketVersioning":           {Refused, "s3_PutBucketVersioning"},
	"PutBucketPolicy":               {Refused, "s3_PutBucketPolicy; Lending refuses it too"},
	"DeleteBucketPolicy":            {Refused, "s3_DeleteBucketPolicy; Lending refuses it too"},
	"PutBucketOwnershipControls":    {Refused, "s3_PutBucketOwnershipControls; Lending refuses it too"},
	"GetBucketOwnershipControls":    {Refused, "s3_GetBucketOwnershipControls, s3_PutBucketAcl"},
	"DeleteBucketOwnershipControls": {Refused, "s3_DeleteBucketOwnershipControls; Lending refuses it too"},
	"PutBucketCors":                 {Refused, "s3_PutBucketCors"},
	"DeleteBucketCors":              {Refused, "s3_DeleteBucketCors"},
	"PutBucketWebsite":              {Refused, "s3_PutBucketWebsite"},
	"GetBucketWebsite":              {Refused, "s3_GetBucketWebsite"},
	"DeleteBucketWebsite":           {Refused, "s3_DeleteBucketWebsite"},
	"ListMultipartUploads":          {Refused, "s3_ListMultipartUploads"},
	"UploadPartCopy":                {Refused, "s3_UploadPartCopy; Lending refuses it too (Q-H)"},
	"GetObjectAcl":                  {Refused, "s3_GetObjectAcl"},
	"GetObjectAttributes":           {Refused, "s3_GetObjectAttributes"},
	"CopyObject":                    {Refused, "s3_CopyObject; Lending refuses it too (Q-H)"},
	"ListObjects":                   {Refused, "s3_ListObjects"},
	"DeleteObjects":                 {Refused, "s3_DeleteObjects; Lending refuses it too (RET-2)"},
	"PutObjectAcl":                  {Refused, "s3_PutObjectAcl; Lending refuses it too"},
	"ListObjectVersions":            {Refused, "s3_ListObjectVersions"},
	"RestoreObject":                 {Refused, "s3_RestoreObject; not on posix"},
	"SelectObjectContent":           {Refused, "s3_SelectObjectContent; not on posix"},
	"GetBucketTagging":              {Refused, "s3_GetBucketTagging"},
	"PutBucketTagging":              {Refused, "s3_PutBucketTagging"},
	"DeleteBucketTagging":           {Refused, "s3_DeleteBucketTagging"},
	"GetObjectTagging":              {Refused, "s3_GetObjectTagging"},
	"PutObjectTagging":              {Refused, "s3_PutObjectTagging (Q-H: metadata only)"},
	"DeleteObjectTagging":           {Refused, "s3_DeleteObjectTagging (Q-H: metadata only)"},
	"PutObjectLockConfiguration":    {Refused, "s3_PutObjectLockConfiguration"},
	"PutObjectRetention":            {Refused, "s3_PutObjectRetention (Q-H: metadata only)"},
	"PutObjectLegalHold":            {Refused, "s3_PutObjectLegalHold (Q-H: metadata only)"},

	"ChangeBucketOwner":    {Unreachable, "admin API only: every PATCH is refused (SEC-8); Lending refuses it too"},
	"ListBucketsAndOwners": {Unreachable, "admin API only: every PATCH is refused (SEC-8); Lending refuses it too"},
}
