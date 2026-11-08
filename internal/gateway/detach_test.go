package gateway

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3response"
)

// requestCtx stands in for the *fasthttp.RequestCtx versitygw hands the
// backend: its Done is the server's shutdown channel, and fasthttp
// recycles its values once the handler returns. Anything that asks for
// Done, Err or Deadline is watching the request past its handler: each
// context.WithCancel below it starts a goroutine that, at shutdown,
// reads the recycled values (the race CI caught on a7d636f).
type requestCtx struct {
	context.Context // the request's values
	done            chan struct{}
	watched         atomic.Bool
}

func (r *requestCtx) Done() <-chan struct{} { r.watched.Store(true); return r.done }
func (r *requestCtx) Err() error            { r.watched.Store(true); return nil }
func (r *requestCtx) Deadline() (time.Time, bool) {
	r.watched.Store(true)
	return time.Time{}, false
}

// A borrower's writes, rewrites, reads, lists and deletes, including a
// rewrite refused over quota, never watch the request's own context: the
// database and the store see a context detached from fasthttp.
func TestARequestsContextIsNotWatchedPastItsHandler(t *testing.T) {
	v1, v2 := []byte("config one"), []byte("config two")
	q := lendTo(t, 3*ov+int64(len(v1)), nil)
	if err := q.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	req := &requestCtx{done: make(chan struct{}), Context: context.WithValue(context.Background(), //nolint:staticcheck // versitygw's key
		"account", auth.Account{Access: q.peer(), Role: auth.RoleUser})}
	l := q.lending()
	put := func(key string, body []byte, meta map[string]string) error {
		n := int64(len(body))
		_, err := l.PutObject(req, s3response.PutObjectInput{Bucket: aws.String(*q.bucket), Key: aws.String(key),
			ContentLength: &n, Body: utils.NewContentLengthReader(bytes.NewReader(body), n), Metadata: meta})
		return err
	}
	if err := put("config", v2, rewriteMeta(v2, sum(v1))); err != error(errQuotaExceeded) {
		t.Fatalf("rewrite over quota: %v", err)
	}
	if err := put("other", v1, hashMeta(v1)); err != error(errQuotaExceeded) {
		t.Fatalf("put over quota: %v", err)
	}
	if _, err := l.HeadObject(req, &s3.HeadObjectInput{Bucket: q.bucket, Key: aws.String("config")}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ListObjectsV2(req, &s3.ListObjectsV2Input{Bucket: q.bucket, StartAfter: aws.String(""), MaxKeys: aws.Int32(1000)}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.DeleteObject(req, &s3.DeleteObjectInput{Bucket: q.bucket, Key: aws.String("config")}); err != nil {
		t.Fatal(err)
	}
	if req.watched.Load() {
		t.Fatal("a request's fasthttp context was watched for cancellation; it is recycled when the handler returns")
	}
}

// Every Lending method versitygw calls with a request's context detaches
// it first, so a method added later cannot hand the fasthttp context to
// the database or the store.
func TestEveryLendingMethodDetachesTheRequestContext(t *testing.T) {
	fs := token.NewFileSet()
	f, err := parser.ParseFile(fs, "lend.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || !fn.Name.IsExported() || len(fn.Type.Params.List) == 0 {
			continue
		}
		p := fn.Type.Params.List[0]
		if sel, ok := p.Type.(*ast.SelectorExpr); !ok || sel.Sel.Name != "Context" || len(p.Names) == 0 || p.Names[0].Name != "ctx" {
			continue
		}
		n++
		if !startsWithDetach(fn.Body) {
			t.Errorf("Lending.%s does not start with ctx = detach(ctx)", fn.Name.Name)
		}
	}
	if n < 50 {
		t.Fatalf("found only %d Lending methods taking a context", n)
	}
}

func startsWithDetach(body *ast.BlockStmt) bool {
	if body == nil || len(body.List) == 0 {
		return false
	}
	as, ok := body.List[0].(*ast.AssignStmt)
	if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 || as.Tok != token.ASSIGN {
		return false
	}
	call, ok := as.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	fn, ok := call.Fun.(*ast.Ident)
	lhs, lok := as.Lhs[0].(*ast.Ident)
	return ok && lok && fn.Name == "detach" && lhs.Name == "ctx"
}
