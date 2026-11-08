package cli

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/transport"
)

// LEND-2, SEC-5: a borrower's request body on the tunnel that stops, or
// trickles below the minimum rate, or never starts though its headers
// promised one, ends within the stall bound, end to end through the
// bridge, versitygw and the lending layer; nothing is stored, and the
// lender serves the next request as usual. Run with -race: nothing reads
// the body behind the store's back.
func TestTunnelEndsStalledBodies(t *testing.T) {
	serving(t, 0)
	a, b := friendNode(t), friendNode(t)
	const stall, minProgress = time.Second, 1000
	a.tuneBridge = func(br *bridge.Bridge) { br.Stall, br.MinProgress = stall, minProgress }
	stop := a.serveLogged()
	defer stop()
	befriend(t, a, b)
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID := b.identity()
	creds, _ := pairCreds(t, bID, aKey)
	hc := tunnelHTTP(t, bID, aAddr)
	cb := s3Over(hc, creds)
	ctx := t.Context()
	sealed := bID.SealedName("laptop")
	if _, err := cb.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}
	cred, err := creds.Retrieve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const size = 64 << 10
	whole := bytes.Repeat([]byte("s"), size)
	// put sends a signed PUT of size bytes whose body comes from feed.
	put := func(key string, feed func(w *io.PipeWriter)) (time.Duration, error) {
		pr, pw := io.Pipe()
		t.Cleanup(func() { pw.CloseWithError(io.ErrClosedPipe) })
		go feed(pw)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://lender:9000/"+sealed+"/"+key, pr)
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = size
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		req.Header.Set("X-Amz-Meta-Lendisk-Sha256", sha256Hex(whole))
		if err := v4.NewSigner().SignHTTP(ctx, cred, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		resp, err := hc.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("RequestTimeout")) {
				err = unexpected{resp.StatusCode, string(body)}
			}
		}
		return time.Since(start), err
	}
	cases := map[string]func(w *io.PipeWriter){
		"stalled": func(w *io.PipeWriter) { w.Write(whole[:10<<10]) },
		"trickling": func(w *io.PipeWriter) {
			for i := 0; i < size; i++ {
				if _, err := w.Write(whole[i : i+1]); err != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		},
		"header only": func(w *io.PipeWriter) {},
	}
	for name, feed := range cases {
		took, err := put("data/"+name, feed)
		if u, ok := err.(unexpected); ok {
			t.Errorf("%s: answered %d %s; want 400 RequestTimeout", name, u.status, u.body)
		}
		if took > 10*stall {
			t.Errorf("%s ended after %v; the bound is %v", name, took, stall)
		}
		t.Logf("%s: ended after %v: %v", name, took, err)
	}
	if got := lentObjects(t, a, bID, "laptop"); len(got) != 0 {
		t.Fatalf("A holds %v", got)
	}
	if _, err := cb.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/ok"),
		Body: bytes.NewReader(whole), Metadata: hashMeta(whole)}); err != nil {
		t.Fatalf("a put after the stalls: %v", err)
	}
}

// unexpected is an answer other than the stall refusal.
type unexpected struct {
	status int
	body   string
}

func (u unexpected) Error() string { return http.StatusText(u.status) }
