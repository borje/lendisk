package cli

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/invite"
	"github.com/borje/lendisk/internal/transport"
)

// borrowOver is borrower's stock S3 client at lender over the tunnel,
// dialled with key, signing with the pair's derived credential.
func borrowOver(t *testing.T, key, borrower *identity.Identity, lender identity.NodeID, addr string) *s3.Client {
	t.Helper()
	creds, _ := pairCreds(t, borrower, lender)
	return s3Over(tunnelHTTP(t, key, addr), creds)
}

// SEC-7 AC, with friendships A–B and A–C: a hostile B, holding every
// credential its node holds (its identity, so its derived secret at A,
// and its dial key for A), sends A every S3 operation versitygw routes
// and every control API call, from its node ID and from its dial key.
//
//   - NET-7, SEC-9: every operation outside the list is refused, and A's
//     store is unchanged; the admitted ones reach nothing but B's own
//     buckets.
//   - #69: every answer on the dial key is the answer on the node ID; an
//     S3 request signed with the dial key, with C's credential or with
//     A's own node ID as the access key is refused on either connection.
//   - RET-2 AC over the tunnel: TestBorrowerCannotPurgeEarly's attack list,
//     from both of B's keys, leaves B's tombstoned object as it was.
//   - #69: a redeem cannot register another friend's dial key, a friend's
//     node ID, its own node ID, the invite key or A's node ID as a dial
//     key.
//   - SEC-10: an unknown identity, and B at C, which is not B's friend,
//     are refused the connection.
//   - After B's friendship is removed (the row, as `peer remove` will,
//     #25), both of B's keys are refused everything on their open
//     connections, like a stranger's.
func TestHostilePeer(t *testing.T) {
	a, b, c := friendNode(t), friendNode(t), friendNode(t)
	if code, _, errOut := a.run("", "", "bucket", "add", "photos", "--profile", "restic"); code != 0 {
		t.Fatal(errOut)
	}
	stopA := a.serveLogged()
	stopped := false
	defer func() {
		if !stopped {
			stopA()
		}
	}()
	befriend(t, a, b)
	befriend(t, a, c)
	ctx := t.Context()
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID, cID := b.identity(), c.identity()
	bDial := bID.DialKeyFor(aKey)
	bSealed, cSealed := bID.SealedName("laptop"), cID.SealedName("laptop")

	// B and C each keep a bucket at A; B's holds a tombstoned object and
	// an upload begun before the delete.
	cS3 := borrowOver(t, cID, cID, aKey, aAddr)
	if _, err := cS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cSealed)}); err != nil {
		t.Fatal(err)
	}
	// One tunnel client per key: two with one key displace each other at
	// the relay, so B's stock clients and its attackers share them.
	bCreds, bSecret := pairCreds(t, bID, aKey)
	_, cSecret := pairCreds(t, cID, aKey)
	bNode := attacker(t, "B on its node ID", bID, aAddr, bID.NodeID().String(), bSecret)
	bOnDial := attacker(t, "B on its dial key", bDial, aAddr, bID.NodeID().String(), bSecret)
	bS3 := s3Over(bOnDial.hc, bCreds)
	if _, err := bS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bSealed)}); err != nil {
		t.Fatal(err)
	}
	body := []byte("the only offsite copy")
	for _, cl := range []struct {
		c      *s3.Client
		bucket string
	}{{bS3, bSealed}, {cS3, cSealed}} {
		if _, err := cl.c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cl.bucket), Key: aws.String("data/ef/pack"),
			Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
			t.Fatal(err)
		}
	}
	up, err := bS3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bSealed), Key: aws.String("data/ef/pack"),
		Metadata: hashMeta(body)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bS3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bSealed), Key: aws.String("data/ef/pack")}); err != nil {
		t.Fatal(err)
	}
	aDB := openDB(t, a)
	tombs, err := aDB.TombstonesOf(ctx, bID.NodeID().String(), bSealed, "data/ef/pack")
	if err != nil || len(tombs) != 1 {
		t.Fatalf("B's tombstone at A: %+v %v", tombs, err)
	}
	tomb := tombs[0]
	bDir := filepath.Join(a.data, "store", gateway.LentName(bID.NodeID(), bSealed))
	tombIntact := func(when string) {
		t.Helper()
		if got, err := os.ReadFile(filepath.Join(bDir, filepath.FromSlash(tomb.Tomb))); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("after %s the tomb holds %q (%v)", when, got, err)
		}
		if ts, err := aDB.TombstonesOf(ctx, bID.NodeID().String(), bSealed, "data/ef/pack"); err != nil || len(ts) != 1 || ts[0].ID != tomb.ID {
			t.Fatalf("after %s the tombstones are %+v (%v)", when, ts, err)
		}
	}
	store := filepath.Join(a.data, "store")
	foreign := []string{cSealed, "photos", gateway.LentName(cID.NodeID(), cSealed), gateway.LentName(bID.NodeID(), bSealed)}

	for _, h := range []*hostile{bNode, bOnDial} {
		// Everything outside NET-7's list, on B's own bucket, where B is
		// most at home, and on its tomb: refused, A's store unchanged.
		before := tree(t, store)
		h.mustRefuse(s3Attacks(bSealed, "data/ef/pack", isRefused))
		h.mustRefuse(s3Attacks(bSealed, tomb.Tomb, isRefused))
		// The admitted ones reach nothing but B's own buckets. (Creating
		// a bucket by any sealed name makes one of B's own, under B's
		// prefix, and listing buckets lists B's alone: both are left out.)
		for _, bucket := range foreign {
			h.mustFail(s3Attacks(bucket, "data/ef/pack", reachesABucket))
		}
		sameTree(t, h.label, before, tree(t, store))
		tombIntact(h.label)
		// Credentials B's connection does not stand for.
		for _, w := range []*hostile{
			h.signingAs(h.label+", signing with its dial key", bDial.NodeID().String(), bSecret),
			h.signingAs(h.label+", signing with C's credential", cID.NodeID().String(), cSecret),
			h.signingAs(h.label+", signing as A", aKey.String(), bSecret),
		} {
			w.mustRefuse(s3Attacks(bSealed, "data/ab/x", nil))
		}
		// B's own credential respelled with whitespace, which versitygw
		// still reads as B's node ID: still refused everything outside
		// the list (#15 review).
		for name, f := range respellings(bID.NodeID().String()) {
			w := h.respelled(h.label+", "+name, f)
			w.mustRefuse(s3Attacks(bSealed, "data/ef/pack", isRefused))
			// The spelling is live: versitygw authenticates it as B.
			head := s3Attacks(bSealed, "x", func(op gateway.Op) bool { return op.Action == "s3_HeadBucket" })[0]
			if r := w.send(head); r.status != http.StatusOK {
				t.Errorf("%s: HeadBucket %v; the respelling is not read as B's key", w.label, r)
			}
		}
		sameTree(t, h.label+" with others' credentials", before, tree(t, store))
	}

	// #69: the dial key is answered exactly as the node ID, for every S3
	// operation and every control call.
	for _, at := range append(s3Attacks(bSealed, "data/ab/cd", nil), controlAttacks(bSealed, cSealed)...) {
		onNode, onDial := bNode.send(at), bOnDial.send(at)
		if onNode.err != nil || onDial.err != nil || onNode.status != onDial.status || onNode.code != onDial.code {
			t.Errorf("%s (%s %s): on the node ID %v, on the dial key %v", at.name, at.method, at.target, onNode, onDial)
		}
	}
	for _, at := range controlAttacks(bSealed, cSealed) {
		r := bOnDial.send(at)
		switch at.name {
		case "version", "list own":
			if r.status != http.StatusOK {
				t.Errorf("B's %s: %v", at.name, r)
			}
		case "manifest get":
			if r.status != http.StatusOK && r.status != http.StatusNotFound {
				t.Errorf("B's %s: %v", at.name, r)
			}
		case "list another's":
			if r.status != http.StatusNotFound || r.code != control.CodeNoSuchBucket {
				t.Errorf("B lists C's bucket: %v", r)
			}
		default:
			if r.status < 400 {
				t.Errorf("B's %s: %v", at.name, r)
			}
		}
	}

	// RET-2 AC over the tunnel, from both of B's keys.
	for _, cl := range []*s3.Client{bS3, s3Over(bNode.hc, bCreds)} {
		// In TestBorrowerCannotPurgeEarly's order, which aborts the old
		// upload before completing it. Completed first, it would revive
		// the object, which loses nothing
		// (TestCompletingAnOldUploadRevivesTheObject).
		attacks := purgeAttacks(cl, bSealed, "data/ef/pack", tomb.Tomb, aws.ToString(up.UploadId), body)
		for _, name := range slices.Sorted(maps.Keys(attacks)) {
			attacks[name]()
			tombIntact(name)
		}
	}

	// Squatting: B creating a bucket under C's sealed name makes one of
	// its own, under its own prefix; C's bucket is untouched and B reads
	// nothing of C's through it (PEER-8).
	cDir := filepath.Join(a.data, "store", gateway.LentName(cID.NodeID(), cSealed))
	cBefore := tree(t, cDir)
	if _, err := bS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cSealed)}); err != nil {
		t.Fatalf("B's bucket by C's sealed name: %v", err)
	}
	if _, err := bS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(cSealed), Key: aws.String("data/ef/pack")}); s3Code(err) != "NoSuchKey" {
		t.Fatalf("B reading C's object through its own bucket of that name: %v", err)
	}
	sameTree(t, "B's bucket by C's sealed name", cBefore, tree(t, cDir))

	// #69: redeem abuses.
	redeemAbuses(t, a, bID, cID, aKey, aAddr)
	if f, err := aDB.Friendships(ctx); err != nil || len(f) != 2 {
		t.Fatalf("A's friends after the redeem abuses: %d %v", len(f), err)
	}
	if p, _, err := aDB.TunnelPeer(ctx, bDial.NodeID().String(), time.Now()); err != nil || p != bID.NodeID().String() {
		t.Fatalf("B's dial key names %q (%v)", p, err)
	}

	// SEC-10: a stranger is refused the connection.
	stranger := strangerID(t)
	strangerSecret, _ := stranger.S3Secret(aKey)
	attacker(t, "a stranger", stranger, aAddr, stranger.NodeID().String(), strangerSecret).
		mustNotConnect(append(s3Attacks(bSealed, "data/ab/x", isAdmitted)[:2], controlAttacks(bSealed, cSealed)[:1]...))

	// After removal both of B's keys are refused everything on their
	// open connections.
	if _, err := aDB.ExecContext(ctx, `DELETE FROM friendship WHERE peer_key = ?`, bID.NodeID().String()); err != nil {
		t.Fatal(err)
	}
	before := tree(t, store)
	for _, h := range []*hostile{bNode, bOnDial} {
		h.label += " after removal"
		h.mustRefuse(append(s3Attacks(bSealed, "data/ab/x", nil), controlAttacks(bSealed, cSealed)...))
	}
	sameTree(t, "after removal", before, tree(t, store))
	stopA()
	bNode.close()
	bOnDial.close()
	stopped = true

	// B at C, which is not its friend, from its node ID and from the
	// dial key it would use: refused the connection (one gateway per
	// process, so C serves once A has stopped).
	stopC := c.serveLogged()
	defer stopC()
	cAddr := c.status().Node.Address
	cKey, err := transport.KeyOf(cAddr)
	if err != nil {
		t.Fatal(err)
	}
	bAtC, _ := bID.S3Secret(cKey)
	for _, key := range []*identity.Identity{bID, bID.DialKeyFor(cKey)} {
		attacker(t, "B at C", key, cAddr, bID.NodeID().String(), bAtC).
			mustNotConnect(append(s3Attacks(cSealed, "data/ab/x", isAdmitted)[:2], controlAttacks(cSealed, cSealed)[:1]...))
	}
}

func strangerID(t *testing.T) *identity.Identity {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// purgeAttacks is TestBorrowerCannotPurgeEarly's attack list (RET-2 AC)
// for a borrower's client c on its bucket, where key was deleted into
// tomb and upload was begun on key before the delete. Each attack's
// answer is ignored: what counts is the tomb afterwards.
func purgeAttacks(c *s3.Client, bucket, key, tomb, upload string, body []byte) map[string]func() {
	ctx := context.Background()
	b, k := aws.String(bucket), func(s string) *string { return aws.String(s) }
	other := []byte("ransom note")
	put := func(key string, data, claimed []byte) {
		c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k(key), Body: bytes.NewReader(data), Metadata: hashMeta(claimed)})
	}
	complete := func(key string, id *string, data []byte) {
		p, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: b, Key: k(key), UploadId: id, PartNumber: aws.Int32(1), Body: bytes.NewReader(data)})
		if err != nil {
			return
		}
		c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: b, Key: k(key), UploadId: id,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: p.ETag, PartNumber: aws.Int32(1)}}}})
	}
	mpu := func(key string, data []byte) {
		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: b, Key: k(key), Metadata: hashMeta(data)})
		if err == nil {
			complete(key, up.UploadId, data)
		}
	}
	del := func(key string) { c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k(key)}) }
	return map[string]func(){
		"delete again":        func() { del(key) },
		"delete version null": func() { c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k(key), VersionId: k("null")}) },
		"delete a version": func() {
			c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: b, Key: k(key), VersionId: k("01HZZZZZZZZZZZZZZZZZZZZZZZ")})
		},
		"multi-delete": func() {
			c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: b, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: k(key)}, {Key: k(tomb)}}}})
		},
		"overwrite":               func() { put(key, other, other) },
		"overwrite claiming hash": func() { put(key, other, body) },
		"multipart other bytes":   func() { mpu(key, other) },
		"complete an old upload":  func() { complete(key, k(upload), other) },
		"abort an old upload": func() {
			c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: b, Key: k(key), UploadId: k(upload)})
		},
		"delete the tomb":         func() { del(tomb) },
		"delete the tomb dir":     func() { del(".lendisk/tomb/" + key) },
		"delete under .LENDISK":   func() { del(strings.Replace(tomb, ".lendisk", ".LENDISK", 1)) },
		"delete through ..":       func() { del("data/../" + tomb) },
		"delete nested .lendisk":  func() { del("data/.lendisk/tomb/x") },
		"put onto the tomb":       func() { put(tomb, other, other) },
		"multipart onto the tomb": func() { mpu(tomb, other) },
		"read the tomb":           func() { c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k(tomb)}) },
		"copy from the tomb": func() {
			c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: b, Key: k(key), CopySource: k(bucket + "/" + tomb)})
		},
		"tag the tomb": func() {
			c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: b, Key: k(tomb), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: k("a"), Value: k("b")}}}})
		},
		"versioning then delete": func() {
			c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: b, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
			del(key)
		},
		"lifecycle expiry": func() {
			c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: b, LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: k("")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)}}}}})
		},
		"delete the bucket": func() { c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: b}) },
	}
}

// redeemAbuses runs #69's redeem abuses at a, which must be serving: a
// hostile B holding a second identity D, which a invites, redeems
// claiming as D's dial key each key it must not register, proving it
// with the key's private half wherever B holds that. Each is refused.
func redeemAbuses(t *testing.T, a *node, bID, cID *identity.Identity, aKey identity.NodeID, aAddr string) {
	t.Helper()
	inv, err := invite.Parse(a.invite("--quota", "1G"))
	if err != nil {
		t.Fatal(err)
	}
	ik, err := inv.Key()
	if err != nil {
		t.Fatal(err)
	}
	region, err := transport.SavedRegion(t.Context(), openDB(t, a))
	if err != nil || region == nil {
		t.Fatalf("A's region: %v", err)
	}
	d := strangerID(t)
	dAddr := transport.AddressOf(d, region)
	cl := tunnelClient(t, ik, aAddr)
	claim := func(name string, dial identity.NodeID, holder *identity.Identity, want int) {
		t.Helper()
		req := control.RedeemRequest{Address: dAddr, Number: 1 << 30, RetentionSeconds: 30 * 86400, DialKey: dial.String()}
		var err error
		if req.Proof, err = control.RedeemProof(d, aKey, ik.NodeID(), dial); err != nil {
			t.Fatal(err)
		}
		if holder != nil {
			if req.DialProof, err = control.DialProof(holder, aKey, ik.NodeID(), d.NodeID()); err != nil {
				t.Fatal(err)
			}
		}
		_, err = cl.Redeem(t.Context(), req)
		var ce *control.CallError
		if !errors.As(err, &ce) || ce.Status != want {
			t.Errorf("a redeem claiming %s as its dial key: %v, want %d", name, err, want)
		}
	}
	claim("B's dial key, proven by B", bID.DialKeyFor(aKey).NodeID(), bID.DialKeyFor(aKey), http.StatusConflict)
	claim("B's node ID, proven by B", bID.NodeID(), bID, http.StatusConflict)
	claim("C's node ID", cID.NodeID(), nil, http.StatusForbidden)
	claim("its own node ID", d.NodeID(), d, http.StatusBadRequest)
	claim("the invite key", ik.NodeID(), ik, http.StatusBadRequest)
	claim("A's node ID", aKey, nil, http.StatusBadRequest)
}
