package replica

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/state"
)

// S3-5 (#98), end to end at a provider: a bucket policy denying
// s3:DeleteObject under the target's prefix makes the credential one
// that may put but not delete. The worker's delete of a user's object
// there is refused (ErrDeleteDenied from the provider's own 403), the
// object stays at the provider, live, counted as a denied delete; once
// the policy is lifted, the daily retry deletes it and the count goes.
// CI runs it against RustFS 1.0.3 (.github/rustfs.sh), which evaluates
// bucket policies for every credential, its root one included; Garage
// has no policies (a key's bucket permission is read, write or owner,
// and write includes delete), and versitygw is not asked. Skipped
// without the variables.
func TestACredentialThatMayNotDeleteKeepsTheObjectAtAProvider(t *testing.T) {
	const p = "LENDISK_TEST_OVERFLOW_VERSIONED_"
	ep := os.Getenv(p + "ENDPOINT")
	if ep == "" {
		t.Skip(p + "ENDPOINT not set (CI runs .github/rustfs.sh)")
	}
	self := newID(t)
	prefix := fmt.Sprintf("ci/%d", time.Now().UnixNano())
	o := state.Overflow{Name: "rustfs", Endpoint: ep, Region: os.Getenv(p + "REGION"), Bucket: os.Getenv(p + "BUCKET"),
		Prefix: prefix, MaxBytes: 1 << 30, EnvPrefix: "X_", Retention: time.Hour, Versioning: state.VersioningEnabled,
		AddedAt: time.Now()}
	c, err := NewOverflow(o, overflow.Credential{AccessKey: os.Getenv(p + "ACCESS_KEY"), SecretKey: os.Getenv(p + "SECRET_KEY")},
		self.SealedName, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// The policy covers this run's prefix only, so other runs and tests
	// sharing the bucket may delete as before.
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":["*"]},`+
		`"Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::%s/%s/*"]}]}`, o.Bucket, prefix)
	if err := c.core.SetBucketPolicy(t.Context(), o.Bucket, policy); err != nil {
		t.Fatal(err)
	}
	lifted := false
	lift := func() {
		if !lifted {
			lifted = true
			if err := c.core.SetBucketPolicy(t.Context(), o.Bucket, ""); err != nil {
				t.Errorf("lifting the bucket policy: %v", err)
			}
		}
	}
	t.Cleanup(lift)

	fx := newFixture(t) // no friend: the copy goes to the target
	if err := fx.db.AddOverflow(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	fx.w.ConnectOverflow = func(state.Overflow) (Replica, error) { return c, nil }
	clock := &fixtureClock{now: time.Now()}
	fx.w.Now = clock.Now
	fx.bucket("laptop", 1)
	fx.write("laptop", "data/ab/denied", "a pack the credential may not delete")
	fx.run()
	fx.drained(o.Place())
	if _, ok, err := c.Stat(t.Context(), "laptop", "data/ab/denied"); err != nil || !ok {
		t.Fatalf("not at the provider: %v, %v", ok, err)
	}

	fx.remove("laptop", "data/ab/denied")
	fx.settle("the refused delete recorded", func() bool {
		h, err := fx.db.OverflowTombHolds(t.Context())
		return err == nil && h["rustfs"].Denied == 1
	})
	fx.drained(o.Place())
	if _, ok, err := c.Stat(t.Context(), "laptop", "data/ab/denied"); err != nil || !ok {
		t.Fatalf("the provider no longer holds an object whose delete it refused: %v, %v", ok, err)
	}
	if cond := fx.condition(o.Place()); cond.Condition != state.ConditionOK {
		t.Fatalf("the target's condition %+v", cond)
	}

	// Delete permission granted: the next daily retry deletes it.
	lift()
	clock.add(state.DeniedRetry + time.Minute)
	fx.settle("the refused delete sent again", func() bool {
		h, err := fx.db.OverflowTombHolds(t.Context())
		return err == nil && h["rustfs"].Denied == 0
	})
	if _, ok, err := c.Stat(t.Context(), "laptop", "data/ab/denied"); err != nil || ok {
		t.Fatalf("still live at the provider after the retry: %v, %v", ok, err)
	}
}
