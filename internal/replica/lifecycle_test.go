package replica

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/lifecycle"

	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/state"
)

// S3-4 (#96): the rule `overflow add --lifecycle` offers expires
// non-current versions under the prefix after the window and aborts
// multipart uploads never completed, so an abandoned upload's parts are
// not billed for ever.
func TestTheOfferedLifecycleRuleAbortsIncompleteUploads(t *testing.T) {
	r := LifecycleRule("b2", "lendisk", 30)
	if r.ID != "lendisk-b2" || r.Status != "Enabled" || r.RuleFilter.Prefix != "lendisk/" ||
		r.NoncurrentVersionExpiration.NoncurrentDays != 30 ||
		r.AbortIncompleteMultipartUpload.DaysAfterInitiation != AbortUploadDays {
		t.Fatalf("rule %+v", r)
	}
	if r := LifecycleRule("b2", "", 1); r.RuleFilter.Prefix != "" {
		t.Fatalf("an empty prefix gives filter prefix %q", r.RuleFilter.Prefix)
	}
	cfg := lifecycle.NewConfiguration()
	cfg.Rules = append(cfg.Rules, r)
	if !coversPrefix(cfg, "lendisk") {
		t.Fatal("the offered rule does not cover its own prefix")
	}
}

// S3-4 (#96): a rule filtered by a tag or by size expires only some of
// the non-current versions under its prefix, so it does not count as
// covering the prefix, and overflow add offers lendisk's own.
func TestATagOrSizeFilteredRuleDoesNotCoverThePrefix(t *testing.T) {
	expire := lifecycle.NoncurrentVersionExpiration{NoncurrentDays: 30}
	for _, c := range []struct {
		name   string
		filter lifecycle.Filter
		covers bool
	}{
		{"prefix", lifecycle.Filter{Prefix: "lendisk/"}, true},
		{"no filter", lifecycle.Filter{}, true},
		{"and prefix", lifecycle.Filter{And: lifecycle.And{Prefix: "lendisk/"}}, true},
		{"tag", lifecycle.Filter{Tag: lifecycle.Tag{Key: "tier", Value: "cold"}}, false},
		{"and tags", lifecycle.Filter{And: lifecycle.And{Prefix: "lendisk/", Tags: []lifecycle.Tag{{Key: "tier", Value: "cold"}}}}, false},
		{"larger than", lifecycle.Filter{ObjectSizeGreaterThan: 1 << 20}, false},
		{"and smaller than", lifecycle.Filter{And: lifecycle.And{Prefix: "lendisk/", ObjectSizeLessThan: 1 << 20}}, false},
	} {
		cfg := lifecycle.NewConfiguration()
		cfg.Rules = []lifecycle.Rule{{ID: "x", Status: "Enabled", RuleFilter: c.filter, NoncurrentVersionExpiration: expire}}
		if got := coversPrefix(cfg, "lendisk"); got != c.covers {
			t.Errorf("%s: covers %v, want %v", c.name, got, c.covers)
		}
	}
}

// S3-4 (#96): overflow add --lifecycle creates the rule at a provider
// that implements lifecycle rules. versitygw and Garage do not; RustFS
// 1.0.3 does, and CI's overflow-versioned job runs this against it
// (.github/rustfs.sh).
func TestOverflowAddCreatesTheLifecycleRuleAtAProvider(t *testing.T) {
	const p = "LENDISK_TEST_OVERFLOW_VERSIONED_"
	ep := os.Getenv(p + "ENDPOINT")
	if ep == "" {
		t.Skip(p + "ENDPOINT not set (CI runs .github/rustfs.sh)")
	}
	self := newID(t)
	now := time.Now().UnixNano()
	prefix := fmt.Sprintf("ci/%d", now)
	name := fmt.Sprintf("lc-%d", now) // a rule of its own each run
	c, err := NewOverflow(state.Overflow{Name: name, Endpoint: ep, Region: os.Getenv(p + "REGION"), Bucket: os.Getenv(p + "BUCKET"),
		Prefix: prefix}, overflow.Credential{AccessKey: os.Getenv(p + "ACCESS_KEY"), SecretKey: os.Getenv(p + "SECRET_KEY")},
		self.SealedName, Options{StallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	check, err := c.CheckOverflow(t.Context(), name, 2)
	if err != nil {
		t.Fatal(err)
	}
	if check.Lifecycle != state.LifecycleCreated {
		t.Fatalf("lifecycle %s, notes %v", check.Lifecycle, check.Notes)
	}
	cfg, err := c.core.GetBucketLifecycle(t.Context(), c.root.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if !coversPrefix(cfg, prefix) {
		t.Fatalf("the rule read back does not cover %s: %+v", prefix, cfg.Rules)
	}
	for _, r := range cfg.Rules {
		if r.ID == "lendisk-"+name && r.AbortIncompleteMultipartUpload.DaysAfterInitiation != AbortUploadDays {
			t.Fatalf("the rule read back aborts uploads after %d days", r.AbortIncompleteMultipartUpload.DaysAfterInitiation)
		}
	}
	// Checked again, the rule is there.
	if again, err := c.CheckOverflow(t.Context(), name, 2); err != nil || again.Lifecycle != state.LifecyclePresent {
		t.Fatalf("checked again: %s, %v", again.Lifecycle, err)
	}
}
