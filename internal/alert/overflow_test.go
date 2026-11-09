package alert

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// putData captures a completed data put that counts units MiB at a place.
func putData(t *testing.T, db *state.DB, bucket, key string, mib int64) {
	t.Helper()
	seqs, err := db.BeginCaptures(t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(t.Context(), seqs[0], mib<<20-state.ObjectOverhead, "h-"+key); err != nil {
		t.Fatal(err)
	}
}

// S3-7, scenario 12 (#38): an overflow target receiving copies raises
// overflow_receiving; at 90% of max_bytes overflow_high, at all of it
// overflow_full, which replaces it; nothing past max_bytes is placed.
func TestOverflowReceivingHighAndFullAreAlerted(t *testing.T) {
	db := openDB(t)
	if err := db.AddBucket(t.Context(), state.Bucket{Name: "laptop", Profile: "restic", Copies: 2,
		AccessKey: "AK", SecretKey: "s", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	addFriend(t, db, "B")
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "b2", Endpoint: "https://s3.example", Bucket: "x",
		MaxBytes: 10 << 20, EnvPrefix: "P_", AddedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if as := active(t, db); len(as) != 0 {
		t.Fatalf("alerts with an empty target %v", as)
	}
	putData(t, db, "laptop", "data/1", 5)
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	a, ok := active(t, db)[KindOverflowReceiving+":b2"]
	if !ok || !strings.Contains(a.Message, "receiving copies") || !strings.Contains(a.Message, "1 object") {
		t.Fatalf("alerts %v", active(t, db))
	}
	putData(t, db, "laptop", "data/2", 4)
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as := active(t, db)
	if a, ok := as[KindOverflowHigh+":b2"]; !ok || !strings.Contains(a.Message, "90%") {
		t.Fatalf("alerts at 90%% %v", as)
	}
	putData(t, db, "laptop", "data/3", 1)
	putData(t, db, "laptop", "data/4", 1) // does not fit
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	as = active(t, db)
	if _, ok := as[KindOverflowFull+":b2"]; !ok {
		t.Fatalf("alerts at 100%% %v", as)
	}
	if _, ok := as[KindOverflowHigh+":b2"]; ok {
		t.Fatal("overflow_high stayed beside overflow_full")
	}
	holds, err := db.OverflowHolds(t.Context())
	if err != nil || holds["b2"].Use != 10<<20 || holds["b2"].DataObjects != 3 {
		t.Fatalf("holds %+v, %v", holds, err)
	}
}

// An overflow target failing as long as a friend may raises
// overflow_unreachable with the reason, such as an unset credential.
func TestAFailingOverflowTargetIsAlerted(t *testing.T) {
	db := openDB(t)
	if err := db.AddOverflow(t.Context(), state.Overflow{Name: "b2", Endpoint: "https://s3.example", Bucket: "x",
		MaxBytes: 10 << 20, EnvPrefix: "P_", AddedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(t.Context(), "overflow:b2", state.ConditionRetrying,
		"P_ACCESS_KEY not set", time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	s := &Service{DB: db, Log: quiet}
	if err := s.evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a, ok := active(t, db)[KindOverflowUnreachable+":b2"]; !ok || !strings.Contains(a.Message, "P_ACCESS_KEY not set") {
		t.Fatalf("alerts %v", active(t, db))
	}
}
