package replica

import (
	"errors"
	"sync"
	"testing"

	"github.com/borje/lendisk/internal/metrics"
)

// OPS-4 (#43): the worker's counters move as it sends: the body bytes
// each place read, every attempt included, the rows each acknowledged
// by operation, and the failed sends it retries, by reason.
func TestCountersMoveWhenReplicationSends(t *testing.T) {
	fx := newFixture(t, "B", "C")
	c := &metrics.Counters{}
	fx.w.Metrics = c
	var mu sync.Mutex
	fails := 2 // C answers the first two puts with a network error
	fx.places["C"].fail = func(op, key string) error {
		mu.Lock()
		defer mu.Unlock()
		if op == "put" && fails > 0 {
			fails--
			return errors.New("connection reset")
		}
		return nil
	}
	fx.write("laptop", "data/1", "0123456789")
	fx.write("laptop", "data/2", "abcde")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	fx.remove("laptop", "data/2")
	fx.drained("B")
	fx.drained("C")
	waitFor(t, "the acknowledgements", func() bool {
		v := metrics.Values(c.Families())
		return v[`lendisk_replication_objects_total{place="B",op="delete"}`] == 1 &&
			v[`lendisk_replication_objects_total{place="C",op="delete"}`] == 1
	})
	v := metrics.Values(c.Families())
	_, _, _, readB := fx.places["B"].snapshot()
	_, _, _, readC := fx.places["C"].snapshot()
	for k, want := range map[string]float64{
		`lendisk_replication_sent_bytes_total{place="B"}`:               15,
		`lendisk_replication_sent_bytes_total{place="C"}`:               float64(readC),
		`lendisk_replication_objects_total{place="B",op="put"}`:         2,
		`lendisk_replication_objects_total{place="C",op="put"}`:         2,
		`lendisk_replication_retries_total{place="C",reason="network"}`: 2,
	} {
		if v[k] != want {
			t.Errorf("%s = %v, want %v", k, v[k], want)
		}
	}
	if readB != 15 || readC <= readB {
		t.Errorf("C read %d bytes, B %d: C's failed attempts send bytes too", readC, readB)
	}
	if n := v[`lendisk_replication_retries_total{place="B",reason="network"}`]; n != 0 {
		t.Errorf("B retried %v times", n)
	}
}
