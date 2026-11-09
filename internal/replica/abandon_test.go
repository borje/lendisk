package replica

import (
	"context"
	"net"
	"net/http/httptrace"
	"sync/atomic"
	"testing"
)

// resettable is a connection that records a reset.
type resettable struct {
	net.Conn
	resets atomic.Int32
}

func (r *resettable) Reset() error { r.resets.Add(1); return nil }

// #89, #92: abandoning resets the connection its request got, but not
// once that connection went back to the pool (the trace's PutIdleConn):
// a stall noticed on the request's last slow read would otherwise reset
// a connection another request may already use. Nor after ended.
func TestAnAbandonResetsOnlyTheRequestsOwnConnection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(trace *httptrace.ClientTrace, ended func())
		want  int32
	}{
		{"in flight", func(*httptrace.ClientTrace, func()) {}, 1},
		{"back in the pool", func(tr *httptrace.ClientTrace, _ func()) { tr.PutIdleConn(nil) }, 0},
		{"not pooled", func(tr *httptrace.ClientTrace, _ func()) { tr.PutIdleConn(context.Canceled) }, 0},
		{"ended", func(_ *httptrace.ClientTrace, ended func()) { ended() }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			ctx, abandon, ended := abandoning(ctx, cancel)
			trace := httptrace.ContextClientTrace(ctx)
			c := &resettable{}
			trace.GotConn(httptrace.GotConnInfo{Conn: c})
			tc.after(trace, ended)
			abandon()
			if n := c.resets.Load(); n != tc.want {
				t.Fatalf("%d resets, want %d", n, tc.want)
			}
			if ctx.Err() == nil {
				t.Fatal("abandon did not cancel")
			}
		})
	}
}
