package gateway

import (
	"context"
	"slices"
	"sync"
)

// keyLocks serialises captured writes and deletes of one key (EP-3,
// #48): a request holds its key's lock from before its queue row is
// written until the row is completed or removed, so rows of one key are
// applied to the store in sequence order and the key's last done row
// always describes the object on disk. A key's entry exists only while
// someone holds or waits for it.
//
// Waiting gives up when the request's context ends (#55), and a request
// naming several keys never holds one while it waits for another, so a
// key held for minutes (a completed upload's hash) delays only the
// requests that name it.
type keyLocks struct {
	mu sync.Mutex
	m  map[string]*keyLock
	// waiting, if set, is called with a key's lock name before a request
	// blocks on it, holding no other lock. Tests only.
	waiting func(name string)
}

// keyLock holds a token in ch while locked, so waiting can be selected
// against a context.
type keyLock struct {
	ch   chan struct{}
	refs int // holders and waiters
}

func lockName(bucket, key string) string { return bucket + "\x00" + key }

// lock takes the locks of the given keys of bucket and returns the
// function that releases them, or ctx's error if it ends first. It
// blocks on one key at a time holding none of the others, and only takes
// the others while they are free, so two requests naming the same keys
// cannot deadlock. The unlock may run on another goroutine.
func (l *keyLocks) lock(ctx context.Context, bucket string, keys ...string) (unlock func(), err error) {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = lockName(bucket, k)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	refs := make([]*keyLock, len(names))
	for i, n := range names {
		refs[i] = l.ref(n)
	}
	held := make([]bool, len(names))
	release := func() {
		for i := range held {
			if held[i] {
				<-refs[i].ch
				held[i] = false
			}
		}
	}
	unrefAll := func() {
		for i, n := range names {
			l.unref(n, refs[i])
		}
	}
	for wait := 0; ; {
		select {
		case refs[wait].ch <- struct{}{}:
		default:
			if l.waiting != nil {
				l.waiting(names[wait])
			}
			select {
			case refs[wait].ch <- struct{}{}:
			case <-ctx.Done():
				unrefAll()
				return nil, ctx.Err()
			}
		}
		held[wait] = true
		busy := -1
		for i := range names {
			if held[i] {
				continue
			}
			select {
			case refs[i].ch <- struct{}{}:
				held[i] = true
			default:
				busy = i
			}
			if busy >= 0 {
				break
			}
		}
		if busy < 0 {
			return func() { release(); unrefAll() }, nil
		}
		// Wait for the busy key holding nothing.
		release()
		wait = busy
	}
}

// tryLock takes one key's lock if nobody holds it.
func (l *keyLocks) tryLock(bucket, key string) (unlock func(), ok bool) {
	n := lockName(bucket, key)
	k := l.ref(n)
	select {
	case k.ch <- struct{}{}:
		return func() { <-k.ch; l.unref(n, k) }, true
	default:
		l.unref(n, k)
		return nil, false
	}
}

func (l *keyLocks) ref(n string) *keyLock {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[string]*keyLock{}
	}
	k := l.m[n]
	if k == nil {
		k = &keyLock{ch: make(chan struct{}, 1)}
		l.m[n] = k
	}
	k.refs++
	return k
}

func (l *keyLocks) unref(n string, k *keyLock) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if k.refs--; k.refs == 0 {
		delete(l.m, n)
	}
}
