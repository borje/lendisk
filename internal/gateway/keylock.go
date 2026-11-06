package gateway

import (
	"slices"
	"sync"
)

// keyLocks serialises captured writes and deletes of one key (EP-3,
// #48): a request holds its key's lock from before its queue row is
// written until the row is completed or removed, so rows of one key are
// applied to the store in sequence order and the key's last done row
// always describes the object on disk. A key's entry exists only while
// someone holds or waits for it.
type keyLocks struct {
	mu sync.Mutex
	m  map[string]*keyLock
}

type keyLock struct {
	sync.Mutex
	refs int // holders and waiters
}

func lockName(bucket, key string) string { return bucket + "\x00" + key }

// lock takes the locks of the given keys of bucket, in sorted order so
// that two requests naming the same keys cannot deadlock, and returns
// the function that releases them. The unlock may run on another
// goroutine.
func (l *keyLocks) lock(bucket string, keys ...string) (unlock func()) {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = lockName(bucket, k)
	}
	slices.Sort(names)
	names = slices.Compact(names)
	held := make([]*keyLock, len(names))
	for i, n := range names {
		held[i] = l.ref(n)
		held[i].Lock()
	}
	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].Unlock()
			l.unref(names[i], held[i])
		}
	}
}

// tryLock takes one key's lock if nobody holds it.
func (l *keyLocks) tryLock(bucket, key string) (unlock func(), ok bool) {
	n := lockName(bucket, key)
	k := l.ref(n)
	if !k.TryLock() {
		l.unref(n, k)
		return nil, false
	}
	return func() { k.Unlock(); l.unref(n, k) }, true
}

func (l *keyLocks) ref(n string) *keyLock {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m = map[string]*keyLock{}
	}
	k := l.m[n]
	if k == nil {
		k = &keyLock{}
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
