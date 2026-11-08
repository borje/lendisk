package gateway

import (
	"context"
	"errors"
	"io/fs"
	"sync"

	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/state"
)

// This file is the lender's side of a borrower's rewrite (LEND-4, RET-1,
// EP-7, #74): a put marked with the hash of the version the key holds
// (MetaReplaces). The borrower marks only keys its bucket's profile
// names rewritable, but the lender does not know the profile and need
// not: whatever key a borrower rewrites, the version replaced is kept as
// a tombstone for the retention window, as a delete keeps it, so a
// borrower marking rewrites cannot lose data within the window (RET-2).
//
//   - The body streams to the store as any put's does. Only once it has
//     ended and matched its declared hash (the body's last read) does
//     the rewrite take the key's lock and look again: the key must still
//     hold the marked version (else 412: a stale or racing rewrite never
//     replaces a newer version), or already the new bytes (a retry whose
//     answer was lost: accepted, nothing moved).
//   - Order, so a crash loses nothing: the tombstone row is written,
//     pending and marked a rewrite; the replaced version is renamed into
//     its tomb; the store publishes the new version, unconditionally
//     (posix checks a condition once before the body too, while the key
//     still holds the replaced version); the row is kept. A failure before the
//     publish moves the old version back (revive), and settleTombs does
//     the same at startup for a pending rewrite row whose key holds
//     nothing. A pending row beside a published new version is kept.
//   - Holding the key's lock from the move to the end serialises the
//     rewrite with every other write that could publish at the key:
//     deletes, purges, revivals, multipart creates and completions, and
//     plain puts, which take the lock once their body is verified and
//     hold it until the store answers (putLent). So nothing publishes
//     between the move and the rewrite's own publication, which is why
//     that publication needs no condition; a plain put whose body ends
//     in that gap publishes after it, under If-None-Match: *, and is
//     refused (#74 review).
//   - Both versions count against quota until the old one is purged: the
//     rewrite is admitted like a new object (size plus overhead), and the
//     old one's count moves from live to tomb unchanged.

// errSame ends a rewrite's body when the key already holds its bytes.
var errSame = errors.New("the key already holds these bytes")

// rewriteLent stores a borrower's marked rewrite of a key that held the
// version mark when the put arrived (in.Bucket is already mapped).
func (l *Lending) rewriteLent(ctx context.Context, peer, sealed string, in s3response.PutObjectInput, b *body, size int64, want, mark string) (s3response.PutObjectOutput, error) {
	var none s3response.PutObjectOutput
	bucket, key := *in.Bucket, deref(in.Key)
	f, err := l.DB.GetFriendship(ctx, peer)
	if errors.Is(err, state.ErrNotFound) {
		return none, errNotFriend
	}
	if err != nil {
		return none, errInternal
	}
	release, err := l.reserve(ctx, peer, size+itemOverhead)
	if err != nil {
		return none, err
	}
	defer release()
	pub := &published{u: l.use(peer)}
	defer pub.release()
	var unlock func()
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	var (
		t    state.Tombstone // the replaced version's row, once written (t.ID != 0)
		same storedObject    // what the key holds when it already holds the new bytes
		once sync.Once
		perr error
	)
	// prepare moves the replaced version into its tomb; it runs once the
	// body is verified, before the store publishes.
	prepare := func() error {
		once.Do(func() { perr = l.replace(ctx, peer, sealed, bucket, key, want, mark, f, pub, &unlock, &t, &same) })
		return perr
	}
	b.onEOF = func() error {
		if b.hex() != want {
			return errBadDigest
		}
		return prepare()
	}
	b.onFull = b.onEOF // the whole declared body is read: move now
	if size == 0 {
		// The store may publish without reading.
		if err := prepare(); err != nil {
			b.failed = err
		}
	}
	if b.failed == nil {
		orig := in.Body
		in.Body = b.reader(orig)
		// Unconditional: posix checks a condition once before the body
		// too, when the key still holds the replaced version. No other
		// write can publish at the key between the move and this
		// publication: every write that publishes there (deletes,
		// revivals, multipart, and plain puts once their body is
		// verified) holds the key's lock to do so, and this rewrite holds
		// it from the move to the end.
		in.IfNoneMatch, in.IfMatch = nil, nil
	}
	var out s3response.PutObjectOutput
	called := b.failed == nil
	if called {
		out, err = l.Next.PutObject(ctx, in)
	}
	failed := b.failed
	if called && err == nil && failed == nil && (b.n != size || b.hex() != want) {
		failed = errBadDigest
	}
	if called && err == nil && failed != nil && failed != errSame {
		l.remove(bucket, key) // the store published a body this layer refused
	}
	if failed == errSame {
		return s3response.PutObjectOutput{ETag: same.etag}, nil
	}
	if err == nil && failed == nil {
		if t.ID != 0 {
			if err := l.DB.KeepTombstone(context.Background(), t.ID); err != nil {
				l.log().Warn("lending: marking a rewrite's tombstone kept failed; the next start settles it", "bucket", bucket, "key", key, "err", err)
			}
		}
		l.count(peer, b.n+itemOverhead)
		return out, nil
	}
	// Not published: the replaced version goes back to its key.
	if t.ID != 0 {
		if _, rerr := l.revive(ctx, bucket, t); rerr != nil && !isCode(rerr, "PreconditionFailed") {
			l.log().Error("lending: moving a rewrite's replaced version back failed; it stays in its tomb", "bucket", bucket, "key", key, "err", rerr)
		}
	}
	if failed != nil {
		return none, failed
	}
	if preconditionFailed(err) {
		// Another put of the key was published after the move.
		cur, has, herr := l.existing(ctx, bucket, key)
		if herr == nil && has && cur.sha256 == want && b.n == cur.size && b.hex() == want {
			return s3response.PutObjectOutput{ETag: cur.etag}, nil
		}
		return none, errKeyChanged
	}
	return out, err
}

// replace is a rewrite's step between its verified body and its
// publication: holding the key's lock (kept in *unlock until the rewrite
// ends) and the borrower's pub lock, it checks the key still holds the
// marked version and moves it into a tomb, its row written pending
// first (*t). If the key already holds want, it returns errSame with
// what is stored in *same.
func (l *Lending) replace(ctx context.Context, peer, sealed, bucket, key, want, mark string, f state.Friendship,
	pub *published, unlock *func(), t *state.Tombstone, same *storedObject) error {
	u, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return errInternal
	}
	*unlock = u
	pub.take()
	cur, has, err := l.existing(ctx, bucket, key)
	switch {
	case err != nil:
		return errInternal
	case has && cur.sha256 == want:
		*same = cur
		return errSame
	case !has || cur.sha256 != mark:
		// Deleted, or replaced by another rewrite, while the body
		// streamed: the marked version is no longer the key's.
		return errKeyChanged
	}
	now := l.now()
	tomb, err := l.tombName(bucket, key, now)
	if err != nil {
		l.log().Error("lending: no tomb for a rewrite's replaced version; refusing the rewrite", "bucket", bucket, "key", key, "err", err)
		return errInternal
	}
	row := state.Tombstone{Peer: peer, Sealed: sealed, Key: key, Tomb: tomb, SHA256: cur.sha256, ETag: cur.etag,
		Size: cur.size, DeletedAt: now, PurgeAfter: now.Add(Retention(f)), Rewrite: true, Pending: true}
	id, err := l.DB.AddTombstone(context.Background(), row)
	if err != nil {
		l.log().Error("lending: recording a rewrite's tombstone failed; refusing the rewrite", "bucket", bucket, "key", key, "err", err)
		return errInternal
	}
	if l.beforeTombMove != nil {
		l.beforeTombMove()
	}
	if err := l.moveIntoTomb(bucket, key, tomb); err != nil {
		l.dropTomb(id)
		if errors.Is(err, fs.ErrNotExist) {
			return errKeyChanged
		}
		l.log().Error("lending: moving a rewrite's replaced version into its tomb failed; refusing the rewrite", "bucket", bucket, "key", key, "err", err)
		return errInternal
	}
	row.ID = id
	*t = row
	if l.afterRewriteMove != nil {
		if err := l.afterRewriteMove(); err != nil {
			return err
		}
	}
	return nil
}
