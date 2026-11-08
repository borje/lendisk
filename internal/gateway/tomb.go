package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// This file is the lender's retention of a borrower's deletes (RET-1,
// RET-2, DESIGN §3.6).
//
//   - A borrower's DeleteObject renames the object into its bucket's tomb
//     directory, TombDir/<key>/<unix nanoseconds>, on the same
//     filesystem, so its bytes, inode and attributes (its hash among
//     them) are kept as they were. The key is then absent from the
//     borrower's GET, HEAD and listings; the bytes and the object's
//     overhead keep counting against quota (LEND-1).
//   - Order, so a crash loses nothing: the tombstone row is written,
//     pending, before the rename and kept after it; a revival marks it
//     pending, links the tomb back to its key, removes the tomb, then
//     drops the row. settleTombs, at every start before anything is
//     served, settles each pending row from what the store holds.
//   - The purger removes a tomb, and frees its count, only once its
//     purge time has passed (Lending.Now). Nothing a borrower can send
//     reaches the tomb: posix is opened with .lendisk excluded at every
//     depth (OpenBackend), so no S3 call reads, writes, lists or deletes
//     there, and checkKey refuses the name besides.
//   - A put of the same hash to a tombstoned key revives the object; a
//     put or multipart upload of another hash is refused (LEND-4), a
//     rewrite's included: a rewrite replaces only a live version.
//   - A rewrite's replaced version is tombstoned the same way
//     (rewrite.go, #74), its row marked Rewrite; settleTombs moves a
//     pending one back if the key holds nothing.
//
// Every tomb step on a key holds that key's lock (Lending.keys) and the
// borrower's publication lock shared, so the reconcile sees each object
// either live or tombstoned, never both or neither.

// TombDir is the directory in each lent bucket holding its tombstoned
// objects (COMPAT-4).
const TombDir = ".lendisk/tomb"

// DefaultRetention is the retention window when neither side of a
// friendship offered one (RET-1).
const DefaultRetention = 30 * 24 * time.Hour

// Retention is how long this node keeps a friend's deletes (RET-1): the
// longer of its own offer, the lender's minimum, and the friend's offer,
// both exchanged at redeem (PEER-1, PEER-2), so a borrower can only make
// the window longer. DefaultRetention when neither was recorded.
func Retention(f state.Friendship) time.Duration {
	r := max(f.OwnRetention, f.PeerRetention)
	if r <= 0 {
		return DefaultRetention
	}
	return r
}

// purgePage is how many due tombstones one purge reads at a time.
const purgePage = 256

func (l *Lending) objectPath(bucket, rel string) string {
	return filepath.Join(l.StoreDir, bucket, filepath.FromSlash(rel))
}

// tombName returns a path for a new tomb of key, under TombDir, that no
// tomb holds yet. The caller holds the key's lock.
func (l *Lending) tombName(bucket, key string, now time.Time) (string, error) {
	n := now.UnixNano()
	for {
		rel := path.Join(TombDir, key, strconv.FormatInt(n, 10))
		_, err := os.Lstat(l.objectPath(bucket, rel))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return rel, nil
		case err != nil:
			// ENOTDIR (a tomb where a directory must go), EACCES, EIO:
			// no later name fares better.
			return "", fmt.Errorf("choosing a tomb for %q: %w", key, err)
		}
		n++
	}
}

// removeEmpty removes the empty directories rel's parents leave, up to
// stop (relative to the bucket, exclusive).
func (l *Lending) removeEmpty(bucket, rel, stop string) {
	for dir := path.Dir(rel); dir != "." && dir != stop && dir != "/"; dir = path.Dir(dir) {
		if os.Remove(l.objectPath(bucket, dir)) != nil {
			return
		}
	}
}

// moveIntoTomb renames bucket/key to bucket/tomb and removes the
// directories that leaves empty.
func (l *Lending) moveIntoTomb(bucket, key, tomb string) error {
	l.tombDirs.Lock()
	defer l.tombDirs.Unlock()
	if err := os.MkdirAll(filepath.Dir(l.objectPath(bucket, tomb)), 0o700); err != nil {
		return err
	}
	if err := os.Rename(l.objectPath(bucket, key), l.objectPath(bucket, tomb)); err != nil {
		return err
	}
	l.removeEmpty(bucket, key, ".")
	return nil
}

// removeTomb unlinks a tomb (gone already is fine) and the tomb
// directories that leaves empty.
func (l *Lending) removeTomb(bucket, tomb string) error {
	l.tombDirs.Lock()
	defer l.tombDirs.Unlock()
	if err := os.Remove(l.objectPath(bucket, tomb)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	l.removeEmpty(bucket, tomb, ".lendisk")
	return nil
}

// deleteLent tombstones a borrower's object (RET-1): the row first,
// pending, then the rename, then the row kept. A key that holds nothing
// (never stored, or tombstoned already) is answered as S3 does, with
// nothing changed. A versioned delete is refused (RET-2).
func (l *Lending) deleteLent(ctx context.Context, peer, sealed string, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	key := deref(in.Key)
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if deref(in.VersionId) != "" {
		return nil, errNotForBorrower
	}
	bucket := *in.Bucket
	unlock, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return nil, errInternal
	}
	defer unlock()
	old, has, err := l.existing(ctx, bucket, key)
	if err != nil {
		return nil, errInternal
	}
	if !has {
		return &s3.DeleteObjectOutput{}, nil
	}
	f, err := l.DB.GetFriendship(ctx, peer)
	if errors.Is(err, state.ErrNotFound) {
		return nil, errNotFriend
	}
	if err != nil {
		return nil, errInternal
	}
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	now := l.now()
	tomb, err := l.tombName(bucket, key, now)
	if err != nil {
		l.log().Error("lending: no tomb for an object; the object stays", "bucket", bucket, "key", key, "err", err)
		return nil, errInternal
	}
	t := state.Tombstone{Peer: peer, Sealed: sealed, Key: key, Tomb: tomb, SHA256: old.sha256,
		ETag: old.etag, Size: old.size, DeletedAt: now, PurgeAfter: now.Add(Retention(f))}
	id, err := l.DB.AddTombstone(context.Background(), t)
	if err != nil {
		l.log().Error("lending: recording a tombstone failed; the object stays", "bucket", bucket, "key", key, "err", err)
		return nil, errInternal
	}
	if l.beforeTombMove != nil {
		l.beforeTombMove()
	}
	if err := l.moveIntoTomb(bucket, key, t.Tomb); err != nil {
		l.dropTomb(id)
		if errors.Is(err, fs.ErrNotExist) {
			return &s3.DeleteObjectOutput{}, nil // gone meanwhile
		}
		l.log().Error("lending: moving an object into its tomb failed; the object stays", "bucket", bucket, "key", key, "err", err)
		return nil, errInternal
	}
	if err := l.DB.KeepTombstone(context.Background(), id); err != nil {
		l.log().Warn("lending: marking a tombstone kept failed; the next start settles it", "bucket", bucket, "key", key, "err", err)
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (l *Lending) dropTomb(id int64) {
	if err := l.DB.DropTombstone(context.Background(), id); err != nil {
		l.log().Warn("lending: forgetting a tombstone failed; the next start settles it", "id", id, "err", err)
	}
}

// sameHash returns the newest of ts with hash want.
func sameHash(ts []state.Tombstone, want string) (state.Tombstone, bool) {
	for i := len(ts) - 1; i >= 0; i-- {
		if ts[i].SHA256 == want {
			return ts[i], true
		}
	}
	return state.Tombstone{}, false
}

// tombsOf returns a key's tombstones; a failure to read them refuses the
// write that asked.
func (l *Lending) tombsOf(ctx context.Context, peer, sealed, key string) ([]state.Tombstone, error) {
	ts, err := l.DB.TombstonesOf(ctx, peer, sealed, key)
	if err != nil {
		l.log().Error("lending: reading a key's tombstones failed; refusing the write", "peer", peer, "key", key, "err", err)
		return nil, errInternal
	}
	return ts, nil
}

// revive moves tombstone t back to its key: pending, linked back (never
// over an object there), the tomb removed, the row dropped. Its count
// moves from tomb to live unchanged. If a live object took the key
// meanwhile, the same bytes drop the tomb and other bytes are refused.
// The caller holds the key's lock and the borrower's pub lock.
func (l *Lending) revive(ctx context.Context, bucket string, t state.Tombstone) (string, error) {
	if err := l.DB.PendTombstone(context.Background(), t.ID); err != nil {
		return "", errInternal
	}
	keep := func() {
		if err := l.DB.KeepTombstone(context.Background(), t.ID); err != nil {
			l.log().Warn("lending: marking a tombstone kept failed; the next start settles it", "id", t.ID, "err", err)
		}
	}
	keyPath := l.objectPath(bucket, t.Key)
	l.tombDirs.Lock()
	err := os.MkdirAll(filepath.Dir(keyPath), 0o700)
	if err == nil {
		err = os.Link(l.objectPath(bucket, t.Tomb), keyPath)
	}
	l.tombDirs.Unlock()
	if errors.Is(err, fs.ErrExist) {
		old, has, herr := l.existing(ctx, bucket, t.Key)
		if herr != nil || !has || old.sha256 != t.SHA256 {
			keep()
			return "", errKeyChanged
		}
		// A put of the same bytes was published meanwhile and counted:
		// the tomb is no longer needed.
		if err := l.removeTomb(bucket, t.Tomb); err != nil {
			keep()
			return old.etag, nil
		}
		if err := l.DB.PurgeTombstone(context.Background(), t.ID, t.Peer, t.Size+itemOverhead); err == nil {
			l.counted(t.Peer, -(t.Size + itemOverhead))
		}
		return old.etag, nil
	}
	if err != nil {
		keep()
		l.log().Error("lending: reviving a tombstoned object failed; it stays tombstoned", "bucket", bucket, "key", t.Key, "err", err)
		return "", errInternal
	}
	if err := l.removeTomb(bucket, t.Tomb); err != nil {
		l.log().Warn("lending: removing a revived object's tomb failed; the next start settles it", "bucket", bucket, "key", t.Key, "err", err)
		return t.ETag, nil
	}
	l.dropTomb(t.ID)
	return t.ETag, nil
}

// putOverTomb answers a put to a key that holds no object but has
// tombstones: another hash is refused before any byte is read; the same
// hash has its body hashed and discarded, like a put of a stored
// object, and revives the tomb.
func (l *Lending) putOverTomb(ctx context.Context, peer, bucket string, b *body, want string, ts []state.Tombstone) (s3response.PutObjectOutput, error) {
	var none s3response.PutObjectOutput
	t, ok := sameHash(ts, want)
	if !ok {
		return none, errKeyChanged
	}
	if _, err := io.Copy(io.Discard, b); err != nil {
		if b.failed != nil {
			return none, b.failed
		}
		return none, errInternal
	}
	if b.n != t.Size || b.hex() != want {
		return none, errBadDigest
	}
	unlock, err := l.keys.lock(ctx, bucket, t.Key)
	if err != nil {
		return none, errInternal
	}
	defer unlock()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	// Purged, revived or stored again while the body streamed?
	ts, err = l.tombsOf(ctx, peer, t.Sealed, t.Key)
	if err != nil {
		return none, err
	}
	if cur, ok := sameHash(ts, want); ok {
		etag, err := l.revive(ctx, bucket, cur)
		return s3response.PutObjectOutput{ETag: etag}, err
	}
	old, has, err := l.existing(ctx, bucket, t.Key)
	if err == nil && has && old.sha256 == want {
		return s3response.PutObjectOutput{ETag: old.etag}, nil
	}
	return none, errInternal // the borrower retries, and stores it anew
}

// settleTombs settles the tombstones a crash left pending, before
// anything is served. A row whose tomb does not exist was never moved,
// or was revived to its end: dropped, the object live. A tomb beside a
// live object of the same hash was being revived: the tomb goes. A
// rewrite's tomb alone was moved by a rewrite whose new version was never
// published: moved back to its key (#74). Any other tomb alone, or beside
// other bytes, stands: kept. The reconcile that follows at start sets the
// counts.
func (l *Lending) settleTombs(ctx context.Context) error {
	ts, err := l.DB.PendingTombstones(ctx)
	if err != nil {
		return err
	}
	for _, t := range ts {
		id, err := identity.ParseNodeID(t.Peer)
		if err != nil {
			l.dropTomb(t.ID)
			continue
		}
		bucket := LentName(id, t.Sealed)
		if _, err := os.Lstat(l.objectPath(bucket, t.Tomb)); errors.Is(err, fs.ErrNotExist) {
			l.dropTomb(t.ID)
			continue
		} else if err != nil {
			l.log().Error("lending: reading a pending tomb failed; left pending", "bucket", bucket, "tomb", t.Tomb, "err", err)
			continue
		}
		old, has, err := l.existing(ctx, bucket, t.Key)
		if err != nil {
			l.log().Error("lending: reading a pending tombstone's key failed; left pending", "bucket", bucket, "key", t.Key, "err", err)
			continue
		}
		if has && old.sha256 == t.SHA256 {
			if err := l.removeTomb(bucket, t.Tomb); err != nil {
				l.log().Error("lending: removing a revived object's tomb failed; left pending", "bucket", bucket, "tomb", t.Tomb, "err", err)
				continue
			}
			l.dropTomb(t.ID)
			continue
		}
		if !has && t.Rewrite {
			// A rewrite moved the version it replaces and never published
			// the new one (#74): the old version goes back to its key.
			if _, err := l.revive(ctx, bucket, t); err != nil {
				l.log().Error("lending: moving a rewrite's replaced version back failed; it stays in its tomb", "bucket", bucket, "key", t.Key, "err", err)
			}
			continue
		}
		if err := l.DB.KeepTombstone(ctx, t.ID); err != nil {
			return err
		}
	}
	if len(ts) > 0 {
		l.log().Info("lending: settled tombstones left pending by the last run", "rows", len(ts))
	}
	return nil
}

// purge removes every tombstone whose purge time has passed (RET-1) and
// frees its count (LEND-1). A tomb that cannot be removed is logged and
// tried again at the next purge.
func (l *Lending) purge(ctx context.Context) error {
	failed := map[int64]bool{}
	var first error
	for {
		due, err := l.DB.DueTombstones(ctx, l.now(), purgePage+len(failed))
		if err != nil {
			return err
		}
		progress := false
		for _, t := range due {
			if failed[t.ID] {
				continue
			}
			progress = true
			if err := l.purgeOne(ctx, t); err != nil {
				failed[t.ID] = true
				if first == nil {
					first = err
				}
				l.log().Error("lending: purging a tomb failed; trying again at the next purge", "peer", t.Peer,
					"key", t.Key, "tomb", t.Tomb, "err", err)
			}
		}
		if !progress || ctx.Err() != nil {
			return first
		}
	}
}

func (l *Lending) purgeOne(ctx context.Context, t state.Tombstone) error {
	id, err := identity.ParseNodeID(t.Peer)
	if err != nil {
		return err
	}
	bucket := LentName(id, t.Sealed)
	unlock, err := l.keys.lock(ctx, bucket, t.Key)
	if err != nil {
		return err
	}
	defer unlock()
	pub := &published{u: l.use(t.Peer)}
	pub.take()
	defer pub.release()
	// The due list was read before the lock: a revive since may have
	// marked the row pending or dropped it.
	rows, err := l.DB.TombstonesOf(ctx, t.Peer, t.Sealed, t.Key)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(rows, func(r state.Tombstone) bool { return r.ID == t.ID && !r.Pending }) {
		return nil
	}
	if err := l.removeTomb(bucket, t.Tomb); err != nil {
		return err
	}
	err = l.DB.PurgeTombstone(context.Background(), t.ID, t.Peer, t.Size+itemOverhead)
	if errors.Is(err, state.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	l.counted(t.Peer, -(t.Size + itemOverhead))
	l.log().Debug("lending: purged a tomb", "peer", t.Peer, "key", t.Key, "deleted_at", t.DeletedAt)
	return nil
}

// purgeLoop purges at start and then every interval until ctx ends.
func (l *Lending) purgeLoop(ctx context.Context, interval time.Duration) {
	for {
		if err := l.purge(ctx); err != nil && ctx.Err() == nil {
			l.log().Error("lending: purging tombs failed; trying again later", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
