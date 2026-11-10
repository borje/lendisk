package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/versity/versitygw/backend/meta"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// This file rebuilds the tombstone rows a lost lendisk.db took with it
// (RET-1, RET-2, #75). The rows are the lender's only record of its
// tombs; without them a tomb is never purged, never counted (LEND-1)
// and never listed. The store holds what a row needs:
//
//   - the key and the delete time are the tomb's path,
//     TombDir/<key>/<unix nanoseconds> (tombName);
//   - the hash and ETag are the tomb's xattrs, kept by the rename;
//   - the size is the file's;
//   - the purge time is the delete time plus the friendship's retention
//     (Retention) as it stands at the rebuild.
//
// Conservative where the store does not say: a tomb whose name is not a
// delete time, or one after now, is taken as deleted at the rebuild, so
// it keeps a whole window. A rewrite's tomb (#74) cannot be told from a
// delete's on disk; it is rebuilt as a delete's, which keeps its bytes
// as long and offers it to undelete, but drops it from the list's
// replaced versions. A missing hash or ETag is rebuilt empty: the bytes
// are kept for the window all the same, and no put revives them.
//
// Rows are rebuilt for every mapped lent bucket at each start, after
// settleTombs and before anything is served, and for a bucket the
// borrower creates again after its row was lost (CreateBucket), before
// the bucket is mapped, so no write of the borrower's reaches a key
// whose tombs have no rows. Each row is written kept, holding the key's
// lock and the borrower's publication lock, and only for a tomb no row
// names. The reconcile counts the rebuilt rows (LEND-1).

// rebuildAllTombs rebuilds the missing tombstone rows of every lent
// bucket. A bucket that fails is logged and left to the next start.
func (l *Lending) rebuildAllTombs(ctx context.Context) error {
	bs, err := l.DB.AllLentBuckets(ctx)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if err := l.rebuildTombs(ctx, b.Peer, b.Sealed); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			l.log().Error("lending: rebuilding a bucket's tombstones failed; trying again at the next start",
				"peer", b.Peer, "sealed", b.Sealed, "err", err)
		}
	}
	return nil
}

// rebuildTombs records a kept tombstone row for each tomb of peer's
// bucket sealed that no row names.
func (l *Lending) rebuildTombs(ctx context.Context, peer, sealed string) error {
	id, err := identity.ParseNodeID(peer)
	if err != nil {
		return err
	}
	bucket := LentName(id, sealed)
	dir, err := filepath.Abs(l.objectPath(bucket, TombDir))
	if err != nil {
		return err
	}
	var tombs []string
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == dir && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll // no tomb was ever made
			}
			return err
		}
		if d.Type().IsRegular() {
			rel, err := filepath.Rel(filepath.Dir(filepath.Dir(dir)), p)
			if err != nil {
				return err
			}
			tombs = append(tombs, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil || len(tombs) == 0 {
		return err
	}
	known, err := l.DB.TombPaths(ctx, peer, sealed)
	if err != nil {
		return err
	}
	retention := DefaultRetention
	if f, err := l.DB.GetFriendship(ctx, peer); err == nil {
		retention = Retention(f)
	} else if !errors.Is(err, state.ErrNotFound) {
		return err
	}
	rebuilt := 0
	for _, tomb := range tombs {
		if known[tomb] {
			continue
		}
		ok, err := l.rebuildTomb(ctx, peer, sealed, bucket, tomb, retention)
		if err != nil {
			return err
		}
		if ok {
			rebuilt++
		}
	}
	if rebuilt > 0 {
		l.log().Warn("lending: rebuilt tombstone rows missing from lendisk.db from the store's tombs",
			"peer", peer, "sealed", sealed, "rows", rebuilt)
	}
	return nil
}

// rebuildTomb records tomb's row, kept, unless a row names it by the
// time the key's lock is held or the tomb is gone. It reports whether it
// wrote one.
func (l *Lending) rebuildTomb(ctx context.Context, peer, sealed, bucket, tomb string, retention time.Duration) (bool, error) {
	key, deletedAt := tombKey(tomb, l.now())
	if key == "" {
		l.log().Warn("lending: a file in the tomb directory names no key; left alone", "bucket", bucket, "tomb", tomb)
		return false, nil
	}
	unlock, err := l.keys.lock(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	defer unlock()
	pub := &published{u: l.use(peer)}
	pub.take()
	defer pub.release()
	ts, err := l.DB.TombstonesOf(ctx, peer, sealed, key)
	if err != nil {
		return false, err
	}
	for _, t := range ts {
		if t.Tomb == tomb {
			return false, nil
		}
	}
	t, err := l.readTomb(bucket, tomb)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil // purged or revived meanwhile
	}
	if err != nil {
		return false, err
	}
	t.Peer, t.Sealed, t.Key = peer, sealed, key
	t.DeletedAt, t.PurgeAfter = deletedAt, deletedAt.Add(retention)
	if t.SHA256 == "" {
		l.log().Warn("lending: a tomb has no hash in its xattrs; kept for its window, no put revives it", "bucket", bucket, "tomb", tomb)
	}
	if _, err := l.DB.RestoreTombstone(context.Background(), t); err != nil {
		return false, err
	}
	return true, nil
}

// tombKey returns the key a tomb path keeps and its delete time: the
// time its name gives, or now when the name is not a time before now.
// The key is "" for a path that names none.
func tombKey(tomb string, now time.Time) (string, time.Time) {
	rel, ok := strings.CutPrefix(tomb, TombDir+"/")
	if !ok {
		return "", time.Time{}
	}
	key, name := path.Dir(rel), path.Base(rel)
	if key == "." || key == "" {
		return "", time.Time{}
	}
	ns, err := strconv.ParseInt(name, 10, 64)
	if err != nil || ns <= 0 {
		return key, now
	}
	at := time.Unix(0, ns)
	if at.After(now) {
		return key, now
	}
	return key, at
}

// readTomb reads what a tomb's file says of it: size, and the hash and
// ETag versitygw's posix stored in its xattrs (empty when missing).
func (l *Lending) readTomb(bucket, tomb string) (state.Tombstone, error) {
	t := state.Tombstone{Tomb: tomb}
	dir, err := filepath.Abs(filepath.Join(l.StoreDir, bucket))
	if err != nil {
		return t, err
	}
	rel := filepath.FromSlash(tomb)
	fi, err := os.Lstat(filepath.Join(dir, rel))
	if err != nil {
		return t, err
	}
	if !fi.Mode().IsRegular() {
		return t, fs.ErrNotExist
	}
	t.Size = fi.Size()
	x := meta.XattrMeta{}
	if b, err := x.RetrieveAttribute(nil, dir, rel, "metadata"); err == nil {
		var m map[string]string
		if json.Unmarshal(b, &m) == nil {
			t.SHA256 = strings.ToLower(m[metaSHA256])
		}
	} else if !errors.Is(err, meta.ErrNoSuchKey) {
		return t, fmt.Errorf("reading a tomb's metadata: %w", err)
	}
	if b, err := x.RetrieveAttribute(nil, dir, rel, "etag"); err == nil {
		t.ETag = string(b)
	} else if !errors.Is(err, meta.ErrNoSuchKey) {
		return t, fmt.Errorf("reading a tomb's ETag: %w", err)
	}
	return t, nil
}
