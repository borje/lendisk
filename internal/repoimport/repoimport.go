// Package repoimport reads an existing backup repository on disk as the
// S3 keys its tool reads through the endpoint (COMPAT-3): a restic
// directory or one of rest-server's repositories, whose paths are their
// keys, and a Kopia filesystem repository, whose sharded paths are
// joined back into Kopia's flat blob IDs.
//
// Rules a caller must keep:
//   - Walk only names files; the caller streams each from File.Path and
//     must not load it whole (large repositories).
//   - A file that is not one of the tool's objects (a temporary file, a
//     README, Kopia's .shards) is never yielded, so a key is always one
//     the tool would have written itself.
//   - Classifying a key (local-only, format 1) is the caller's, through
//     the tool's profile: this package knows only layouts.
package repoimport

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// File is one object of a repository: its key in the bucket and the
// file holding it.
type File struct {
	Key  string
	Path string
	Size int64
}

// Repo is a repository found on disk.
type Repo struct {
	Dir  string
	Tool string
}

// marker is the file each tool's repository has at its top.
var marker = map[string]string{"restic": "config", "kopia": kopiaSuffixed("kopia.repository")}

// Open checks that dir holds a repository of tool (restic or kopia) and
// returns it. A directory with rest-server's repositories below it is
// refused naming them, since each is a bucket of its own.
func Open(dir, tool string) (*Repo, error) {
	m, ok := marker[tool]
	if !ok {
		return nil, fmt.Errorf("only restic and Kopia repositories can be imported, not %q", tool)
	}
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("repository %s: %w", dir, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("repository %s is not a directory", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, m)); err == nil {
		return &Repo{Dir: dir, Tool: tool}, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("repository %s: %w", dir, err)
	}
	if tool == "restic" {
		if found := restServerRepos(dir); len(found) > 0 {
			return nil, fmt.Errorf("%s is not a restic repository but holds rest-server's repositories %s: import each as its own bucket, giving its directory",
				dir, strings.Join(found, ", "))
		}
	}
	return nil, fmt.Errorf("%s is not a %s repository: it has no %s at its top", dir, tool, m)
}

// restServerRepos lists the restic repositories in dir's subdirectories
// as rest-server lays them out: <data>/<repo>, or with --private-repos
// <data>/<user> and <data>/<user>/<repo> (rest-server 0.14.0,
// handlers.go MaxFolderDepth = 2).
func restServerRepos(dir string) []string {
	var found []string
	var look func(d string, depth int)
	look = func(d string, depth int) {
		ents, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range ents {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			sub := filepath.Join(d, e.Name())
			if _, err := os.Stat(filepath.Join(sub, "config")); err == nil {
				found = append(found, sub)
			} else if depth < 2 {
				look(sub, depth+1)
			}
		}
	}
	look(dir, 1)
	return found
}

// Walk calls fn with each of the repository's objects, in lexical order
// of their paths, stopping at fn's first error.
func (r *Repo) Walk(fn func(File) error) error {
	keyOf := resticKey
	if r.Tool == "kopia" {
		keyOf = kopiaKey
	}
	return filepath.WalkDir(r.Dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !de.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(r.Dir, p)
		if err != nil {
			return err
		}
		key, ok := keyOf(filepath.ToSlash(rel))
		if !ok {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			return err
		}
		return fn(File{Key: key, Path: p, Size: info.Size()})
	})
}

var resticID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// resticKey is the key of a file of a restic repository: its path, in
// restic's default layout (restic 0.19.1 internal/backend/layout,
// used by its local and S3 backends alike, and rest-server's
// repo.go getObjectPath): config at the top, data/<first two hex
// digits>/<id>, and keys, index, snapshots and locks flat, every object
// named by a 64-digit hex ID. Anything else is not an object: restic's
// local backend writes <name>-tmp-<random> and rest-server
// <id>.rest-server-temp<random> before renaming.
func resticKey(rel string) (string, bool) {
	parts := strings.Split(rel, "/")
	switch {
	case len(parts) == 1:
		return rel, rel == "config"
	case len(parts) == 2:
		switch parts[0] {
		case "keys", "index", "snapshots", "locks":
			return rel, resticID.MatchString(parts[1])
		}
	case len(parts) == 3 && parts[0] == "data":
		return rel, resticID.MatchString(parts[2]) && strings.HasPrefix(parts[2], parts[1])
	}
	return "", false
}

// kopiaBlobSuffix is what Kopia's sharded storage appends to a blob's
// file name once it is written (Kopia 0.23.1
// repo/blob/sharded/sharded.go CompleteBlobSuffix).
const kopiaBlobSuffix = ".f"

func kopiaSuffixed(id string) string { return id + kopiaBlobSuffix }

// kopiaKey is the blob ID of a file of a Kopia filesystem repository,
// which Kopia's S3 storage uses as the key. The filesystem storage
// splits a blob ID longer than .shards' maxNonShardedLength (20) into
// directories by the shard sizes ([1,3] in a repository Kopia 0.23
// created, [3,3] before .shards existed, per-prefix overrides possible;
// sharded_parameters.go GetShardDirectoryAndBlob) and appends ".f".
// Whatever the sizes, the ID is every directory name and the file name
// joined, the suffix removed, which is how Kopia's own listing reads it
// back (sharded.go ListBlobs, getBlobIDFromFileName); a file without the
// suffix (.shards, a temporary <name>.f.tmp.<hex>) is not a blob.
func kopiaKey(rel string) (string, bool) {
	if !strings.HasSuffix(rel, kopiaBlobSuffix) {
		return "", false
	}
	id := strings.ReplaceAll(strings.TrimSuffix(rel, kopiaBlobSuffix), "/", "")
	// A temporary file whose random hex is "f" ends in ".f" too.
	return id, id != "" && !strings.Contains(id, kopiaBlobSuffix+".tmp.")
}
