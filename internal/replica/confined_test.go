package replica

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// D12: minio-go is confined to this package, so replacing it (with the
// AWS SDK, should it be withdrawn) touches one package. Every Go file
// under cmd/ and internal/, tests included, is checked; spikes are their
// own modules and are not.
func TestOnlyReplicaImportsMinioGo(t *testing.T) {
	const minio = "github.com/minio/minio-go"
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", root, err)
	}
	here := filepath.Join(root, "internal", "replica")
	fset := token.NewFileSet()
	seen, replicaImports := 0, false
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, de fs.DirEntry, err error) error {
			if err != nil || de.IsDir() || !strings.HasSuffix(p, ".go") {
				return err
			}
			f, err := parser.ParseFile(fset, p, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			seen++
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if path != minio && !strings.HasPrefix(path, minio+"/") {
					continue
				}
				if filepath.Dir(p) == here {
					replicaImports = true
					continue
				}
				rel, _ := filepath.Rel(root, p)
				t.Errorf("%s imports %s; only internal/replica may (D12)", rel, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if seen == 0 || !replicaImports {
		t.Fatalf("the search found %d files and no minio-go import in internal/replica: it is not looking in the right place", seen)
	}
}
