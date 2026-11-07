package gateway

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api"
)

// SEC-9 AC: every method of the pinned backend.Backend interface is
// classified, and the table names no method the interface lacks.
func TestBackendMethodsAllClassified(t *testing.T) {
	iface := reflect.TypeFor[backend.Backend]()
	have := map[string]bool{}
	for m := range iface.Methods() {
		have[m.Name] = true
		c, ok := BackendMethods[m.Name]
		if !ok {
			t.Errorf("backend.Backend.%s is not classified in BackendMethods (SEC-9): admitted, refused, internal or unreachable?", m.Name)
			continue
		}
		if c.Why == "" {
			t.Errorf("BackendMethods[%s] gives no reason", m.Name)
		}
	}
	for name := range BackendMethods {
		if !have[name] {
			t.Errorf("BackendMethods names %s, which backend.Backend no longer has", name)
		}
	}
}

// versitygwSource is the directory of the pinned versitygw module, found
// through a function of it, so the test reads the router it is built with.
func versitygwSource(t *testing.T) string {
	t.Helper()
	f := runtime.FuncForPC(reflect.ValueOf(s3api.New).Pointer())
	file, _ := f.FileLine(f.Entry())
	if file == "" {
		t.Fatal("cannot find versitygw's source")
	}
	return filepath.Dir(filepath.Dir(file))
}

// routedActions returns the value of every metrics action versitygw's
// router names, read from its source: router.go names them as
// metrics.ActionX, and metrics/actions.go gives each its value.
func routedActions(t *testing.T) []string {
	t.Helper()
	dir := versitygwSource(t)
	fs := token.NewFileSet()
	values := map[string]string{}
	af, err := parser.ParseFile(fs, filepath.Join(dir, "metrics", "actions.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(af, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Values) != len(vs.Names) {
			return true
		}
		for i, name := range vs.Names {
			if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				v, _ := strconv.Unquote(lit.Value)
				values[name.Name] = v
			}
		}
		return true
	})
	rf, err := parser.ParseFile(fs, filepath.Join(dir, "s3api", "router.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(rf, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "metrics" && strings.HasPrefix(sel.Sel.Name, "Action") {
			v, ok := values[sel.Sel.Name]
			if !ok {
				t.Fatalf("router.go names metrics.%s, which actions.go does not define", sel.Sel.Name)
			}
			if !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		return true
	})
	if len(out) < 90 {
		t.Fatalf("found only %d routed actions in %s; the parse is wrong", len(out), dir)
	}
	return out
}

// SEC-9 AC: every S3 action the pinned versitygw routes is in Surface,
// and Surface names none it no longer routes.
func TestSurfaceCoversEveryRoutedAction(t *testing.T) {
	routed := routedActions(t)
	seen := map[string]bool{}
	for _, op := range Surface {
		if seen[op.Action] {
			t.Errorf("Surface names %s twice", op.Action)
		}
		seen[op.Action] = true
		if !slices.Contains(routed, op.Action) {
			t.Errorf("Surface names %s, which versitygw no longer routes", op.Action)
		}
		if op.Class != Admitted && op.Class != Refused {
			t.Errorf("%s: an S3 action is admitted or refused, not %q", op.Action, op.Class)
		}
		if op.Why == "" {
			t.Errorf("%s gives no reason", op.Action)
		}
	}
	for _, a := range routed {
		if !seen[a] {
			t.Errorf("versitygw routes %s, which Surface does not classify (SEC-9)", a)
		}
	}
}
