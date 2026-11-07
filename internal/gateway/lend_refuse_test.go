package gateway

import (
	"bufio"
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/versity/versitygw/auth"
)

// SEC-9, #15 review: Lending refuses, for a borrower's authenticated
// account, every backend method SEC-9 classifies Refused, before it
// touches anything, so the gate's unverified reading of the access key
// is not the only wall. Lending is called directly with a nil Next and
// no database: reaching either would panic.
func TestLendingRefusesEveryRefusedMethodToABorrower(t *testing.T) {
	l := &Lending{}
	ctx := context.WithValue(context.Background(), "account", //nolint:staticcheck // versitygw's key
		auth.Account{Access: newIdentity(t).NodeID().String(), Role: auth.RoleUserPlus})
	v := reflect.ValueOf(l)
	for name, m := range BackendMethods {
		if m.Class != Refused {
			continue
		}
		meth := v.MethodByName(name)
		if !meth.IsValid() {
			t.Fatalf("Lending has no %s", name)
		}
		args := []reflect.Value{reflect.ValueOf(ctx)}
		for i := 1; i < meth.Type().NumIn(); i++ {
			in := meth.Type().In(i)
			if in.Kind() == reflect.Pointer {
				args = append(args, reflect.New(in.Elem()))
			} else {
				args = append(args, reflect.Zero(in))
			}
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("%s from a borrower reached the store or the database: %v", name, p)
				}
			}()
			out := meth.Call(args)
			last := out[len(out)-1]
			if f, ok := last.Interface().(func(*bufio.Writer)); ok {
				var b bytes.Buffer
				w := bufio.NewWriter(&b)
				f(w)
				w.Flush()
				if b.Len() != 0 {
					t.Errorf("%s from a borrower answered %q", name, b.String())
				}
				return
			}
			err, _ := last.Interface().(error)
			if err != errNotForBorrower {
				t.Errorf("%s from a borrower: %v, want AccessDenied", name, err)
			}
		}()
	}
}
