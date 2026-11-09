package overflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func env(m map[string]string) Getenv {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// SEC-3: the credential comes from two variables named by the prefix,
// the default one from the target's name; an unset or empty one is an
// error that names the variables and never a value.
func TestLookupReadsTheTwoVariablesAndNamesMissingOnes(t *testing.T) {
	p := DefaultEnvPrefix("my-b2")
	if p != "LENDISK_OVERFLOW_MY_B2_" {
		t.Fatalf("default prefix %q", p)
	}
	c, err := Lookup(env(map[string]string{p + "ACCESS_KEY": "AKID", p + "SECRET_KEY": "s3cr3t"}), p)
	if err != nil || c.AccessKey != "AKID" || c.SecretKey != "s3cr3t" {
		t.Fatalf("Lookup = %v, %v", c, err)
	}
	_, err = Lookup(env(map[string]string{p + "ACCESS_KEY": "AKID", p + "SECRET_KEY": ""}), p)
	if err == nil || !strings.Contains(err.Error(), p+"SECRET_KEY not set") || strings.Contains(err.Error(), "AKID") {
		t.Fatalf("empty secret: %v", err)
	}
	_, err = Lookup(env(nil), p)
	if err == nil || !strings.Contains(err.Error(), p+"ACCESS_KEY and "+p+"SECRET_KEY") {
		t.Fatalf("nothing set: %v", err)
	}
}

// SEC-3: a credential printed, logged or marshalled by mistake shows no
// key.
func TestCredentialIsRedactedEverywhere(t *testing.T) {
	c := Credential{AccessKey: "AKIDVISIBLE", SecretKey: "SECRETVISIBLE"}
	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("x", "cred", c, "wrapped", struct{ C Credential }{c})
	js, _ := json.Marshal(struct{ C Credential }{c})
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v %+v %#v %s %q", c, c, c, c, c), logs.String(), string(js)} {
		if strings.Contains(s, "VISIBLE") {
			t.Fatalf("credential shown: %s", s)
		}
	}
	if got := c.Redact("bad key AKIDVISIBLE with SECRETVISIBLE"); strings.Contains(got, "VISIBLE") {
		t.Fatalf("Redact left %q", got)
	}
}

func TestNamesAndPrefixesAreChecked(t *testing.T) {
	for _, ok := range []string{"b2", "wasabi-eu", "a"} {
		if err := CheckName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "B2", "2b", "b2:x", "b_2", strings.Repeat("a", 33)} {
		if CheckName(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if CheckEnvPrefix("MY_B2_") != nil || CheckEnvPrefix("1X") == nil || CheckEnvPrefix("A-B") == nil {
		t.Error("env prefix check")
	}
}
