// Package overflow owns what identifies an overflow target outside
// lendisk.db: its name's form and its credential, read from the
// environment (S3-1, SEC-3, DESIGN §3.9, §3.11).
//
// Rules a caller must keep:
//   - A credential is read from the environment only, by the process
//     that uses it (Lookup), and is never written to a file, a log, the
//     state database or `status`. Its only copy outside memory is the
//     sealed node manifest (ID-4), which SEC-3 allows.
//   - Credential formats and logs itself redacted, so a credential that
//     reaches fmt or slog by mistake shows no secret. Use AccessKey and
//     SecretKey only where the provider is called or the manifest is
//     built.
//   - Errors name the variables, never their values.
package overflow

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
)

// nameRE is an overflow target's name: it names its place, its default
// environment variables and its lines in status.
var nameRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// CheckName refuses a name that is not 1 to 32 lower-case letters,
// digits and hyphens, starting with a letter.
func CheckName(name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("overflow target name %q: use 1 to 32 lower-case letters, digits and hyphens, starting with a letter", name)
	}
	return nil
}

// envPrefixRE is a prefix of environment variable names.
var envPrefixRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// CheckEnvPrefix refuses a prefix that cannot begin a variable name.
func CheckEnvPrefix(prefix string) error {
	if !envPrefixRE.MatchString(prefix) {
		return fmt.Errorf("environment prefix %q: use letters, digits and underscores, not starting with a digit", prefix)
	}
	return nil
}

// DefaultEnvPrefix is the prefix of name's variables unless overridden
// (overflow add --env-prefix): LENDISK_OVERFLOW_<NAME>_, the name upper
// case with hyphens as underscores.
func DefaultEnvPrefix(name string) string {
	return "LENDISK_OVERFLOW_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// EnvNames are the variables holding the access key and the secret key
// for prefix.
func EnvNames(prefix string) (access, secret string) {
	return prefix + "ACCESS_KEY", prefix + "SECRET_KEY"
}

// Credential is an overflow target's S3 access key and secret key.
type Credential struct {
	AccessKey string
	SecretKey string
}

// redacted is how a credential prints.
const redacted = "[credential redacted]"

// String, GoString and Format keep the keys out of anything printed.
func (Credential) String() string               { return redacted }
func (Credential) GoString() string             { return redacted }
func (Credential) Format(f fmt.State, _ rune)   { fmt.Fprint(f, redacted) }
func (Credential) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (Credential) MarshalText() ([]byte, error) { return []byte(redacted), nil }

// Getenv reads one variable; ok is false when it is unset.
type Getenv func(name string) (value string, ok bool)

// Lookup reads the credential for prefix with getenv. Both variables
// must be set and non-empty; the error names the ones that are not, and
// says where to set them.
func Lookup(getenv Getenv, prefix string) (Credential, error) {
	an, sn := EnvNames(prefix)
	a, aok := getenv(an)
	s, sok := getenv(sn)
	var missing []string
	if !aok || a == "" {
		missing = append(missing, an)
	}
	if !sok || s == "" {
		missing = append(missing, sn)
	}
	if len(missing) > 0 {
		return Credential{}, fmt.Errorf("%s not set: set it in the daemon's environment (systemd EnvironmentFile= or the container's secrets), never in the config file",
			strings.Join(missing, " and "))
	}
	return Credential{AccessKey: a, SecretKey: s}, nil
}

// Redact replaces every occurrence of the credential's keys in s, for a
// provider error that may echo them.
func (c Credential) Redact(s string) string {
	for _, k := range []string{c.SecretKey, c.AccessKey} {
		if k != "" {
			s = strings.ReplaceAll(s, k, "[redacted]")
		}
	}
	return s
}
