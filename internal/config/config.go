// Package config loads lendisk.toml (OPS-6). Every setting can also be given
// as a command-line flag and as an environment variable; precedence is
// flag, then environment, then file, then default. The daemon never writes
// this file.
//
// Names are derived from the TOML path: the key `log.level` is the flag
// `--log-level` and the variable `LENDISK_LOG_LEVEL`. A setting is a leaf
// field of Config with a `toml` tag and a `help` tag; RegisterFlags walks
// the struct, so a new field gets its flag and variable without further
// code.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/pflag"
)

const (
	// DefaultPath is where the config file is looked for without
	// --config or LENDISK_CONFIG; its directory is the config directory.
	DefaultPath = "/etc/lendisk/lendisk.toml"
	// EnvConfig names the config file, like --config.
	EnvConfig = "LENDISK_CONFIG"
	envPrefix = "LENDISK_"
)

// Config is the user's settings. Add new settings here, with toml, help and
// (when not empty) default tags.
type Config struct {
	Role    string `toml:"role" default:"full" help:"node role: full, or lender for a node that only lends (PEER-9)"`
	DataDir string `toml:"data_dir" default:"/var/lib/lendisk" help:"data directory: lendisk.db, store/, run/"`
	Log     Log    `toml:"log"`
	Tunnel  Tunnel `toml:"tunnel"`
}

// Log is the [log] section (OPS-5).
type Log struct {
	Level  string `toml:"level" default:"info" help:"log level: debug, info, warn or error"`
	Format string `toml:"format" default:"text" help:"log format: text (logfmt) or json"`
}

// Tunnel is the [tunnel] section: the peer connection (NET-1, SEC-14).
type Tunnel struct {
	Region string `toml:"region" help:"DERP relay region in this node's address, by ID or code; empty picks the nearest once. Changing it changes the address"`
}

// Validate checks values that have a fixed set of choices.
func (c *Config) Validate() error {
	var errs []error
	if c.Role != "full" && c.Role != "lender" {
		errs = append(errs, fmt.Errorf("role = %q: want \"full\" or \"lender\"", c.Role))
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level = %q: want debug, info, warn or error", c.Log.Level))
	}
	if c.Log.Format != "text" && c.Log.Format != "json" {
		errs = append(errs, fmt.Errorf("log.format = %q: want text or json", c.Log.Format))
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("data_dir is empty"))
	} else if !filepath.IsAbs(c.DataDir) {
		errs = append(errs, fmt.Errorf("data_dir = %q: must be an absolute path", c.DataDir))
	}
	return errors.Join(errs...)
}

// Loaded is a resolved configuration and where it came from.
type Loaded struct {
	Config
	Path       string   // the config file
	FileExists bool     // false if the file was absent and defaults applied
	Unknown    []string // keys in the file this version does not know
}

// ConfigDir is the directory holding the config file, the identity key and
// other secrets (mode 0700).
func (l *Loaded) ConfigDir() string { return filepath.Dir(l.Path) }

// setting is one leaf of Config.
type setting struct {
	key   string // TOML path, e.g. "log.level"
	help  string
	def   string
	field []int // reflect index path into Config
}

func (s setting) Flag() string {
	return strings.ReplaceAll(strings.ReplaceAll(s.key, ".", "-"), "_", "-")
}
func (s setting) Env() string {
	return envPrefix + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(s.key))
}

func settings() []setting {
	var out []setting
	var walk func(t reflect.Type, prefix string, idx []int)
	walk = func(t reflect.Type, prefix string, idx []int) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := f.Tag.Get("toml")
			if name == "" || name == "-" {
				continue
			}
			key := prefix + name
			path := append(append([]int{}, idx...), i)
			if f.Type.Kind() == reflect.Struct {
				walk(f.Type, key+".", path)
				continue
			}
			out = append(out, setting{key: key, help: f.Tag.Get("help"), def: f.Tag.Get("default"), field: path})
		}
	}
	walk(reflect.TypeOf(Config{}), "", nil)
	return out
}

// Flags holds the registered flag set so Load can tell which were set.
type Flags struct {
	fs     *pflag.FlagSet
	config *string
	vals   map[string]*string // key -> flag value
}

// RegisterFlags adds --config and one flag per setting to fs.
func RegisterFlags(fs *pflag.FlagSet) *Flags {
	f := &Flags{fs: fs, vals: map[string]*string{}}
	f.config = fs.String("config", "", "config file (env "+EnvConfig+", default "+DefaultPath+")")
	for _, s := range settings() {
		f.vals[s.key] = fs.String(s.Flag(), "", fmt.Sprintf("%s (env %s, default %q)", s.help, s.Env(), s.def))
	}
	return f
}

// Load resolves the configuration. A missing config file is not an error:
// defaults, environment and flags still apply.
func (f *Flags) Load() (*Loaded, error) {
	return f.load(os.LookupEnv)
}

func (f *Flags) load(getenv func(string) (string, bool)) (*Loaded, error) {
	l := &Loaded{Path: DefaultPath}
	if v, ok := getenv(EnvConfig); ok && v != "" {
		l.Path = v
	}
	if f.fs.Changed("config") {
		l.Path = *f.config
	}
	if abs, err := filepath.Abs(l.Path); err == nil {
		l.Path = abs
	}

	ss := settings()
	rv := reflect.ValueOf(&l.Config).Elem()
	for _, s := range ss {
		rv.FieldByIndex(s.field).SetString(s.def)
	}

	md, err := toml.DecodeFile(l.Path, &l.Config)
	switch {
	case err == nil:
		l.FileExists = true
		for _, k := range md.Undecoded() {
			if t := md.Type(k...); t == "Hash" || t == "ArrayHash" {
				continue // report the keys, not the tables holding them
			}
			l.Unknown = append(l.Unknown, k.String())
		}
		sort.Strings(l.Unknown)
	case errors.Is(err, fs.ErrNotExist):
	default:
		return nil, fmt.Errorf("reading %s: %w", l.Path, err)
	}

	for _, s := range ss {
		if v, ok := getenv(s.Env()); ok {
			rv.FieldByIndex(s.field).SetString(v)
		}
		if f.fs.Changed(s.Flag()) {
			rv.FieldByIndex(s.field).SetString(*f.vals[s.key])
		}
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("configuration: %w", err)
	}
	return l, nil
}
