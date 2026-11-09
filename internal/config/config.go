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
//
// An array of tables (`[[endpoint.listen]]`) is one setting: a repeatable
// flag and one variable, each entry written compactly as comma-separated
// key=value pairs using the table's keys, entries separated by ';'
// (`address=192.168.1.10:9000,tls=off;interface=eth0,port=9000`). Values
// cannot contain ',' or ';'. A flag or variable replaces the file's whole
// list; an empty variable clears it.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/spf13/pflag"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/pace"
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
	Role     string   `toml:"role" default:"full" help:"node role: full, or lender for a node that only lends (PEER-9)"`
	DataDir  string   `toml:"data_dir" default:"/var/lib/lendisk" help:"data directory: lendisk.db, store/, run/"`
	Store    Store    `toml:"store"`
	Endpoint Endpoint `toml:"endpoint"`
	Tunnel   Tunnel   `toml:"tunnel"`
	Alerts   Alerts   `toml:"alerts"`
	// Replication is [replication] (D6, §3.4).
	Replication Replication `toml:"replication"`
	// Lending is [lending]: what this node lends its friends (LEND-1).
	Lending Lending `toml:"lending"`
	Log     Log     `toml:"log"`
}

// Lending is the [lending] section (LEND-1, LEND-8).
type Lending struct {
	MaxTotal string `toml:"max_total" help:"the most this node lends all friends together: the sum of its numbers for its friendships, and of its open invites, may not exceed it (LEND-1, LEND-8), such as 240G; empty means what the store's filesystem can hold, its size less store.free_space_floor"`
	// BorrowerLimit is [[lending.borrower_limit]] (LEND-5).
	BorrowerLimit []BorrowerLimit `toml:"borrower_limit" help:"rate limits on one friend as a borrower (LEND-5), repeatable: what this node receives from it (its uploads) and sends it (answers to its reads), in bytes a second, at least 16K each or empty for none; changes apply without a restart; e.g. 'peer=<node id>,receive=2M,send=4M'"`
}

// BorrowerLimit is one [[lending.borrower_limit]] entry (LEND-5).
type BorrowerLimit struct {
	Peer    string `toml:"peer"`
	Receive string `toml:"receive"`
	Send    string `toml:"send"`
}

// Store is the [store] section: one store for the node's own buckets and
// what it lends (D15).
type Store struct {
	Backend        string `toml:"backend" default:"posix" help:"store backend; posix is the only one in v1"`
	Path           string `toml:"path" help:"store directory (default <data_dir>/store)"`
	FreeSpaceFloor string `toml:"free_space_floor" default:"5%" help:"refuse writes below this much free space: a percentage or a size such as 20G (LEND-2, EP-12)"`
	ReconcileEvery string `toml:"reconcile_every" default:"24h" help:"how often the store is compared with the index, besides at start, so objects written or removed outside the S3 endpoint are replicated (EP-14), such as 24h or 6h"`
	// ReconcileDeleteLimit is a percentage: a guard against an unmounted
	// or emptied store deleting a backup at every friend.
	ReconcileDeleteLimit Number `toml:"reconcile_delete_limit" default:"10" help:"a reconcile pass that finds more than this percentage of a bucket's objects missing from the store deletes none of them until you confirm with lendisk bucket reconcile (EP-14); 1 to 100, where 100 never asks"`
}

// Endpoint is the [endpoint] section (EP-1, EP-5, EP-17).
type Endpoint struct {
	Loopback string   `toml:"loopback" default:"127.0.0.1:9000" help:"loopback address of the S3 endpoint for backup tools on this host"`
	Listen   []Listen `toml:"listen" help:"network addresses of the S3 endpoint for other machines (EP-17), repeatable; entries separated by ';', fields by ',', e.g. 'address=192.168.1.10:9000,tls=off;interface=eth0,port=9000'"`
}

// Listen is one [[endpoint.listen]] entry (EP-17): an IP address with a
// port, or an interface whose addresses are used with Port.
type Listen struct {
	Address   string `toml:"address"`
	Interface string `toml:"interface"`
	Port      int    `toml:"port"`
	// TLS is "generate" (the default), "files" with Cert and Key, or "off".
	TLS  string `toml:"tls"`
	Cert string `toml:"cert"`
	Key  string `toml:"key"`
}

// String names the entry for messages: its address, or interface:port.
func (l Listen) String() string {
	if l.Interface != "" {
		return net.JoinHostPort(l.Interface, strconv.Itoa(l.Port))
	}
	return l.Address
}

func (l Listen) check() error {
	switch {
	case l.Address != "" && l.Interface != "":
		return errors.New("give address or interface, not both")
	case l.Address != "":
		if l.Port != 0 {
			return errors.New("the port goes in address (host:port)")
		}
		host, port, err := net.SplitHostPort(l.Address)
		if err != nil {
			return err
		}
		if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
			return fmt.Errorf("bad port %q", port)
		}
		ip := net.ParseIP(host)
		switch {
		case host == "":
			return errors.New("empty host: name one address, such as 192.168.1.10:9000")
		case ip == nil:
			return errors.New("host must be an IP address; for a name that changes, use interface and port")
		case ip.IsUnspecified():
			return errors.New("wildcard address refused: name one address, or an interface")
		}
	case l.Interface != "":
		if l.Port < 1 || l.Port > 65535 {
			return errors.New("an interface entry needs port = 1 to 65535")
		}
	default:
		return errors.New("needs address, or interface and port")
	}
	switch l.TLS {
	case "generate", "off":
		if l.Cert != "" || l.Key != "" {
			return fmt.Errorf("cert and key are used only with tls = \"files\"")
		}
	case "files":
		if !filepath.IsAbs(l.Cert) || !filepath.IsAbs(l.Key) {
			return errors.New("tls = \"files\" needs cert and key as absolute paths")
		}
	default:
		return fmt.Errorf("tls = %q: want generate, files or off", l.TLS)
	}
	return nil
}

// StorePath is the store directory, defaulting to <data_dir>/store.
func (c *Config) StorePath() string {
	if c.Store.Path != "" {
		return c.Store.Path
	}
	return filepath.Join(c.DataDir, "store")
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

// Alerts is the [alerts] section (OPS-2, OPS-3).
type Alerts struct {
	Command          string `toml:"command" help:"absolute path of one executable program, run for each alert with a JSON object on stdin, without a shell or arguments (put arguments in a small script); its output is logged when it fails (OPS-3); empty for none"`
	Webhook          string `toml:"webhook" help:"http or https URL each alert's JSON is POSTed to, redirects not followed (OPS-3); may hold a secret, never logged in full; empty for none"`
	UnreachableAfter string `toml:"unreachable_after" default:"24h" help:"alert when a friend has not answered for this long and has been retried for at least an hour (OPS-2), such as 24h or 90m"`
}

// Replication is the [replication] section: pooled placement (D6,
// §3.4, EP-6).
type Replication struct {
	Copies       Number `toml:"copies" default:"2" help:"how many friends hold each object of a bucket created without --copies (EP-6); 1 means one lost friend loses data, and bucket add and status say so"`
	RerouteAfter string `toml:"reroute_after" default:"1h" help:"move the unsent copies at a friend that has failed this long to another friend with room (D6, REPL-1), such as 1h or 90m"`
	ReplaceAfter string `toml:"replace_after" default:"168h" help:"a friend failing this long since its last answer is no longer room for copies (REPL-12, REPL-13), such as 168h"`
	// The rest is pacing (REPL-6, REPL-7); it applies without a restart
	// (the daemon re-reads the file, §3.4 REPL-7).
	PartSize      string      `toml:"part_size" default:"16M" help:"objects larger than this are sent in parts of this size and resume at the next part after an interruption (REPL-6); 5M to 5G"`
	Schedule      string      `toml:"schedule" help:"when replication sends, in local time: windows such as '01:00-07:00', comma-separated; one ending before it starts runs past midnight; empty means at any time (REPL-7). A closing window stops a transfer at the end of its part"`
	UploadLimit   string      `toml:"upload_limit" help:"most bytes a second replication sends, all friends together (REPL-7), such as 2M; at least 16K, empty for no limit"`
	DownloadLimit string      `toml:"download_limit" help:"most bytes a second this node fetches from friends (the read-through), all friends together (REPL-7); at least 16K, empty for no limit"`
	PeerLimit     []PeerLimit `toml:"peer_limit" help:"per-friend limits on what replication sends to it and fetches from it (REPL-7), repeatable, bytes a second, at least 16K each or empty for none; e.g. 'peer=<node id>,upload=1M,download=4M'"`
}

// PeerLimit is one [[replication.peer_limit]] entry (REPL-7).
type PeerLimit struct {
	Peer     string `toml:"peer"`
	Upload   string `toml:"upload"`
	Download string `toml:"download"`
}

// rate reads a limit in bytes a second; empty is none (0).
func rate(s string) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, nil
	}
	v, err := ParseSize(s)
	if err != nil {
		return 0, fmt.Errorf("%w (bytes a second)", err)
	}
	if v > 1<<50 {
		return 0, fmt.Errorf("%q: too large", s)
	}
	if err := pace.CheckRate(int64(v)); err != nil || v == 0 {
		if v == 0 {
			return 0, fmt.Errorf("%q: give at least 16K, or leave it empty for no limit", s)
		}
		return 0, err
	}
	return int64(v), nil
}

// PartSize is replication.part_size in bytes, which Validate checked.
func (c *Config) PartSize() int64 {
	v, _ := ParseSize(c.Replication.PartSize)
	return int64(v)
}

// Pace is the pacing settings (REPL-7, LEND-5), which Validate checked.
func (c *Config) Pace() pace.Settings {
	var s pace.Settings
	s.Up, _ = rate(c.Replication.UploadLimit)
	s.Down, _ = rate(c.Replication.DownloadLimit)
	s.Schedule, _ = pace.ParseSchedule(c.Replication.Schedule)
	for _, p := range c.Replication.PeerLimit {
		if s.Peers == nil {
			s.Peers = map[string]pace.Rates{}
		}
		id, _ := identity.ParseNodeID(p.Peer)
		var r pace.Rates
		r.Up, _ = rate(p.Upload)
		r.Down, _ = rate(p.Download)
		s.Peers[id.String()] = r
	}
	for _, p := range c.Lending.BorrowerLimit {
		if s.Lend == nil {
			s.Lend = map[string]pace.Rates{}
		}
		id, _ := identity.ParseNodeID(p.Peer)
		var r pace.Rates
		r.Down, _ = rate(p.Receive)
		r.Up, _ = rate(p.Send)
		s.Lend[id.String()] = r
	}
	return s
}

// checkPacing validates the pacing settings.
func (c *Config) checkPacing() []error {
	var errs []error
	if v, err := ParseSize(c.Replication.PartSize); err != nil || v < 5<<20 || v > 5<<30 {
		errs = append(errs, fmt.Errorf("replication.part_size = %q: want a size from 5M to 5G, such as 16M", c.Replication.PartSize))
	}
	if _, err := pace.ParseSchedule(c.Replication.Schedule); err != nil {
		errs = append(errs, fmt.Errorf("replication.schedule: %w", err))
	}
	for _, r := range []struct{ key, v string }{
		{"replication.upload_limit", c.Replication.UploadLimit},
		{"replication.download_limit", c.Replication.DownloadLimit},
	} {
		if _, err := rate(r.v); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.key, err))
		}
	}
	type entry struct {
		peer   string
		limits [][2]string
	}
	check := func(key string, es []entry) {
		seen := map[string]bool{}
		for i, e := range es {
			id, err := identity.ParseNodeID(e.peer)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s entry %d: peer = %q: want the friend's node ID, as `lendisk peer list` shows it", key, i+1, e.peer))
				continue
			}
			if seen[id.String()] {
				errs = append(errs, fmt.Errorf("%s entry %d: a second entry for %s; give each friend one", key, i+1, e.peer))
			}
			seen[id.String()] = true
			for _, l := range e.limits {
				if _, err := rate(l[1]); err != nil {
					errs = append(errs, fmt.Errorf("%s entry %d: %s: %w", key, i+1, l[0], err))
				}
			}
		}
	}
	var peers, lend []entry
	for _, p := range c.Replication.PeerLimit {
		peers = append(peers, entry{p.Peer, [][2]string{{"upload", p.Upload}, {"download", p.Download}}})
	}
	for _, p := range c.Lending.BorrowerLimit {
		lend = append(lend, entry{p.Peer, [][2]string{{"receive", p.Receive}, {"send", p.Send}}})
	}
	check("replication.peer_limit", peers)
	check("lending.borrower_limit", lend)
	return errs
}

// Number is an integer setting kept as text, so the flag and variable
// work like every other setting and the file may give it as a number.
type Number string

// UnmarshalText accepts a TOML integer or string.
func (n *Number) UnmarshalText(b []byte) error { *n = Number(b); return nil }

// Int is the number, which Validate checked.
func (n Number) Int() int { i, _ := strconv.Atoi(string(n)); return i }

// RerouteAfter is replication.reroute_after, which Validate checked.
func (c *Config) RerouteAfter() time.Duration {
	d, _ := time.ParseDuration(c.Replication.RerouteAfter)
	return d
}

// ReplaceAfter is replication.replace_after, which Validate checked.
func (c *Config) ReplaceAfter() time.Duration {
	d, _ := time.ParseDuration(c.Replication.ReplaceAfter)
	return d
}

// MaxTotal is lending.max_total in bytes, which Validate checked; ok is
// false when it is unset: the cap is then what the store's filesystem
// can hold, which the caller reads.
func (c *Config) MaxTotal() (n int64, ok bool) {
	if strings.TrimSpace(c.Lending.MaxTotal) == "" {
		return 0, false
	}
	v, _ := ParseSize(c.Lending.MaxTotal)
	return int64(v), true
}

// ReconcileEvery is store.reconcile_every, which Validate checked.
func (c *Config) ReconcileEvery() time.Duration {
	d, _ := time.ParseDuration(c.Store.ReconcileEvery)
	return d
}

// UnreachableAfter is alerts.unreachable_after, which Validate checked.
func (c *Config) UnreachableAfter() time.Duration {
	d, _ := time.ParseDuration(c.Alerts.UnreachableAfter)
	return d
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
	if c.Store.Backend != "posix" {
		errs = append(errs, fmt.Errorf("store.backend = %q: only \"posix\" is supported", c.Store.Backend))
	}
	if c.Store.Path != "" && !filepath.IsAbs(c.Store.Path) {
		errs = append(errs, fmt.Errorf("store.path = %q: must be an absolute path", c.Store.Path))
	}
	if _, err := ParseFloor(c.Store.FreeSpaceFloor); err != nil {
		errs = append(errs, fmt.Errorf("store.free_space_floor: %w", err))
	}
	if d, err := time.ParseDuration(c.Store.ReconcileEvery); err != nil || d < time.Minute {
		errs = append(errs, fmt.Errorf("store.reconcile_every = %q: want a duration of a minute or more, such as 24h", c.Store.ReconcileEvery))
	}
	if n, err := strconv.Atoi(string(c.Store.ReconcileDeleteLimit)); err != nil || n < 1 || n > 100 {
		errs = append(errs, fmt.Errorf("store.reconcile_delete_limit = %q: want a whole percentage from 1 to 100", c.Store.ReconcileDeleteLimit))
	}
	if err := checkLoopback(c.Endpoint.Loopback); err != nil {
		errs = append(errs, fmt.Errorf("endpoint.loopback = %q: %w", c.Endpoint.Loopback, err))
	}
	if c.Role == "lender" && len(c.Endpoint.Listen) > 0 {
		errs = append(errs, errors.New("endpoint.listen: a lender-only node has no S3 endpoint (PEER-9); remove the entries or set role = \"full\""))
	}
	for i, l := range c.Endpoint.Listen {
		if err := l.check(); err != nil {
			errs = append(errs, fmt.Errorf("endpoint.listen entry %d (%s): %w", i+1, l, err))
		}
	}
	if d, err := time.ParseDuration(c.Alerts.UnreachableAfter); err != nil || d <= 0 {
		errs = append(errs, fmt.Errorf("alerts.unreachable_after = %q: want a positive duration such as 24h or 90m", c.Alerts.UnreachableAfter))
	}
	if n, err := strconv.Atoi(string(c.Replication.Copies)); err != nil || n < 1 {
		errs = append(errs, fmt.Errorf("replication.copies = %q: want a whole number, at least 1", c.Replication.Copies))
	}
	for _, d := range []struct{ key, v string }{
		{"replication.reroute_after", c.Replication.RerouteAfter},
		{"replication.replace_after", c.Replication.ReplaceAfter},
	} {
		if v, err := time.ParseDuration(d.v); err != nil || v <= 0 {
			errs = append(errs, fmt.Errorf("%s = %q: want a positive duration such as 1h or 168h", d.key, d.v))
		}
	}
	errs = append(errs, c.checkPacing()...)
	if m := strings.TrimSpace(c.Lending.MaxTotal); m != "" {
		if v, err := ParseSize(m); err != nil || v == 0 || v > 1<<62 {
			errs = append(errs, fmt.Errorf("lending.max_total = %q: want a size above zero such as 240G, or empty for no cap", c.Lending.MaxTotal))
		}
	}
	if c.Alerts.Webhook != "" {
		// Never echo the URL: its path or query may be a secret.
		u, err := url.Parse(c.Alerts.Webhook)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, errors.New("alerts.webhook: want an http:// or https:// URL with a host"))
		}
	}
	return errors.Join(errs...)
}

// checkLoopback accepts host:port where host is a loopback IP literal or
// "localhost" (EP-5: never a network address).
func checkLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("bad port %q", port)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("must be a loopback address such as 127.0.0.1:9000; LAN addresses are [[endpoint.listen]] entries")
	}
	return nil
}

// Floor is a free-space floor: a fraction of the filesystem or a byte count.
type Floor struct {
	Percent float64 // 0 when Bytes is used
	Bytes   uint64
}

// ParseFloor reads "5%", "0.5%", "20G", "512M" or a plain byte count.
func ParseFloor(s string) (Floor, error) {
	s = strings.TrimSpace(s)
	if p, ok := strings.CutSuffix(s, "%"); ok {
		v, err := strconv.ParseFloat(p, 64)
		if err != nil || v < 0 || v > 100 {
			return Floor{}, fmt.Errorf("%q: want a percentage between 0%% and 100%%", s)
		}
		return Floor{Percent: v}, nil
	}
	b, err := ParseSize(s)
	if err != nil {
		return Floor{}, err
	}
	return Floor{Bytes: b}, nil
}

// ParseSize reads a byte count with an optional K, M, G or T suffix
// (powers of 1024, an optional trailing "B" or "iB" ignored).
func ParseSize(s string) (uint64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	t = strings.TrimSuffix(strings.TrimSuffix(t, "B"), "I")
	mult := uint64(1)
	if n := len(t); n > 0 {
		switch t[n-1] {
		case 'K':
			mult = 1 << 10
		case 'M':
			mult = 1 << 20
		case 'G':
			mult = 1 << 30
		case 'T':
			mult = 1 << 40
		}
		if mult != 1 {
			t = t[:n-1]
		}
	}
	v, err := strconv.ParseFloat(t, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%q: want a size such as 20G or 512M", s)
	}
	return uint64(v * float64(mult)), nil
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
	list  bool  // an array of tables, in the compact form
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
			list := f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Struct
			out = append(out, setting{key: key, help: f.Tag.Get("help"), def: f.Tag.Get("default"), field: path, list: list})
		}
	}
	walk(reflect.TypeOf(Config{}), "", nil)
	return out
}

// Flags holds the registered flag set so Load can tell which were set.
type Flags struct {
	fs     *pflag.FlagSet
	config *string
	vals   map[string]*string   // key -> flag value
	lists  map[string]*[]string // key -> repeated flag values
}

// RegisterFlags adds --config and one flag per setting to fs.
func RegisterFlags(fs *pflag.FlagSet) *Flags {
	f := &Flags{fs: fs, vals: map[string]*string{}, lists: map[string]*[]string{}}
	f.config = fs.String("config", "", "config file (env "+EnvConfig+", default "+DefaultPath+")")
	for _, s := range settings() {
		if s.list {
			f.lists[s.key] = fs.StringArray(s.Flag(), nil, fmt.Sprintf("%s (env %s)", s.help, s.Env()))
			continue
		}
		f.vals[s.key] = fs.String(s.Flag(), "", fmt.Sprintf("%s (env %s, default %q)", s.help, s.Env(), s.def))
	}
	return f
}

// parseList reads the compact form of an array of tables into a slice of
// type t: entries separated by ';', key=value fields by ',', keys being
// the element's toml tags.
func parseList(t reflect.Type, s string) (reflect.Value, error) {
	out := reflect.MakeSlice(t, 0, 0)
	et := t.Elem()
	for _, entry := range strings.Split(s, ";") {
		if strings.TrimSpace(entry) == "" {
			continue
		}
		v := reflect.New(et).Elem()
		for _, kv := range strings.Split(entry, ",") {
			k, val, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok {
				return out, fmt.Errorf("%q: want key=value", kv)
			}
			i := -1
			for j := 0; j < et.NumField(); j++ {
				if et.Field(j).Tag.Get("toml") == k {
					i = j
				}
			}
			if i < 0 {
				return out, fmt.Errorf("unknown key %q", k)
			}
			switch fv := v.Field(i); fv.Kind() {
			case reflect.Int:
				n, err := strconv.Atoi(val)
				if err != nil {
					return out, fmt.Errorf("%s = %q: want a number", k, val)
				}
				fv.SetInt(int64(n))
			default:
				fv.SetString(val)
			}
		}
		out = reflect.Append(out, v)
	}
	return out, nil
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
		if !s.list {
			rv.FieldByIndex(s.field).SetString(s.def)
		}
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
		if s.list {
			fv := rv.FieldByIndex(s.field)
			from := s.Env()
			v, ok := getenv(from)
			if f.fs.Changed(s.Flag()) {
				from, v, ok = "--"+s.Flag(), strings.Join(*f.lists[s.key], ";"), true
			}
			if ok {
				list, err := parseList(fv.Type(), v)
				if err != nil {
					return nil, fmt.Errorf("configuration: %s: %w", from, err)
				}
				fv.Set(list)
			}
			continue
		}
		if v, ok := getenv(s.Env()); ok {
			rv.FieldByIndex(s.field).SetString(v)
		}
		if f.fs.Changed(s.Flag()) {
			rv.FieldByIndex(s.field).SetString(*f.vals[s.key])
		}
	}
	for i := range l.Endpoint.Listen {
		if l.Endpoint.Listen[i].TLS == "" {
			l.Endpoint.Listen[i].TLS = "generate"
		}
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("configuration: %w", err)
	}
	return l, nil
}
