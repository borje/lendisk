// Package profile owns the tool profiles (EP-7): data files that sort
// every key a backup tool writes into snapshot, metadata, data or
// local-only, and say whether the tool is inside the encryption guarantee
// (EP-8).
//
// Profiles are TOML files compiled into the binary (profiles/*.toml) and
// overridable in <config>/profiles/: a file there replaces the compiled-in
// profile of its name, or adds a profile, so adding one needs no release.
//
// Rules a caller must keep:
//   - A key no rule matches is data, and a profile name with no file
//     classifies every key as data; Classify never fails.
//   - Local-only keys are stored and served but never queued (EP-7).
//   - A snapshot key is also metadata for placement (REPL-12).
//   - A rewritable key keeps its class: Rewritable only says the tool
//     rewrites it in place in normal use, so a changed version is sent
//     to each place as a rewrite rather than refused (EP-7, EP-11).
package profile

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// Class is what a key is to replication (EP-7).
type Class string

const (
	// Snapshot keys mean a backup is complete; status names them. They
	// are placed like metadata.
	Snapshot Class = "snapshot"
	// Metadata keys are placed at every friend (REPL-12).
	Metadata Class = "metadata"
	// Data keys are placed at `copies` places.
	Data Class = "data"
	// LocalOnly keys are stored and served but never queued.
	LocalOnly Class = "local-only"
)

// Profile is one tool's key classes. Each list holds glob patterns in
// which * matches any run of characters, slashes included, and ? one
// character.
type Profile struct {
	Name      string   `toml:"-"`
	Encrypted bool     `toml:"encrypted"`
	LocalOnly []string `toml:"local_only"`
	Snapshot  []string `toml:"snapshot"`
	Metadata  []string `toml:"metadata"`
	Data      []string `toml:"data"`
	// HashNamedKeys are the keys the tool names by the SHA-256 of their
	// content, in hex, as their last path segment: recovery rejects a
	// listed hash that is not the name (ID-5).
	HashNamedKeys []string `toml:"hash_named"`
	// RewritableKeys are the few keys the tool rewrites in place in
	// normal use (EP-7, v0.11.3): a changed version is replicated as a
	// rewrite, which each lender stores, keeping the version it
	// replaces as a tombstone (LEND-4, RET-1).
	RewritableKeys []string `toml:"rewritable"`
}

// Rewritable reports whether the tool rewrites key in place in normal
// use (EP-7).
func (p *Profile) Rewritable(key string) bool {
	for _, pat := range p.RewritableKeys {
		if glob(pat, key) {
			return true
		}
	}
	return false
}

// HashNamed reports whether the tool names key by the SHA-256 of its
// content (NameHash).
func (p *Profile) HashNamed(key string) bool {
	for _, pat := range p.HashNamedKeys {
		if glob(pat, key) {
			return true
		}
	}
	return false
}

// NameHash is the hash a hash-named key's name carries: its last path
// segment.
func NameHash(key string) string { return key[strings.LastIndexByte(key, '/')+1:] }

// Classify returns the class of key: local-only, else snapshot, else
// metadata, else data, by the first list a pattern of matches.
func (p *Profile) Classify(key string) Class {
	for _, c := range []struct {
		patterns []string
		class    Class
	}{{p.LocalOnly, LocalOnly}, {p.Snapshot, Snapshot}, {p.Metadata, Metadata}} {
		for _, pat := range c.patterns {
			if glob(pat, key) {
				return c.class
			}
		}
	}
	return Data
}

// Set is the profiles this node knows, by name.
type Set map[string]*Profile

// none is what a name with no profile file gets: everything is data.
var none = &Profile{Name: "none"}

// Get returns the named profile. A name with no profile file classifies
// every key as data, which is what EP-7 says of a key no rule matches.
func (s Set) Get(name string) *Profile {
	if p, ok := s[name]; ok {
		return p
	}
	return none
}

//go:embed profiles/*.toml
var builtin embed.FS

// Load reads the compiled-in profiles and then dir/*.toml, which replace
// or add profiles by file name. dir may be empty or absent.
func Load(dir string) (Set, error) {
	set := Set{}
	ents, err := builtin.ReadDir("profiles")
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		b, err := builtin.ReadFile("profiles/" + e.Name())
		if err != nil {
			return nil, err
		}
		if err := set.add(e.Name(), b); err != nil {
			return nil, fmt.Errorf("compiled-in profile %w", err)
		}
	}
	if dir == "" {
		return set, nil
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.toml"))
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading profile: %w", err)
		}
		if err := set.add(f, b); err != nil {
			return nil, fmt.Errorf("profile %w; fix or remove the file", err)
		}
	}
	return set, nil
}

func (s Set) add(file string, b []byte) error {
	var p Profile
	md, err := toml.NewDecoder(bytes.NewReader(b)).Decode(&p)
	if err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	if un := md.Undecoded(); len(un) > 0 {
		return fmt.Errorf("%s: unknown key %q (expected encrypted, local_only, snapshot, metadata, data, hash_named, rewritable)", file, un[0].String())
	}
	for _, l := range [][]string{p.LocalOnly, p.Snapshot, p.Metadata, p.Data, p.HashNamedKeys, p.RewritableKeys} {
		for _, pat := range l {
			if pat == "" {
				return fmt.Errorf("%s: empty pattern", file)
			}
		}
	}
	p.Name = strings.TrimSuffix(filepath.Base(file), ".toml")
	s[p.Name] = &p
	return nil
}

// glob reports whether key matches pattern, where * matches any run of
// characters (slashes included) and ? exactly one.
func glob(pattern, key string) bool {
	// Iterative wildcard match with backtracking to the last *.
	p, k := 0, 0
	star, mark := -1, 0
	for k < len(key) {
		switch {
		case p < len(pattern) && (pattern[p] == '?' || pattern[p] == key[k]):
			p++
			k++
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, k
			p++
		case star >= 0:
			p = star + 1
			mark++
			k = mark
		default:
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
