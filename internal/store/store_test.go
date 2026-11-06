package store

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/borje/lendisk/internal/config"
)

func TestPrepareRealDirectory(t *testing.T) {
	dir := t.TempDir() + "/store"
	if err := Prepare(dir, config.Floor{Percent: 0}); err != nil {
		t.Skipf("temp dir cannot hold a store here: %v", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Fatalf("mode %04o", fi.Mode().Perm())
	}
}

// #53: generic buckets hold plaintext, so an existing store others can
// read is refused, unless it is still empty, when it is tightened.
func TestPrepareRefusesStoreOthersCanRead(t *testing.T) {
	ok := Checker{
		Xattr:  func(string) error { return nil },
		Statfs: func(string) (Space, error) { return Space{Total: 1 << 40, Free: 1 << 40, FSType: "ext4"}, nil },
	}
	dir := t.TempDir() + "/store"
	os.Mkdir(dir, 0o755)
	os.Chmod(dir, 0o755)
	if err := ok.Prepare(dir, config.Floor{}); err != nil {
		t.Fatalf("empty store: %v", err)
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o700 {
		t.Fatalf("empty store left at %04o", fi.Mode().Perm())
	}
	os.Mkdir(dir+"/laptop", 0o755)
	os.Chmod(dir, 0o750)
	err := ok.Prepare(dir, config.Floor{})
	if err == nil || !strings.Contains(err.Error(), "chmod 700 "+dir) || !strings.Contains(err.Error(), "0750") {
		t.Fatalf("readable store: %v", err)
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o750 {
		t.Fatalf("mode changed to %04o", fi.Mode().Perm())
	}
}

// EP-12 AC: refuse below the free-space floor, naming the filesystem.
func TestFloorRefused(t *testing.T) {
	c := Checker{
		Xattr:  func(string) error { return nil },
		Statfs: func(string) (Space, error) { return Space{Total: 1000 << 30, Free: 30 << 30, FSType: "ext4"}, nil },
	}
	err := c.Check("/s", config.Floor{Percent: 5})
	if err == nil || !strings.Contains(err.Error(), "ext4") || !strings.Contains(err.Error(), "free-space floor") {
		t.Fatalf("5%% of 1000G with 30G free: %v", err)
	}
	if err := c.Check("/s", config.Floor{Bytes: 20 << 30}); err != nil {
		t.Fatalf("20G floor with 30G free: %v", err)
	}
	// A real filesystem never has 100% free.
	if err := DefaultChecker.Check(t.TempDir(), config.Floor{Percent: 100}); err == nil {
		t.Fatal("100% floor accepted on a real filesystem")
	}
}

// EP-12 AC: refuse a filesystem without xattrs, naming it and the property.
func TestNoXattrRefused(t *testing.T) {
	c := Checker{
		Xattr:  func(string) error { return errors.New("operation not supported") },
		Statfs: func(string) (Space, error) { return Space{Total: 1 << 40, Free: 1 << 40, FSType: "vfat"}, nil },
	}
	err := c.Check("/mnt/usb", config.Floor{Percent: 5})
	if err == nil || !strings.Contains(err.Error(), "vfat") || !strings.Contains(err.Error(), "extended attributes") {
		t.Fatalf("got %v", err)
	}
}

func TestBucketDir(t *testing.T) {
	if d, err := BucketDir("/s", "laptop"); err != nil || d != "/s/laptop" {
		t.Fatalf("%s %v", d, err)
	}
	for _, bad := range []string{"", ".", "..", "a/b", "../x"} {
		if _, err := BucketDir("/s", bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}
