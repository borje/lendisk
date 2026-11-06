package gateway

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// EP-2 AC: capture's wall-clock overhead on a restic backup, against the
// same store with capture off, must be under 5%. It is a measurement, so
// it runs only when asked:
//
//	LENDISK_OVERHEAD_MB=2048 LENDISK_OVERHEAD_DIR=/var/tmp/x \
//	  go test -run TestCaptureOverhead -v ./internal/gateway
//
// LENDISK_OVERHEAD_DIR should be on the disk under test (t.TempDir() is
// often tmpfs). LENDISK_OVERHEAD_RUNS sets the runs per mode (default 3),
// alternated after an untimed warm-up run. The source is random, so
// every byte is new to restic and reaches the store.
func TestCaptureOverhead(t *testing.T) {
	mb, _ := strconv.Atoi(os.Getenv("LENDISK_OVERHEAD_MB"))
	if mb <= 0 {
		t.Skip("set LENDISK_OVERHEAD_MB to measure capture overhead (EP-2)")
	}
	restic, err := exec.LookPath("restic")
	if err != nil {
		t.Skip("restic not installed")
	}
	runs := 3
	if n, err := strconv.Atoi(os.Getenv("LENDISK_OVERHEAD_RUNS")); err == nil && n > 0 {
		runs = n
	}
	base := os.Getenv("LENDISK_OVERHEAD_DIR")
	if base == "" {
		base = t.TempDir()
	} else {
		base, err = os.MkdirTemp(base, "overhead-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(base) })
	}

	// The source and one repository at a time, with room to spare.
	var st unix.Statfs_t
	if err := unix.Statfs(base, &st); err == nil && st.Bavail*uint64(st.Bsize) < uint64(mb)<<20*5/2 {
		t.Fatalf("%s has %d MiB free; measuring %d MiB needs %d", base, st.Bavail*uint64(st.Bsize)>>20, mb, mb*5/2)
	}
	src := filepath.Join(base, "src")
	os.MkdirAll(src, 0o700)
	rng := rand.New(rand.NewPCG(1, 2))
	buf := make([]byte, 1<<20)
	for i := 0; i < mb; i++ {
		for j := 0; j < len(buf); j += 8 {
			v := rng.Uint64()
			for k := 0; k < 8; k++ {
				buf[j+k] = byte(v >> (8 * k))
			}
		}
		// 4 MiB files, as a mix of file sizes would not change what the
		// store sees: 16 MiB packs.
		f := filepath.Join(src, fmt.Sprintf("f%04d", i/4))
		fh, err := os.OpenFile(f, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		fh.Write(buf)
		fh.Close()
	}

	backup := func(capture bool) (time.Duration, int) {
		t.Helper()
		dir := filepath.Join(base, "run")
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0o700)
		defer os.RemoveAll(dir)
		h := newHarnessIn(t, dir, map[string]string{"laptop": "restic"})
		stop := h.serve(!capture)
		defer stop()
		addr := forward(t, h.socket)
		k := h.keys["laptop"]
		env := append(os.Environ(), "AWS_ACCESS_KEY_ID="+k[0], "AWS_SECRET_ACCESS_KEY="+k[1],
			"RESTIC_REPOSITORY=s3:http://"+addr+"/laptop", "RESTIC_PASSWORD=pw", "RESTIC_CACHE_DIR="+filepath.Join(dir, "cache"))
		run := func(args ...string) {
			cmd := exec.Command(restic, args...)
			cmd.Env = env
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("restic %v: %v\n%s", args, err, out)
			}
		}
		run("init")
		start := time.Now()
		run("backup", "--no-scan", src)
		took := time.Since(start)
		es, _ := h.db.QueueEntries(context.Background(), "laptop")
		return took, len(es)
	}

	backup(true) // warm-up: page cache, restic's first start
	var on, off []time.Duration
	for i := 0; i < runs; i++ {
		for _, capture := range []bool{i%2 == 0, i%2 != 0} {
			took, rows := backup(capture)
			t.Logf("run %d capture=%v: %v (%.0f MiB/s), %d queue rows", i, capture, took.Round(time.Millisecond),
				float64(mb)/took.Seconds(), rows)
			if capture {
				on = append(on, took)
			} else {
				off = append(off, took)
			}
		}
	}
	mOn, mOff := median(on), median(off)
	overhead := (mOn.Seconds() - mOff.Seconds()) / mOff.Seconds() * 100
	t.Logf("restic backup of %d MiB, median of %d: capture on %v, off %v, overhead %+.1f%%",
		mb, runs, mOn.Round(time.Millisecond), mOff.Round(time.Millisecond), overhead)
	if overhead >= 5 {
		t.Errorf("capture overhead %.1f%% is not under 5%% (EP-2)", overhead)
	}
}

func median(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[len(s)/2]
}

// forward is a loopback TCP port relaying bytes to the gateway socket,
// the bridge's job in the daemon.
func forward(t *testing.T, sock string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				u, err := net.Dial("unix", sock)
				if err != nil {
					return
				}
				defer u.Close()
				go func() { io.Copy(u, c); u.(*net.UnixConn).CloseWrite() }()
				io.Copy(c, u)
			}()
		}
	}()
	return ln.Addr().String()
}
