package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/time/rate"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: minio-go-spike friend|upload [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "friend":
		fs := flag.NewFlagSet("friend", flag.ExitOnError)
		store := fs.String("store", "./data/store", "posix store root (needs xattrs)")
		addr := fs.String("addr", "127.0.0.1:7070", "TCP address to serve S3 on")
		fs.Parse(os.Args[2:])
		log.Printf("friend: versitygw on %s, store %s, %s/%s", *addr, *store, spikeAccess, spikeSecret)
		if err := runFriend(ctx, *store, *addr); err != nil {
			log.Fatal(err)
		}
	case "upload":
		os.Exit(uploadCmd(ctx, os.Args[2:]))
	default:
		fmt.Fprintln(os.Stderr, "unknown command", os.Args[1])
		os.Exit(2)
	}
}

// uploadCmd is the process the resume test kills. It prints one line per
// event on stdout; the test reads them.
func uploadCmd(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	var t Target
	var j Job
	fs.StringVar(&t.Endpoint, "endpoint", "127.0.0.1:7070", "host:port")
	fs.StringVar(&t.Region, "region", "us-east-1", "")
	fs.StringVar(&t.Access, "access", spikeAccess, "")
	fs.StringVar(&t.Secret, "secret", spikeSecret, "")
	fs.StringVar(&t.Bucket, "bucket", "spike", "")
	fs.StringVar(&t.Checksum, "checksum", "", `"" or "sha256"`)
	fs.BoolVar(&t.UnsignedPayload, "unsigned", false, "DisableContentSha256")
	fs.StringVar(&j.Source, "source", "", "file to upload")
	fs.StringVar(&j.Key, "key", "", "object key")
	fs.StringVar(&j.StatePath, "state", "", "resume state file")
	fs.StringVar(&j.SHA256Hex, "sha256", "", "object SHA-256 (from the queue row)")
	fs.Int64Var(&j.PartSize, "part-size", 5<<20, "")
	bps := fs.Float64("rate", 0, "bytes/s, 0 = unlimited")
	win := fs.String("window", "", "HH:MM[:SS]-HH:MM[:SS]")
	fs.Parse(args)

	w, err := ParseWindow(*win)
	if err != nil {
		fmt.Println("error", err)
		return 2
	}
	l := &Limits{}
	if *bps > 0 {
		l.Peer = rate.NewLimiter(rate.Limit(*bps), 64<<10)
	}
	core, err := newCore(t)
	if err != nil {
		fmt.Println("error", err)
		return 1
	}
	info, err := Upload(ctx, core, t, j, l, w, Events{
		Resumed: func(id string, have []int) { fmt.Printf("resumed %s have=%v\n", id, have) },
		PartStart: func(n int, off, size int64) {
			fmt.Printf("part-start %d off=%d size=%d\n", n, off, size)
		},
		PartDone: func(n int, etag string) {
			fmt.Printf("part-done %d etag=%s source-bytes=%d\n", n, etag, l.Bytes.Load())
		},
	})
	switch {
	case errors.Is(err, ErrWindowClosed):
		fmt.Printf("window-closed source-bytes=%d\n", l.Bytes.Load())
		return 3
	case err != nil:
		fmt.Println("error", err)
		return 1
	}
	fmt.Printf("complete etag=%s source-bytes=%d\n", info.ETag, l.Bytes.Load())
	return 0
}
