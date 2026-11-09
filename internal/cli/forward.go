package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// dialLock is the lock a process holds while it dials friends with this
// node's dial keys: `serve` for its whole run, `peer forward` while it
// forwards. Two processes dialling one friend with one key displace each
// other at the relay and both stall (#69, DESIGN §3.7).
const dialLock = "dial.lock"

// takeDialLock takes dialLock, or says which process holds it.
func takeDialLock(confDir, holder string) (func(), error) {
	release, err := tryLock(confDir, dialLock)
	if errors.Is(err, errLocked) {
		return nil, fmt.Errorf("another lendisk process dials your friends with this node's keys (`lendisk serve` "+
			"or `lendisk peer forward`); two at once stall each other at the relay, so stop it before %s", holder)
	}
	return release, err
}

func (a *app) peerForwardCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "forward <friend> <loopback address>",
		Short: "Expose a friend's replica of your buckets on a local address",
		Long: `peer forward opens a listener on the loopback address given, such as
127.0.0.1:9100, for as long as it runs, and copies each connection into
the tunnel to the friend, named by its node ID (NET-7b). A backup tool
or any S3 client on this machine then reads your buckets at that friend
directly, with no lendisk replication code in the path.

It prints the credential for that friend (computed now, never written
anywhere) as lines to paste into the shell, and for each of your
buckets the friend holds, its name there and the commands that copy its
keys into a local directory and read it with restic. Copied from enough
friends into one directory, the keys are the repository.

The friend's daemon applies every rule it applies to your own daemon: a
write must carry its SHA-256, so a backup tool cannot write or prune
through the forward; a delete is a tombstone it keeps for the retention
window; and anything outside the operations replication uses is refused.

Stop ` + "`lendisk serve`" + ` while it runs: both dial your friends with the
same keys, and two at once stall each other.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPeerForward(cmd, args[0], args[1])
		},
	}
}

func (a *app) runPeerForward(cmd *cobra.Command, friend, listen string) error {
	peer, err := identity.ParseNodeID(strings.TrimSpace(friend))
	if err != nil {
		return usageError{fmt.Errorf("%q is not a node ID: %w; `lendisk peer list` shows your friends", friend, err)}
	}
	if err := loopbackOnly(listen); err != nil {
		return usageError{err}
	}
	ctx := a.ctx
	if ctx == nil {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
	}
	cfg, log, db, id, err := a.openNode(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	f, err := db.GetFriendship(ctx, peer.String())
	if errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("%s is not a friend of this node; `lendisk peer list` shows your friends", peer)
	} else if err != nil {
		return err
	}
	held, err := heldBuckets(ctx, db, peer.String())
	if err != nil {
		return err
	}
	release, err := takeDialLock(cfg.ConfigDir(), "forwarding")
	if err != nil {
		return err
	}
	defer release()
	// NET-7a: computed now, printed, never written.
	secret, err := id.S3Secret(peer)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w; give a free loopback address such as 127.0.0.1:9100", listen, err)
	}
	defer ln.Close()
	dl, err := transport.NewFriendDialer(id, log)
	if err != nil {
		return err
	}
	defer dl.Close()

	printForward(a.out, ln.Addr().String(), peer.String(), id, secret, held)
	if a.serving != nil {
		a.serving(ln.Addr().String())
	}
	fw := &forward{ln: ln, log: log, dialTimeout: a.redeemTimeout(),
		dial: func(ctx context.Context) (net.Conn, error) { return dl.Dial(ctx, peer, f.Address) }}
	fw.run(ctx)
	return nil
}

// loopbackOnly refuses an address that is not a loopback IP and port:
// the forward is a listener SEC-8 admits on loopback only.
func loopbackOnly(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q is not an address: %w; give a loopback address such as 127.0.0.1:9100", addr, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("%q is not a loopback address: the forward serves this machine only; give one such as 127.0.0.1:9100", addr)
	}
	return nil
}

// heldBucket is one of the node's buckets a friend holds.
type heldBucket struct {
	state.Bucket
	Objects int64
}

// heldBuckets is the node's buckets peer holds an acknowledged object
// of, from the node's own map (placement).
func heldBuckets(ctx context.Context, db *state.DB, peer string) ([]heldBucket, error) {
	bs, err := db.Buckets(ctx)
	if err != nil {
		return nil, err
	}
	var out []heldBucket
	for _, b := range bs {
		at, err := db.HeldAt(ctx, b.Name)
		if err != nil {
			return nil, err
		}
		if n := at[peer]; n > 0 {
			out = append(out, heldBucket{Bucket: b, Objects: n})
		}
	}
	return out, nil
}

// printForward prints NET-7b's lines: the credential to paste, then per
// bucket held at the friend its sealed name and the commands that copy
// its keys into a local directory named for the bucket, and read it.
func printForward(w io.Writer, addr, peer string, self *identity.Identity, secret string, held []heldBucket) {
	url := "http://" + addr
	fmt.Fprintf(w, "Forwarding %s to %s through the tunnel until you stop this command (Ctrl-C).\n\n", addr, peer)
	fmt.Fprintf(w, "The credential for this friend, computed now and written nowhere; paste it into the shell:\n\n")
	fmt.Fprintf(w, "  export AWS_ACCESS_KEY_ID=%s\n", self.NodeID())
	fmt.Fprintf(w, "  export AWS_SECRET_ACCESS_KEY=%s\n\n", secret)
	if len(held) == 0 {
		fmt.Fprintf(w, "This friend holds none of your buckets yet.\n")
		return
	}
	fmt.Fprintf(w, "Your buckets this friend holds, by the name each has there, with the commands\n"+
		"that copy its keys into a local directory and read the friend's copy in place:\n")
	for _, b := range held {
		sealed := self.SealedName(b.Name)
		fmt.Fprintf(w, "\n  %s  %s  (%d objects)\n", b.Name, sealed, b.Objects)
		fmt.Fprintf(w, "    rclone copy \":s3,provider=Other,env_auth=true,endpoint='%s',list_version=2:%s\" %s\n", url, sealed, b.Name)
		if b.Profile == "restic" {
			fmt.Fprintf(w, "    restic --no-lock -o s3.region=us-east-1 -r s3:%s/%s snapshots\n", url, sealed)
		}
	}
	fmt.Fprintf(w, "\nRun the rclone line at each friend you forward to, into the same directory;\n"+
		"with enough friends (the recovery sheet says how many) the directory is the\n"+
		"repository, such as `restic -r %s snapshots`. Both lines only read. The\n"+
		"friend refuses a backup tool's writes through the forward, which carry no\n"+
		"SHA-256, and keeps anything deleted through it as a tombstone.\n", held[0].Name)
}

// forward copies each connection accepted on ln into a new connection
// to the friend (NET-7b). It adds nothing: the friend's gate and
// lending store see the tool's requests as the tool sent them.
type forward struct {
	ln          net.Listener
	dial        func(context.Context) (net.Conn, error)
	dialTimeout time.Duration
	log         *slog.Logger
}

// run serves until ctx ends, then closes the listener and every open
// connection.
func (f *forward) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	go func() {
		<-ctx.Done()
		f.ln.Close()
	}()
	for {
		c, err := f.ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				f.log.Error("forward stopped accepting", "err", err)
			}
			return
		}
		wg.Go(func() { f.serve(ctx, c) })
	}
}

// serve dials the friend for one accepted connection and splices the two.
func (f *forward) serve(ctx context.Context, c net.Conn) {
	dctx, cancel := context.WithTimeout(ctx, f.dialTimeout)
	t, err := f.dial(dctx)
	cancel()
	if err != nil {
		c.Close()
		f.log.Warn("could not reach the friend; is its daemon running?", "err", err)
		return
	}
	stop := context.AfterFunc(ctx, func() { c.Close(); t.Close() })
	defer stop()
	splice(c, t)
}

// splice copies a and b into each other until both directions end, then
// closes both. A direction that ends half-closes the other side where it
// can; where it cannot, it closes both, so neither copy waits forever.
func splice(a, b net.Conn) {
	var wg sync.WaitGroup
	half := func(dst, src net.Conn) {
		io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); !ok || cw.CloseWrite() != nil {
			a.Close()
			b.Close()
		}
	}
	wg.Go(func() { half(b, a) })
	wg.Go(func() { half(a, b) })
	wg.Wait()
	a.Close()
	b.Close()
}
