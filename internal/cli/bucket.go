package cli

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// Profiles a bucket can have (EP-6, EP-7).
var profiles = []string{"restic", "kopia", "generic"}

// ReservedBucket is the lender-side manifests bucket (ID-4).
const ReservedBucket = "lendisk-manifests"

var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)

// validBucketName applies S3's naming rules without dots (path-style only,
// and dots invite TLS and IP-address confusion) and lendisk's reservations
// (§3: lent-* holds friends' buckets, lendisk-manifests their manifests).
func validBucketName(name string) error {
	switch {
	case !bucketNameRE.MatchString(name) || strings.Contains(name, "--"):
		return fmt.Errorf("bucket name %q: use 3 to 63 lowercase letters, digits and single hyphens, starting and ending with a letter or digit", name)
	case strings.HasPrefix(name, "lent-"):
		return fmt.Errorf("bucket name %q: names beginning with lent- are reserved for space lent to friends", name)
	case name == ReservedBucket:
		return fmt.Errorf("bucket name %q is reserved", name)
	}
	return nil
}

func (a *app) bucketCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "bucket", Short: "Manage this node's buckets"}
	var profile string
	var copies int
	var noEncryption bool
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Create a bucket and print the lines to configure a backup tool",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runBucketAdd(cmd.Context(), args[0], profile, copies, cmd.Flags().Changed("copies"), noEncryption)
		},
	}
	add.Flags().StringVar(&profile, "profile", "", "backup tool: "+strings.Join(profiles, ", ")+" (required)")
	add.Flags().IntVar(&copies, "copies", 0, "how many friends hold each object (default replication.copies, 2)")
	add.Flags().BoolVar(&noEncryption, "i-understand-no-encryption", false,
		"required for --profile generic: its tools do not encrypt, so friends can read what they hold (EP-8)")
	list := &cobra.Command{
		Use:   "list",
		Short: "List buckets",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, db, err := a.openState(cmd.Context())
			if err != nil {
				return err
			}
			defer db.Close()
			bs, err := db.Buckets(cmd.Context())
			if err != nil {
				return err
			}
			for _, b := range bs {
				fmt.Fprintf(a.out, "%s\t%s\tcopies=%d\n", b.Name, b.Profile, b.Copies)
			}
			return nil
		},
	}
	show := &cobra.Command{
		Use:   "show <name>",
		Short: "Print a bucket's configuration lines again, credential included",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, db, err := a.openState(cmd.Context())
			if err != nil {
				return err
			}
			defer db.Close()
			b, err := db.GetBucket(cmd.Context(), args[0])
			if errors.Is(err, state.ErrNotFound) {
				return fmt.Errorf("no bucket %q; `lendisk bucket list` shows them", args[0])
			} else if err != nil {
				return err
			}
			printToolLines(a.out, cfg, b)
			return nil
		},
	}
	resolve := &cobra.Command{
		Use:   "resolve <bucket> <key> <friend>",
		Short: "After recover, use a friend's version of an object the friends disagree about",
		Long: `After recover (ID-5), an object the friends list in different versions,
as many friends for one as for another, is in conflict: no version is
used until you pick one. ` + "`lendisk status`" + ` lists each with what every
friend holds. resolve uses the version the named friend (its node ID)
holds, and counts every friend holding that version as holding it.`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, db, err := a.openState(cmd.Context())
			if err != nil {
				return err
			}
			defer db.Close()
			db.Clock = a.clock
			err = db.ResolveKey(cmd.Context(), args[0], args[1], args[2])
			switch {
			case errors.Is(err, state.ErrNotConflict):
				return fmt.Errorf("%s in %s is not a conflict; nothing to resolve", args[1], args[0])
			case errors.Is(err, state.ErrNotFound):
				return fmt.Errorf("friend %s does not list %s in bucket %s, or recovery found no such object; `lendisk status` lists the conflicts",
					args[2], args[1], args[0])
			case err != nil:
				return err
			}
			fmt.Fprintf(a.out, "%s in %s: using the version friend %s holds.\n", args[1], args[0], args[2])
			return nil
		},
	}
	var setCopies int
	set := &cobra.Command{
		Use:   "set <name> --copies N",
		Short: "Change a bucket's copies; the daemon adds or removes copies to match",
		Long: `set changes how many friends hold each object of the bucket (EP-6).
The running daemon then places the missing copies, read from this node's
store, or deletes the surplus ones, never leaving an object at fewer
places than it has (REPL-13); ` + "`lendisk status`" + ` shows the re-placement.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("copies") {
				return usageError{errors.New("--copies is required")}
			}
			return a.runBucketSet(cmd.Context(), args[0], setCopies)
		},
	}
	set.Flags().IntVar(&setCopies, "copies", 0, "how many friends hold each object (required)")
	var allow int64
	reconcile := &cobra.Command{
		Use:   "reconcile <name> --allow-deletes N",
		Short: "Allow the deletes the store reconciler held back for a bucket",
		Long: `The daemon compares each bucket's directory in the store with what it
captured (EP-14), and enqueues objects removed outside the S3 endpoint as
deletes, which every friend then applies (keeping each as a tombstone
for its retention window). When more than store.reconcile_delete_limit
percent of a bucket's objects are missing at once, as when a directory
was emptied or a disk is not mounted, it deletes none of them and holds
them back. ` + "`lendisk status`" + ` shows how many. If you removed them,
allow their deletes with --allow-deletes and that count (or more): the
daemon's next pass, within a minute, enqueues them. Otherwise put the
objects back, and the hold ends.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("allow-deletes") {
				return usageError{errors.New("--allow-deletes is required: the count status shows")}
			}
			if allow < 1 {
				return usageError{errors.New("--allow-deletes must be at least 1")}
			}
			return a.runBucketReconcile(cmd.Context(), args[0], allow)
		},
	}
	reconcile.Flags().Int64Var(&allow, "allow-deletes", 0, "let the next pass delete up to this many missing objects (required)")
	cmd.AddCommand(add, list, show, resolve, set, reconcile)
	return cmd
}

// runBucketSet changes bucket name's copies (EP-6, REPL-13, #26). The
// daemon notices the change and re-places; nothing here needs it.
func (a *app) runBucketSet(ctx context.Context, name string, copies int) error {
	if copies < 1 {
		return usageError{errors.New("--copies must be at least 1")}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.SetCopies(ctx, name, copies); errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("no bucket %q; `lendisk bucket list` shows them", name)
	} else if err != nil {
		return err
	}
	fmt.Fprintf(a.out, "Bucket %s now keeps %d copies of each object; the daemon adds or removes copies to match (`lendisk status` shows it).\n",
		name, copies)
	if copies == 1 {
		fmt.Fprintln(a.out, singleCopyWarning)
	}
	db.Clock, db.RerouteAfter, db.ReplaceAfter = a.clock, cfg.RerouteAfter(), cfg.ReplaceAfter()
	if places, err := db.PoolSize(ctx); err != nil {
		return err
	} else if places < copies {
		fmt.Fprintf(a.out, "Your pool has %d places, fewer than copies %d: objects are held at fewer places until more friends join (`lendisk invite --quota <size>`).\n",
			places, copies)
	}
	return nil
}

// runBucketReconcile allows bucket's held deletes (EP-14, #8); the
// running daemon's reconciler enqueues them.
func (a *app) runBucketReconcile(ctx context.Context, name string, n int64) error {
	_, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.GetBucket(ctx, name); errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("no bucket %q; `lendisk bucket list` shows them", name)
	} else if err != nil {
		return err
	}
	held, err := db.AllowDeletes(ctx, name, n)
	switch {
	case errors.Is(err, state.ErrNothingHeld):
		return fmt.Errorf("no deletes of bucket %s are held back; nothing to allow", name)
	case errors.Is(err, state.ErrAllowTooFew):
		return fmt.Errorf("%d deletes are held back for bucket %s, more than %d; allow at least %d (`lendisk status` shows them)",
			held, name, n, held)
	case err != nil:
		return err
	}
	fmt.Fprintf(a.out, "The daemon's next reconcile pass of %s, within a minute, enqueues up to %d deletes of objects missing from the store; each friend keeps what it deletes as a tombstone for its retention window. If more are missing by then, it holds them back again.\n",
		name, n)
	return nil
}

// openState loads the config, checks the node is initialised and opens
// lendisk.db. One-shot commands log to stderr.
func (a *app) openState(ctx context.Context) (*config.Loaded, *state.DB, error) {
	cfg, log, err := a.load(a.errOut)
	if err != nil {
		return nil, nil, err
	}
	if _, err := identity.Load(cfg.ConfigDir()); err != nil {
		return nil, nil, err
	}
	db, err := state.Open(ctx, cfg.DataDir, log)
	if err != nil {
		return nil, nil, err
	}
	return cfg, db, nil
}

func (a *app) runBucketAdd(ctx context.Context, name, profile string, copies int, explicit, noEncryption bool) error {
	if err := validBucketName(name); err != nil {
		return usageError{err}
	}
	if !contains(profiles, profile) {
		return usageError{fmt.Errorf("--profile must be one of %s", strings.Join(profiles, ", "))}
	}
	if explicit && copies < 1 {
		return usageError{errors.New("--copies must be at least 1")}
	}
	if profile == "generic" && !noEncryption {
		return usageError{errors.New("a generic bucket is not encrypted by its tool: every friend that holds a copy can read it. " +
			"Use restic or Kopia, or add --i-understand-no-encryption")}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if !explicit {
		copies = cfg.Replication.Copies.Int()
	}
	if cfg.Role == "lender" {
		return errors.New("a lender-only node has no buckets of its own; set role = \"full\" first")
	}
	access, err := newAccessKey(a.rng())
	if err != nil {
		return err
	}
	secret, err := newSecretKey(a.rng())
	if err != nil {
		return err
	}
	b := state.Bucket{
		Name: name, Profile: profile, Copies: copies,
		AccessKey: access, SecretKey: secret, CreatedAt: a.now(),
	}
	if err := db.AddBucket(ctx, b); errors.Is(err, state.ErrExists) {
		return fmt.Errorf("bucket %q already exists; `lendisk bucket show %s` prints its lines", name, name)
	} else if err != nil {
		return err
	}
	be, err := gateway.OpenBackend(cfg.StorePath())
	if err == nil {
		err = gateway.CreateBucket(ctx, be, name, b.AccessKey)
	}
	if err != nil {
		db.DeleteBucket(ctx, name)
		return err
	}
	fmt.Fprintf(a.out, "Created bucket %s (profile %s, copies %d).\n", name, profile, copies)
	if profile == "generic" {
		fmt.Fprintln(a.out, noEncryptionWarning)
	}
	if copies == 1 {
		fmt.Fprintln(a.out, singleCopyWarning)
	}
	db.Clock, db.RerouteAfter, db.ReplaceAfter = a.clock, cfg.RerouteAfter(), cfg.ReplaceAfter()
	if places, err := db.PoolSize(ctx); err != nil {
		return err
	} else if places < copies {
		fmt.Fprintf(a.out, "Your pool has %d places, fewer than copies %d: objects are held at fewer places until more friends join (`lendisk invite --quota <size>`).\n",
			places, copies)
	}
	fmt.Fprintln(a.out)
	printToolLines(a.out, cfg, b)
	return nil
}

// What bucket add and status say of a bucket at copies 1, however
// copies was set, and status of a pool of one friend (EP-6). Neither is
// an alert.
const (
	singleCopyWarning = "Warning: copies = 1. Each object is held by one friend only, so if that friend's disk is lost, this bucket has no offsite copy left. Add a second friend, or run `lendisk bucket set <name> --copies 2`."
	singleCopyLine    = "! one copy: losing that friend's disk loses the offsite copy; 2 is recommended"
	singleFriendLine  = "Friends (1: one lost disk loses every offsite copy)"
)

// What bucket add and status say of a generic bucket (EP-8, P-3).
const (
	noEncryptionWarning = "Warning: this bucket is not encrypted by its tool. Every friend that holds a copy can read it: it is outside lendisk's trust guarantee."
	outsideTrustLine    = "! not encrypted: outside the trust guarantee; friends can read its objects"
)

// printToolLines prints the lines for loopback and, for each
// [[endpoint.listen]] address, for another machine (EP-17).
func printToolLines(w io.Writer, cfg *config.Loaded, b state.Bucket) {
	toolLines(w, b, "on this machine", "http://"+cfg.Endpoint.Loopback, "")
	for _, e := range cfg.Endpoint.Listen {
		addrs := bridge.Resolve([]config.Listen{e}, nil)
		if len(addrs) == 0 {
			fmt.Fprintf(w, "\n%s has no address now; `lendisk bucket show %s` prints its lines once it has one.\n", e, b.Name)
		}
		for _, a := range addrs {
			fmt.Fprintln(w)
			cacert := ""
			switch {
			case a.Plaintext():
				fmt.Fprintf(w, "Warning: %s is plain HTTP. Request metadata and object contents cross the network\n"+
					"unencrypted; only restic and Kopia buckets are protected by the tool's own encryption.\n\n", a.URL())
			case e.TLS == "generate":
				cacert = "endpoint.crt"
				fmt.Fprintf(w, "Copy %s to the other machine as endpoint.crt;\n"+
					"`lendisk endpoint cert` prints its SHA-256 fingerprint to check the copy.\n\n",
					filepath.Join(cfg.ConfigDir(), bridge.CertFile))
			}
			toolLines(w, b, "on another machine, at "+a.URL(), a.URL(), cacert)
		}
	}
	fmt.Fprintf(w, "\nKeep the secret key private. `lendisk bucket show %s` prints these lines again.\n", b.Name)
}

// toolLines prints one endpoint's lines; cacert, if set, is the file the
// tool must trust as the certificate authority.
func toolLines(w io.Writer, b state.Bucket, where, url, cacert string) {
	hostPort := strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://")
	switch b.Profile {
	case "restic":
		fmt.Fprintf(w, "For restic %s:\n\n", where)
		fmt.Fprintf(w, "  export AWS_ACCESS_KEY_ID=%s\n", b.AccessKey)
		fmt.Fprintf(w, "  export AWS_SECRET_ACCESS_KEY=%s\n", b.SecretKey)
		fmt.Fprintf(w, "  export RESTIC_REPOSITORY=s3:%s/%s\n", url, b.Name)
		if cacert != "" {
			fmt.Fprintf(w, "  restic --cacert %s init\n", cacert)
		} else {
			fmt.Fprintf(w, "  restic init\n")
		}
	case "kopia":
		tlsFlag := " --disable-tls"
		if strings.HasPrefix(url, "https://") {
			tlsFlag = ""
			if cacert != "" {
				tlsFlag = " --root-ca-pem-path=" + cacert
			}
		}
		fmt.Fprintf(w, "For Kopia 0.9 or later (repository format 2 or later) %s:\n\n", where)
		fmt.Fprintf(w, "  kopia repository create s3 --endpoint=%s%s --bucket=%s \\\n", hostPort, tlsFlag, b.Name)
		fmt.Fprintf(w, "    --access-key=%s --secret-access-key=%s\n", b.AccessKey, b.SecretKey)
	default:
		fmt.Fprintf(w, "For any S3 client %s (path-style addressing):\n\n", where)
		fmt.Fprintf(w, "  endpoint:   %s\n", url)
		if cacert != "" {
			fmt.Fprintf(w, "  CA cert:    %s\n", cacert)
		}
		fmt.Fprintf(w, "  region:     us-east-1\n")
		fmt.Fprintf(w, "  bucket:     %s\n", b.Name)
		fmt.Fprintf(w, "  access key: %s\n", b.AccessKey)
		fmt.Fprintf(w, "  secret key: %s\n", b.SecretKey)
		fmt.Fprintf(w, "\nThe key cannot create buckets; for rclone set no_check_bucket = true.\n")
		fmt.Fprintf(w, "For rclone also pass --disable Copy,Move --no-update-modtime: lendisk refuses server-side copies,\n"+
			"so rclone must re-upload instead of copying or moving and leave stored modtimes alone.\n"+
			"rclone's --backup-dir and --suffix need a server-side copy and cannot be used here.\n")
	}
}

// newAccessKey returns an AWS-shaped access key ID: LDK and 17 characters.
func newAccessKey(r io.Reader) (string, error) {
	b := make([]byte, 11)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("make access key: %w", err)
	}
	return "LDK" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)[:17], nil
}

// newSecretKey returns 240 random bits as 40 URL-safe characters.
func newSecretKey(r io.Reader) (string, error) {
	b := make([]byte, 30)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("make secret key: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
