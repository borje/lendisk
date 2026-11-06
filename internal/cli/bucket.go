package cli

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

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
	add := &cobra.Command{
		Use:   "add <name>",
		Short: "Create a bucket and print the lines to configure a backup tool",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runBucketAdd(cmd.Context(), args[0], profile, copies)
		},
	}
	add.Flags().StringVar(&profile, "profile", "", "backup tool: "+strings.Join(profiles, ", ")+" (required)")
	add.Flags().IntVar(&copies, "copies", 2, "how many friends hold each object")
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
	cmd.AddCommand(add, list, show)
	return cmd
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

func (a *app) runBucketAdd(ctx context.Context, name, profile string, copies int) error {
	if err := validBucketName(name); err != nil {
		return usageError{err}
	}
	if !contains(profiles, profile) {
		return usageError{fmt.Errorf("--profile must be one of %s", strings.Join(profiles, ", "))}
	}
	if copies < 1 {
		return usageError{errors.New("--copies must be at least 1")}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if cfg.Role == "lender" {
		return errors.New("a lender-only node has no buckets of its own; set role = \"full\" first")
	}
	b := state.Bucket{
		Name: name, Profile: profile, Copies: copies,
		AccessKey: newAccessKey(), SecretKey: newSecretKey(), CreatedAt: time.Now(),
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
	fmt.Fprintf(a.out, "Created bucket %s (profile %s, copies %d).\n\n", name, profile, copies)
	printToolLines(a.out, cfg, b)
	return nil
}

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
		fmt.Fprintf(w, "For Kopia %s:\n\n", where)
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
	}
}

// newAccessKey returns an AWS-shaped access key ID: LDK and 17 characters.
func newAccessKey() string {
	b := make([]byte, 11)
	rand.Read(b)
	return "LDK" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)[:17]
}

// newSecretKey returns 240 random bits as 40 URL-safe characters.
func newSecretKey() string {
	b := make([]byte, 30)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
