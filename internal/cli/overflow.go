package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Overflow targets (S3-1, S3-4, S3-5, S3-7, #38): `overflow add` and the
// daemon's start check of their credentials.

// overflowCheckTimeout bounds overflow add's calls to the provider.
const overflowCheckTimeout = 2 * time.Minute

func (a *app) overflowCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "overflow", Short: "Manage overflow targets: S3 buckets holding the copies friends have no room for"}
	var f overflowFlags
	add := &cobra.Command{
		Use:   "add s3 <name>",
		Short: "Add an S3 bucket to the pool for the copies no friend has room for",
		Long: `Add an S3 bucket to the pool as an overflow target. Copies go there only
when fewer than a bucket's copies friends have room for them, and never
more than --max-bytes. Each of this node's buckets is kept under
<prefix>/<sealed name>/ in the backup tool's own layout.

The credential is read from the environment, never from a flag or the
config file: <env-prefix>ACCESS_KEY and <env-prefix>SECRET_KEY, by
default LENDISK_OVERFLOW_<NAME>_ACCESS_KEY and ..._SECRET_KEY. Set them
here and in the daemon's environment (systemd EnvironmentFile= or the
container's secrets). Create a credential that can put, get (earlier
versions too), list and delete objects, but not remove object versions:
add tests it and says what it found.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return err
			}
			if args[0] != "s3" {
				return fmt.Errorf("unknown overflow target type %q: only s3 is supported", args[0])
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runOverflowAdd(cmd.Context(), args[1], f)
		},
	}
	add.Flags().StringVar(&f.endpoint, "endpoint", "", "the provider's S3 endpoint URL, such as https://s3.eu-central-003.backblazeb2.com (required)")
	add.Flags().StringVar(&f.bucket, "bucket", "", "the bucket, which must exist (required)")
	add.Flags().StringVar(&f.prefix, "prefix", "lendisk", "the key prefix this node's buckets go under")
	add.Flags().StringVar(&f.region, "region", "", "the bucket's region, if the provider needs one named (empty: ask the provider)")
	add.Flags().StringVar(&f.maxBytes, "max-bytes", "", "the most this node stores there, such as 450G: the room it adds to the pool (required)")
	add.Flags().StringVar(&f.envPrefix, "env-prefix", "", "the prefix of the credential's environment variables (default LENDISK_OVERFLOW_<NAME>_)")
	add.Flags().StringVar(&f.retention, "retention", "30d", "how long deleted copies are kept by the provider's lifecycle rule")
	add.Flags().BoolVar(&f.lifecycle, "lifecycle", false, "create the lifecycle rule expiring non-current versions after --retention, if the bucket has none")
	add.Flags().StringVar(&f.price, "price-per-gb", "", "the provider's price per GB and month, for status's cost estimate, such as 0.006")
	cmd.AddCommand(add)
	return cmd
}

type overflowFlags struct {
	endpoint, bucket, prefix, region, maxBytes, envPrefix, retention, price string
	lifecycle                                                               bool
}

func (a *app) runOverflowAdd(ctx context.Context, name string, f overflowFlags) error {
	if err := overflow.CheckName(name); err != nil {
		return usageError{err}
	}
	if u, err := url.Parse(f.endpoint); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" ||
		u.User != nil || strings.Trim(u.Path, "/") != "" {
		return usageError{fmt.Errorf("--endpoint %q: give the provider's URL, such as https://s3.example.com, without a path", f.endpoint)}
	}
	if f.bucket == "" {
		return usageError{errors.New("--bucket is required")}
	}
	if f.maxBytes == "" {
		return usageError{errors.New("--max-bytes is required: the most this node stores there, such as 450G (S3-7)")}
	}
	maxBytes, err := parseNumber(f.maxBytes)
	if err != nil {
		return usageError{fmt.Errorf("--max-bytes: %w", err)}
	}
	ret, err := parseDays(f.retention)
	if err != nil {
		return usageError{fmt.Errorf("--retention: %w", err)}
	}
	var price float64
	if f.price != "" {
		if price, err = strconv.ParseFloat(f.price, 64); err != nil || price < 0 {
			return usageError{fmt.Errorf("--price-per-gb %q: want a number such as 0.006", f.price)}
		}
	}
	envPrefix := f.envPrefix
	if envPrefix == "" {
		envPrefix = overflow.DefaultEnvPrefix(name)
	} else if err := overflow.CheckEnvPrefix(envPrefix); err != nil {
		return usageError{err}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	id, err := identity.Load(cfg.ConfigDir())
	if err != nil {
		return err
	}
	if _, err := db.GetOverflow(ctx, name); err == nil {
		return fmt.Errorf("overflow target %s exists already", name)
	} else if !errors.Is(err, state.ErrNotFound) {
		return err
	}
	cred, err := overflow.Lookup(a.getenv(), envPrefix)
	if err != nil {
		return err
	}
	o := state.Overflow{Name: name, Endpoint: strings.TrimRight(f.endpoint, "/"), Region: f.region, Bucket: f.bucket,
		Prefix: strings.Trim(f.prefix, "/"), MaxBytes: maxBytes, EnvPrefix: envPrefix, Retention: ret, PricePerGB: price,
		AddedAt: a.now()}
	c, err := replica.NewOverflow(o, cred, id.SealedName, replica.Options{})
	if err != nil {
		return usageError{err}
	}
	days := 0
	if f.lifecycle {
		days = max(int((ret+24*time.Hour-1)/(24*time.Hour)), 1)
	}
	cctx, cancel := context.WithTimeout(ctx, overflowCheckTimeout)
	defer cancel()
	check, err := c.CheckOverflow(cctx, name, days)
	if err != nil {
		return fmt.Errorf("overflow target %s not added: the credential from %s does not work there: %s",
			name, strings.TrimSuffix(envPrefix, "_")+"_*", cred.Redact(err.Error()))
	}
	o.Versioning, o.Lifecycle, o.VersionRemoval = check.Versioning, check.Lifecycle, check.VersionRemoval
	if err := db.AddOverflow(ctx, o); err != nil {
		if errors.Is(err, state.ErrExists) {
			return fmt.Errorf("overflow target %s exists already", name)
		}
		return err
	}
	an, sn := overflow.EnvNames(envPrefix)
	fmt.Fprintf(a.out, "Overflow target %s added: bucket %s at %s, keys under %s/, up to %s\n",
		name, o.Bucket, o.Endpoint, strings.TrimPrefix(o.Prefix+"/<sealed bucket name>", "/"), formatSize(o.MaxBytes))
	fmt.Fprintf(a.out, "  credential: tested; versioning %s; lifecycle rule %s; version removal %s\n",
		o.Versioning, o.Lifecycle, o.VersionRemoval)
	for _, n := range check.Notes {
		fmt.Fprintf(a.out, "  %s\n", n)
	}
	fmt.Fprintf(a.out, "Copies go there only when fewer than a bucket's copies friends have room.\n")
	fmt.Fprintf(a.out, "The daemon reads %s and %s from its environment: set them there (systemd EnvironmentFile= or the container's secrets) and restart `lendisk serve`; it refuses to start without them.\n", an, sn)
	return nil
}

// checkOverflowCredentials refuses to serve while an overflow target's
// credential is not in the environment (SEC-3): the daemon would
// otherwise place copies at a target it cannot reach.
func checkOverflowCredentials(ctx context.Context, db *state.DB, getenv overflow.Getenv) error {
	targets, err := db.Overflows(ctx)
	if err != nil {
		return err
	}
	for _, o := range targets {
		if _, err := overflow.Lookup(getenv, o.EnvPrefix); err != nil {
			return fmt.Errorf("overflow target %s: %w", o.Name, err)
		}
	}
	return nil
}
