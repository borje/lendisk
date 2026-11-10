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

	"github.com/borje/lendisk/internal/capacity"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/overflow"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Overflow targets (S3-1, S3-4, S3-5, S3-7, VER-1a, #38, #96, #105): `overflow add`,
// `overflow set` and `overflow remove`, and the daemon's start check of
// their credentials.

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
	add.Flags().StringVar(&f.retention, "retention", "30d", "how long deleted copies are kept: at a target with versioning, by the lifecycle rule (see --lifecycle); at one without, lendisk holds each delete this long before it sends it (S3-4)")
	add.Flags().BoolVar(&f.lifecycle, "lifecycle", false, "create the lifecycle rule expiring non-current versions after --retention, if the bucket has none")
	add.Flags().StringVar(&f.price, "price-per-gb", "", "the provider's price per GB and month, for status's cost estimate, such as 0.006")
	cmd.AddCommand(add)

	var sf overflowFlags
	set := &cobra.Command{
		Use:   "set <name>",
		Short: "Change an overflow target's --max-bytes, its --full-cycle, or the endpoint its bucket is reached at",
		Long: `Change an overflow target (#96). --max-bytes is the most this node
stores there: raised, more copies may go there; lowered below what it
holds, no new copy goes there, and what it holds moves back to friends
as they have room (drain-back).

--endpoint (and --region) is where the same bucket is reached: a
provider's new URL. The credential is tested there first, as by add,
and nothing changes if it does not work. To move to another bucket or
provider, add a new target and remove this one.

--full-cycle gives the target a rolling full check (VER-1a): every
object this node holds there is downloaded and hashed once in that many
days, a slice a day, within the download limits. Off by default, as
each download costs the provider's egress fee; 0 or off turns it off
again. The target is still checked daily by the provider's stored
checksums and weekly by a downloaded sample (VER-2).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sf.fullCycleSet = cmd.Flags().Changed("full-cycle")
			return a.runOverflowSet(cmd.Context(), args[0], cmd.Flags().Changed("region"), sf)
		},
	}
	set.Flags().StringVar(&sf.maxBytes, "max-bytes", "", "the most this node stores there, such as 450G")
	set.Flags().StringVar(&sf.endpoint, "endpoint", "", "the provider's S3 endpoint URL the bucket is reached at now")
	set.Flags().StringVar(&sf.region, "region", "", "the bucket's region at the new endpoint (empty: ask the provider)")
	set.Flags().StringVar(&sf.fullCycle, "full-cycle", "", "download every object held there once in this many days, such as 90 or 90d (VER-1a); 0 or off for none, the default")
	cmd.AddCommand(set)

	var keep bool
	remove := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove an overflow target once its copies are held elsewhere",
		Long: `Remove an overflow target (#96). No new copy goes there from now on.
The daemon then copies each object it holds to a friend or another
target, so every bucket keeps its copies throughout, and only then
deletes its copies there, the retention-preserving way: a delete
marker at a target with versioning (the lifecycle rule expires it), a
delete held for --retention at one without. The target is dropped once
nothing is left there; ` + "`lendisk status`" + ` shows it until then.
Keep ` + "`lendisk serve`" + ` running, with the target's credential in its
environment.

Copies that find no room elsewhere stay at the target until a friend
or target is added, or a quota raised.

With --keep-data the copies are left at the target and nothing more is
deleted there (deletes it holds for the retention window are dropped
unsent): the target is dropped as soon as its data is held elsewhere.
Given again with --keep-data, a removal under way stops deleting.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runOverflowRemove(cmd.Context(), args[0], keep)
		},
	}
	remove.Flags().BoolVar(&keep, "keep-data", false, "leave the copies at the target, deleting nothing there")
	cmd.AddCommand(remove)
	return cmd
}

// checkEndpoint refuses an endpoint that is not a provider's URL.
func checkEndpoint(ep string) error {
	if u, err := url.Parse(ep); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" ||
		u.User != nil || strings.Trim(u.Path, "/") != "" {
		return usageError{fmt.Errorf("--endpoint %q: give the provider's URL, such as https://s3.example.com, without a path", ep)}
	}
	return nil
}

// noTarget is the error for a target name that is not there.
func noTarget(name string) error {
	return fmt.Errorf("no overflow target %s: `lendisk status` lists them", name)
}

func (a *app) runOverflowSet(ctx context.Context, name string, regionSet bool, f overflowFlags) error {
	if f.maxBytes == "" && f.endpoint == "" && !regionSet && !f.fullCycleSet {
		return usageError{errors.New("nothing to change: give --max-bytes, --full-cycle, --endpoint or --region")}
	}
	var cycle time.Duration
	if f.fullCycleSet {
		var err error
		if cycle, err = parseFullCycle(f.fullCycle); err != nil {
			return usageError{fmt.Errorf("--full-cycle: %w", err)}
		}
	}
	if regionSet && f.endpoint == "" {
		return usageError{errors.New("--region goes with --endpoint: the bucket is reached somewhere else")}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	o, err := db.GetOverflow(ctx, name)
	if errors.Is(err, state.ErrNotFound) {
		return noTarget(name)
	} else if err != nil {
		return err
	}
	if o.Removing() {
		return fmt.Errorf("overflow target %s is being removed; nothing to change", name)
	}
	if f.maxBytes != "" {
		if o.MaxBytes, err = parseNumber(f.maxBytes); err != nil {
			return usageError{fmt.Errorf("--max-bytes: %w", err)}
		}
	}
	if f.fullCycleSet {
		o.FullCycle = cycle
	}
	var check *replica.Check
	if f.endpoint != "" {
		if err := checkEndpoint(f.endpoint); err != nil {
			return err
		}
		o.Endpoint, o.Region = strings.TrimRight(f.endpoint, "/"), f.region
		// The credential is tested where the bucket is reached now; a
		// failure changes nothing (S3-1).
		id, err := identity.Load(cfg.ConfigDir())
		if err != nil {
			return err
		}
		cred, err := overflow.Lookup(a.getenv(), o.EnvPrefix)
		if err != nil {
			return err
		}
		c, err := replica.NewOverflow(o, cred, id.SealedName, replica.Options{})
		if err != nil {
			return usageError{err}
		}
		cctx, cancel := context.WithTimeout(ctx, overflowCheckTimeout)
		defer cancel()
		ch, err := c.CheckOverflow(cctx, name, 0)
		if err != nil {
			return fmt.Errorf("overflow target %s not changed: the credential from %s does not work at %s: %s",
				name, strings.TrimSuffix(o.EnvPrefix, "_")+"_*", o.Endpoint, cred.Redact(err.Error()))
		}
		check = &ch
		o.Lifecycle, o.VersionRemoval = ch.Lifecycle, ch.VersionRemoval
		// The same bucket (#96 review): the objects the target holds
		// must be there, or the endpoint reaches another bucket and every
		// copy there would count as held where it is not.
		if err := sameBucket(cctx, db, c, o); err != nil {
			return fmt.Errorf("overflow target %s not changed: %w", name, err)
		}
	}
	// Its versioning as read there is recorded with the change, as the
	// daily read records one (#97).
	versioning := ""
	if check != nil {
		versioning = check.Versioning
	}
	if _, err := db.UpdateOverflow(ctx, o, versioning, a.now()); errors.Is(err, state.ErrNotFound) {
		return noTarget(name)
	} else if err != nil {
		return err
	}
	fmt.Fprintf(a.out, "Overflow target %s: bucket %s at %s, up to %s\n", name, o.Bucket, o.Endpoint, formatSize(o.MaxBytes))
	if f.fullCycleSet {
		if o.FullCycle > 0 {
			fmt.Fprintf(a.out, "  full cycle: every object held there is downloaded and hashed once in %s, a slice a day, within the download limits; the provider charges for each download\n",
				formatCycle(o.FullCycle))
		} else {
			fmt.Fprintln(a.out, "  full cycle: off; the target is checked by the provider's stored checksums and a downloaded sample")
		}
	}
	if check != nil {
		fmt.Fprintf(a.out, "  credential: tested there; versioning %s; lifecycle rule %s; version removal %s\n",
			check.Versioning, check.Lifecycle, check.VersionRemoval)
		for _, n := range check.Notes {
			fmt.Fprintf(a.out, "  %s\n", n)
		}
	}
	if holds, err := db.OverflowHolds(ctx); err == nil && holds[name].Use > o.MaxBytes {
		fmt.Fprintf(a.out, "  It holds %s, more than that: no new copy goes there, and what it holds moves back to friends as they have room.\n",
			formatSize(holds[name].Use))
	}
	fmt.Fprintln(a.out, "The daemon uses the change at its next look at the targets; no restart is needed.")
	return nil
}

// parseFullCycle reads --full-cycle (VER-1a): 0 or off for none, else
// days, bare (90) or as the config file gives a cycle (90d, 36h).
func parseFullCycle(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "0" || strings.EqualFold(s, "off") {
		return 0, nil
	}
	if n, err := strconv.ParseUint(s, 10, 16); err == nil {
		s = strconv.FormatUint(n, 10) + "d"
	}
	return config.ParseCycle(s)
}

// formatCycle writes a cycle in days, or hours when not whole days.
func formatCycle(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%d days", d/(24*time.Hour))
	}
	return d.String()
}

// sameBucketSample is how many of a target's objects `overflow set
// --endpoint` asks the new endpoint for.
const sameBucketSample = 3

// sameBucket checks that c, the client at o's new endpoint, reaches the
// bucket the target is: up to sameBucketSample of the objects it
// acknowledged, spread over the write order, must be there with the
// hash lendisk stored (#96 review). A target holding nothing passes.
func sameBucket(ctx context.Context, db *state.DB, c *replica.Client, o state.Overflow) error {
	sample, err := db.HeldSample(ctx, o.Place(), sameBucketSample)
	if err != nil {
		return err
	}
	for _, s := range sample {
		st, ok, err := c.Stat(ctx, s.Bucket, s.Key)
		switch {
		case err != nil:
			return fmt.Errorf("asking %s for an object the target holds: %w", o.Endpoint, err)
		case !ok:
			return fmt.Errorf("%s has no %s of bucket %s, which the target holds: it does not reach the same bucket; "+
				"to move to another bucket or provider, add a new target and remove this one", o.Endpoint, s.Key, s.Bucket)
		case st.SHA256 != "" && st.SHA256 != s.SHA256:
			return fmt.Errorf("%s holds another version of %s of bucket %s than the target: it does not reach the same bucket",
				o.Endpoint, s.Key, s.Bucket)
		}
	}
	return nil
}

func (a *app) runOverflowRemove(ctx context.Context, name string, keep bool) error {
	_, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	before, err := db.GetOverflow(ctx, name)
	if errors.Is(err, state.ErrNotFound) {
		return noTarget(name)
	} else if err != nil {
		return err
	}
	if err := db.RemoveOverflow(ctx, name, keep, a.now()); errors.Is(err, state.ErrNotFound) {
		return noTarget(name)
	} else if err != nil {
		return err
	}
	o, err := db.GetOverflow(ctx, name)
	if err != nil {
		return err
	}
	holds, err := db.OverflowHolds(ctx)
	if err != nil {
		return err
	}
	h := holds[name]
	w := a.out
	if before.Removing() {
		fmt.Fprintf(w, "Overflow target %s is being removed since %s.\n", name, o.RemovingAt.UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintf(w, "Overflow target %s is being removed: no new copy goes there.\n", name)
	}
	fmt.Fprintf(w, "  It holds %s for which it is one of the copies (%s); each is copied to a friend or another target first, so every bucket keeps its copies.\n",
		countObjects(h.DataObjects), formatSize(h.DataBytes))
	switch {
	case o.KeepData:
		fmt.Fprintln(w, "  Its copies are then left there (--keep-data): nothing more is deleted there.")
	case o.ClientSide():
		fmt.Fprintf(w, "  Its copies there are then deleted, each held for its retention of %s first (no versioning: retention client-side).\n",
			formatDays(o.Retention))
	default:
		fmt.Fprintln(w, "  Its copies there are then deleted as delete markers, which its lifecycle rule expires.")
	}
	fmt.Fprintln(w, "  The target is dropped once nothing is left there; `lendisk status` shows it until then.")
	if plan, err := capacity.Read(ctx, db, state.DefaultCopies); err == nil && plan.Size() > 0 && plan.Protectable() < plan.Size() {
		fmt.Fprintf(w, "  ! Without it the pool protects %s of the repository's %s: the copies that find no room stay at the target until you add a friend or target, or a quota is raised.\n",
			formatSize(plan.Protectable()), formatSize(plan.Size()))
	}
	fmt.Fprintln(w, "Keep `lendisk serve` running, with the target's credential in its environment: the daemon does the moving.")
	return nil
}

type overflowFlags struct {
	endpoint, bucket, prefix, region, maxBytes, envPrefix, retention, price string
	lifecycle                                                               bool
	// fullCycle is set's --full-cycle, given when fullCycleSet (VER-1a).
	fullCycle    string
	fullCycleSet bool
}

func (a *app) runOverflowAdd(ctx context.Context, name string, f overflowFlags) error {
	if err := overflow.CheckName(name); err != nil {
		return usageError{err}
	}
	if err := checkEndpoint(f.endpoint); err != nil {
		return err
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

// overflowLister builds the client recovery lists an overflow target
// with (#96), its credential from getenv: in recover, the user's
// shell, where it may not be set yet (the list then waits for the
// daemon); in serve, the daemon's environment.
func overflowLister(getenv overflow.Getenv, id *identity.Identity) func(o state.Overflow) (recovery.Target, error) {
	return func(o state.Overflow) (recovery.Target, error) {
		cred, err := overflow.Lookup(getenv, o.EnvPrefix)
		if err != nil {
			return nil, err
		}
		return replica.NewOverflow(o, cred, id.SealedName, replica.Options{})
	}
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
