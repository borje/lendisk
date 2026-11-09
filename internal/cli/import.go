package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/logging"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/repoimport"
	"github.com/borje/lendisk/internal/state"
)

// importProfiles are the tools whose repositories bucket import reads
// (COMPAT-3).
var importProfiles = []string{"restic", "kopia"}

func (a *app) bucketImportCmd() *cobra.Command {
	var prof string
	var copies int
	var resume bool
	cmd := &cobra.Command{
		Use:   "import <name> <path> --profile restic|kopia",
		Short: "Create a bucket holding an existing restic or Kopia repository",
		Long: `import creates a bucket and puts into it the repository at <path>: a
restic directory, one of rest-server's repositories (<data>/<repo>, or
<data>/<user> with --private-repos), or a Kopia filesystem repository,
whose sharded files take Kopia's flat S3 names (COMPAT-3). Each file is
captured as a backup tool's upload is, with its hash, so it replicates
like a fresh backup; nothing is acknowledged. restic's locks and Kopia's
logs are left out. It needs no running daemon; a running one starts
replicating as the files arrive. A Kopia repository in format version 1
is refused (COMPAT-1).

An import that stops partway leaves the bucket with what it imported:
run the same command with --resume to put the rest.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runBucketImport(cmd.Context(), args[0], args[1], prof, copies, cmd.Flags().Changed("copies"), resume)
		},
	}
	cmd.Flags().StringVar(&prof, "profile", "", "the repository's tool: "+strings.Join(importProfiles, ", ")+" (required)")
	cmd.Flags().IntVar(&copies, "copies", 0, "how many friends hold each object (default replication.copies, 2)")
	cmd.Flags().BoolVar(&resume, "resume", false, "continue an import that stopped, into the bucket it created")
	return cmd
}

// importScan is what a first walk of a repository finds.
type importScan struct {
	files, bytes, localOnly int64
	format1Key              string // a key only Kopia format 1 writes
	upgraded                bool   // a key the format upgrade writes
}

// runBucketImport creates bucket name and puts the repository at path
// into it through the capture decorator, in process (COMPAT-3, #9).
func (a *app) runBucketImport(ctx context.Context, name, path, prof string, copies int, explicit, resume bool) error {
	if err := checkBucketArgs(name, prof, importProfiles, copies, explicit); err != nil {
		return err
	}
	if resume && explicit {
		return usageError{errors.New("--resume keeps the bucket's copies; change them with `lendisk bucket set`")}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	repo, err := repoimport.Open(path, prof)
	if err != nil {
		return err
	}
	set, err := profile.Load(filepath.Join(cfg.ConfigDir(), "profiles"))
	if err != nil {
		return err
	}
	p := set.Get(prof)
	var scan importScan
	if err := repo.Walk(func(f repoimport.File) error {
		if p.Classify(f.Key) == profile.LocalOnly {
			scan.localOnly++
			return nil
		}
		scan.files++
		scan.bytes += f.Size
		switch p.Format(f.Key) {
		case profile.Format1:
			if scan.format1Key == "" {
				scan.format1Key = f.Key
			}
		case profile.Upgraded:
			scan.upgraded = true
		}
		return nil
	}); err != nil {
		return fmt.Errorf("reading repository %s: %w", path, err)
	}
	if scan.format1Key != "" && !scan.upgraded {
		return fmt.Errorf("%s is a Kopia repository in format version 1 (it has %s), which lendisk does not support (COMPAT-1): "+
			"connect Kopia 0.9 or later to it, run `kopia repository set-parameters --upgrade`, then import it", path, scan.format1Key)
	}
	if scan.files == 0 {
		return fmt.Errorf("repository %s holds no objects to import", path)
	}

	var b state.Bucket
	if resume {
		b, err = db.GetBucket(ctx, name)
		if errors.Is(err, state.ErrNotFound) {
			return fmt.Errorf("no bucket %q to resume; run import without --resume", name)
		} else if err != nil {
			return err
		}
		if b.Profile != prof {
			return fmt.Errorf("bucket %s has profile %s, not %s", name, b.Profile, prof)
		}
	} else if b, err = a.createBucket(ctx, cfg, db, name, prof, copies, explicit); err != nil {
		return err
	}
	be, err := gateway.OpenBackend(cfg.StorePath())
	if err != nil {
		return err
	}
	d := &gateway.Decorator{Backend: be, Uploads: be, DB: db, Profiles: set, Now: a.clock,
		Log: logging.New(a.errOut, cfg.Log.Level, cfg.Log.Format)}
	fmt.Fprintf(a.errOut, "Importing %d objects (%s) from %s into bucket %s.\n", scan.files, humanSize(scan.bytes), path, name)
	pr := &importProgress{a: a, total: scan}
	err = repo.Walk(func(f repoimport.File) error {
		if p.Classify(f.Key) == profile.LocalOnly {
			return nil
		}
		if resume {
			// An object an earlier run captured whole is not put again.
			if src, ok, err := db.Source(ctx, name, f.Key); err != nil {
				return err
			} else if ok && src.Size == f.Size {
				pr.add(f.Size)
				return nil
			}
		}
		if err := importFile(ctx, d, name, f); err != nil {
			return err
		}
		pr.add(f.Size)
		return nil
	})
	if err != nil {
		return fmt.Errorf("import of %s stopped after %d of %d objects: %w; bucket %s holds those imported so far: "+
			"fix the cause and run the same command with --resume", path, pr.files, scan.files, err, name)
	}
	fmt.Fprintf(a.out, "Imported %d objects (%s) into bucket %s (profile %s, copies %d).\n",
		scan.files, humanSize(scan.bytes), name, prof, b.Copies)
	if scan.localOnly > 0 {
		fmt.Fprintf(a.out, "Left out %d files the tool keeps for itself (restic's locks, Kopia's logs).\n", scan.localOnly)
	}
	fmt.Fprintln(a.out, "Nothing is acknowledged yet: the daemon replicates the bucket like a fresh backup (`lendisk status` shows it).")
	if err := a.copiesNotes(ctx, cfg, db, b.Copies); err != nil {
		return err
	}
	fmt.Fprintln(a.out)
	printToolLines(a.out, cfg, b, true)
	return nil
}

// importFile streams one file into the bucket through the decorator,
// which hashes it as it passes and queues it (EP-2, EP-3); it is never
// read whole into memory.
func importFile(ctx context.Context, d *gateway.Decorator, bucket string, f repoimport.File) error {
	r, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer r.Close()
	size := f.Size
	if _, err := d.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(f.Key),
		Body: r, ContentLength: &size}); err != nil {
		return fmt.Errorf("putting %s as %s: %w", f.Path, f.Key, err)
	}
	return nil
}

// importProgress writes a line to stderr each time another whole
// percent of the bytes (of the objects, for a repository of empty files)
// is imported.
type importProgress struct {
	a            *app
	total        importScan
	files, bytes int64
	shown        int64
}

func (p *importProgress) add(size int64) {
	p.files++
	p.bytes += size
	pct := p.files * 100 / p.total.files
	if p.total.bytes > 0 {
		pct = p.bytes * 100 / p.total.bytes
	}
	if pct > p.shown || p.files == p.total.files {
		p.shown = pct
		fmt.Fprintf(p.a.errOut, "  %3d%%  %d/%d objects, %s\n", pct, p.files, p.total.files, humanSize(p.bytes))
	}
}
