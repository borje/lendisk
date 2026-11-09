package cli

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/pace"
)

// defaultReloadEvery is how often the daemon looks at its config file
// for changed pacing settings.
const defaultReloadEvery = 10 * time.Second

// paceSettings are cfg's pacing settings (REPL-6, REPL-7, LEND-5).
func paceSettings(cfg *config.Config) pace.Settings {
	s := cfg.Pace()
	s.PartSize = cfg.PartSize()
	return s
}

// withoutPacing is cfg with its pacing settings cleared, to tell other
// changes apart.
func withoutPacing(cfg config.Config) config.Config {
	cfg.Replication.PartSize, cfg.Replication.Schedule = "", ""
	cfg.Replication.UploadLimit, cfg.Replication.DownloadLimit = "", ""
	cfg.Replication.PeerLimit, cfg.Lending.BorrowerLimit = nil, nil
	cfg.Store.BitrotReadLimit = ""
	return cfg
}

// reloader applies changed pacing settings without a restart (§3.4
// REPL-7): it re-reads the configuration, with the same flags and
// environment, when the file changes (looked at every Every) or when
// the daemon gets SIGHUP (Hup). A file that does not load keeps the
// settings in force, with an error logged; a change to any other
// setting is logged as waiting for the next start. The daemon never
// writes the file.
type reloader struct {
	Load  func() (*config.Loaded, error)
	Path  string
	Pace  *pace.Pacer
	Log   *slog.Logger
	Every time.Duration
	Hup   <-chan os.Signal
	// started is the configuration the daemon started with.
	started config.Config
}

type stamp struct {
	ok    bool
	mod   time.Time
	size  int64
	inode uint64
}

func stampOf(path string) stamp {
	fi, err := os.Stat(path)
	if err != nil {
		return stamp{}
	}
	s := stamp{ok: true, mod: fi.ModTime(), size: fi.Size()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		s.inode = uint64(st.Ino)
	}
	return s
}

// Run watches until ctx ends.
func (r *reloader) Run(ctx context.Context) {
	every := r.Every
	if every <= 0 {
		every = defaultReloadEvery
	}
	last := stampOf(r.Path)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		why := ""
		select {
		case <-ctx.Done():
			return
		case <-r.Hup:
			why = "SIGHUP"
		case <-t.C:
			if s := stampOf(r.Path); s != last {
				why = "the file changed"
			}
		}
		if why == "" {
			continue
		}
		last = stampOf(r.Path)
		r.reload(why)
	}
}

func (r *reloader) reload(why string) {
	cfg, err := r.Load()
	if err != nil {
		r.Log.Error("configuration: not reloaded; the settings in force stay", "why", why, "err", err)
		return
	}
	s := paceSettings(&cfg.Config)
	r.Pace.Set(s)
	r.Log.Info("configuration reloaded: pacing applied", "why", why, "part_size", s.PartSize,
		"schedule", s.Schedule.String(), "upload_limit", s.Up, "download_limit", s.Down,
		"peer_limits", len(s.Peers), "borrower_limits", len(s.Lend), "bitrot_read_limit", s.Scrub)
	if !reflect.DeepEqual(withoutPacing(cfg.Config), withoutPacing(r.started)) {
		r.Log.Warn("configuration: settings other than part_size, schedule and the limits changed; they take effect at the next start")
	}
}
