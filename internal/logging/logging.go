// Package logging builds the structured logger (OPS-5). The daemon logs to
// stdout, where journald or the container runtime collects it; one-shot
// commands log to stderr so their stdout stays clean for output such as
// `status --json`.
package logging

import (
	"io"
	"log/slog"
)

// New returns a logger writing format ("text" or "json") at level
// ("debug", "info", "warn" or "error") to w. Values are validated by config.
func New(w io.Writer, level, format string) *slog.Logger {
	var lv slog.Level
	_ = lv.UnmarshalText([]byte(level))
	opts := &slog.HandlerOptions{Level: lv}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
