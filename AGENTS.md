# lendisk — agent guide

lendisk is one static Go binary: friends lend each other disk for offsite S3 backups. Work arrives as GitHub issues on borje/lendisk; the spec is two documents ([SPEC.md](SPEC.md) explains the split).

## Where truth lives

- **REQUIREMENTS.md** — what the product does, with stable IDs (`ID-1`, `REPL-3`, `SEC-10`…). Changing it changes the product: stop and ask the user.
- **DESIGN.md** — how it is built. §2 decisions (`D5`…), §3 realisation per area (component names, paths, formats, algorithms), §5 gates and questions (`Q-I`…). When code settles or changes a design detail, edit §3 in the same commit.
- **The issue** — what to build now. Its checkboxes are the completion criterion: every box met and tested, or a follow-up issue filed for what is left.

Cite requirement IDs in package comments and tests where the code realises them (`// Package logging builds the structured logger (OPS-5).`).

## Delivering an issue

1. Read the issue, its blockers' commits, and the DESIGN §3 section it touches.
2. Build test-first on a branch or worktree.
3. Run the CI checks locally: `gofmt -l cmd internal`, `go vet ./...`, `go test -race ./...`, and `CGO_ENABLED=0 GOARCH=arm GOARM=7 go build ./cmd/lendisk` when you add a dependency.
4. Merge to `main` and push (no PR unless asked). The finishing commit's body ends with `Closes #N`. Check CI is green after the push.
5. Stop and ask the user on `needs-human` issues and on product or design decisions the spec leaves open.

Commit subject: what the user can now do, then `(#N)`. Body: short bullets of what was built and which DESIGN sections changed.

## Code conventions

- Layout: `cmd/lendisk` is a thin `main`; everything lives under `internal/<area>`. One package per area, named for what it owns.
- Every package starts with a package comment saying what it owns and the rules a caller must keep.
- **Pure Go, CGO off.** Builds must stay static for amd64, arm64 and armv7; a dependency that needs cgo is a design decision.
- **Config:** a setting is a leaf field of `config.Config` with `toml`, `help` and `default` tags; the flag and `LENDISK_*` variable follow automatically. The daemon never writes the config file; anything it learns goes in the state database.
- **State:** `internal/state/migrations.go` is append-only. Add a new `Migration` with the next `Version`; never edit a released one. Add tables and nullable columns only, so the previous minor release still runs (the rule is in the `state` package comment). Two branches adding migrations collide on `Version`: renumber when rebasing.
- **`status --json`:** add fields and sections; never rename or remove. `StatusSchema` stays 1.
- **Logging:** `log/slog` passed in, never a global. Daemon logs to stdout, one-shot commands to stderr so stdout stays machine-readable.
- **Errors:** wrap with `%w`; messages say what is wrong and what to do (`no identity: run \`lendisk init\` first`). Usage errors exit 2, other failures 1.
- **Files:** secrets and the database are created at 0600, their directories at 0700, from the first byte.
- **Testability through injection:** randomness, clocks, stdin and contexts are fields on the owning struct (see `cli.app`); tests set them. Use real files under `t.TempDir()` and real SQLite, not mocks.
- **Tests** are named for the behaviour (`TestMistypedNodeIDRejected`). CLI tests drive `app.run` through the `node` helper in `internal/cli/cli_test.go`, one `node` per simulated machine.

## Spikes

A spike is a throwaway prototype answering DESIGN §5 questions. It lives in `spikes/<name>/` as its own Go module, never imported by `cmd/` or `internal/`. Dependencies are pinned by `go.sum`; `vendor/` is not committed. Done means a README with the pinned versions and how to run it, the answers written into DESIGN §5, and any DESIGN decision it contradicts corrected. Then check the issues that depend on it against the findings.
