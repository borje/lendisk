# Spike: embedded versitygw (issue #2)

Throwaway prototype. versitygw v1.10.0 (tag commit 72f75bd, 2026-10-22),
pinned by go.sum (`go mod vendor` to vendor it), embedded with `embedgw.RunVersityGW` on the `posix` backend,
behind a pass-through decorator and an external `IAMService` with one
hardcoded account (`borrower1`). S3 is served on one unix socket and
nothing else. Answers DESIGN.md §5 Q-I; the findings are recorded there.

Needs Go 1.26 (versitygw v1.10.0's `go` line) and a store directory with
xattrs.

## Run

    go test -v .              # Q-I.2, Q-I.5, listeners, admin API, object lock
    RESTIC=~/.local/bin/restic ./restic-e2e.sh   # restic through a TCP->unix forward

`go run . -store DIR -sock DIR/run/gw.sock -forward 127.0.0.1:9000`
runs the gateway by hand. Credentials: `borrower1` /
`borrower1-secret-key-0123456789`.

Files: `gateway.go` (backend and `embedgw.Config`), `decorator.go`,
`iam.go`, `gate.go` (a fiber middleware through `Config.S3Options`),
`main.go` (flags and the byte-copy forward).

## Answers

- **Account in the backend call (Q-I.2): yes, no patch.** The backend
  gets `*fasthttp.RequestCtx` as its `context.Context`, and the auth
  middleware's fiber `Locals` are fasthttp user values, so
  `ctx.Value(string(utils.ContextKeyAccount)).(auth.Account)` works in
  the decorator. `TestAccountReachesBackend` and the restic run print
  `account=borrower1(role=userplus)` on every call. An unsigned request
  reaches `GetBucketPolicy`/`GetBucketAcl` with a zero `Account`
  (`TestAnonymous`).
- **Custom errors (Q-I.5): yes.** An unwrapped `s3err.APIError` returned
  by the backend reaches the AWS SDK intact: `QuotaExceeded` 507 and
  `PreconditionFailed` 412; a HEAD carries the status only. A wrapped
  error (`fmt.Errorf("%w")`) becomes 500 `InternalError`, because the
  controller uses a type assertion. The `AccessDenied`-with-message
  fallback also works but is not needed.
- **Listeners.** With only `Ports = [sock]` the process holds the unix
  socket (mode 0600 via `SocketPerm`) and no TCP or UDP socket
  (`TestListeners`; `ss -lntupx` in the script shows only the socket and
  the forward). Web UI, website, metrics and events stay off when their
  fields are empty.
- **Admin API is not off.** With `AdminPorts` empty it is mounted on the
  S3 socket (all routes `PATCH`). A user account gets
  `XAdminAccessDenied`; the root account, which `RunVersityGW` requires,
  gets 200 and the secrets `ListUserAccounts` returns
  (`TestAdminAPIOnS3Socket`). `gate.go` refuses `PATCH`.
- **Versioning is off; object lock is not.** No `VersioningDir`:
  `PutBucketVersioning` gives 501 `VersioningNotConfigured`. But
  `CreateBucket` with `x-amz-bucket-object-lock-enabled: true` succeeds
  and the bucket reports lock enabled; `gate.go` refuses the header.
- **Refusals belong in a middleware, not only the decorator.**
  versitygw calls `GetBucketAcl` and `GetBucketPolicy` on the backend for
  every request's access check (about 80 each in one restic run), so
  those methods cannot be refused in the decorator. `s3api.WithMiddleware`
  through `Config.S3Options` runs before routing and authentication.
- **restic.** 0.19.1 `init`, two `backup`s, `check --read-data`,
  `restore` (byte-identical) through the forward, and
  `restic -r <store>/laptop snapshots` on the plain files.
- `posix.New` `chdir`s the process to the store root unless
  `PosixOpts.AbsolutePaths` is set.
- Q-Z: temp files live in `<bucket>/.sgwtmp` (`O_TMPFILE` then link),
  multipart parts in `<bucket>/.sgwtmp/multipart/`, so they stay on the
  bucket's filesystem.
