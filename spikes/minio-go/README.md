# Spike: minio-go replica client gate (issue #4)

Throwaway. Answers DESIGN.md §5 Q-Y items 1, 2 and 4, 2026-11-06. The
findings are recorded in DESIGN.md.

Pinned by go.sum (`go mod vendor` vendors them locally):

- `github.com/minio/minio-go/v7` **v7.4.1** (2026-10-12)
- `golang.org/x/time` **v0.16.0** (`rate`)
- `github.com/versity/versitygw` **v1.10.0**, the "friend", as in spike #2
- Garage: Docker image `dxflrs/garage:v2.5.0`
  (`sha256:677e5b758e4425a325b811c425c026bf35a4bf42f7ddbaa773de3f4fd779a5a5`)

Needs Go 1.26 (versitygw's `go` line), Docker, and a temp directory with
xattrs. Builds with `CGO_ENABLED=0 GOARCH=arm GOARM=7`.

## Run

    go test -v .          # ~90 s; starts a friend in-process and a Garage container
    SKIP_GARAGE=1 go test -v .                 # friend only
    eval "$(./garage.sh garage /tmp/g | tail -1 | sed 's/^/export /')"
    go test -v .          # reuses the Garage in GARAGE_ENDPOINT/ACCESS/SECRET

By hand: `go run . friend -store DIR -addr 127.0.0.1:7070` serves
versitygw on TCP (credentials `borrower1` /
`borrower1-secret-key-0123456789`); `go run . upload -source F -key K
-state S.json -sha256 HEX -rate BYTES_PER_S -window HH:MM-HH:MM
[-checksum sha256 -unsigned]` uploads in 5 MiB parts and prints one line
per event; kill it and run it again to resume.

Files: `resume.go` (target, state file, the resume loop, `putPart`),
`limits.go` (token buckets around a reader, the window), `friend.go`
and `iam.go` (versitygw on TCP), `garage.sh` (single-node Garage with a
key and a bucket), `main.go` (the `friend` and `upload` commands), and
the tests: `spike_test.go` (Q-Y.1), `checksum_test.go` (Q-Y.2),
`limits_test.go` (Q-Y.4).

## Answers

**Q-Y.1, resume after SIGKILL: yes, on both.** `TestResumeAfterSIGKILL`
runs the `upload` command as a subprocess on a 28 MiB object (six 5 MiB
parts at 8 MiB/s), waits for `part-done 3` and `part-start 4`, sleeps
300 ms and sends SIGKILL. The state file (upload ID plus part number,
ETag and checksum of each acknowledged part, replaced by
write-fsync-rename) holds parts 1 to 3; `ListObjectParts` at the target
shows the same three, the half-sent part 4 is not there. A second run
prints `resumed <id> have=[1 2 3]`, starts at part 4, reads 13 631 488 of
29 360 128 source bytes, completes, and the object's SHA-256 matches,
with `x-amz-meta-lendisk-sha256` set. Same result against versitygw and
Garage, with no request checksum and with the SHA-256 trailer.
The loop only needs `NewMultipartUpload`, `PutObjectPart`,
`ListObjectParts` and `CompleteMultipartUpload` from `minio.Core`.
minio-go retries only seekable bodies; the limiter's reader is not one,
so the client is built with `MaxRetries: 1` and the worker owns retries.

**Q-Y.2, request checksums.** What minio-go v7.4.1 sends, recorded by a
wrapping `http.RoundTripper` (`TestMinioGoDefaults`,
`TestSHA256RequestChecksum`):

- Default `Options` (`TrailingHeaders: false`): **no request checksum
  at all**, unlike the AWS SDK. Over plain HTTP (a friend through the
  tunnel) every PUT and part is `aws-chunked` with
  `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`: a SigV4
  signature per 64 KiB chunk, so the client SHA-256s every byte for the
  signature. Over HTTPS it sends `UNSIGNED-PAYLOAD` (api.go,
  `newRequest`). Both targets accept this.
- `TrailingHeaders: true` turns on CRC32C by default. A single PUT
  carries `x-amz-checksum-algorithm: CRC32C` and is accepted; a
  multipart upload over plain HTTP sends each part's CRC32C as a
  **signed** trailer (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`),
  which **both targets refuse**: versitygw `400 BadDigest`, Garage
  `400 InvalidRequest: Invalid payload signature`. The cause is
  minio-go's framing: it ends the trailer line with `\n` then `\r\n`
  (`pkg/signer/request-signature-streaming.go`, `addSignedTrailer`),
  where AWS's format and versitygw's parser
  (`s3api/utils/signed-chunk-reader.go`, reads the checksum up to `\r`)
  expect `\r\n`. The same happens with a SHA-256 in a signed trailer.

SHA-256, per target, correct then wrong value:

| Call | Payload | versitygw v1.10.0 | Garage v2.5.0 |
|---|---|---|---|
| single PUT, `x-amz-checksum-sha256` header | signed chunks | accepted / 400 BadDigest | accepted / 400 InvalidDigest |
| single PUT, header | `UNSIGNED-PAYLOAD` | accepted / 400 BadDigest | accepted / 400 InvalidDigest |
| part, SHA-256 header (part read twice) | signed chunks | accepted / 400 BadDigest | accepted / 400 InvalidDigest |
| part, SHA-256 header | unsigned | accepted / 400 BadDigest | accepted / 400 InvalidDigest |
| part, SHA-256 trailer (one read) | signed trailer | **refused either way** | **refused either way** |
| part, SHA-256 trailer (one read) | `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | accepted / 400 BadDigest | accepted / 400 InvalidDigest |
| part, none | either | accepted | accepted |

So both targets accept **and verify** a SHA-256 request checksum, and a
client can send none. The working one-read combination is
`DisableContentSha256: true` (which over HTTPS is what minio-go does
anyway) with the per-part SHA-256 in an unsigned trailer
(`PutObjectPartOptions.Trailer`, a placeholder of the final length,
filled by a hashing reader at EOF; `CreateMultipartUpload` carries
`x-amz-checksum-algorithm: SHA256`). A single PUT carries the whole
object's SHA-256, which is REPL-10's hash from the queue row, as a
header at no extra read. S3 has no whole-object SHA-256 for multipart
(full-object checksums are CRC types only), so a multipart object's
REPL-10 hash stays in the metadata. `TrailingHeaders` stays off.

**Q-Y.4, limiter and timetable: yes, both hold to within one part.**
The reader cuts each `Read` to the smallest burst (64 KiB) and calls
`WaitN` on the peer and the global bucket.

- 4 MiB/s, four 5 MiB parts: parts took 1.236 to 1.255 s (3.98 to 4.05
  MiB/s) at both targets; 20 MiB in 5.023 s (friend) and 5.042 s
  (Garage) against an ideal 5.000 s. A 20 MiB download through the same
  wrapper: 4.008 and 4.001 MiB/s.
- `SetLimit` from 2 to 8 MiB/s 3 s in, mid-part 2: 1.991 MiB/s before,
  8.140 MiB/s in the first 250 ms after, 7.873 MiB/s for the rest.
- Two places in parallel, 4 MiB/s each, sharing a 6 MiB/s global
  bucket: 6.006 MiB/s combined.
- Window closing 10 %, 50 % and 90 % into part 2 (1.25 s per part): the
  transfer stopped at the end of part 2 each time, 1.11 to 1.18 s,
  0.61 s and 0.12 s after the close, with 0.90 to 0.95, 0.49 to 0.50 and
  0.10 parts sent after it; when the window reopened it sent parts 3
  and 4 only and the object matched.
