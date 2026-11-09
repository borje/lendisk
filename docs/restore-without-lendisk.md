# Restoring without lendisk

lendisk promises zero lock-in (P-5): with no lendisk running anywhere,
your backups are readable with free software. This page says how. The
normal restore, with lendisk, is `lendisk recover` and then your backup
tool as usual (see the [README](../README.md#restoring)); use this page
when that is not an option.

You need:

- your **recovery sheet** (`lendisk recovery-sheet`): each bucket's name
  at your friends, and how many friends' replicas a restore combines;
- your **backup tool's password**, which lendisk never had;
- `restic` or `kopia`, and for some steps
  [versitygw](https://github.com/versity/versitygw) 1.10.0 and
  [rclone](https://rclone.org).

The [store layout](store-layout.md) names every file involved.

## Your own copy

If your node's disk survived, your own copy is complete.

**restic**: the bucket's directory is a restic repository:

```sh
restic -r /var/lib/lendisk/store/laptop --no-lock snapshots
```

**Kopia** (or any tool, through S3): start the storage engine alone on
the store, as below, and connect to it read-only:

```sh
kopia repository connect s3 --endpoint=127.0.0.1:7070 --disable-tls \
  --bucket=vault --access-key=restore --secret-access-key=<secret> --readonly
kopia snapshot list
```

## Starting the storage engine alone

lendisk's store is versitygw's `posix` layout, and versitygw 1.10.0 serves
it on its own. Install it from its
[release page](https://github.com/versity/versitygw/releases/tag/v1.10.0),
or with Go:

```sh
go install github.com/versity/versitygw/cmd/versitygw@v1.10.0
```

Stop lendisk on that machine, then:

```sh
versitygw --access restore --secret <secret> --port 127.0.0.1:7070 --readonly \
  posix --exclude-dirs .lendisk /var/lib/lendisk/store
```

- `--access` and `--secret` are a credential you make up for this run:
  the root account, which reads every bucket.
- `--readonly` refuses every write, so nothing in the store changes.
  restic then needs `--no-lock`, and Kopia `--readonly` when connecting.
- `--exclude-dirs .lendisk` hides lendisk's own files: the tombs in
  friends' buckets and the manifests.
- Give the store directory: `/var/lib/lendisk/store` by default, or your
  `store.path`.

Every bucket in the store is now an S3 bucket at `http://127.0.0.1:7070`,
your own under their names and your friends' as `lent-<id16>-<sealed>`:

```sh
export AWS_ACCESS_KEY_ID=restore AWS_SECRET_ACCESS_KEY=<secret>
restic -r s3:http://127.0.0.1:7070/laptop --no-lock snapshots
```

## Your friends' replicas

When your own copy is gone, your backups are with your friends. Each
friend holds a **replica** of each of your buckets: all of its metadata and
the data objects placed there. The recovery sheet names the bucket as it
is stored at every friend: `lent-<id16>-<sealed>`.

### How many replicas

With `copies` = c and N places (friends plus overflow targets), every data
object is at c places, so the replicas of **any N − c + 1** places hold
every object (any one, when N ≤ c). The recovery sheet prints this number
for each bucket. For example, three friends at the default two copies: any
two friends' replicas.

### Getting a replica

Ask each friend for one of:

1. **An export** (the easiest). On their node, with lendisk running or
   not:

   ```sh
   lendisk export --lent <stored name> <directory>
   ```

   adding `--kopia-layout` for a Kopia bucket. The directory holds the
   replica as plain files in the backup tool's layout (see
   [the export layout](store-layout.md#the-export-layout)); they hand you
   the directory, on a disk or over the network.
2. **A copy of the bucket's directory**, `<store>/<stored name>/`, made
   with their lendisk stopped. For restic it is ready to combine; for
   Kopia it holds flat blob IDs (see [Kopia's layout](#kopias-layout)
   below).
3. **The keys through the storage engine alone**, if their lendisk will
   not run: they start versitygw on their store as above (it needs no
   lendisk) and copy the bucket with any S3 client, such as rclone:

   ```sh
   rclone copy --config /dev/null \
     ":s3,provider=Other,access_key_id=restore,secret_access_key=<secret>,endpoint='http://127.0.0.1:7070',force_path_style=true:<stored name>" \
     replica-1
   ```

   For a Kopia bucket this too gives flat blob IDs.

An **overflow target** holds your bucket's keys under the `s3://` path the
recovery sheet gives; copy them with any S3 client and its access key.

### Combining them

Put each replica in a directory of its own, `replica-1`, `replica-2` and so
on, side by side, and combine them into `repo`, leaving out the
housekeeping directories. The same key always has the same bytes wherever
it is held, so the union is a plain copy:

```sh
mkdir -p repo
for r in replica-*/; do cp -R "$r". repo/; done
rm -rf repo/.lendisk repo/.sgwtmp repo/.vgwlocks
```

or, with rsync:

```sh
mkdir -p repo
for r in replica-*/; do
  rsync -a --exclude=/.lendisk/ --exclude=/.sgwtmp/ \
    --exclude=/.vgwlocks/ "$r" repo/
done
```

These are the commands on your recovery sheet.

### Opening the repository

**restic**:

```sh
restic -r repo check --read-data      # every byte, against restic's own hashes
restic -r repo restore latest --target <directory>
```

**Kopia**, from exports made with `--kopia-layout`:

```sh
kopia repository connect filesystem --path repo
kopia snapshot verify --verify-files-percent=100
kopia snapshot restore <snapshot ID> <directory>
```

### Kopia's layout

Kopia's S3 storage, which lendisk serves, names each blob by its ID with
no directories; its filesystem storage shards the IDs into directories and
appends `.f`. `lendisk export --kopia-layout` writes the second. Replicas
copied any other way hold the first: combine them as above into `flat`
instead of `repo`, then lay them out for Kopia (bash):

```bash
mkdir -p repo
printf '%s' '{"default":[1,3],"maxNonShardedLength":20}' > repo/.shards
for f in flat/*; do
  id=${f#flat/}
  if [ ${#id} -le 20 ]; then cp "$f" "repo/$id.f"
  else mkdir -p "repo/${id:0:1}/${id:1:3}"; cp "$f" "repo/${id:0:1}/${id:1:3}/${id:4}.f"; fi
done
```

This is the layout `--kopia-layout` writes; then connect as above.

## What was tested

Scenario 7 runs as an automated test (`TestScenarioZeroLockIn`): four
nodes, a restic bucket at two copies; lendisk is stopped everywhere and the
fourth node's store deleted; versitygw alone, started as above, serves the
first node's own copy to `restic --no-lock snapshots`; the replicas at two
friends are copied with the rclone command above, combined with the cp
commands, and `restic check --read-data` passes; then the same with
`lendisk export` on each friend, and a restore. For Kopia,
`TestKopiaExportsFromTwoLendersFormARepository` connects read-only through
versitygw alone and combines two `--kopia-layout` exports into a filesystem
repository that verifies and restores.
