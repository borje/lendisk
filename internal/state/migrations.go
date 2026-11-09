package state

// migrations is the full, append-only schema history. Never edit a released
// entry; add a new one. See the package comment for the Compat rule.
var migrations = []Migration{
	{
		Version: 1, Compat: 1, Release: "0.1",
		SQL: `
		-- Facts about this node: its ID (to catch a key file and database
		-- that belong to different identities), when it was initialised.
		CREATE TABLE node (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
	},
	{
		Version: 2, Compat: 1, Release: "0.1",
		SQL: `
		-- The node's own buckets (EP-6): one per backup repository, each with
		-- its own S3 credential, resolved by the IAM service (SEC-3).
		CREATE TABLE buckets (
			name       TEXT PRIMARY KEY,
			profile    TEXT NOT NULL,
			copies     INTEGER NOT NULL,
			access_key TEXT NOT NULL UNIQUE,
			secret_key TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
	},
	{
		Version: 3, Compat: 1, Release: "0.1",
		SQL: `
		-- Who the tunnel admits (SEC-10). Keys are node IDs in display
		-- form. Only what admission reads is here; the friendship's
		-- address, numbers and retention come with invites (#12).
		CREATE TABLE friendship (
			peer_key   TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL -- unix seconds
		);
		-- An invite admits its derived key until it is used or expires.
		CREATE TABLE invite (
			invite_key TEXT PRIMARY KEY,
			expires_at INTEGER NOT NULL, -- unix seconds
			used_at    INTEGER           -- unix seconds; NULL while unused
		);`,
	},
	{
		Version: 4, Compat: 1, Release: "0.1",
		SQL: `
		-- The replication queue (REPL-1 to REPL-3): one row per captured
		-- write or delete of an own bucket's key (EP-2, EP-3), in write
		-- sequence. A row is pending while the backend call is in flight
		-- and done once it succeeded; startup resolves pending rows.
		CREATE TABLE queue (
			seq        INTEGER PRIMARY KEY AUTOINCREMENT, -- write sequence
			bucket     TEXT NOT NULL,
			key        TEXT NOT NULL,
			op         TEXT NOT NULL,    -- put | delete
			class      TEXT NOT NULL,    -- snapshot | metadata | data (EP-7)
			state      TEXT NOT NULL,    -- pending | done
			size       INTEGER,          -- put: bytes
			sha256     TEXT,             -- put: hex SHA-256 of the whole object
			token      TEXT,             -- PutObject: its lendisk-capture metadata
			etag       TEXT,             -- multipart: the ETag it completes with
			upload_id  TEXT,             -- multipart: the upload it completed
			created_at INTEGER NOT NULL  -- unix seconds
		);
		CREATE INDEX queue_key ON queue (bucket, key, seq);
		CREATE INDEX queue_pending ON queue (state) WHERE state = 'pending';`,
	},
	{
		Version: 5, Compat: 1, Release: "0.1",
		SQL: `
		-- A friendship's terms (PEER-2, LEND-8, RET-1). Numbers are bytes,
		-- retentions seconds. The quota is the smaller number, computed.
		ALTER TABLE friendship ADD COLUMN address        TEXT;    -- the friend's tc… address
		ALTER TABLE friendship ADD COLUMN own_number     INTEGER; -- what this node lends the friend
		ALTER TABLE friendship ADD COLUMN peer_number    INTEGER; -- what the friend lends, as last reported
		ALTER TABLE friendship ADD COLUMN own_retention  INTEGER; -- how long this node keeps the friend's deletes
		ALTER TABLE friendship ADD COLUMN peer_retention INTEGER; -- how long the friend keeps this node's
		-- What an invite offers: the inviting side's terms. The secret is
		-- never stored, only the public key derived from it (invite_key).
		ALTER TABLE invite ADD COLUMN number    INTEGER;
		ALTER TABLE invite ADD COLUMN retention INTEGER;`,
	},
	{
		Version: 6, Compat: 1, Release: "0.1",
		SQL: `
		-- The lender's mapping table (PEER-8, SEC-16): each bucket a
		-- borrower created here, by the borrower's node ID and the
		-- bucket's sealed name, never its real one. The store holds it as
		-- lent-<id16>-<sealed>. A borrower's bucket listing is these rows.
		CREATE TABLE lent_bucket (
			peer_key   TEXT NOT NULL,    -- the borrower's node ID in display form
			sealed     TEXT NOT NULL,    -- s<26>, the name on the wire
			created_at INTEGER NOT NULL, -- unix seconds
			PRIMARY KEY (peer_key, sealed)
		);`,
	},
	{
		Version: 7, Compat: 1, Release: "0.1",
		SQL: `
		-- Placement (D6, REPL-1, REPL-10): one row per queue row and place
		-- it was placed at, decided in the transaction that completes the
		-- capture. A place's queued rows in seq order are its FIFO. On
		-- acknowledgement the row keeps the hash the place verified: the
		-- index of acknowledged hashes. Bucket, key and op are copied from the
		-- queue row so the index outlives it. Place is the friend's node ID.
		CREATE TABLE placement (
			seq      INTEGER NOT NULL, -- the queue row
			place    TEXT NOT NULL,
			bucket   TEXT NOT NULL,
			key      TEXT NOT NULL,
			op       TEXT NOT NULL,    -- put | delete
			state    TEXT NOT NULL,    -- queued | acked | refused
			sha256   TEXT,             -- acked put: the hash the place verified
			detail   TEXT,             -- refused: why
			acked_at INTEGER,          -- unix seconds
			PRIMARY KEY (place, seq)
		);
		CREATE INDEX placement_queued ON placement (place, seq) WHERE state = 'queued';
		CREATE INDEX placement_key ON placement (place, bucket, key, seq);
		-- How each place answered last (#18): ok, retrying, quota (paused
		-- for quota, LEND-2) or removed (PEER-5). last_ok is when it last
		-- acknowledged anything, D6's last-reachable time.
		CREATE TABLE place (
			place     TEXT PRIMARY KEY,
			condition TEXT NOT NULL,
			detail    TEXT,
			since     INTEGER NOT NULL, -- unix seconds: condition entered
			last_ok   INTEGER           -- unix seconds
		);`,
	},
	{
		Version: 8, Compat: 1, Release: "0.1",
		SQL: `
		-- Redeem retries (PEER-2, #51). On the inviting side, the node
		-- that redeemed an invite, so it alone may redeem it again until
		-- it expires; NULL on rows used before this column.
		ALTER TABLE invite ADD COLUMN redeemed_by TEXT;
		-- On the invited side, a redeem started but not yet answered:
		-- the terms this node offered, kept so that running peer add
		-- again repeats them. Deleted with the friendship row's insert.
		CREATE TABLE pending_redeem (
			invite_key TEXT PRIMARY KEY, -- node ID of the invite key
			peer_key   TEXT NOT NULL,    -- the inviting node's ID
			number     INTEGER NOT NULL, -- what this node lends, bytes
			retention  INTEGER NOT NULL, -- seconds
			started_at INTEGER NOT NULL  -- unix seconds
		);`,
	},
	{
		Version: 9, Compat: 1, Release: "0.1",
		SQL: `
		-- Why a placement row was refused (#50): 'store' when the node's
		-- own store could not supply the object as captured (gone, changed,
		-- a directory, too large, rotted); the refusal counts against the
		-- bucket. NULL when the place itself refused (EP-11).
		ALTER TABLE placement ADD COLUMN cause TEXT;`,
	},
	{
		Version: 10, Compat: 1, Release: "0.1",
		SQL: `
		-- What each borrower stores here (LEND-1, SEC-5): bytes of its
		-- objects and of the parts of its open uploads, 4 KiB for each object,
		-- bucket and open upload (later plus
		-- tombstones), moved by every completed write and delete, and set
		-- from a listing of its buckets when reconciled.
		CREATE TABLE lent_usage (
			peer_key      TEXT PRIMARY KEY, -- the borrower's node ID
			bytes         INTEGER NOT NULL,
			reconciled_at INTEGER           -- unix seconds; NULL until reconciled
		);
		-- A borrower's multipart upload in progress (LEND-4): the hash it
		-- declared when it began, checked when it completes, and the part
		-- bytes counted for it in lent_usage (LEND-1).
		CREATE TABLE lent_upload (
			upload_id  TEXT PRIMARY KEY,
			peer_key   TEXT NOT NULL,    -- the borrower's node ID
			sealed     TEXT NOT NULL,    -- the bucket's sealed name
			key        TEXT NOT NULL,
			sha256     TEXT NOT NULL,    -- hex, x-amz-meta-lendisk-sha256
			created_at INTEGER NOT NULL, -- unix seconds
			counted    INTEGER NOT NULL DEFAULT 0 -- part bytes counted
		);
		-- The size counted for each part of a borrower's upload, so a part
		-- sent again counts only what it changes (LEND-1).
		CREATE TABLE lent_part (
			upload_id TEXT NOT NULL,
			part      INTEGER NOT NULL,
			size      INTEGER NOT NULL,
			PRIMARY KEY (upload_id, part)
		);`,
	},
	{
		Version: 11, Compat: 1, Release: "0.1",
		SQL: `
		-- Pending alerts (OPS-2, OPS-3, #13): one row per condition, keyed
		-- kind:subject, so an alert is raised once while its condition
		-- holds. cleared_at is set when it ends; a later raise of the same
		-- key replaces the row and counts up raise_no, which a delivery
		-- names, so the send of an earlier raise never marks a later one.
		-- hook_at and webhook_at record delivery of the current raise to
		-- each channel; NULL until delivered.
		CREATE TABLE alert (
			key        TEXT PRIMARY KEY, -- kind, or kind:subject
			kind       TEXT NOT NULL,    -- friend_unreachable, store_unreadable, …
			subject    TEXT NOT NULL,    -- the friend's node ID, the bucket, or ''
			message    TEXT NOT NULL,
			raise_no   INTEGER NOT NULL, -- 1, 2, … per key
			raised_at  INTEGER NOT NULL, -- unix seconds
			cleared_at INTEGER,          -- unix seconds; NULL while it holds
			hook_at    INTEGER,          -- unix seconds
			webhook_at INTEGER           -- unix seconds
		);
		-- Refusals, which the alert evaluator counts every minute.
		CREATE INDEX placement_refused ON placement (place) WHERE state = 'refused';`,
	},
	{
		Version: 12, Compat: 1, Release: "0.1",
		SQL: `
		-- Pooled placement (D6, REPL-1, REPL-12, #19). placement.state
		-- gains 'dropped': an unsent copy at a full or gone friend with no
		-- other friend with room, taken out of its FIFO as a deficit.
		-- What a placement row adds to its place's use while queued or
		-- acked: a put its size plus the 4 KiB per object the lender counts
		-- (0 when the place already holds those bytes), a delete minus what
		-- the place held of the key. NULL counts as 0.
		ALTER TABLE placement ADD COLUMN size INTEGER;
		-- The borrower's count of what each place holds or will hold (D6
		-- step 1): the sum of its queued and acked rows' size, kept by the
		-- triggers below, so placing an object reads one row per place.
		CREATE TABLE place_use (
			place TEXT PRIMARY KEY,
			bytes INTEGER NOT NULL
		);
		UPDATE placement SET size = (SELECT q.size FROM queue q WHERE q.seq = placement.seq) + 4096
			WHERE op = 'put';
		UPDATE placement SET size = -coalesce((SELECT sum(a.size) FROM placement a
			WHERE a.place = placement.place AND a.bucket = placement.bucket AND a.key = placement.key
			AND a.op = 'put' AND a.state = 'acked' AND a.seq < placement.seq
			AND a.seq > coalesce((SELECT max(d.seq) FROM placement d WHERE d.place = a.place
				AND d.bucket = a.bucket AND d.key = a.key AND d.op = 'delete' AND d.seq < placement.seq), 0)), 0)
			WHERE op = 'delete';
		INSERT INTO place_use (place, bytes)
			SELECT place, coalesce(sum(size), 0) FROM placement WHERE state IN ('queued', 'acked') GROUP BY place;
		CREATE TRIGGER place_use_insert AFTER INSERT ON placement
		WHEN NEW.state IN ('queued', 'acked') BEGIN
			INSERT OR IGNORE INTO place_use (place, bytes) VALUES (NEW.place, 0);
			UPDATE place_use SET bytes = bytes + coalesce(NEW.size, 0) WHERE place = NEW.place;
		END;
		CREATE TRIGGER place_use_update AFTER UPDATE OF place, state, size ON placement BEGIN
			UPDATE place_use SET bytes = bytes - coalesce(OLD.size, 0)
				WHERE place = OLD.place AND OLD.state IN ('queued', 'acked');
			INSERT OR IGNORE INTO place_use (place, bytes) VALUES (NEW.place, 0);
			UPDATE place_use SET bytes = bytes + coalesce(NEW.size, 0)
				WHERE place = NEW.place AND NEW.state IN ('queued', 'acked');
		END;
		CREATE TRIGGER place_use_delete AFTER DELETE ON placement
		WHEN OLD.state IN ('queued', 'acked') BEGIN
			UPDATE place_use SET bytes = bytes - coalesce(OLD.size, 0) WHERE place = OLD.place;
		END;
		-- The metadata barrier (REPL-1) looks for a bucket's queued rows
		-- before a sequence; status counts each object's places.
		CREATE INDEX placement_barrier ON placement (bucket, seq) WHERE state = 'queued';
		CREATE INDEX placement_seq ON placement (seq);
		-- A bucket's metadata, for what is not yet sent to a place (D6).
		CREATE INDEX queue_metadata ON queue (bucket, key, seq) WHERE class IN ('metadata', 'snapshot');`,
	},
	{
		Version: 13, Compat: 1, Release: "0.1",
		SQL: `
		-- The key the friend dials with (ID-1, SEC-10, #69), node-ID form,
		-- exchanged at redeem; admitted with the friend's rights and named
		-- as the friend. NULL for a friendship made before it existed.
		ALTER TABLE friendship ADD COLUMN dial_key TEXT;
		CREATE UNIQUE INDEX friendship_dial_key ON friendship (dial_key);`,
	},
	{
		Version: 14, Compat: 1, Release: "0.1",
		SQL: `
		-- Node manifest delivery (ID-4, #28): per friend, the manifest
		-- version it last acknowledged and the last attempt. The current
		-- version is in the node table (manifest_seq, manifest_hash,
		-- manifest_updated_at). A friend without a row, with seq differing
		-- from the current version, or last delivered a day ago or more is
		-- pushed to, once its backoff after failures has passed. A
		-- friendship committed again drops its row.
		CREATE TABLE manifest_push (
			peer_key     TEXT PRIMARY KEY, -- the friend's node ID
			seq          INTEGER,          -- version acknowledged; NULL until one is
			delivered_at INTEGER,          -- unix seconds
			tried_at     INTEGER NOT NULL, -- unix seconds: the last attempt
			error        TEXT,             -- the last attempt's failure; NULL after a success
			failures     INTEGER NOT NULL DEFAULT 0 -- failed attempts since the last success
		);`,
	},
	{
		Version: 15, Compat: 1, Release: "0.1",
		SQL: `
		-- Recovery (ID-5, #29): the lists of a place's copy of a bucket
		-- still to be read to rebuild placement. recover adds one per
		-- friend and bucket in the manifest; a list read to its end drops
		-- its row, in the transaction that records its last page. A place
		-- that was offline keeps its rows until the daemon lists it.
		CREATE TABLE relist (
			place     TEXT NOT NULL,              -- the friend's node ID
			bucket    TEXT NOT NULL,              -- the own bucket's name
			after     TEXT NOT NULL DEFAULT '',   -- the last key recorded: the next page starts after it
			tried_at  INTEGER,                    -- unix seconds: the last failed attempt
			error     TEXT,                       -- its failure
			failures  INTEGER NOT NULL DEFAULT 0, -- failed attempts in a row
			PRIMARY KEY (place, bucket)
		);`,
	},
	{
		Version: 16, Compat: 1, Release: "0.1",
		SQL: `
		-- Recovery's verdict per key (ID-5, #29 review): what each place
		-- listed, and which version counts. A key is held when the places
		-- agree, or most of them do, or the user picked one; single when one
		-- place lists it; conflict when places tie, or one place lists a
		-- metadata key that another listed place lacks. Only the version
		-- that counts has a queue row (seq) and acked placement rows.
		ALTER TABLE relist ADD COLUMN seen INTEGER; -- keys and pages read so far: capped
		CREATE TABLE listed (
			bucket TEXT NOT NULL,
			key    TEXT NOT NULL,
			place  TEXT NOT NULL,
			size   INTEGER NOT NULL,
			sha256 TEXT NOT NULL,
			class  TEXT NOT NULL,
			named  INTEGER NOT NULL DEFAULT 0, -- 1: the key is named by its hash, which matched
			PRIMARY KEY (bucket, key, place)
		);
		CREATE TABLE recovered_key (
			bucket  TEXT NOT NULL,
			key     TEXT NOT NULL,
			verdict TEXT NOT NULL, -- held | single | conflict
			reason  TEXT,          -- why, for status
			sha256  TEXT,          -- the version that counts; NULL for a conflict
			seq     INTEGER,       -- the queue row recovery made for it
			picked  TEXT,          -- the hash the user picked (bucket resolve)
			PRIMARY KEY (bucket, key)
		);`,
	},
	{
		Version: 17, Compat: 1, Release: "0.1",
		SQL: `
		-- Recovery (#29 re-review): pages read of a list, capped apart from
		-- its entries; and a list abandoned past a cap, kept so its place
		-- does not count as listed to its end, but no longer read.
		ALTER TABLE relist ADD COLUMN pages INTEGER;
		ALTER TABLE relist ADD COLUMN abandoned INTEGER;`,
	},
	{
		Version: 18, Compat: 1, Release: "0.1",
		SQL: `
		-- Recovery (#29 re-review): a list read to its end is kept, done,
		-- so only places that listed a bucket count as having listed it (a
		-- friend added later has no row); settled is set once the keys a
		-- finished or abandoned list affects were decided again, in short
		-- batches after it ended.
		ALTER TABLE relist ADD COLUMN done INTEGER;
		ALTER TABLE relist ADD COLUMN settled INTEGER;`,
	},
	{
		Version: 19, Compat: 1, Release: "0.1",
		SQL: `
		-- A borrower's deleted object, kept for the retention window
		-- (RET-1, RET-2, #17): its bytes moved to tomb, a path inside the
		-- bucket's .lendisk/tomb/ directory, on the same filesystem. The
		-- row is written, pending, before the move and kept (pending 0)
		-- after it; startup settles a pending row from what the store
		-- holds. The purger removes the tomb and the row after purge_after.
		CREATE TABLE tombstone (
			id          INTEGER PRIMARY KEY,
			peer_key    TEXT NOT NULL,    -- the borrower's node ID
			sealed      TEXT NOT NULL,    -- the bucket's sealed name
			key         TEXT NOT NULL,
			tomb        TEXT NOT NULL,    -- the bytes' path relative to the bucket directory
			sha256      TEXT NOT NULL,    -- hex, as the object was stored (LEND-4)
			etag        TEXT NOT NULL,
			size        INTEGER NOT NULL,
			deleted_at  INTEGER NOT NULL, -- unix seconds
			purge_after INTEGER NOT NULL, -- unix seconds
			pending     INTEGER NOT NULL DEFAULT 1
		);
		CREATE INDEX tombstone_key ON tombstone (peer_key, sealed, key);
		CREATE INDEX tombstone_purge ON tombstone (pending, purge_after);`,
	},
	{
		Version: 20, Compat: 1, Release: "0.1",
		SQL: `
		-- Rewritable keys (EP-7, LEND-4, RET-1, #74). A captured write of
		-- a key its bucket's profile names rewritable: a changed version
		-- is sent to each place as a rewrite. On the lender, a tombstone
		-- of the version a rewrite replaced, which startup settles by
		-- moving the bytes back if the new version was never published.
		-- In recovery, whether a listed key is rewritable, and the
		-- versions its place replaced by rewrites (hex hashes, space
		-- separated), which name a newer version where places disagree.
		ALTER TABLE queue ADD COLUMN rewritable INTEGER;    -- 1: rewritable in the bucket's profile
		ALTER TABLE tombstone ADD COLUMN rewrite INTEGER;   -- 1: replaced by a rewrite, not deleted
		ALTER TABLE listed ADD COLUMN rewritable INTEGER;   -- 1: rewritable in the bucket's profile
		ALTER TABLE listed ADD COLUMN replaced TEXT;        -- hashes the place replaced by rewrites`,
	},
	{
		Version: 21, Compat: 1, Release: "0.1",
		SQL: `
		-- Deletes (REPL-2, #21): when the place's sender took the row to
		-- send it. A delete cancels a queued put no sender has taken,
		-- and follows one that was taken, which may have landed. A row
		-- queued now may have been sent by an older lendisk: taken, at
		-- an unknown time (0). placement.state gains 'abandoned': a
		-- taken row cancelled or re-routed unacknowledged.
		ALTER TABLE placement ADD COLUMN started_at INTEGER; -- unix seconds; NULL until taken
		UPDATE placement SET started_at = 0 WHERE state = 'queued';
		CREATE INDEX placement_queued_key ON placement (bucket, key) WHERE state = 'queued';`,
	},
	{
		Version: 22, Compat: 1, Release: "0.1",
		SQL: `
		-- The offsite watermark (REPL-11, #20): a pending row of a
		-- completed upload that cannot be matched to the object in the
		-- store (its upload ID unreadable, e.g. xattrs lost in a copy)
		-- may never settle; the reason, so status and an alert say so.
		-- Read only while the row is pending.
		ALTER TABLE queue ADD COLUMN unmatchable TEXT;`,
	},
	{
		Version: 23, Compat: 1, Release: "0.1",
		SQL: `
		-- The immutability policy (EP-11, #22). On a done put of a key
		-- that is not rewritable: the SHA-256 a place acknowledged for
		-- the key, which this write changed; such a put is placed
		-- nowhere. On a placement row refused with cause 'immutable' (a
		-- lender's 412): the SHA-256 the place said it holds, '' if it
		-- could not be learnt.
		ALTER TABLE queue ADD COLUMN immutable TEXT;
		ALTER TABLE placement ADD COLUMN held TEXT;
		CREATE INDEX queue_immutable ON queue (bucket, key) WHERE immutable IS NOT NULL;`,
	},
	{
		Version: 24, Compat: 1, Release: "0.1",
		SQL: `
		-- Ending a friendship (PEER-5, SEC-15, #25). The friendship row is
		-- deleted, which refuses both of the friend's keys; this row keeps
		-- what the retention window needs: both keys (to cut their open
		-- connections), the address (to tell the friend), and when its
		-- replica here, and the copies it holds of this node's data, are
		-- purged. Deleted after the purge, or when the two become friends
		-- again.
		CREATE TABLE former_friend (
			peer_key     TEXT PRIMARY KEY, -- the former friend's node ID
			dial_key     TEXT,             -- the key it dialled this node with (#69)
			address      TEXT,             -- its tc… address
			removed_at   INTEGER NOT NULL, -- unix seconds
			purge_after  INTEGER NOT NULL, -- unix seconds: removed_at plus the retention window
			by_peer      INTEGER,          -- 1: the friend ended it (its remove call)
			notified_at  INTEGER,          -- unix seconds: the friend was told, or told this node
			notify_error TEXT              -- why the last attempt to tell it failed
		);`,
	},
	{
		Version: 25, Compat: 1, Release: "0.1",
		SQL: `
		-- The quota call (LEND-7, LEND-8, PEER-4, #24): what each side last
		-- learnt of the other. told_number is this node's own number as
		-- the friend last acknowledged it in an answer: while it differs
		-- from own_number the change is still to be told. seen_at is when
		-- the friend last called, or answered a call. The friend's
		-- figures are its own report: its repository size (rounded up to
		-- a GiB), its count of what it stores here (peer_use), and, from
		-- an answer, its count as lender of what this node stores there
		-- (lent_there). path is how the last answered call went (direct
		-- or relayed); exchange_error why the last call failed, NULL once
		-- one is answered.
		ALTER TABLE friendship ADD COLUMN told_number INTEGER;
		ALTER TABLE friendship ADD COLUMN seen_at INTEGER;
		ALTER TABLE friendship ADD COLUMN peer_repo_size INTEGER;
		ALTER TABLE friendship ADD COLUMN peer_use INTEGER;
		ALTER TABLE friendship ADD COLUMN lent_there INTEGER;
		ALTER TABLE friendship ADD COLUMN peer_protocol TEXT;
		ALTER TABLE friendship ADD COLUMN path TEXT;
		ALTER TABLE friendship ADD COLUMN exchange_error TEXT;`,
	},
	{
		Version: 26, Compat: 1, Release: "0.1",
		SQL: `
		-- #24 review. reported_at is when the friend's own quota call last
		-- applied its number (unix nanoseconds): an answer to a call this
		-- node sent before then carries an older number and does not
		-- overwrite it.
		ALTER TABLE friendship ADD COLUMN reported_at INTEGER;
		-- What deletes at a place have taken off place_use since the
		-- place's last quota answer (#68): the lender keeps a deleted
		-- object counted until it purges the tomb, so the borrower's use
		-- there is place_use plus this until the next answer says
		-- otherwise. A place has a row from its first answer
		-- (state.DB.QuotaAnswered sets it to 0); before that its deletes
		-- free at once, as before. The triggers mirror place_use's for
		-- rows of negative size, and never take it below 0.
		CREATE TABLE place_freed (
			place TEXT PRIMARY KEY,
			bytes INTEGER NOT NULL
		);
		CREATE TRIGGER place_freed_insert AFTER INSERT ON placement
		WHEN NEW.state IN ('queued', 'acked') AND coalesce(NEW.size, 0) < 0 BEGIN
			UPDATE place_freed SET bytes = bytes - NEW.size WHERE place = NEW.place;
		END;
		CREATE TRIGGER place_freed_update AFTER UPDATE OF place, state, size ON placement BEGIN
			UPDATE place_freed SET bytes = max(bytes + OLD.size, 0)
				WHERE place = OLD.place AND OLD.state IN ('queued', 'acked') AND coalesce(OLD.size, 0) < 0;
			UPDATE place_freed SET bytes = bytes - NEW.size
				WHERE place = NEW.place AND NEW.state IN ('queued', 'acked') AND coalesce(NEW.size, 0) < 0;
		END;
		CREATE TRIGGER place_freed_delete AFTER DELETE ON placement
		WHEN OLD.state IN ('queued', 'acked') AND coalesce(OLD.size, 0) < 0 BEGIN
			UPDATE place_freed SET bytes = max(bytes + OLD.size, 0) WHERE place = OLD.place;
		END;`,
	},
	{
		Version: 27, Compat: 1, Release: "0.1",
		SQL: `
		-- Kopia format version 1 (COMPAT-1, #79). format1_at is when the
		-- bucket first captured a key only an unsupported old repository
		-- format writes (profile format1_keys), format1_key that key;
		-- upgraded_at when it captured the key the tool's upgrade out of
		-- that format writes (upgraded_keys). The bucket holds a format-1
		-- repository while format1_at is set and upgraded_at is not; a
		-- format-1 key captured after upgraded_at marks nothing. Unix
		-- seconds.
		ALTER TABLE buckets ADD COLUMN format1_at INTEGER;
		ALTER TABLE buckets ADD COLUMN format1_key TEXT;
		ALTER TABLE buckets ADD COLUMN upgraded_at INTEGER;`,
	},
	{
		Version: 28, Compat: 1, Release: "0.1",
		SQL: `
		-- Re-placement (REPL-13, #26). placement.state gains 'dropping': a
		-- copy the place holds (it was acked) that re-placement deletes
		-- there, in the place's FIFO as a delete of the key at the row's
		-- sequence; and 'drained': that delete acknowledged, the place
		-- keeping the bytes as a tombstone for the window (RET-1).
		-- replaces is, on a copy re-placement placed, the place whose
		-- copy it replaces: that copy is marked dropping when this one is
		-- acknowledged. replaced_at is when re-placement placed or
		-- queued the row again (unix seconds; NULL for a row placed at
		-- capture), so status tells re-placement from the backlog.
		ALTER TABLE placement ADD COLUMN replaces TEXT;
		ALTER TABLE placement ADD COLUMN replaced_at INTEGER;
		CREATE INDEX placement_dropping ON placement (place, seq) WHERE state = 'dropping';
		-- A copy marked dropping leaves place_use (it is no longer acked)
		-- but the lender counts it until it purges the tomb: like a
		-- delete's freed bytes, it is counted back in place_freed until the
		-- place's next quota answer (#24, #68).
		CREATE TRIGGER place_freed_dropping AFTER UPDATE OF state ON placement
		WHEN OLD.state = 'acked' AND NEW.state = 'dropping' AND coalesce(OLD.size, 0) > 0 BEGIN
			UPDATE place_freed SET bytes = bytes + OLD.size WHERE place = OLD.place;
		END;
		-- Per bucket, since when the re-placement pass has found live
		-- objects below copies with nowhere to put the missing copies
		-- (OPS-2's below-copies alert), and how many at its last pass. No
		-- row while none are.
		CREATE TABLE below_copies (
			bucket     TEXT PRIMARY KEY,
			since      INTEGER NOT NULL, -- unix seconds
			objects    INTEGER NOT NULL,
			bytes      INTEGER NOT NULL,
			checked_at INTEGER NOT NULL  -- unix seconds: the last pass
		);`,
	},
	{
		Version: 29, Compat: 1, Release: "0.1",
		SQL: `
		-- Verification (VER-1, VER-3, VER-4, #27): per friend and bucket,
		-- the challenges of that friend's replica. checked_at is the last
		-- answer, passed or failed, from which the next daily challenge is
		-- due; verified_at the last pass, full_at the last pass of
		-- verify --full; unverified_at the failure that marked the
		-- replica unverified (NULL while it is not), detail what failed.
		-- error and error_at are the last check that got no answer (a
		-- transient error, never a failure), retried within the hour. Unix
		-- seconds.
		CREATE TABLE replica_check (
			place         TEXT NOT NULL,
			bucket        TEXT NOT NULL,
			checked_at    INTEGER,
			verified_at   INTEGER,
			full_at       INTEGER,
			unverified_at INTEGER,
			detail        TEXT,
			error         TEXT,
			error_at      INTEGER,
			PRIMARY KEY (place, bucket)
		);
		-- lendisk verify asks the daemon for a check here and reads the
		-- outcome back (VER-3): verified, failed, error or nothing.
		CREATE TABLE verify_request (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			place        TEXT NOT NULL,
			bucket       TEXT NOT NULL,
			full         INTEGER NOT NULL,
			requested_at INTEGER NOT NULL,
			done_at      INTEGER,
			outcome      TEXT,
			checked      INTEGER,
			skipped      INTEGER,
			detail       TEXT
		);`,
	},
	{
		Version: 30, Compat: 1, Release: "0.1",
		SQL: `
		-- When a place last answered with the refusal PEER-5 prescribes for
		-- a removed node (condition removed, #83): with since, it tells a
		-- friend that keeps refusing from one stray refusal frozen in the
		-- condition. Unix seconds; NULL before any refusal.
		ALTER TABLE place ADD COLUMN refused_at INTEGER;`,
	},
	{
		Version: 31, Compat: 1, Release: "0.1",
		SQL: `
		-- Multipart replication (REPL-6, #23). Rows refused before #61 told
		-- the causes apart have cause 'store' though the only reason was
		-- their size: every object above one PUT (5 GiB) was refused for it
		-- before its file was looked at. They become 'oversize', so
		-- RequeueOversize matches the cause alone and never re-queues a
		-- genuine store refusal of a large object.
		UPDATE placement SET cause = 'oversize'
			WHERE state = 'refused' AND cause = 'store' AND op = 'put'
			AND (SELECT q.size FROM queue q WHERE q.seq = placement.seq) > 5368709120;
		-- A placement row's open multipart upload at its place: the upload
		-- ID, the part size it was begun with, and the version it was
		-- marked as replacing (x-amz-meta-lendisk-replaces, #74; NULL for
		-- none). Kept until the row is acknowledged or leaves the FIFO, so a
		-- restart continues at the next part (REPL-6).
		CREATE TABLE transfer (
			place      TEXT NOT NULL,
			seq        INTEGER NOT NULL,
			upload_id  TEXT NOT NULL,
			part_size  INTEGER NOT NULL,
			replaces   TEXT,
			started_at INTEGER NOT NULL, -- unix seconds
			PRIMARY KEY (place, seq)
		);
		-- Each part the place acknowledged: its number, ETag, SHA-256 (hex,
		-- the part's request checksum) and size.
		CREATE TABLE transfer_part (
			place  TEXT NOT NULL,
			seq    INTEGER NOT NULL,
			number INTEGER NOT NULL,
			etag   TEXT NOT NULL,
			sha256 TEXT NOT NULL,
			size   INTEGER NOT NULL,
			PRIMARY KEY (place, seq, number)
		);
		-- A borrower's multipart upload marked as replacing a version
		-- (LEND-4, #74, #23): the hash it named at create, checked again at
		-- completion. NULL for an upload of a new object.
		ALTER TABLE lent_upload ADD COLUMN replaces TEXT;`,
	},
	{
		Version: 32, Compat: 1, Release: "0.1",
		SQL: `
		-- Since when a place has not answered ok (#65, from the #21
		-- review): the start of its run of failing answers (retrying,
		-- quota, removed), kept as the condition changes among them and
		-- through a wait on this node's store, cleared by ok. A friend
		-- alternating between network errors and 507s is then away once
		-- the run lasts replication.reroute_after, though since restarts
		-- at each change. Unix seconds; NULL reads as since (rows from
		-- before this migration, and the previous release's writes).
		ALTER TABLE place ADD COLUMN failing_since INTEGER;`,
	},
	{
		Version: 33, Compat: 1, Release: "0.1",
		SQL: `
		-- The store reconciler (EP-14, #8). queue.local: when this node knew
		-- the done put's object to be in its own store (unix seconds): the
		-- capture stored it, the read-through fetched it, or a reconcile
		-- pass listed it. NULL on a recovered row until its object is read
		-- through, and on rows of earlier releases until a pass lists them.
		-- Only a key whose latest done row is a put marked here, and whose
		-- object the store no longer has, is enqueued as a delete.
		ALTER TABLE queue ADD COLUMN local INTEGER;
		-- 1 on a row the reconciler wrote (found outside the endpoint),
		-- NULL otherwise; status counts them.
		ALTER TABLE queue ADD COLUMN reconciled INTEGER;
		CREATE INDEX queue_reconciled ON queue (bucket) WHERE reconciled = 1;
		-- Per own bucket: the last pass, the deletes it held back for the
		-- user's confirmation (held of held_of objects held locally, since
		-- held_at), and the deletes the user allowed for the next pass
		-- (lendisk bucket reconcile --allow-deletes), tied to the hold it
		-- was given for (allowed_for = that hold's held_at, allowed_held =
		-- its count). window_seq: the queue's last seq when an allowance
		-- was last used; the delete limit counts the reconciler's deletes
		-- after it (and within 7 days). Unix seconds.
		CREATE TABLE reconcile (
			bucket       TEXT PRIMARY KEY,
			passed_at    INTEGER,
			held         INTEGER,
			held_of      INTEGER,
			held_at      INTEGER,
			allowed      INTEGER,
			allowed_for  INTEGER,
			allowed_held INTEGER,
			window_seq   INTEGER
		);`,
	},
	{
		Version: 34, Compat: 1, Release: "0.1",
		SQL: `
		-- Undelete (RET-3, #32): what the undelete command asks the daemon,
		-- which dials the friends. stage: 'asked' (the daemon previews it from
		-- the friends' lists), 'previewed' (preview holds the JSON the
		-- command shows), 'confirmed' (the user said yes: the daemon asks
		-- every friend to restore), then 'done' or 'failed' with the JSON
		-- result. since is unix seconds (0: every tomb in the window);
		-- rewrites is 1 when rewrites' tombs count too. requested_at and
		-- updated_at are unix seconds.
		CREATE TABLE undelete_request (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			bucket       TEXT NOT NULL,
			since        INTEGER NOT NULL,
			rewrites     INTEGER,
			requested_at INTEGER NOT NULL,
			stage        TEXT NOT NULL,
			preview      TEXT,
			result       TEXT,
			updated_at   INTEGER
		);`,
	},
	{
		Version: 35, Compat: 1, Release: "0.1",
		SQL: `
		-- The keys an undelete's preview listed (#32 review): what the user
		-- confirmed. A key a friend says it restored that this node has no
		-- history of is taken only if its request's preview listed it, so a
		-- friend cannot add keys the bucket never had. Dropped with the
		-- request.
		CREATE TABLE undelete_key (
			request INTEGER NOT NULL,
			key     TEXT NOT NULL,
			PRIMARY KEY (request, key)
		) WITHOUT ROWID;`,
	},
	{
		Version: 36, Compat: 1, Release: "0.1",
		SQL: `
		-- Bit-rot sampling (VER-5, #31). queue.stamp: the identity of the
		-- file whose bytes this node hashed to the done put's hash, recorded
		-- when it wrote or verified them ('<inode>:<size>:<mtime ns>'); NULL
		-- while unverified (rows of earlier releases, recovered rows, keys
		-- marked held from a listing). A file with another identity was
		-- edited; one with the same identity and other bytes is rot.
		ALTER TABLE queue ADD COLUMN stamp TEXT;
		-- bitrot_sweep is the sampler's place in its sweep of the store, one
		-- row: the last object it checked (store bucket, key), when the
		-- current cycle began, how many cycles it finished, the objects and
		-- bytes checked in this cycle, its last step and the bytes it owed
		-- then. Unix seconds.
		CREATE TABLE bitrot_sweep (
			id         INTEGER PRIMARY KEY CHECK (id = 1),
			bucket     TEXT NOT NULL,
			key        TEXT NOT NULL,
			cycle_at   INTEGER NOT NULL,
			cycles     INTEGER NOT NULL,
			objects    INTEGER NOT NULL,
			bytes      INTEGER NOT NULL,
			checked_at INTEGER,
			stepped_at INTEGER,
			owed       REAL
		);
		-- Objects whose bytes no longer hash to what the index holds, one row
		-- per store bucket and key. An own bucket's (borrower NULL): seq is
		-- the queue row whose hash they failed, stamp its queue.stamp then
		-- (another one: the version was stored again); refetch 1 while a
		-- friend holds the version and none has served it yet, repaired_at
		-- when a verified copy replaced it (or the key was written since),
		-- detail why it is not. A lent bucket's: borrower is the friend's
		-- node ID and sealed its bucket; the row lasts while the object
		-- does. Unix seconds.
		CREATE TABLE bitrot (
			bucket      TEXT NOT NULL,
			key         TEXT NOT NULL,
			borrower    TEXT,
			sealed      TEXT,
			seq         INTEGER,
			stamp       TEXT,
			sha256      TEXT NOT NULL,
			found       TEXT NOT NULL,
			size        INTEGER NOT NULL,
			found_at    INTEGER NOT NULL,
			refetch     INTEGER,
			repaired_at INTEGER,
			detail      TEXT,
			PRIMARY KEY (bucket, key)
		);`,
	},
}
