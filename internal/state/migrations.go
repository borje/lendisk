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
		// Schema 13 in DESIGN: #28 takes 12 and merges first. Version 12
		// here only until this branch is rebased onto #28, as versions
		// must have no gaps; the rebase makes it 13.
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
}
