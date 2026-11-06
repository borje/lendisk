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
}
