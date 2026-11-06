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
			sha256     TEXT,             -- put: hex; multipart: <hex>-<parts>
			token      TEXT,             -- PutObject: its lendisk-capture metadata
			etag       TEXT,             -- multipart: the ETag it completes with
			upload_id  TEXT,             -- multipart: the upload it completed
			created_at INTEGER NOT NULL  -- unix seconds
		);
		CREATE INDEX queue_key ON queue (bucket, key, seq);
		CREATE INDEX queue_pending ON queue (state) WHERE state = 'pending';
		-- A multipart object's parts, in order (REPL-10, §3.3).
		CREATE TABLE queue_part (
			seq    INTEGER NOT NULL REFERENCES queue (seq) ON DELETE CASCADE,
			number INTEGER NOT NULL,
			size   INTEGER NOT NULL,
			sha256 TEXT NOT NULL,
			PRIMARY KEY (seq, number)
		);
		-- Parts of open multipart uploads, hashed as they were written.
		CREATE TABLE upload_part (
			upload_id TEXT NOT NULL,
			number    INTEGER NOT NULL,
			size      INTEGER NOT NULL,
			sha256    TEXT NOT NULL,
			PRIMARY KEY (upload_id, number)
		);`,
	},
}
