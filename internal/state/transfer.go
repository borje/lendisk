package state

import (
	"context"
	"database/sql"
	"errors"
)

// Transfer is a placement row's open multipart upload at its place
// (REPL-6, #23): kept as parts complete, so a restart or an interruption
// continues at the next part. It belongs to the placement row (place,
// seq), as each place has its own upload of the queue row's object.
type Transfer struct {
	Place    string
	Seq      int64
	UploadID string
	PartSize int64
	// Replaces is the version the upload was created marked as replacing
	// (x-amz-meta-lendisk-replaces, #74), "" for none.
	Replaces string
	Parts    []TransferPart // by number
	// Bucket and Key are the row's, filled by StaleTransfers.
	Bucket, Key string
}

// TransferPart is one part the place acknowledged.
type TransferPart struct {
	Number int
	ETag   string
	SHA256 string // hex: the part's request checksum
	Size   int64
}

// BeginTransfer records the upload t begins for its row, replacing any
// earlier one of the row and its parts (that upload was gone at the
// place, or another part size or mark was needed).
func (db *DB) BeginTransfer(ctx context.Context, t Transfer) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM transfer_part WHERE place = ? AND seq = ?`, t.Place, t.Seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO transfer (place, seq, upload_id, part_size, replaces, started_at)
		VALUES (?, ?, ?, ?, ?, ?)`, t.Place, t.Seq, t.UploadID, t.PartSize, nullStr(t.Replaces), db.now().Unix()); err != nil {
		return err
	}
	return tx.Commit()
}

// AddTransferPart records a part the place acknowledged; a part of that
// number recorded before is replaced.
func (db *DB) AddTransferPart(ctx context.Context, place string, seq int64, p TransferPart) error {
	_, err := db.ExecContext(ctx, `INSERT OR REPLACE INTO transfer_part (place, seq, number, etag, sha256, size)
		VALUES (?, ?, ?, ?, ?, ?)`, place, seq, p.Number, p.ETag, p.SHA256, p.Size)
	return err
}

// Transfer returns the row's open upload at place with its parts; ok is
// false when there is none.
func (db *DB) Transfer(ctx context.Context, place string, seq int64) (Transfer, bool, error) {
	t := Transfer{Place: place, Seq: seq}
	var replaces sql.NullString
	err := db.QueryRowContext(ctx, `SELECT upload_id, part_size, replaces FROM transfer WHERE place = ? AND seq = ?`,
		place, seq).Scan(&t.UploadID, &t.PartSize, &replaces)
	if errors.Is(err, sql.ErrNoRows) {
		return Transfer{}, false, nil
	}
	if err != nil {
		return Transfer{}, false, err
	}
	t.Replaces = replaces.String
	rows, err := db.QueryContext(ctx, `SELECT number, etag, sha256, size FROM transfer_part
		WHERE place = ? AND seq = ? ORDER BY number`, place, seq)
	if err != nil {
		return Transfer{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var p TransferPart
		if err := rows.Scan(&p.Number, &p.ETag, &p.SHA256, &p.Size); err != nil {
			return Transfer{}, false, err
		}
		t.Parts = append(t.Parts, p)
	}
	return t, true, rows.Err()
}

// DropTransfer forgets the row's upload at place and its parts.
func (db *DB) DropTransfer(ctx context.Context, place string, seq int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM transfer_part WHERE place = ? AND seq = ?`, place, seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM transfer WHERE place = ? AND seq = ?`, place, seq); err != nil {
		return err
	}
	return tx.Commit()
}

// StaleTransfers returns the uploads whose placement row is no longer
// queued at their place (acknowledged, refused, cancelled, re-routed or
// gone), with the row's bucket and key, for the worker to abort at the
// place and forget. Parts are not read.
func (db *DB) StaleTransfers(ctx context.Context) ([]Transfer, error) {
	rows, err := db.QueryContext(ctx, `SELECT t.place, t.seq, t.upload_id, t.part_size, coalesce(t.replaces, ''),
		coalesce(q.bucket, ''), coalesce(q.key, '')
		FROM transfer t LEFT JOIN queue q ON q.seq = t.seq
		WHERE NOT EXISTS (SELECT 1 FROM placement p WHERE p.place = t.place AND p.seq = t.seq AND p.state = 'queued')
		ORDER BY t.place, t.seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Transfer
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.Place, &t.Seq, &t.UploadID, &t.PartSize, &t.Replaces, &t.Bucket, &t.Key); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
