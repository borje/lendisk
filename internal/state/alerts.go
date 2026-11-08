package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Alert is a pending or cleared alert (OPS-2): one row per condition,
// keyed by kind and subject, so a condition that holds raises it once.
type Alert struct {
	Key     string // Kind, or Kind:Subject
	Kind    string
	Subject string // the friend's node ID, the bucket, or empty
	Message string
	// Raise counts the key's raises (1, 2, …): a delivery names it, so
	// the send of an earlier raise never marks a later one.
	Raise int64
	// RaisedAt is when the condition was first seen; ClearedAt when it
	// ended, zero while it holds.
	RaisedAt, ClearedAt time.Time
	// HookAt and WebhookAt are when this raise was delivered to each
	// channel (OPS-3); zero until then.
	HookAt, WebhookAt time.Time
}

// Delivery channels (OPS-3), for MarkAlertDelivered.
const (
	ChannelHook    = "hook"
	ChannelWebhook = "webhook"
)

// SyncAlerts makes the active alerts of kinds exactly current, at now:
// an alert in current that is not active is raised (a new row, or a
// cleared row raised anew with its deliveries reset); one already
// active keeps its raise and deliveries and takes the new message; an
// active alert of kinds missing from current is cleared. Alerts of other
// kinds are untouched. It returns what it raised and cleared.
func (db *DB) SyncAlerts(ctx context.Context, kinds []string, current []Alert, now time.Time) (raised, cleared []Alert, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	owned := map[string]bool{}
	for _, k := range kinds {
		owned[k] = true
	}
	active := map[string]Alert{}
	rows, err := tx.QueryContext(ctx, alertSelect+` WHERE cleared_at IS NULL`)
	if err != nil {
		return nil, nil, err
	}
	as, err := scanAlerts(rows)
	if err != nil {
		return nil, nil, err
	}
	for _, a := range as {
		if owned[a.Kind] {
			active[a.Key] = a
		}
	}
	seen := map[string]bool{}
	for _, a := range current {
		if !owned[a.Kind] {
			return nil, nil, fmt.Errorf("alert %s: kind %s is not among %v", a.Key, a.Kind, kinds)
		}
		seen[a.Key] = true
		if prev, ok := active[a.Key]; ok {
			if prev.Message != a.Message {
				if _, err := tx.ExecContext(ctx, `UPDATE alert SET message = ? WHERE key = ?`, a.Message, a.Key); err != nil {
					return nil, nil, err
				}
			}
			continue
		}
		if err := tx.QueryRowContext(ctx, `INSERT INTO alert (key, kind, subject, message, raise_no, raised_at)
			VALUES (?, ?, ?, ?, 1, ?)
			ON CONFLICT (key) DO UPDATE SET kind = excluded.kind, subject = excluded.subject,
				message = excluded.message, raise_no = alert.raise_no + 1, raised_at = excluded.raised_at,
				cleared_at = NULL, hook_at = NULL, webhook_at = NULL
			RETURNING raise_no`,
			a.Key, a.Kind, a.Subject, a.Message, now.Unix()).Scan(&a.Raise); err != nil {
			return nil, nil, err
		}
		a.RaisedAt, a.ClearedAt, a.HookAt, a.WebhookAt = time.Unix(now.Unix(), 0), time.Time{}, time.Time{}, time.Time{}
		raised = append(raised, a)
	}
	for key, a := range active {
		if seen[key] {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE alert SET cleared_at = ? WHERE key = ?`, now.Unix(), key); err != nil {
			return nil, nil, err
		}
		a.ClearedAt = time.Unix(now.Unix(), 0)
		cleared = append(cleared, a)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return raised, cleared, nil
}

// ActiveAlerts returns the alerts whose condition holds, oldest first.
func (db *DB) ActiveAlerts(ctx context.Context) ([]Alert, error) {
	rows, err := db.QueryContext(ctx, alertSelect+` WHERE cleared_at IS NULL ORDER BY raised_at, key`)
	if err != nil {
		return nil, err
	}
	return scanAlerts(rows)
}

// MarkAlertDelivered records that a's raise (a.Raise) reached channel
// at now. It does nothing when the alert was cleared or raised anew
// since, even within the same second: that raise is delivered on its
// own.
func (db *DB) MarkAlertDelivered(ctx context.Context, a Alert, channel string, now time.Time) error {
	col := map[string]string{ChannelHook: "hook_at", ChannelWebhook: "webhook_at"}[channel]
	if col == "" {
		return fmt.Errorf("unknown alert channel %q", channel)
	}
	_, err := db.ExecContext(ctx, `UPDATE alert SET `+col+` = ?
		WHERE key = ? AND raise_no = ? AND cleared_at IS NULL`, now.Unix(), a.Key, a.Raise)
	return err
}

// RefusedAtPlaces returns, per place, how many rows the place itself
// refused for good (LEND-4) and not written again since: the Refused of
// PlaceStats without the FIFO's scan. Its immutability refusals (412,
// EP-11) count too, though ChangedKeys also reports them: a friend
// answering 412 to everything is at fault, and place_refused names it
// (#22 review). Places with none are absent.
func (db *DB) RefusedAtPlaces(ctx context.Context) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT place, count(*) FROM placement p
		WHERE state = 'refused' AND (cause IS NULL OR cause = '`+CauseImmutable+`') AND `+current+` GROUP BY place`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var p string
		var n int64
		if err := rows.Scan(&p, &n); err != nil {
			return nil, err
		}
		out[p] = n
	}
	return out, rows.Err()
}

const alertSelect = `SELECT key, kind, subject, message, raise_no, raised_at, cleared_at, hook_at, webhook_at FROM alert`

func scanAlerts(rows *sql.Rows) ([]Alert, error) {
	defer rows.Close()
	var out []Alert
	unix := func(v sql.NullInt64) time.Time {
		if !v.Valid {
			return time.Time{}
		}
		return time.Unix(v.Int64, 0)
	}
	for rows.Next() {
		var a Alert
		var raised int64
		var cleared, hook, webhook sql.NullInt64
		if err := rows.Scan(&a.Key, &a.Kind, &a.Subject, &a.Message, &a.Raise, &raised, &cleared, &hook, &webhook); err != nil {
			return nil, err
		}
		a.RaisedAt, a.ClearedAt, a.HookAt, a.WebhookAt = time.Unix(raised, 0), unix(cleared), unix(hook), unix(webhook)
		out = append(out, a)
	}
	return out, rows.Err()
}
