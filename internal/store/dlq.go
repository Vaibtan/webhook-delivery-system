package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// AckDLQ is the atomic claim that IS the ack: it clears in_dlq for the single
// (subscription, webhook) DLQ row. Exactly one concurrent caller wins (the WHERE
// in_dlq=TRUE predicate); others get ok=false → 404.
func (r *DeliveryLogRepo) AckDLQ(ctx context.Context, subscriptionID, webhookID string) (string, bool, error) {
	const q = `UPDATE delivery_logs SET in_dlq = FALSE, updated_at = NOW()
		WHERE subscription_id = $1 AND webhook_id = $2 AND in_dlq = TRUE
		RETURNING id`
	var claimedID string
	err := r.pool.QueryRow(ctx, q, subscriptionID, webhookID).Scan(&claimedID)
	if err != nil {
		if isAbsent(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("store: ack dlq: %w", err)
	}
	return claimedID, true, nil
}

// ReplayDLQ atomically claims the DLQ row and, in the same transaction, inserts a
// fresh pending row that starts a NEW chain (replay_number+1, attempt_number=1,
// next_retry_at=now()) under the same webhook_id. Exactly one concurrent replay
// wins. Returns the claimed (old) id and the new chain id; ok=false → 404.
func (r *DeliveryLogRepo) ReplayDLQ(ctx context.Context, subscriptionID, webhookID string) (string, string, bool, error) {
	var claimedID, newID string
	ok := false
	err := r.inTx(ctx, "replay", func(tx pgx.Tx) error {
		const claim = `UPDATE delivery_logs SET in_dlq = FALSE, updated_at = NOW()
			WHERE subscription_id = $1 AND webhook_id = $2 AND in_dlq = TRUE
			RETURNING id, replay_number`
		var replayNumber int
		if err := tx.QueryRow(ctx, claim, subscriptionID, webhookID).Scan(&claimedID, &replayNumber); err != nil {
			if isAbsent(err) {
				return nil // no DLQ entry to claim ⇒ ok stays false (idempotent no-op)
			}
			return fmt.Errorf("store: claim replay: %w", err)
		}

		const insertNewChain = `
			INSERT INTO delivery_logs
				(webhook_id, subscription_id, target_url, payload, event_type,
				 attempt_number, replay_number, status, next_retry_at)
			SELECT webhook_id, subscription_id, target_url, payload, event_type,
				   1, $2, 'pending', NOW()
			FROM delivery_logs WHERE id = $1
			RETURNING id`
		if err := tx.QueryRow(ctx, insertNewChain, claimedID, replayNumber+1).Scan(&newID); err != nil {
			return fmt.Errorf("store: insert replay chain: %w", err)
		}
		ok = true
		return nil
	})
	if err != nil {
		return "", "", false, err
	}
	return claimedID, newID, ok, nil
}

// DeleteExpired is the sole retention-driven deleter (plan Adv §1). It removes
// only terminal, non-DLQ rows past the retention horizon — NEVER pending
// (undelivered work) or in_dlq (un-triaged) rows.
func (r *DeliveryLogRepo) DeleteExpired(ctx context.Context, retention time.Duration) (int64, error) {
	const q = `DELETE FROM delivery_logs
		WHERE created_at < NOW() - ($1 || ' seconds')::interval
		  AND NOT in_dlq
		  AND status <> 'pending'`
	tag, err := r.pool.Exec(ctx, q, secondsArg(retention))
	if err != nil {
		return 0, fmt.Errorf("store: delete expired: %w", err)
	}
	return tag.RowsAffected(), nil
}

// PruneIdempotency removes ingest_idempotency rows older than ttl (plan §5).
func (r *DeliveryLogRepo) PruneIdempotency(ctx context.Context, ttl time.Duration) (int64, error) {
	const q = `DELETE FROM ingest_idempotency WHERE created_at < NOW() - ($1 || ' seconds')::interval`
	tag, err := r.pool.Exec(ctx, q, secondsArg(ttl))
	if err != nil {
		return 0, fmt.Errorf("store: prune idempotency: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ListInDLQ returns ids of all rows currently flagged in_dlq=TRUE.
func (r *DeliveryLogRepo) ListInDLQ(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT id FROM delivery_logs WHERE in_dlq = TRUE`)
	if err != nil {
		return nil, fmt.Errorf("store: list in_dlq: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0, 16)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan in_dlq id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// IsInDLQ reports whether the given row is currently flagged in_dlq=TRUE.
func (r *DeliveryLogRepo) IsInDLQ(ctx context.Context, id string) (bool, error) {
	var inDLQ bool
	err := r.pool.QueryRow(ctx, `SELECT in_dlq FROM delivery_logs WHERE id = $1`, id).Scan(&inDLQ)
	if err != nil {
		if isAbsent(err) {
			return false, nil // gone ⇒ treat as not-in-DLQ (reconciler LREMs it)
		}
		return false, fmt.Errorf("store: is in_dlq: %w", err)
	}
	return inDLQ, nil
}

// ForceAckOldDLQ clears in_dlq for entries older than maxAge (the DLQ_MAX_AGE
// safety valve), returning affected ids so the caller can LREM + log.
func (r *DeliveryLogRepo) ForceAckOldDLQ(ctx context.Context, maxAge time.Duration) ([]string, error) {
	const q = `UPDATE delivery_logs SET in_dlq = FALSE, updated_at = NOW()
		WHERE in_dlq = TRUE AND updated_at < NOW() - ($1 || ' seconds')::interval
		RETURNING id`
	rows, err := r.pool.Query(ctx, q, secondsArg(maxAge))
	if err != nil {
		return nil, fmt.Errorf("store: force-ack old dlq: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0, 8)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan force-ack id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// secondsArg renders a duration as an integer-seconds text arg for an interval cast.
func secondsArg(d time.Duration) string {
	s := int64(d.Seconds())
	if s < 0 {
		s = 0
	}
	return strconv.FormatInt(s, 10)
}
