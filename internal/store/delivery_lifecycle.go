package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// IngestPending runs the atomic ingest transaction (plan §5):
//
//  1. If an idempotency key is present, INSERT … ON CONFLICT DO NOTHING. A first
//     key takes the row; a repeat key conflicts → read back the stored webhook_id
//     and return it as an idempotent no-op (no new delivery row, no enqueue).
//  2. Otherwise (first key, or no key) INSERT the initial pending delivery row.
//
// The dedupe record and the delivery row commit together, so a crash can never
// acknowledge a key whose webhook row was not durably inserted.
func (r *DeliveryLogRepo) IngestPending(ctx context.Context, p domain.IngestParams) (domain.IngestResult, error) {
	var result domain.IngestResult
	err := r.inTx(ctx, "ingest", func(tx pgx.Tx) error {
		webhookID := p.WebhookID

		if p.IdempotencyKey != "" {
			const insertIdem = `
				INSERT INTO ingest_idempotency (subscription_id, idempotency_key, webhook_id)
				VALUES ($1, $2, $3)
				ON CONFLICT (subscription_id, idempotency_key) DO NOTHING
				RETURNING webhook_id`
			var inserted string
			err := tx.QueryRow(ctx, insertIdem, p.SubscriptionID, p.IdempotencyKey, webhookID).Scan(&inserted)
			switch {
			case err == nil:
				// First time for this key — proceed to insert the delivery row.
				webhookID = inserted
			case errors.Is(err, pgx.ErrNoRows):
				// Conflict: the INSERT blocked until the prior txn committed, so the
				// stored row is now visible. Return the stored webhook_id; the tx
				// commits as an idempotent no-op.
				const selectIdem = `SELECT webhook_id FROM ingest_idempotency
					WHERE subscription_id = $1 AND idempotency_key = $2`
				var stored string
				if err := tx.QueryRow(ctx, selectIdem, p.SubscriptionID, p.IdempotencyKey).Scan(&stored); err != nil {
					return fmt.Errorf("store: read stored idempotency webhook_id: %w", err)
				}
				result = domain.IngestResult{WebhookID: stored, Duplicate: true}
				return nil
			default:
				return fmt.Errorf("store: insert idempotency: %w", err)
			}
		}

		const insertPending = `
			INSERT INTO delivery_logs
				(webhook_id, subscription_id, target_url, payload, event_type,
				 attempt_number, replay_number, status, next_retry_at)
			VALUES ($1, $2, $3, $4, $5, 1, 0, 'pending', NOW())
			RETURNING id`
		var logID string
		err := tx.QueryRow(ctx, insertPending,
			webhookID, p.SubscriptionID, p.TargetURL, p.Payload, nullStr(p.EventType),
		).Scan(&logID)
		if err != nil {
			if isForeignKeyViolation(err) {
				return domain.ErrNotFound // subscription gone
			}
			return fmt.Errorf("store: insert pending delivery_log: %w", err)
		}
		result = domain.IngestResult{WebhookID: webhookID, DeliveryLogID: logID}
		return nil
	})
	if err != nil {
		return domain.IngestResult{}, err
	}
	return result, nil
}

// CountByStatusSince counts attempt rows by status created at/after `since`.
func (r *DeliveryLogRepo) CountByStatusSince(ctx context.Context, since time.Time) (domain.StatusCounts, error) {
	const q = `SELECT status, COUNT(*) FROM delivery_logs WHERE created_at >= $1 GROUP BY status`
	rows, err := r.pool.Query(ctx, q, since)
	if err != nil {
		return nil, fmt.Errorf("store: count by status: %w", err)
	}
	defer rows.Close()

	counts := make(domain.StatusCounts, 4)
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("store: scan status count: %w", err)
		}
		counts[domain.DeliveryStatus(status)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate status counts: %w", err)
	}
	return counts, nil
}

// MarkSuccess CAS-transitions a pending row to success AND resets the
// subscription's consecutive_failures to 0 (only when non-zero) in ONE
// transaction (plan Adv §2). The status='pending' predicate makes only the first
// finisher win; if it loses the CAS, the counter is left untouched.
func (r *DeliveryLogRepo) MarkSuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (bool, error) {
	won := false
	err := r.inTx(ctx, "success", func(tx pgx.Tx) error {
		const cas = `UPDATE delivery_logs
			SET status = 'success', http_status = $2, error_details = NULL, updated_at = NOW()
			WHERE id = $1 AND status = 'pending'`
		tag, err := tx.Exec(ctx, cas, id, httpStatus)
		if err != nil {
			return fmt.Errorf("store: mark success: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // lost the CAS
		}
		const reset = `UPDATE subscriptions SET consecutive_failures = 0, updated_at = NOW()
			WHERE id = $1 AND consecutive_failures <> 0`
		if _, err := tx.Exec(ctx, reset, subscriptionID); err != nil {
			return fmt.Errorf("store: reset consecutive_failures: %w", err)
		}
		won = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return won, nil
}

// FinalizeFailure is the terminal-failure transaction (plan §0/Adv §1/Adv §2): in
// ONE tx it CAS-transitions the row to final_failure + in_dlq=TRUE AND increments
// the subscription's consecutive_failures, auto-disabling it (is_active=FALSE) at
// disableThreshold. Returns won (CAS succeeded → caller LPUSHes the DLQ) and
// disabled (the sub just crossed the threshold → caller evicts its cache).
func (r *DeliveryLogRepo) FinalizeFailure(ctx context.Context, id, subscriptionID string, httpStatus *int, errorDetails string, disableThreshold int) (bool, bool, error) {
	won, disabled := false, false
	err := r.inTx(ctx, "finalize", func(tx pgx.Tx) error {
		const cas = `UPDATE delivery_logs
			SET status = 'final_failure', in_dlq = TRUE, http_status = $2, error_details = $3, updated_at = NOW()
			WHERE id = $1 AND status = 'pending'`
		tag, err := tx.Exec(ctx, cas, id, httpStatus, nullStr(errorDetails))
		if err != nil {
			return fmt.Errorf("store: cas final_failure: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // lost the CAS
		}

		const bump = `UPDATE subscriptions
			SET consecutive_failures = consecutive_failures + 1,
			    is_active = (consecutive_failures + 1 < $2) AND is_active,
			    updated_at = NOW()
			WHERE id = $1
			RETURNING is_active`
		var isActive bool
		switch err := tx.QueryRow(ctx, bump, subscriptionID, disableThreshold).Scan(&isActive); {
		case err == nil:
			disabled = !isActive // was active (deliverer only finalizes active subs) ⇒ just disabled
		case errors.Is(err, pgx.ErrNoRows):
			// Subscription vanished (concurrent delete cascade); the final_failure
			// stands and there is nothing to disable.
		default:
			return fmt.Errorf("store: bump consecutive_failures: %w", err)
		}
		won = true
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return won, disabled, nil
}

// MarkFinalFailure CAS-transitions a pending row to final_failure AND sets
// in_dlq=TRUE in the SAME statement, so the durable DLQ signal commits atomically
// with the terminal status (plan Adv §1). The caller LPUSHes post-commit.
func (r *DeliveryLogRepo) MarkFinalFailure(ctx context.Context, id string, httpStatus *int, errorDetails string) (bool, error) {
	const q = `UPDATE delivery_logs
		SET status = 'final_failure', in_dlq = TRUE, http_status = $2, error_details = $3, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'`
	tag, err := r.pool.Exec(ctx, q, id, httpStatus, nullStr(errorDetails))
	if err != nil {
		return false, fmt.Errorf("store: mark final_failure: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FailAndScheduleRetry runs the failed→successor transaction (plan §0/§4b):
//
//  1. CAS the prev row pending→failed_attempt (0 rows ⇒ another worker already
//     finalized it ⇒ won=false, abort with no side effects).
//  2. INSERT a successor pending row that copies the chain identity and bumps
//     attempt_number, with the supplied next_retry_at.
//
// Both happen in one transaction, so a crash can never leave a failed_attempt
// with no pending successor. The caller ZADDs successorID post-commit.
func (r *DeliveryLogRepo) FailAndScheduleRetry(ctx context.Context, prevID string, httpStatus *int, errorDetails string, nextRetryAt time.Time) (string, bool, error) {
	var successorID string
	won := false
	err := r.inTx(ctx, "retry", func(tx pgx.Tx) error {
		const casPrev = `UPDATE delivery_logs
			SET status = 'failed_attempt', http_status = $2, error_details = $3, updated_at = NOW()
			WHERE id = $1 AND status = 'pending'`
		tag, err := tx.Exec(ctx, casPrev, prevID, httpStatus, nullStr(errorDetails))
		if err != nil {
			return fmt.Errorf("store: cas failed_attempt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // lost the CAS — already finalized elsewhere
		}

		const insertSuccessor = `
			INSERT INTO delivery_logs
				(webhook_id, subscription_id, target_url, payload, event_type,
				 attempt_number, replay_number, status, next_retry_at)
			SELECT webhook_id, subscription_id, target_url, payload, event_type,
				   attempt_number + 1, replay_number, 'pending', $2
			FROM delivery_logs WHERE id = $1
			RETURNING id`
		if err := tx.QueryRow(ctx, insertSuccessor, prevID, nextRetryAt).Scan(&successorID); err != nil {
			return fmt.Errorf("store: insert retry successor: %w", err)
		}
		won = true
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return successorID, won, nil
}

// RescheduleSamePending pushes a pending row's next_retry_at to `at` WITHOUT
// consuming an attempt (breaker denial / per-sub concurrency requeue, plan §3 /
// Adv §3). The caller ZADDs post-commit. attempt_number is untouched.
func (r *DeliveryLogRepo) RescheduleSamePending(ctx context.Context, id string, at time.Time) (bool, error) {
	const q = `UPDATE delivery_logs SET next_retry_at = $2, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'`
	tag, err := r.pool.Exec(ctx, q, id, at)
	if err != nil {
		return false, fmt.Errorf("store: reschedule same pending: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FindDuePending returns ids of pending rows whose next_retry_at is past due —
// either a lost enqueue or a crashed worker whose lease expired. In-flight rows
// are hidden by their (future) lease, so they are not returned.
func (r *DeliveryLogRepo) FindDuePending(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `SELECT id FROM delivery_logs
		WHERE status = 'pending' AND next_retry_at <= NOW()
		ORDER BY next_retry_at ASC
		LIMIT $1`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("store: find due pending: %w", err)
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan due pending id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate due pending: %w", err)
	}
	return ids, nil
}

// ClaimPending applies the visibility lease (plan §0/§1): a conditional CAS that
// stamps next_retry_at into the future ONLY if the row is pending AND currently
// due (next_retry_at <= now()). The due predicate is what makes the claim the
// mutual-exclusion point — two workers draining duplicate queue entries for the
// same still-pending row cannot both pass it. Returns true iff this caller
// claimed the row. The lease is computed with the DB clock (NOW()) for consistency.
func (r *DeliveryLogRepo) ClaimPending(ctx context.Context, id string, lease time.Duration) (bool, error) {
	const q = `UPDATE delivery_logs
		SET next_retry_at = NOW() + ($2 || ' seconds')::interval, updated_at = NOW()
		WHERE id = $1 AND status = 'pending' AND next_retry_at <= NOW()`
	leaseSecs := int64(lease.Seconds())
	if leaseSecs < 1 {
		leaseSecs = 1
	}
	tag, err := r.pool.Exec(ctx, q, id, strconv.FormatInt(leaseSecs, 10))
	if err != nil {
		if isInvalidUUID(err) {
			return false, nil
		}
		return false, fmt.Errorf("store: claim pending: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// inTx runs fn inside a single transaction. It owns the begin/rollback/commit
// ritual — and crucially the deferred Rollback — so the connection-leak-safety
// invariant lives in ONE place instead of being re-typed by every transactional
// method. fn does only its SQL: a non-nil return rolls back and is propagated
// verbatim (so sentinels like domain.ErrNotFound survive errors.Is at the
// caller); a nil return commits. op labels begin/commit failures.
func (r *DeliveryLogRepo) inTx(ctx context.Context, op string, fn func(pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin %s tx: %w", op, err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit %s: %w", op, err)
	}
	return nil
}
