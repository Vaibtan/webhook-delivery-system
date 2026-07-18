package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// IngestPending runs the atomic ingest transaction:
//
//  1. If an idempotency key is present, INSERT … ON CONFLICT DO NOTHING. A first
//     key takes the row; a repeat key conflicts → read back the stored webhook_id
//     and return it as an idempotent no-op (no new delivery row, no enqueue).
//  2. Otherwise (first key, or no key) INSERT the initial pending delivery row.
//
// The dedupe record and the delivery row commit together, so a crash can never
// acknowledge a key whose webhook row was not durably inserted.
func (r *DeliveryLogRepo) IngestPending(ctx context.Context, p domain.IngestParams) (domain.IngestResult, error) {
	if !json.Valid(p.Payload) {
		return domain.IngestResult{}, fmt.Errorf("%w: payload must be valid JSON", domain.ErrInvalidInput)
	}
	if len(p.TargetURL) > domain.MaxTargetURLLength {
		return domain.IngestResult{}, fmt.Errorf("%w: target_url must be at most %d characters", domain.ErrInvalidInput, domain.MaxTargetURLLength)
	}
	if err := domain.ValidateEventType(p.EventType); err != nil {
		return domain.IngestResult{}, err
	}
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
				if isForeignKeyViolation(err) {
					return domain.ErrNotFound // subscription deleted after authentication
				}
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

// MarkSuccess CAS-transitions a pending row to success and resets a non-zero
// subscription failure counter in the same transaction. reset reports whether
// cached subscription state changed.
func (r *DeliveryLogRepo) MarkSuccess(ctx context.Context, id, subscriptionID string, httpStatus int) (won, reset bool, err error) {
	err = r.inTx(ctx, "success", func(tx pgx.Tx) error {
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
		const resetCounter = `UPDATE subscriptions SET consecutive_failures = 0, updated_at = NOW()
			WHERE id = $1 AND consecutive_failures <> 0`
		tag, err = tx.Exec(ctx, resetCounter, subscriptionID)
		if err != nil {
			return fmt.Errorf("store: reset consecutive_failures: %w", err)
		}
		won = true
		reset = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return won, reset, nil
}

// FinalizeFailure CAS-transitions the row to final_failure + in_dlq=TRUE and increments
// the subscription's consecutive_failures, auto-disabling it (is_active=FALSE) at
// disableThreshold. Returns won (CAS succeeded → caller LPUSHes the DLQ) and
// disabled (the subscription crossed the threshold).
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

// MarkDropped CAS-transitions a pending row to a terminal, non-DLQ state. The
// final_failure status remains useful to operators, while in_dlq=FALSE prevents
// the reconciler from publishing an intentional policy drop to the DLQ.
func (r *DeliveryLogRepo) MarkDropped(ctx context.Context, id string, errorDetails string) (bool, error) {
	const q = `UPDATE delivery_logs
		SET status = 'final_failure', in_dlq = FALSE, http_status = NULL,
			error_details = $2, next_retry_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'`
	tag, err := r.pool.Exec(ctx, q, id, nullStr(errorDetails))
	if err != nil {
		return false, fmt.Errorf("store: mark dropped: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FailAndScheduleRetry runs the failed-to-successor transaction:
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
// consuming an attempt. The caller schedules it after commit; attempt_number is
// untouched.
func (r *DeliveryLogRepo) RescheduleSamePending(ctx context.Context, id string, at time.Time) (bool, error) {
	const q = `UPDATE delivery_logs SET next_retry_at = $2, updated_at = NOW()
		WHERE id = $1 AND status = 'pending'`
	tag, err := r.pool.Exec(ctx, q, id, at)
	if err != nil {
		return false, fmt.Errorf("store: reschedule same pending: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FindDuePending returns a stable keyset page of due pending rows.
func (r *DeliveryLogRepo) FindDuePending(ctx context.Context, limit int, after *domain.RecoveryCursor) ([]domain.DuePending, error) {
	const base = `SELECT id, next_retry_at
		FROM delivery_logs
		WHERE status = 'pending' AND next_retry_at <= NOW()`
	var (
		rows pgx.Rows
		err  error
	)
	if after == nil {
		rows, err = r.pool.Query(ctx, base+`
			ORDER BY next_retry_at ASC, id ASC
			LIMIT $1`, limit)
	} else {
		rows, err = r.pool.Query(ctx, base+`
			AND (next_retry_at > $2 OR (next_retry_at = $2 AND id > $3))
			ORDER BY next_retry_at ASC, id ASC
			LIMIT $1`, limit, after.DueAt, after.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: find due pending: %w", err)
	}
	defer rows.Close()

	items := make([]domain.DuePending, 0, limit)
	for rows.Next() {
		var item domain.DuePending
		if err := rows.Scan(&item.ID, &item.DueAt); err != nil {
			return nil, fmt.Errorf("store: scan due pending: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate due pending: %w", err)
	}
	return items, nil
}

// RearmLeaseLessPending atomically claims and rearms one batch of sufficiently
// old rows that have no visibility lease. SKIP LOCKED lets concurrent recovery
// workers divide the batch without duplicate ownership.
func (r *DeliveryLogRepo) RearmLeaseLessPending(ctx context.Context, limit int, orphanThreshold time.Duration) ([]string, error) {
	const q = `WITH candidates AS (
		SELECT id FROM delivery_logs
		WHERE status = 'pending' AND next_retry_at IS NULL
		  AND created_at <= NOW() - ($2 || ' seconds')::interval
		ORDER BY created_at ASC, id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE delivery_logs AS d
	SET next_retry_at = NOW(), updated_at = NOW()
	FROM candidates AS c
	WHERE d.id = c.id
	RETURNING d.id`
	rows, err := r.pool.Query(ctx, q, limit, secondsArg(orphanThreshold))
	if err != nil {
		return nil, fmt.Errorf("store: rearm lease-less pending: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan rearmed pending: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate rearmed pending: %w", err)
	}
	return ids, nil
}

// ClaimPending applies the visibility lease with a conditional CAS that
// stamps next_retry_at into the future ONLY if the row is pending AND currently
// due (next_retry_at <= now()). The due predicate is what makes the claim the
// mutual-exclusion point — two workers draining duplicate queue entries for the
// same still-pending row cannot both pass it. Returns true iff this caller
// claimed the row. The lease is computed with the DB clock (NOW()) for consistency.
func (r *DeliveryLogRepo) ClaimPending(ctx context.Context, id string, lease time.Duration) (bool, error) {
	const q = `UPDATE delivery_logs
		SET next_retry_at = NOW() + ($2 || ' seconds')::interval, updated_at = NOW()
		WHERE id = $1 AND status = 'pending' AND next_retry_at <= NOW()`
	tag, err := r.pool.Exec(ctx, q, id, secondsArg(lease))
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
