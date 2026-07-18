package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// DeliveryLogRepo persists delivery attempts in PostgreSQL.
type DeliveryLogRepo struct {
	pool *pgxpool.Pool
}

// NewDeliveryLogRepo constructs a DeliveryLogRepo.
func NewDeliveryLogRepo(pool *pgxpool.Pool) *DeliveryLogRepo {
	return &DeliveryLogRepo{pool: pool}
}

const dlColumns = `id, webhook_id, subscription_id, target_url, payload, event_type,
	attempt_number, replay_number, status, http_status, error_details, next_retry_at,
	in_dlq, created_at, updated_at`

// scanDeliveryLog scans one row (dlColumns order) into a DeliveryLog, mapping
// nullable columns (event_type, http_status, error_details, next_retry_at).
func scanDeliveryLog(row pgx.Row) (*domain.DeliveryLog, error) {
	var d domain.DeliveryLog
	var (
		eventType    *string
		httpStatus   *int
		errorDetails *string
		status       string
	)
	if err := row.Scan(
		&d.ID, &d.WebhookID, &d.SubscriptionID, &d.TargetURL, &d.Payload, &eventType,
		&d.AttemptNumber, &d.ReplayNumber, &status, &httpStatus, &errorDetails, &d.NextRetryAt,
		&d.InDLQ, &d.CreatedAt, &d.UpdatedAt,
	); err != nil {
		return nil, err
	}
	d.Status = domain.DeliveryStatus(status)
	d.HTTPStatus = httpStatus
	if eventType != nil {
		d.EventType = *eventType
	}
	if errorDetails != nil {
		d.ErrorDetails = *errorDetails
	}
	return &d, nil
}

// GetByID returns a single attempt row by id, or domain.ErrNotFound.
func (r *DeliveryLogRepo) GetByID(ctx context.Context, id string) (*domain.DeliveryLog, error) {
	q := `SELECT ` + dlColumns + ` FROM delivery_logs WHERE id = $1`
	d, err := scanDeliveryLog(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if isAbsent(err) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("store: get delivery_log: %w", err)
	}
	return d, nil
}

// ListByWebhookID returns all attempt rows for a webhook, ordered by
// (replay_number, attempt_number) — the status-endpoint order (§0).
func (r *DeliveryLogRepo) ListByWebhookID(ctx context.Context, webhookID string) ([]*domain.DeliveryLog, error) {
	q := `SELECT ` + dlColumns + ` FROM delivery_logs WHERE webhook_id = $1
		ORDER BY replay_number ASC, attempt_number ASC`
	return r.queryList(ctx, q, webhookID)
}

// ListBySubscription returns the most recent attempt rows for a subscription
// (newest first), capped at limit. Backs GET /subscriptions/{id}/attempts.
func (r *DeliveryLogRepo) ListBySubscription(ctx context.Context, subscriptionID string, limit int) ([]*domain.DeliveryLog, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT ` + dlColumns + ` FROM delivery_logs WHERE subscription_id = $1
		ORDER BY created_at DESC LIMIT $2`
	return r.queryList(ctx, q, subscriptionID, limit)
}

func (r *DeliveryLogRepo) queryList(ctx context.Context, q string, args ...any) ([]*domain.DeliveryLog, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		if isInvalidUUID(err) {
			return []*domain.DeliveryLog{}, nil
		}
		return nil, fmt.Errorf("store: query delivery_logs: %w", err)
	}
	return collectDeliveryLogs(rows)
}

func collectDeliveryLogs(rows pgx.Rows) ([]*domain.DeliveryLog, error) {
	defer rows.Close()
	out := make([]*domain.DeliveryLog, 0, 8)
	for rows.Next() {
		d, err := scanDeliveryLog(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan delivery_log: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate delivery_logs: %w", err)
	}
	return out, nil
}

// nullStr maps "" → nil so an absent value stores SQL NULL.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
