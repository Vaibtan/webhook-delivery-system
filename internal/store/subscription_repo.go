package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// SubscriptionRepo persists subscriptions in PostgreSQL. Listing uses keyset
// pagination.
type SubscriptionRepo struct {
	pool *pgxpool.Pool
}

// NewSubscriptionRepo constructs a SubscriptionRepo.
func NewSubscriptionRepo(pool *pgxpool.Pool) *SubscriptionRepo {
	return &SubscriptionRepo{pool: pool}
}

// subColumns is the canonical SELECT column list, in scan order.
const subColumns = `id, target_url, secret_key, previous_secret_key, secret_rotated_at,
	event_types, is_active, consecutive_failures, created_at, updated_at`

// scanSubscription scans one row (in subColumns order) into a Subscription.
// previous_secret_key is nullable → mapped to "" when NULL.
func scanSubscription(row pgx.Row) (*domain.Subscription, error) {
	var s domain.Subscription
	var prevSecret *string
	if err := row.Scan(
		&s.ID, &s.TargetURL, &s.SecretKey, &prevSecret, &s.SecretRotatedAt,
		&s.EventTypes, &s.IsActive, &s.ConsecutiveFailures, &s.CreatedAt, &s.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if prevSecret != nil {
		s.PreviousSecretKey = *prevSecret
	}
	return &s, nil
}

// Create inserts a new subscription. The id and timestamps are assigned by
// Postgres (gen_random_uuid + NOW) and read back into s.
func (r *SubscriptionRepo) Create(ctx context.Context, s *domain.Subscription) error {
	const q = `
		INSERT INTO subscriptions (target_url, secret_key, event_types, is_active)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at`
	if s.EventTypes == nil {
		s.EventTypes = []string{}
	}
	err := r.pool.QueryRow(ctx, q, s.TargetURL, s.SecretKey, s.EventTypes, s.IsActive).
		Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		return fmt.Errorf("store: create subscription: %w", err)
	}
	return nil
}

// GetByID returns the subscription, or domain.ErrNotFound if absent.
func (r *SubscriptionRepo) GetByID(ctx context.Context, id string) (*domain.Subscription, error) {
	q := `SELECT ` + subColumns + ` FROM subscriptions WHERE id = $1`
	s, err := scanSubscription(r.pool.QueryRow(ctx, q, id))
	if err != nil {
		if isAbsent(err) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("store: get subscription: %w", err)
	}
	return s, nil
}

// List returns one keyset page ordered by (created_at DESC, id DESC). It fetches
// limit+1 rows to determine whether a next cursor exists.
func (r *SubscriptionRepo) List(ctx context.Context, limit int, after *domain.Cursor) (domain.Page[*domain.Subscription], error) {
	if limit <= 0 {
		limit = 20
	}
	const q = `
		SELECT ` + subColumns + `
		FROM subscriptions
		WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1::timestamptz, $2::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $3`

	var afterTime any
	var afterID any
	if after != nil {
		afterTime = after.CreatedAt
		afterID = after.ID
	}

	rows, err := r.pool.Query(ctx, q, afterTime, afterID, limit+1)
	if err != nil {
		return domain.Page[*domain.Subscription]{}, fmt.Errorf("store: list subscriptions: %w", err)
	}
	defer rows.Close()

	items := make([]*domain.Subscription, 0, limit+1)
	for rows.Next() {
		s, err := scanSubscription(rows)
		if err != nil {
			return domain.Page[*domain.Subscription]{}, fmt.Errorf("store: scan subscription: %w", err)
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return domain.Page[*domain.Subscription]{}, fmt.Errorf("store: iterate subscriptions: %w", err)
	}

	page := domain.Page[*domain.Subscription]{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.NextCursor = &domain.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

// Update writes target_url, event_types, and is_active (the operator-mutable
// fields) and bumps updated_at. Returns ErrNotFound if the row is gone.
func (r *SubscriptionRepo) Update(ctx context.Context, s *domain.Subscription) error {
	const q = `
		UPDATE subscriptions
		SET target_url = $2, event_types = $3, is_active = $4, updated_at = NOW()
		WHERE id = $1
		RETURNING updated_at`
	if s.EventTypes == nil {
		s.EventTypes = []string{}
	}
	err := r.pool.QueryRow(ctx, q, s.ID, s.TargetURL, s.EventTypes, s.IsActive).Scan(&s.UpdatedAt)
	if err != nil {
		if isAbsent(err) {
			return domain.ErrNotFound
		}
		return fmt.Errorf("store: update subscription: %w", err)
	}
	return nil
}

// RotateSecret moves the current secret to previous_secret_key, installs
// newSecret as secret_key, and stamps secret_rotated_at (starting the grace
// window). Returns the updated subscription, or ErrNotFound.
func (r *SubscriptionRepo) RotateSecret(ctx context.Context, id, newSecret string) (*domain.Subscription, error) {
	q := `UPDATE subscriptions
		SET previous_secret_key = secret_key, secret_key = $2, secret_rotated_at = NOW(), updated_at = NOW()
		WHERE id = $1
		RETURNING ` + subColumns
	s, err := scanSubscription(r.pool.QueryRow(ctx, q, id, newSecret))
	if err != nil {
		if isAbsent(err) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("store: rotate secret: %w", err)
	}
	return s, nil
}

// Delete removes a subscription (cascading its delivery_logs). Returns
// ErrNotFound if no row matched.
func (r *SubscriptionRepo) Delete(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		if isInvalidUUID(err) {
			return domain.ErrNotFound
		}
		return fmt.Errorf("store: delete subscription: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}
