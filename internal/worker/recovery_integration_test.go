//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
	"github.com/Vaibtan/webhook-delivery-system/internal/store"
	"github.com/Vaibtan/webhook-delivery-system/internal/testutil"
)

// An orphaned pending RETRY row (its enqueue was lost, or its worker crashed and
// the lease expired) must be reclaimed and re-enqueued by the recovery scan
func TestRecoveryReclaimsOrphanedPendingRetry(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)

	logs := store.NewDeliveryLogRepo(pool)
	subs := store.NewSubscriptionRepo(pool)
	q := queue.New(rdb)

	sub := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))

	// A retry row (attempt 2) that is past due but never made it to the queue.
	var orphanID string
	err := pool.QueryRow(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, next_retry_at)
		VALUES (gen_random_uuid(), $1, $2, '{"x":1}', 2, 0, 'pending', NOW() - interval '30 seconds')
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&orphanID)
	require.NoError(t, err)

	// An in-flight row (lease live: next_retry_at in the future) must NOT be reclaimed.
	var inflightID string
	err = pool.QueryRow(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, next_retry_at)
		VALUES (gen_random_uuid(), $1, $2, '{"y":2}', 1, 0, 'pending', NOW() + interval '5 minutes')
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&inflightID)
	require.NoError(t, err)

	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), depth, "queue starts empty")

	rw := NewRecoveryWorker(logs, q, time.Minute, 100, 15*time.Minute)
	rw.scan(ctx)

	depth, err = q.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), depth, "exactly the due orphan is re-enqueued (in-flight lease excluded)")

	got, err := q.Dequeue(ctx, time.Second)
	require.NoError(t, err)
	require.Equal(t, orphanID, got, "the re-enqueued id is the orphaned due row, not the leased one")
}

func TestRecoveryDrainsMoreThanOneBatch(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)

	logs := store.NewDeliveryLogRepo(pool)
	subs := store.NewSubscriptionRepo(pool)
	q := queue.New(rdb)
	sub := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))
	_, err := pool.Exec(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, next_retry_at)
		SELECT gen_random_uuid(), $1, $2, jsonb_build_object('n', n), 1, 0, 'pending',
		       NOW() - interval '1 minute' - (n * interval '1 millisecond')
		FROM generate_series(1, 250) AS n`, sub.ID, sub.TargetURL)
	require.NoError(t, err)

	rw := NewRecoveryWorker(logs, q, time.Minute, 100, 15*time.Minute)
	rw.scan(ctx)

	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(250), depth)
}

func TestRecoveryUsesOrphanThresholdForLeaseLessRows(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)

	logs := store.NewDeliveryLogRepo(pool)
	subs := store.NewSubscriptionRepo(pool)
	q := queue.New(rdb)
	sub := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))
	var oldID, youngID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, status, next_retry_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, '{}', 'pending', NULL, NOW() - interval '20 minutes')
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&oldID))
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, status, next_retry_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, '{}', 'pending', NULL, NOW() - interval '5 minutes')
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&youngID))

	rw := NewRecoveryWorker(logs, q, time.Minute, 100, 15*time.Minute)
	rw.scan(ctx)

	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), depth)
	got, err := q.Dequeue(ctx, time.Second)
	require.NoError(t, err)
	require.Equal(t, oldID, got)
	require.NotEqual(t, youngID, got)
	old, err := logs.GetByID(ctx, oldID)
	require.NoError(t, err)
	require.NotNil(t, old.NextRetryAt, "recovered lease-less row must be claimable by the worker CAS")
}

func TestRecoveryDrainsLeaseLessRowsAcrossBatchesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)

	logs := store.NewDeliveryLogRepo(pool)
	subs := store.NewSubscriptionRepo(pool)
	q := queue.New(rdb)
	sub := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))
	_, err := pool.Exec(ctx, `
		INSERT INTO delivery_logs
			(webhook_id, subscription_id, target_url, payload, status, next_retry_at, created_at)
		SELECT gen_random_uuid(), $1, $2, jsonb_build_object('n', n), 'pending', NULL,
		       NOW() - interval '20 minutes' - (n * interval '1 millisecond')
		FROM generate_series(1, 250) AS n`, sub.ID, sub.TargetURL)
	require.NoError(t, err)

	NewRecoveryWorker(logs, q, time.Minute, 100, 15*time.Minute).scan(ctx)

	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(250), depth)
	var leaseLess int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM delivery_logs WHERE status = 'pending' AND next_retry_at IS NULL`,
	).Scan(&leaseLess))
	assert.Zero(t, leaseLess)
}
