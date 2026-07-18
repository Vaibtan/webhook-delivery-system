//go:build integration

package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/idgen"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
	"github.com/Vaibtan/webhook-delivery-system/internal/store"
	"github.com/Vaibtan/webhook-delivery-system/internal/testutil"
)

func newSub(t *testing.T, subs *store.SubscriptionRepo) *domain.Subscription {
	t.Helper()
	s := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(context.Background(), s))
	return s
}

// Cleanup must preserve pending and unacknowledged DLQ rows.
func TestCleanupNeverDeletesPendingOrDLQ(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	testutil.Truncate(t, pool)
	logs := store.NewDeliveryLogRepo(pool)
	sub := newSub(t, store.NewSubscriptionRepo(pool))

	// All rows are old (created 100 days ago).
	ins := func(status string, inDLQ bool) string {
		var id string
		require.NoError(t, pool.QueryRow(ctx, `
			INSERT INTO delivery_logs (webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, in_dlq, created_at)
			VALUES (gen_random_uuid(), $1, $2, '{}', 1, 0, $3, $4, NOW() - interval '100 days')
			RETURNING id`, sub.ID, sub.TargetURL, status, inDLQ).Scan(&id))
		return id
	}
	pendingID := ins("pending", false)
	successID := ins("success", false)
	dlqID := ins("final_failure", true)
	failedID := ins("failed_attempt", false)

	deleted, err := logs.DeleteExpired(ctx, 0) // retention 0 → everything old enough
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted, "only terminal non-DLQ rows (success, failed_attempt) reaped")

	exists := func(id string) bool {
		_, err := logs.GetByID(ctx, id)
		return err == nil
	}
	assert.True(t, exists(pendingID), "pending row must survive cleanup")
	assert.True(t, exists(dlqID), "un-acked DLQ row must survive cleanup")
	assert.False(t, exists(successID), "old success reaped")
	assert.False(t, exists(failedID), "old failed_attempt reaped")
}

// PostgreSQL idempotency inserts one delivery row and returns one webhook ID to
// every concurrent caller.
func TestConcurrentSameKeyIngestExactlyOneRow(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	testutil.Truncate(t, pool)
	logs := store.NewDeliveryLogRepo(pool)
	sub := newSub(t, store.NewSubscriptionRepo(pool))

	const N = 16
	results := make([]domain.IngestResult, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wid, _ := idgen.NewUUIDv4()
			<-start
			results[i], errs[i] = logs.IngestPending(ctx, domain.IngestParams{
				SubscriptionID: sub.ID,
				IdempotencyKey: "same-key",
				WebhookID:      wid,
				TargetURL:      sub.TargetURL,
				Payload:        []byte(`{"x":1}`),
			})
		}(i)
	}
	close(start)
	wg.Wait()

	nonDup, storedWebhook := 0, ""
	for i := 0; i < N; i++ {
		require.NoError(t, errs[i])
		if !results[i].Duplicate {
			nonDup++
			storedWebhook = results[i].WebhookID
		}
	}
	assert.Equal(t, 1, nonDup, "exactly one ingest creates the delivery row")

	for i := 0; i < N; i++ {
		assert.Equal(t, storedWebhook, results[i].WebhookID, "all callers see the same webhook_id")
	}

	var rowCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM delivery_logs WHERE webhook_id=$1`, storedWebhook).Scan(&rowCount))
	assert.Equal(t, 1, rowCount, "exactly one delivery_logs row exists for the key")
}

// The visibility-lease CAS admits one worker for duplicate queue entries.
func TestConcurrentDuplicateDequeueClaimsOnce(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	testutil.Truncate(t, pool)
	logs := store.NewDeliveryLogRepo(pool)
	sub := newSub(t, store.NewSubscriptionRepo(pool))

	var id string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO delivery_logs (webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, next_retry_at)
		VALUES (gen_random_uuid(), $1, $2, '{}', 1, 0, 'pending', NOW() - interval '1 second')
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&id))

	const N = 12
	var won atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := logs.ClaimPending(ctx, id, time.Minute)
			require.NoError(t, err)
			if ok {
				won.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int32(1), won.Load(), "exactly one worker claims the row")
}

// Concurrent replays produce exactly one new chain.
func TestConcurrentReplayExactlyOneNewChain(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	testutil.Truncate(t, pool)
	logs := store.NewDeliveryLogRepo(pool)
	sub := newSub(t, store.NewSubscriptionRepo(pool))

	var webhookID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO delivery_logs (webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, in_dlq)
		VALUES (gen_random_uuid(), $1, $2, '{}', 5, 0, 'final_failure', TRUE)
		RETURNING webhook_id`, sub.ID, sub.TargetURL).Scan(&webhookID))

	const N = 10
	var okCount atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, ok, err := logs.ReplayDLQ(ctx, sub.ID, webhookID)
			require.NoError(t, err)
			if ok {
				okCount.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), okCount.Load(), "exactly one replay wins")
	var maxReplay int
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(replay_number) FROM delivery_logs WHERE webhook_id=$1`, webhookID).Scan(&maxReplay))
	assert.Equal(t, 1, maxReplay, "exactly one new chain (replay_number=1) created")
}

// Recovery also reclaims a lost first-attempt enqueue.
func TestRecoveryReclaimsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)
	logs := store.NewDeliveryLogRepo(pool)
	sub := newSub(t, store.NewSubscriptionRepo(pool))
	q := queue.New(rdb)

	var id string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO delivery_logs (webhook_id, subscription_id, target_url, payload, attempt_number, replay_number, status, next_retry_at)
		VALUES (gen_random_uuid(), $1, $2, '{}', 1, 0, 'pending', NOW())
		RETURNING id`, sub.ID, sub.TargetURL).Scan(&id))

	items, err := logs.FindDuePending(ctx, 100, nil)
	require.NoError(t, err)
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	require.Contains(t, ids, id, "first-attempt due pending row is found by recovery scan")

	require.NoError(t, q.Enqueue(ctx, id))
	depth, err := q.Depth(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func TestIntentionalDropIsTerminalButNotInDLQ(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	testutil.Truncate(t, pool)

	subs := store.NewSubscriptionRepo(pool)
	logs := store.NewDeliveryLogRepo(pool)
	sub := &domain.Subscription{TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))
	result, err := logs.IngestPending(ctx, domain.IngestParams{
		SubscriptionID: sub.ID,
		WebhookID:      "123e4567-e89b-12d3-a456-426614174000",
		TargetURL:      sub.TargetURL,
		Payload:        []byte(`{"ok":true}`),
	})
	require.NoError(t, err)

	won, err := logs.MarkDropped(ctx, result.DeliveryLogID, "subscription deactivated")
	require.NoError(t, err)
	require.True(t, won)
	got, err := logs.GetByID(ctx, result.DeliveryLogID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusFinalFailure, got.Status)
	require.False(t, got.InDLQ)
	require.Nil(t, got.NextRetryAt)

	ids, err := logs.ListInDLQ(ctx)
	require.NoError(t, err)
	require.NotContains(t, ids, result.DeliveryLogID)
}
