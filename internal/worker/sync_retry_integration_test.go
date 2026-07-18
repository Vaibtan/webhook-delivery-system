//go:build integration

package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/scheduler"
	"github.com/Vaibtan/webhook-delivery-system/internal/store"
	"github.com/Vaibtan/webhook-delivery-system/internal/subscription"
	"github.com/Vaibtan/webhook-delivery-system/internal/testutil"
)

func TestSynchronousFirstAttemptRetriesThroughBackgroundPipeline(t *testing.T) {
	ctx := context.Background()
	pool := testutil.Pool(t)
	rdb := testutil.Redis(t)
	testutil.Truncate(t, pool)
	testutil.FlushRedis(t, rdb)

	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	subs := store.NewSubscriptionRepo(pool)
	logs := store.NewDeliveryLogRepo(pool)
	tasks := queue.New(rdb)
	retries := scheduler.New(rdb)
	dlq := queue.NewDLQ(rdb)
	subscriptionState := subscription.New(rdb, subs, logs, subscription.Config{
		CacheTTL:         time.Minute,
		RegistryIdleTTL:  time.Hour,
		RateLimitPerSec:  100,
		RateLimitBurst:   100,
		ConcurrencyLimit: 1,
	})
	sub := &domain.Subscription{TargetURL: target.URL, SecretKey: "secret", IsActive: true}
	require.NoError(t, subs.Create(ctx, sub))
	result, err := logs.IngestPending(ctx, domain.IngestParams{
		SubscriptionID: sub.ID,
		WebhookID:      "123e4567-e89b-12d3-a456-426614174000",
		TargetURL:      target.URL,
		Payload:        []byte(`{"ok":true}`),
	})
	require.NoError(t, err)

	deliverer := NewDeliverer(logs, subscriptionState, retries, dlq, target.Client(), DelivererConfig{
		VisibilityTimeout:    time.Second,
		MaxRetryAttempts:     2,
		RetryBaseDelay:       10 * time.Millisecond,
		RetryMaxDelay:        10 * time.Millisecond,
		AutoDisableThreshold: 5,
	}, testDeliveryControls())
	// This is the SYNC_DELIVERY request path's first attempt. It fails, persists a
	// successor, and schedules it for the always-on background pipeline.
	require.NoError(t, deliverer.Process(ctx, result.DeliveryLogID))

	runCtx, cancel := context.WithCancel(ctx)
	workersDone := make(chan error, 2)
	workerPool := NewPool(tasks, deliverer, 1, time.Second)
	poller := NewRetrySchedulerWorker(retries, 5*time.Millisecond)
	go func() { workersDone <- workerPool.Start(runCtx) }()
	go func() { workersDone <- poller.Run(runCtx) }()

	require.Eventually(t, func() bool {
		rows, queryErr := logs.ListByWebhookID(ctx, result.WebhookID)
		return queryErr == nil && len(rows) == 2 && rows[1].Status == domain.StatusSuccess
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-workersDone)
	require.NoError(t, <-workersDone)
	assert.Equal(t, int32(2), calls.Load())
}
