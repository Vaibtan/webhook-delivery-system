package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

type recordingDeliveryStore struct {
	log           *domain.DeliveryLog
	dropped       bool
	droppedDetail string
}

func (s *recordingDeliveryStore) GetByID(context.Context, string) (*domain.DeliveryLog, error) {
	return s.log, nil
}
func (s *recordingDeliveryStore) ClaimPending(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *recordingDeliveryStore) MarkDropped(_ context.Context, _ string, detail string) (bool, error) {
	s.dropped = true
	s.droppedDetail = detail
	return true, nil
}
func (s *recordingDeliveryStore) FailAndScheduleRetry(context.Context, string, *int, string, time.Time) (string, bool, error) {
	return "", false, nil
}
func (s *recordingDeliveryStore) RescheduleSamePending(context.Context, string, time.Time) (bool, error) {
	return false, nil
}

type recordingSubscriptionState struct {
	sub             *domain.Subscription
	successes       int
	finalFailures   int
	autoDisablement bool
}

func (s *recordingSubscriptionState) GetByID(context.Context, string) (*domain.Subscription, error) {
	return s.sub, nil
}

func (s *recordingSubscriptionState) RecordDeliverySuccess(context.Context, string, string, int) (bool, error) {
	s.successes++
	return true, nil
}

func (s *recordingSubscriptionState) RecordFinalFailure(context.Context, string, string, *int, string, int) (bool, bool, error) {
	s.finalFailures++
	return true, s.autoDisablement, nil
}

type noopScheduler struct{}

func (noopScheduler) Schedule(context.Context, string, time.Time) error { return nil }

type recordingDLQ struct{ pushes int }

func (q *recordingDLQ) Push(context.Context, string) error {
	q.pushes++
	return nil
}

type allowBreaker struct{}

func (allowBreaker) Allow() (bool, int64) { return true, 0 }
func (allowBreaker) RecordSuccess(int64)  {}
func (allowBreaker) RecordFailure(int64)  {}

type availableSem struct{}

func (availableSem) TryAcquire() bool { return true }
func (availableSem) Release()         {}

type discardMetrics struct{}

func (discardMetrics) RecordDelivery(time.Duration, bool) {}

func testDeliveryControls() DeliveryControls {
	return DeliveryControls{
		BreakerFor:   func(string) Breaker { return allowBreaker{} },
		SemaphoreFor: func(string) Sem { return availableSem{} },
		Metrics:      discardMetrics{},
	}
}

func TestInactiveSubscriptionIsDroppedWithoutDLQ(t *testing.T) {
	logs := &recordingDeliveryStore{log: &domain.DeliveryLog{
		ID: "delivery", SubscriptionID: "subscription", Status: domain.StatusPending,
	}}
	dlq := &recordingDLQ{}
	d := NewDeliverer(
		logs,
		&recordingSubscriptionState{sub: &domain.Subscription{ID: "subscription", IsActive: false}},
		noopScheduler{}, dlq, http.DefaultClient,
		DelivererConfig{VisibilityTimeout: time.Minute},
		testDeliveryControls(),
	)

	require.NoError(t, d.Process(context.Background(), "delivery"))
	assert.True(t, logs.dropped)
	assert.Equal(t, "subscription deactivated", logs.droppedDetail)
	assert.Zero(t, dlq.pushes)
}

func TestSuccessRecordsOutcomeThroughSubscriptionState(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	logs := &recordingDeliveryStore{log: &domain.DeliveryLog{
		ID: "delivery", WebhookID: "webhook", SubscriptionID: "subscription",
		TargetURL: target.URL, Payload: []byte(`{"ok":true}`), Status: domain.StatusPending,
	}}
	state := &recordingSubscriptionState{
		sub: &domain.Subscription{ID: "subscription", SecretKey: "secret", IsActive: true},
	}
	d := NewDeliverer(
		logs,
		state,
		noopScheduler{}, &recordingDLQ{}, target.Client(),
		DelivererConfig{VisibilityTimeout: time.Minute},
		testDeliveryControls(),
	)

	require.NoError(t, d.Process(context.Background(), "delivery"))
	assert.Equal(t, 1, state.successes)
}

func TestAutoDisableFinalizationUsesSubscriptionState(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	defer target.Close()
	logs := &recordingDeliveryStore{log: &domain.DeliveryLog{
		ID: "delivery", WebhookID: "webhook", SubscriptionID: "subscription",
		TargetURL: target.URL, Payload: []byte(`{"ok":true}`), AttemptNumber: 1, Status: domain.StatusPending,
	}}
	dlq := &recordingDLQ{}
	state := &recordingSubscriptionState{
		sub:             &domain.Subscription{ID: "subscription", SecretKey: "secret", IsActive: true},
		autoDisablement: true,
	}
	d := NewDeliverer(
		logs,
		state,
		noopScheduler{}, dlq, target.Client(),
		DelivererConfig{VisibilityTimeout: time.Minute, MaxRetryAttempts: 1, AutoDisableThreshold: 5},
		testDeliveryControls(),
	)

	require.NoError(t, d.Process(context.Background(), "delivery"))
	assert.Equal(t, 1, state.finalFailures)
	assert.Equal(t, 1, dlq.pushes)
}

func TestTruncatePreservesValidUTF8AndByteLimit(t *testing.T) {
	got := truncate(strings.Repeat("界", 200), maxErrorDetailLen)
	assert.LessOrEqual(t, len(got), maxErrorDetailLen)
	assert.True(t, strings.ToValidUTF8(got, "") == got)

	invalid := truncate("bad\xffvalue", maxErrorDetailLen)
	assert.Equal(t, "bad�value", invalid)
}
