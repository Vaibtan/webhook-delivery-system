//go:build integration

package subscription

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/testutil"
)

type blockingRepository struct {
	mu      sync.Mutex
	sub     *domain.Subscription
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingRepository) Create(context.Context, *domain.Subscription) error { return nil }
func (r *blockingRepository) GetByID(context.Context, string) (*domain.Subscription, error) {
	r.mu.Lock()
	snapshot := *r.sub
	r.mu.Unlock()
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	return &snapshot, nil
}
func (r *blockingRepository) List(context.Context, int, *domain.Cursor) (domain.Page[*domain.Subscription], error) {
	return domain.Page[*domain.Subscription]{}, nil
}
func (r *blockingRepository) Update(_ context.Context, sub *domain.Subscription) error {
	r.mu.Lock()
	copy := *sub
	r.sub = &copy
	r.mu.Unlock()
	return nil
}
func (r *blockingRepository) Delete(context.Context, string) error { return nil }
func (r *blockingRepository) RotateSecret(context.Context, string, string) (*domain.Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := *r.sub
	return &snapshot, nil
}

type recordingOutcomes struct {
	markSuccess     func() (bool, bool, error)
	finalizeFailure func() (bool, bool, error)
}

func (o recordingOutcomes) MarkSuccess(context.Context, string, string, int) (bool, bool, error) {
	return o.markSuccess()
}

func (o recordingOutcomes) FinalizeFailure(context.Context, string, string, *int, string, int) (bool, bool, error) {
	return o.finalizeFailure()
}

func newTestState(t *testing.T, repo repository, outcomes deliveryOutcomes) *State {
	t.Helper()
	return New(testutil.Redis(t), repo, outcomes, Config{
		CacheTTL:         time.Minute,
		RegistryIdleTTL:  time.Hour,
		RateLimitPerSec:  1,
		RateLimitBurst:   1,
		ConcurrencyLimit: 1,
	})
}

func TestReadThroughCannotRestoreStaleValueAfterUpdate(t *testing.T) {
	ctx := context.Background()
	const id = "123e4567-e89b-12d3-a456-426614174000"
	repo := &blockingRepository{
		sub:     &domain.Subscription{ID: id, TargetURL: "https://old.example", SecretKey: "old", IsActive: true},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	state := newTestState(t, repo, recordingOutcomes{})
	require.NoError(t, state.rdb.Del(ctx, key(id)).Err())
	t.Cleanup(func() { _ = state.rdb.Del(context.Background(), key(id)).Err() })

	readDone := make(chan error, 1)
	go func() {
		_, err := state.GetByID(ctx, id)
		readDone <- err
	}()
	<-repo.started

	updated := &domain.Subscription{ID: id, TargetURL: "https://new.example", SecretKey: "new", IsActive: false}
	updateDone := make(chan error, 1)
	go func() { updateDone <- state.Update(ctx, updated) }()

	select {
	case <-updateDone:
		t.Fatal("update must wait for the in-progress read-through of the same subscription")
	case <-time.After(25 * time.Millisecond):
	}
	close(repo.release)
	require.NoError(t, <-readDone)
	require.NoError(t, <-updateDone)

	got, err := state.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "https://new.example", got.TargetURL)
	assert.Equal(t, "new", got.SecretKey)
	assert.False(t, got.IsActive)
}

func TestDeliveryMutationHasNoCommitToEvictWindow(t *testing.T) {
	ctx := context.Background()
	const id = "223e4567-e89b-12d3-a456-426614174000"
	releaseFirstRead := make(chan struct{})
	close(releaseFirstRead)
	repo := &blockingRepository{
		sub:     &domain.Subscription{ID: id, TargetURL: "https://old.example", SecretKey: "old", IsActive: true},
		started: make(chan struct{}),
		release: releaseFirstRead,
	}
	committed := make(chan struct{})
	finishMutation := make(chan struct{})
	state := newTestState(t, repo, recordingOutcomes{
		markSuccess: func() (bool, bool, error) {
			err := repo.Update(ctx, &domain.Subscription{
				ID: id, TargetURL: "https://new.example", SecretKey: "new", IsActive: false,
			})
			close(committed)
			<-finishMutation
			return true, err == nil, err
		},
	})
	require.NoError(t, state.rdb.Del(ctx, key(id)).Err())
	t.Cleanup(func() { _ = state.rdb.Del(context.Background(), key(id)).Err() })
	_, err := state.GetByID(ctx, id)
	require.NoError(t, err)

	mutationDone := make(chan error, 1)
	go func() {
		_, err := state.RecordDeliverySuccess(ctx, "delivery", id, httpStatusOK)
		mutationDone <- err
	}()
	<-committed

	readDone := make(chan *domain.Subscription, 1)
	go func() {
		got, _ := state.GetByID(ctx, id)
		readDone <- got
	}()
	select {
	case <-readDone:
		t.Fatal("cache read escaped between delivery commit and invalidation")
	case <-time.After(25 * time.Millisecond):
	}
	close(finishMutation)
	require.NoError(t, <-mutationDone)
	got := <-readDone
	require.NotNil(t, got)
	assert.Equal(t, "new", got.SecretKey)
	assert.False(t, got.IsActive)
}

func TestUnchangedDeliveryOutcomeKeepsCachedSubscription(t *testing.T) {
	ctx := context.Background()
	const id = "323e4567-e89b-12d3-a456-426614174000"
	repo := &blockingRepository{
		sub:     &domain.Subscription{ID: id, TargetURL: "https://example.com", SecretKey: "secret", IsActive: true},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	close(repo.release)
	state := newTestState(t, repo, recordingOutcomes{
		markSuccess: func() (bool, bool, error) { return true, false, nil },
	})
	t.Cleanup(func() { _ = state.rdb.Del(context.Background(), key(id)).Err() })

	_, err := state.GetByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), state.rdb.Exists(ctx, key(id)).Val())

	_, err = state.RecordDeliverySuccess(ctx, "delivery", id, httpStatusOK)
	require.NoError(t, err)
	assert.Equal(t, int64(1), state.rdb.Exists(ctx, key(id)).Val())
}

func TestPolicyMutationPreservesActiveConcurrencyControls(t *testing.T) {
	ctx := context.Background()
	const id = "423e4567-e89b-12d3-a456-426614174000"
	release := make(chan struct{})
	close(release)
	repo := &blockingRepository{
		sub:     &domain.Subscription{ID: id, TargetURL: "https://example.com", SecretKey: "secret", IsActive: true},
		started: make(chan struct{}),
		release: release,
	}
	state := newTestState(t, repo, recordingOutcomes{})
	t.Cleanup(func() { _ = state.rdb.Del(context.Background(), key(id)).Err() })

	assert.True(t, state.Allow(id))
	assert.False(t, state.Allow(id))
	oldSemaphore := state.Semaphore(id)
	require.True(t, oldSemaphore.TryAcquire())
	assert.False(t, oldSemaphore.TryAcquire())

	require.NoError(t, state.Update(ctx, repo.sub))
	assert.False(t, state.Allow(id))
	assert.Same(t, oldSemaphore, state.Semaphore(id))
	_, err := state.RotateSecret(ctx, id, "new-secret")
	require.NoError(t, err)
	assert.Same(t, oldSemaphore, state.Semaphore(id))
	oldSemaphore.Release()
}

func TestDeleteRetiresDerivedSubscriptionState(t *testing.T) {
	ctx := context.Background()
	const id = "623e4567-e89b-12d3-a456-426614174000"
	release := make(chan struct{})
	close(release)
	repo := &blockingRepository{
		sub:     &domain.Subscription{ID: id, TargetURL: "https://example.com", SecretKey: "secret", IsActive: true},
		started: make(chan struct{}),
		release: release,
	}
	state := newTestState(t, repo, recordingOutcomes{})
	oldSemaphore := state.Semaphore(id)

	require.NoError(t, state.Delete(ctx, id))
	assert.NotSame(t, oldSemaphore, state.Semaphore(id))
}

func TestFinalFailureAppliesOnlyCommittedConsequences(t *testing.T) {
	outcomeErr := errors.New("outcome failed")
	tests := []struct {
		name             string
		won              bool
		disabled         bool
		outcomeErr       error
		wantCache        int64
		wantRuntimeReset bool
	}{
		{name: "lost cas", wantCache: 1},
		{name: "terminal failure", won: true},
		{name: "auto-disabled", won: true, disabled: true, wantRuntimeReset: true},
		{name: "store error", outcomeErr: outcomeErr, wantCache: 1},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			id := fmt.Sprintf("523e4567-e89b-12d3-a456-42661417400%d", i)
			release := make(chan struct{})
			close(release)
			repo := &blockingRepository{
				sub: &domain.Subscription{
					ID: id, TargetURL: "https://example.com", SecretKey: "secret", IsActive: true,
				},
				started: make(chan struct{}),
				release: release,
			}
			state := newTestState(t, repo, recordingOutcomes{
				finalizeFailure: func() (bool, bool, error) { return tt.won, tt.disabled, tt.outcomeErr },
			})
			t.Cleanup(func() { _ = state.rdb.Del(context.Background(), key(id)).Err() })
			_, err := state.GetByID(ctx, id)
			require.NoError(t, err)
			oldSemaphore := state.Semaphore(id)

			won, disabled, err := state.RecordFinalFailure(ctx, "delivery", id, nil, "failed", 5)
			if tt.outcomeErr != nil {
				require.ErrorIs(t, err, tt.outcomeErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.won, won)
			assert.Equal(t, tt.disabled, disabled)
			assert.Equal(t, tt.wantCache, state.rdb.Exists(ctx, key(id)).Val())
			assert.Equal(t, tt.wantRuntimeReset, state.Semaphore(id) != oldSemaphore)
		})
	}
}

const httpStatusOK = 200
