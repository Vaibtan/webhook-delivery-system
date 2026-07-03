package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// fakeQueue serves a fixed list of ids, then reports empty.
type fakeQueue struct {
	mu    sync.Mutex
	items []string
}

func (q *fakeQueue) Enqueue(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, id)
	return nil
}

func (q *fakeQueue) Dequeue(ctx context.Context, timeout time.Duration) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return "", domain.ErrQueueEmpty
	}
	id := q.items[0]
	q.items = q.items[1:]
	return id, nil
}

func (q *fakeQueue) Depth(ctx context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.items)), nil
}

// delivererFunc adapts a function to domain.Deliverer.
type delivererFunc func(ctx context.Context, id string) error

func (f delivererFunc) Process(ctx context.Context, id string) error { return f(ctx, id) }

func TestPoolProcessesAllTasks(t *testing.T) {
	q := &fakeQueue{items: []string{"a", "b", "c", "d", "e"}}
	var processed atomic.Int32
	d := delivererFunc(func(ctx context.Context, id string) error {
		processed.Add(1)
		return nil
	})
	pool := NewPool(q, d, 3, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()

	require.Eventually(t, func() bool { return processed.Load() == 5 }, 3*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	assert.False(t, pool.Running())
}

// A delivery that returns a non-nil error must NOT tear down the pool (§1 rule 1).
func TestPoolDeliveryErrorDoesNotTearDownPool(t *testing.T) {
	q := &fakeQueue{items: []string{"ok1", "boom", "ok2", "ok3"}}
	var processed atomic.Int32
	d := delivererFunc(func(ctx context.Context, id string) error {
		processed.Add(1)
		if id == "boom" {
			return errors.New("unexpected delivery error")
		}
		return nil
	})
	pool := NewPool(q, d, 2, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()

	// All 4 (including the ones after "boom") must still be processed.
	require.Eventually(t, func() bool { return processed.Load() == 4 }, 3*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

// An in-flight delivery must finish after ctx is cancelled (graceful drain, §1
// rule 2: context.WithoutCancel + drain deadline).
func TestPoolDrainsInFlightOnCancel(t *testing.T) {
	q := &fakeQueue{items: []string{"slow"}}
	started := make(chan struct{})
	var completed atomic.Bool
	d := delivererFunc(func(ctx context.Context, id string) error {
		close(started)
		// Simulate slow delivery; uses the (drain) ctx, which must NOT be cancelled.
		select {
		case <-time.After(300 * time.Millisecond):
			completed.Store(true)
		case <-ctx.Done():
			// Should not happen before the drain deadline.
		}
		return nil
	})
	pool := NewPool(q, d, 1, 2*time.Second) // generous drain deadline

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()

	<-started // delivery is in flight
	cancel()  // SIGTERM equivalent: stop dequeuing, but in-flight must drain
	require.NoError(t, <-done)
	assert.True(t, completed.Load(), "in-flight delivery should complete during drain, not abort")
}
