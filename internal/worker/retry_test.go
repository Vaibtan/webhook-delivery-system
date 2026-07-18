package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

type pagedDueFinder struct {
	items []domain.DuePending
	calls int
}

func (f *pagedDueFinder) FindDuePending(_ context.Context, limit int, after *domain.RecoveryCursor) ([]domain.DuePending, error) {
	f.calls++
	start := 0
	if after != nil {
		for i, item := range f.items {
			if item.ID == after.ID {
				start = i + 1
				break
			}
		}
	}
	end := min(start+limit, len(f.items))
	return f.items[start:end], nil
}

func (f *pagedDueFinder) RearmLeaseLessPending(context.Context, int, time.Duration) ([]string, error) {
	return nil, nil
}

func TestRecoveryScanDrainsAllPages(t *testing.T) {
	due := make([]domain.DuePending, 250)
	for i := range due {
		due[i] = domain.DuePending{ID: string(rune(i + 1)), DueAt: time.Unix(int64(i), 0)}
	}
	finder := &pagedDueFinder{items: due}
	q := &fakeQueue{}
	w := NewRecoveryWorker(finder, q, time.Minute, 100, 15*time.Minute)

	w.scan(context.Background())

	depth, err := q.Depth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(250), depth)
	assert.Equal(t, 3, finder.calls)
}

func TestRetryDelayCapsLargeAttemptWithoutOverflow(t *testing.T) {
	const maxDelay = 15 * time.Minute
	delay := retryDelay(10_000, 10*time.Second, maxDelay)
	assert.Greater(t, delay, time.Duration(0))
	assert.LessOrEqual(t, delay, maxDelay+maxDelay/5)
}
