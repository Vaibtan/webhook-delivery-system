package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram()
	s := h.Snapshot()
	assert.Equal(t, int64(0), s.Count)
	assert.Equal(t, int64(0), s.P99)
}

func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram()
	// 100 samples: 90 around 10ms, 9 around 200ms, 1 at 5s.
	for i := 0; i < 90; i++ {
		h.Observe(10 * time.Millisecond)
	}
	for i := 0; i < 9; i++ {
		h.Observe(200 * time.Millisecond)
	}
	h.Observe(5 * time.Second)

	s := h.Snapshot()
	assert.Equal(t, int64(100), s.Count)
	assert.GreaterOrEqual(t, s.Max, int64(5000))
	assert.LessOrEqual(t, s.P50, int64(13), "p50 in the 10ms cluster")
	assert.LessOrEqual(t, s.P95, int64(233), "p95 in the 200ms cluster")
	assert.GreaterOrEqual(t, s.P99, int64(233), "p99 reaches the tail")
	assert.Greater(t, s.Avg, int64(0))
}

func TestCollectorCounters(t *testing.T) {
	c := NewCollector()
	c.RecordDelivery(10*time.Millisecond, true)
	c.RecordDelivery(20*time.Millisecond, false)
	c.RecordDelivery(30*time.Millisecond, true)
	a, s, f := c.Counters()
	assert.Equal(t, int64(3), a)
	assert.Equal(t, int64(2), s)
	assert.Equal(t, int64(1), f)
	assert.Equal(t, int64(3), c.Latency().Count)
}
