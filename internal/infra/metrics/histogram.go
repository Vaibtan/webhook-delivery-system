// Package metrics provides a bounded, mutex-guarded latency histogram and a
// delivery metrics collector — stdlib only (no Prometheus). Percentiles are
// computed from fixed log-linear buckets at scrape time, O(1) per observation.
package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// defaultBounds are log-linear millisecond bucket upper-bounds (Fibonacci-ish
// spacing from 1ms to 60s); the final implicit bucket captures overflow.
var defaultBounds = []float64{
	1, 2, 3, 5, 8, 13, 21, 34, 55, 89,
	144, 233, 377, 610, 1000, 1600, 2600, 4200, 6800, 10000,
	16000, 26000, 42000, 60000,
}

// Histogram is a concurrency-safe latency histogram.
type Histogram struct {
	mu     sync.Mutex
	bounds []float64
	counts []int64 // len(bounds)+1; last is the overflow bucket
	total  int64
	sumMs  float64
	minMs  float64
	maxMs  float64
}

// NewHistogram constructs a histogram with the default bucket layout.
func NewHistogram() *Histogram {
	return &Histogram{bounds: defaultBounds, counts: make([]int64, len(defaultBounds)+1)}
}

// Observe records one latency sample.
func (h *Histogram) Observe(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	h.mu.Lock()
	defer h.mu.Unlock()
	i := sort.SearchFloat64s(h.bounds, ms) // first bound >= ms (== len for overflow)
	h.counts[i]++
	if h.total == 0 || ms < h.minMs {
		h.minMs = ms
	}
	if ms > h.maxMs {
		h.maxMs = ms
	}
	h.total++
	h.sumMs += ms
}

// HistogramSnapshot is a point-in-time view in milliseconds.
type HistogramSnapshot struct {
	Count int64 `json:"count"`
	Min   int64 `json:"min"`
	Max   int64 `json:"max"`
	Avg   int64 `json:"avg"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
	P99   int64 `json:"p99"`
}

// Snapshot computes the current distribution summary.
func (h *Histogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return HistogramSnapshot{}
	}
	return HistogramSnapshot{
		Count: h.total,
		Min:   int64(math.Round(h.minMs)),
		Max:   int64(math.Round(h.maxMs)),
		Avg:   int64(math.Round(h.sumMs / float64(h.total))),
		P50:   h.percentileLocked(0.50),
		P95:   h.percentileLocked(0.95),
		P99:   h.percentileLocked(0.99),
	}
}

// percentileLocked estimates the p-quantile as the upper bound of the bucket in
// which the cumulative count first crosses p×total. Caller holds the lock.
func (h *Histogram) percentileLocked(p float64) int64 {
	target := p * float64(h.total)
	var cum float64
	for i, c := range h.counts {
		cum += float64(c)
		if cum >= target {
			if i >= len(h.bounds) {
				return int64(math.Round(h.maxMs)) // overflow bucket → use observed max
			}
			return int64(math.Round(h.bounds[i]))
		}
	}
	return int64(math.Round(h.maxMs))
}
