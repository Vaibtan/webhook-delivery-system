package metrics

import (
	"expvar"
	"sync/atomic"
	"time"
)

// Collector aggregates delivery counters and the latency histogram. Cumulative
// counters are also published to expvar (zero-cost stdlib metrics).
type Collector struct {
	hist      *Histogram
	attempts  atomic.Int64
	successes atomic.Int64
	failures  atomic.Int64
	startTime time.Time
}

// NewCollector constructs a Collector. It does NOT touch any process-global
// state; call PublishExpvar once during startup wiring to expose it on expvar.
func NewCollector() *Collector {
	return &Collector{hist: NewHistogram(), startTime: time.Now()}
}

// PublishExpvar exposes c's cumulative counters and latency distribution on
// expvar under "webhook_delivery". Call it EXACTLY ONCE per process during
// startup wiring — expvar.Publish panics on a duplicate key, which is why this is
// an explicit composition-root step rather than a side effect of NewCollector.
func PublishExpvar(c *Collector) {
	expvar.Publish("webhook_delivery", expvar.Func(func() any {
		a, s, f := c.Counters()
		return map[string]any{
			"attempts_total":  a,
			"successes_total": s,
			"failures_total":  f,
			"latency_ms":      c.Latency(),
		}
	}))
}

// RecordDelivery records one delivery attempt's latency and outcome.
func (c *Collector) RecordDelivery(latency time.Duration, success bool) {
	c.hist.Observe(latency)
	c.attempts.Add(1)
	if success {
		c.successes.Add(1)
	} else {
		c.failures.Add(1)
	}
}

// Latency returns the current latency distribution.
func (c *Collector) Latency() HistogramSnapshot { return c.hist.Snapshot() }

// Uptime is the time since the collector was created.
func (c *Collector) Uptime() time.Duration { return time.Since(c.startTime) }

// Counters returns the cumulative (attempts, successes, failures).
func (c *Collector) Counters() (int64, int64, int64) {
	return c.attempts.Load(), c.successes.Load(), c.failures.Load()
}
