package api

import (
	"net/http"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/metrics"
)

// monitorResponse is the /monitor JSON shape (plan § monitor).
type monitorResponse struct {
	UptimeSeconds      int64                     `json:"uptime_seconds"`
	CurrentTime        string                    `json:"current_time"`
	DeliveryMetrics    map[string]int            `json:"delivery_metrics"`
	QueueDepth         int64                     `json:"queue_depth"`
	DeadLetterQueueDep int64                     `json:"dead_letter_queue_depth"`
	CircuitBreakers    map[string]string         `json:"circuit_breakers"`
	DeliveryLatencyMs  metrics.HistogramSnapshot `json:"delivery_latency_ms"`
}

// handleMonitor returns detailed system health: uptime, delivery metrics (by
// status, from the DB), queue/DLQ depths (Redis), circuit-breaker states, and
// the delivery latency distribution. Admin auth.
func (s *Server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	resp := monitorResponse{
		CurrentTime:     time.Now().UTC().Format(time.RFC3339),
		DeliveryMetrics: map[string]int{},
		CircuitBreakers: map[string]string{},
	}
	resp.UptimeSeconds = int64(s.opts.Metrics.Uptime().Seconds())
	resp.DeliveryLatencyMs = s.opts.Metrics.Latency()

	// Delivery metrics by status (all-time), from Postgres.
	if counts, err := s.opts.DeliveryLogs.CountByStatusSince(ctx, time.Time{}); err == nil {
		total := 0
		for status, n := range counts {
			resp.DeliveryMetrics[string(status)] = n
			total += n
		}
		resp.DeliveryMetrics["total"] = total
		// Ensure all statuses are present (zero when absent).
		for _, st := range []domain.DeliveryStatus{domain.StatusPending, domain.StatusSuccess, domain.StatusFailedAttempt, domain.StatusFinalFailure} {
			if _, ok := resp.DeliveryMetrics[string(st)]; !ok {
				resp.DeliveryMetrics[string(st)] = 0
			}
		}
	}

	if n, err := s.opts.TaskQueue.Depth(ctx); err == nil {
		resp.QueueDepth = n
	}
	if n, err := s.opts.DLQ.Depth(ctx); err == nil {
		resp.DeadLetterQueueDep = n
	}
	resp.CircuitBreakers = s.opts.BreakerStates()

	writeJSON(w, http.StatusOK, resp)
}
