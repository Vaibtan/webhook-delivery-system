package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// statusResponse mirrors the Python get_webhook_status shape, enhanced with
// replay_number ordering. The top-level "current" fields come from the latest
// attempt of the latest chain (MAX(replay_number)) so a replay's progress is
// never masked by the original chain's old final_failure (plan §0).
type statusResponse struct {
	WebhookID      string          `json:"webhook_id"`
	SubscriptionID string          `json:"subscription_id"`
	EventType      string          `json:"event_type,omitempty"`
	Status         string          `json:"status"`
	TargetURL      string          `json:"target_url"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	LastAttemptAt  time.Time       `json:"last_attempt_at"`
	NextRetryAt    *time.Time      `json:"next_retry_at,omitempty"`
	Attempts       []attemptView   `json:"attempts"`
	Statistics     statistics      `json:"statistics"`
}

type attemptView struct {
	AttemptNumber int        `json:"attempt_number"`
	ReplayNumber  int        `json:"replay_number"`
	Status        string     `json:"status"`
	HTTPStatus    *int       `json:"http_status,omitempty"`
	ErrorDetails  string     `json:"error_details,omitempty"`
	NextRetryAt   *time.Time `json:"next_retry_at,omitempty"`
	Timestamp     time.Time  `json:"timestamp"`
}

type statistics struct {
	TotalAttempts int `json:"total_attempts"`
	Successful    int `json:"successful"`
	Failed        int `json:"failed"`
	Pending       int `json:"pending"`
	FinalFailure  int `json:"final_failure"`
}

// handleWebhookStatus returns the full attempt history for a webhook plus a
// statistics block. Admin auth.
func (s *Server) handleWebhookStatus(w http.ResponseWriter, r *http.Request) {
	webhookID := r.PathValue("webhook_id")
	logs, err := s.opts.DeliveryLogs.ListByWebhookID(r.Context(), webhookID)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if len(logs) == 0 {
		writeError(w, http.StatusNotFound, "webhook not found")
		return
	}

	// logs are ordered by (replay_number, attempt_number) ASC; the last is the
	// latest attempt of the latest chain = the current outcome.
	current := logs[len(logs)-1]

	attempts := make([]attemptView, 0, len(logs))
	stats := statistics{TotalAttempts: len(logs)}
	for _, l := range logs {
		attempts = append(attempts, toAttemptView(l))
		switch l.Status {
		case domain.StatusSuccess:
			stats.Successful++
		case domain.StatusFailedAttempt:
			stats.Failed++
		case domain.StatusPending:
			stats.Pending++
		case domain.StatusFinalFailure:
			stats.FinalFailure++
		}
	}

	writeJSON(w, http.StatusOK, statusResponse{
		WebhookID:      current.WebhookID,
		SubscriptionID: current.SubscriptionID,
		EventType:      current.EventType,
		Status:         string(current.Status),
		TargetURL:      current.TargetURL,
		Payload:        current.Payload,
		CreatedAt:      current.CreatedAt,
		UpdatedAt:      current.UpdatedAt,
		LastAttemptAt:  current.UpdatedAt,
		NextRetryAt:    current.NextRetryAt,
		Attempts:       attempts,
		Statistics:     stats,
	})
}

// metricsSummaryResponse aggregates attempt rows by status over the window.
type metricsSummaryResponse struct {
	WindowHours   int     `json:"window_hours"`
	TotalAttempts int     `json:"total_attempts"`
	Successful    int     `json:"successful"`
	Failed        int     `json:"failed"`
	Pending       int     `json:"pending"`
	FinalFailure  int     `json:"final_failure"`
	SuccessRate   float64 `json:"success_rate"`
}

// handleMetricsSummary counts delivery rows by status over the last `hours`
// (default 24). Admin auth.
func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	hours := parseHours(r, 24)
	since := time.Now().Add(-time.Duration(hours) * time.Hour)

	counts, err := s.opts.DeliveryLogs.CountByStatusSince(r.Context(), since)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	successful := counts[domain.StatusSuccess]
	var rate float64
	if total > 0 {
		rate = float64(successful) / float64(total)
	}
	writeJSON(w, http.StatusOK, metricsSummaryResponse{
		WindowHours:   hours,
		TotalAttempts: total,
		Successful:    successful,
		Failed:        counts[domain.StatusFailedAttempt],
		Pending:       counts[domain.StatusPending],
		FinalFailure:  counts[domain.StatusFinalFailure],
		SuccessRate:   rate,
	})
}
