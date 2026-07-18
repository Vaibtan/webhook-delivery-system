package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// --- subscription DTOs ------------------------------------------------------

// subscriptionResponse is the public representation of a subscription. It NEVER
// includes secret_key or previous_secret_key (secret redaction).
type subscriptionResponse struct {
	ID                  string     `json:"id"`
	TargetURL           string     `json:"target_url"`
	EventTypes          []string   `json:"event_types"`
	IsActive            bool       `json:"is_active"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	SecretRotatedAt     *time.Time `json:"secret_rotated_at,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// createSubscriptionResponse augments the public view with the plaintext secret,
// returned exactly once at create time.
type createSubscriptionResponse struct {
	subscriptionResponse
	SecretKey string `json:"secret_key"`
}

// listSubscriptionsResponse is the keyset-paginated list envelope (no total).
type listSubscriptionsResponse struct {
	Items      []subscriptionResponse `json:"items"`
	NextCursor *string                `json:"next_cursor"`
}

// createSubscriptionRequest is the POST body. target_url is required; is_active
// defaults to true when omitted.
type createSubscriptionRequest struct {
	TargetURL  string   `json:"target_url"`
	EventTypes []string `json:"event_types"`
	IsActive   *bool    `json:"is_active"`
}

// updateSubscriptionRequest is the PUT body. All fields are optional (pointers)
// for partial-update semantics.
type updateSubscriptionRequest struct {
	TargetURL  *string   `json:"target_url"`
	EventTypes *[]string `json:"event_types"`
	IsActive   *bool     `json:"is_active"`
}

// attemptResponse is the per-attempt view for GET /subscriptions/{id}/attempts.
type attemptResponse struct {
	ID            string          `json:"id"`
	WebhookID     string          `json:"webhook_id"`
	EventType     string          `json:"event_type,omitempty"`
	AttemptNumber int             `json:"attempt_number"`
	ReplayNumber  int             `json:"replay_number"`
	Status        string          `json:"status"`
	HTTPStatus    *int            `json:"http_status,omitempty"`
	ErrorDetails  string          `json:"error_details,omitempty"`
	NextRetryAt   *time.Time      `json:"next_retry_at,omitempty"`
	InDLQ         bool            `json:"in_dlq"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

func toSubscriptionResponse(s *domain.Subscription) subscriptionResponse {
	types := s.EventTypes
	if types == nil {
		types = []string{}
	}
	return subscriptionResponse{
		ID:                  s.ID,
		TargetURL:           s.TargetURL,
		EventTypes:          types,
		IsActive:            s.IsActive,
		ConsecutiveFailures: s.ConsecutiveFailures,
		SecretRotatedAt:     s.SecretRotatedAt,
		CreatedAt:           s.CreatedAt,
		UpdatedAt:           s.UpdatedAt,
	}
}

func toAttemptResponse(d *domain.DeliveryLog) attemptResponse {
	return attemptResponse{
		ID:            d.ID,
		WebhookID:     d.WebhookID,
		EventType:     d.EventType,
		AttemptNumber: d.AttemptNumber,
		ReplayNumber:  d.ReplayNumber,
		Status:        string(d.Status),
		HTTPStatus:    d.HTTPStatus,
		ErrorDetails:  d.ErrorDetails,
		NextRetryAt:   d.NextRetryAt,
		InDLQ:         d.InDLQ,
		Payload:       d.Payload,
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
	}
}

// toAttemptView projects a delivery-log row onto the compact per-attempt shape
// used by GET /status/{webhook_id} (Timestamp = the row's UpdatedAt). It is the
// single home for the status-endpoint attempt projection (cf. toAttemptResponse
// for the fuller /subscriptions/{id}/attempts view).
func toAttemptView(d *domain.DeliveryLog) attemptView {
	return attemptView{
		AttemptNumber: d.AttemptNumber,
		ReplayNumber:  d.ReplayNumber,
		Status:        string(d.Status),
		HTTPStatus:    d.HTTPStatus,
		ErrorDetails:  d.ErrorDetails,
		NextRetryAt:   d.NextRetryAt,
		Timestamp:     d.UpdatedAt,
	}
}

// --- keyset cursor codec ----------------------------------------------------

// encodeCursor renders a domain.Cursor as an opaque base64url token.
func encodeCursor(c *domain.Cursor) *string {
	if c == nil {
		return nil
	}
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "," + c.ID
	enc := base64.RawURLEncoding.EncodeToString([]byte(raw))
	return &enc
}

// decodeCursor parses an opaque cursor token. An empty string yields (nil, nil)
// meaning "start from the beginning".
func decodeCursor(token string) (*domain.Cursor, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cursor")
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid cursor")
	}
	if !domain.ValidUUID(parts[1]) {
		return nil, fmt.Errorf("invalid cursor")
	}
	return &domain.Cursor{CreatedAt: ts, ID: parts[1]}, nil
}
