package domain

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxTargetURLLength mirrors subscriptions.target_url and
	// delivery_logs.target_url (VARCHAR(2048)). Validate at the HTTP boundary so
	// oversized input is a client error rather than a database error.
	MaxTargetURLLength = 2048
	// MaxEventTypeLength mirrors delivery_logs.event_type (VARCHAR(100)).
	MaxEventTypeLength = 100
)

// Subscription is a webhook endpoint registration. IDs remain UUID text so the
// domain package does not depend on a database driver.
type Subscription struct {
	ID                  string
	TargetURL           string
	SecretKey           string
	PreviousSecretKey   string     // "" when never rotated
	SecretRotatedAt     *time.Time // nil when never rotated; start of the grace window
	EventTypes          []string
	IsActive            bool
	ConsecutiveFailures int
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// ValidateTargetURL enforces the target-URL contract: a well-formed absolute
// URL that is HTTPS, unless allowHTTP permits plain HTTP (local dev only, via
// ALLOW_HTTP_URLS=true). Returns ErrInvalidInput-wrapped errors on failure.
func ValidateTargetURL(raw string, allowHTTP bool) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("%w: target_url is required", ErrInvalidInput)
	}
	if !utf8.ValidString(raw) || utf8.RuneCountInString(raw) > MaxTargetURLLength {
		return fmt.Errorf("%w: target_url must be at most %d characters", ErrInvalidInput, MaxTargetURLLength)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: target_url is not a valid URL: %w", ErrInvalidInput, err)
	}
	if u.Host == "" {
		return fmt.Errorf("%w: target_url must be absolute with a host", ErrInvalidInput)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if allowHTTP {
			return nil
		}
		return fmt.Errorf("%w: target_url must use https (set ALLOW_HTTP_URLS=true to permit http)", ErrInvalidInput)
	default:
		return fmt.Errorf("%w: target_url scheme %q is not allowed", ErrInvalidInput, u.Scheme)
	}
}

// ValidateEventType enforces the storage contract for an optional ingest event
// type. The empty value is valid and means that no type was supplied.
func ValidateEventType(eventType string) error {
	if !utf8.ValidString(eventType) || utf8.RuneCountInString(eventType) > MaxEventTypeLength {
		return fmt.Errorf("%w: event_type must be at most %d characters", ErrInvalidInput, MaxEventTypeLength)
	}
	return nil
}

// AcceptsEvent reports whether this subscription should receive an event of the
// given type. An empty EventTypes list accepts every event.
func (s *Subscription) AcceptsEvent(eventType string) bool {
	if len(s.EventTypes) == 0 {
		return true
	}
	for _, t := range s.EventTypes {
		if t == eventType {
			return true
		}
	}
	return false
}
