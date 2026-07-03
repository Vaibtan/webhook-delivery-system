package domain

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Subscription is a webhook endpoint registration. It mirrors the Python
// `Subscription` model plus the NEW fields introduced by the Go port:
// is_active, consecutive_failures, and the dual-secret rotation columns.
//
// IDs are plain strings (UUID text) so this package keeps ZERO non-stdlib
// imports — the dependency rule (arrows point inward). The store adapter is
// responsible for UUID generation and any driver-specific encoding.
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

// AcceptsEvent reports whether this subscription should receive an event of the
// given type. An empty EventTypes list means "all events" (parity with the
// Python exact-match filter, which treats no filter as accept-all).
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
