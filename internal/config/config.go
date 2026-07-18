// Package config loads and validates runtime configuration from the environment
// into a single typed struct using only the standard library. The field set and
// defaults are the source of truth for the README configuration table.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-parsed, validated runtime configuration.
type Config struct {
	// Required infrastructure.
	DatabaseURL string // DATABASE_URL — Postgres DSN for the pgxpool
	RedisURL    string // REDIS_URL   — Redis URL (queue, scheduler, DLQ, cache)

	// HTTP server.
	Port string // PORT — listen port (string so it composes into ":"+Port)

	// Delivery / worker pool.
	WebhookTimeout    time.Duration // WEBHOOK_TIMEOUT    — per-delivery HTTP client timeout
	WorkerConcurrency int           // WORKER_CONCURRENCY — errgroup.SetLimit for the pool
	PerSubConcurrency int           // PER_SUB_CONCURRENCY — max in-flight per subscription
	DrainTimeout      time.Duration // DRAIN_TIMEOUT      — graceful in-flight drain deadline

	// Retry / backoff.
	MaxRetryAttempts int           // MAX_RETRY_ATTEMPTS — total attempts (1 initial + 4) before DLQ
	RetryBaseDelay   time.Duration // RETRY_BASE_DELAY   — base of the ×3 backoff curve
	RetryMaxDelay    time.Duration // RETRY_MAX_DELAY    — backoff cap guard

	// Recovery / lease.
	VisibilityTimeout    time.Duration // VISIBILITY_TIMEOUT     — lease stamped on a claimed pending row
	OrphanThreshold      time.Duration // ORPHAN_THRESHOLD       — coarse backstop for lease-less pending rows
	RecoveryScanInterval time.Duration // RECOVERY_SCAN_INTERVAL — orphan-recovery scan period

	// Retention / DLQ.
	LogRetention   time.Duration // LOG_RETENTION_HOURS — cleanup horizon for non-DLQ rows
	DLQMaxAge      time.Duration // DLQ_MAX_AGE         — force-ack age for un-acked DLQ entries
	IdempotencyTTL time.Duration // IDEMPOTENCY_TTL     — prune horizon for ingest_idempotency

	// Caching / in-memory registries.
	CacheTTL        time.Duration // CACHE_TTL          — subscription read-through cache TTL
	RegistryIdleTTL time.Duration // REGISTRY_IDLE_TTL  — idle eviction for bucket/semaphore/breaker registries

	// Rate limiting.
	RateLimitPerSec int // RATE_LIMIT_PER_SEC — token-bucket refill rate per subscription
	RateLimitBurst  int // RATE_LIMIT_BURST   — token-bucket capacity per subscription

	// Circuit breaker.
	BreakerThreshold    int           // BREAKER_THRESHOLD     — consecutive failures that trip Open
	BreakerResetTimeout time.Duration // BREAKER_RESET_TIMEOUT — Open→Half-Open probe delay

	// Auto-disable.
	AutoDisableThreshold int // AUTO_DISABLE_THRESHOLD — consecutive final_failures that set is_active=FALSE

	// Signing / secrets.
	SignatureDriftWindow time.Duration // SIGNATURE_DRIFT_WINDOW — allowed Webhook-Timestamp clock skew
	SecretGraceWindow    time.Duration // SECRET_GRACE_WINDOW    — previous-secret acceptance window

	// Behavioural toggles.
	AllowHTTPURLs bool // ALLOW_HTTP_URLS — permit non-HTTPS target URLs (local dev)
	SyncDelivery  bool // SYNC_DELIVERY   — deliver synchronously in the ingest request
	EnablePprof   bool // ENABLE_PPROF    — mount /api/v1/debug/pprof/

	// API / auth.
	CORSAllowedOrigins []string // CORS_ALLOWED_ORIGINS — explicit origin allow-list (never "*" with credentials)
	AdminAPIKey        string   // ADMIN_API_KEY        — Bearer token for management routes (fails closed if unset)
}

// Addr returns the HTTP listen address (":<port>").
func (c *Config) Addr() string { return ":" + c.Port }

// Load reads configuration from the environment, applying the documented
// defaults, and validates that required values are present and sane.
func Load() (*Config, error) {
	// A set-but-unparseable env var is collected as an error (rather than silently
	// falling back to the default), so a fat-fingered tuning knob fails fast at
	// startup instead of degrading to default behaviour. Unset vars use the default.
	var errs []error
	record := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	getEnvInt := func(key string, def int) int { v, err := parseEnvInt(key, def); record(err); return v }
	getEnvBool := func(key string, def bool) bool { v, err := parseEnvBool(key, def); record(err); return v }
	getEnvDuration := func(key string, def time.Duration) time.Duration {
		v, err := parseEnvDuration(key, def)
		record(err)
		return v
	}
	getEnvHours := func(key string, def time.Duration) time.Duration {
		v, err := parseEnvHours(key, def)
		record(err)
		return v
	}

	c := &Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisURL:    os.Getenv("REDIS_URL"),
		Port:        getEnv("PORT", "8080"),

		WebhookTimeout:    getEnvDuration("WEBHOOK_TIMEOUT", 10*time.Second),
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 50),
		PerSubConcurrency: getEnvInt("PER_SUB_CONCURRENCY", 5),
		DrainTimeout:      getEnvDuration("DRAIN_TIMEOUT", 30*time.Second),

		MaxRetryAttempts: getEnvInt("MAX_RETRY_ATTEMPTS", 5),
		RetryBaseDelay:   getEnvDuration("RETRY_BASE_DELAY", 10*time.Second),
		RetryMaxDelay:    getEnvDuration("RETRY_MAX_DELAY", 15*time.Minute),

		VisibilityTimeout:    getEnvDuration("VISIBILITY_TIMEOUT", 60*time.Second),
		OrphanThreshold:      getEnvDuration("ORPHAN_THRESHOLD", 15*time.Minute),
		RecoveryScanInterval: getEnvDuration("RECOVERY_SCAN_INTERVAL", 5*time.Minute),

		LogRetention:   getEnvHours("LOG_RETENTION_HOURS", 72*time.Hour),
		DLQMaxAge:      getEnvDuration("DLQ_MAX_AGE", 720*time.Hour),
		IdempotencyTTL: getEnvDuration("IDEMPOTENCY_TTL", 5*time.Minute),

		CacheTTL:        getEnvDuration("CACHE_TTL", 5*time.Minute),
		RegistryIdleTTL: getEnvDuration("REGISTRY_IDLE_TTL", time.Hour),

		RateLimitPerSec: getEnvInt("RATE_LIMIT_PER_SEC", 100),
		RateLimitBurst:  getEnvInt("RATE_LIMIT_BURST", 200),

		BreakerThreshold:    getEnvInt("BREAKER_THRESHOLD", 5),
		BreakerResetTimeout: getEnvDuration("BREAKER_RESET_TIMEOUT", 30*time.Second),

		AutoDisableThreshold: getEnvInt("AUTO_DISABLE_THRESHOLD", 5),

		SignatureDriftWindow: getEnvDuration("SIGNATURE_DRIFT_WINDOW", 5*time.Minute),
		SecretGraceWindow:    getEnvDuration("SECRET_GRACE_WINDOW", 24*time.Hour),

		AllowHTTPURLs: getEnvBool("ALLOW_HTTP_URLS", false),
		SyncDelivery:  getEnvBool("SYNC_DELIVERY", false),
		EnablePprof:   getEnvBool("ENABLE_PPROF", false),

		CORSAllowedOrigins: getEnvCSV("CORS_ALLOWED_ORIGINS"),
		AdminAPIKey:        os.Getenv("ADMIN_API_KEY"),
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("config: invalid environment: %w", errors.Join(errs...))
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate enforces the required-field and sanity constraints. ADMIN_API_KEY is
// intentionally NOT required here: the admin route group fails closed at request
// time if it is unset, while health/ready/ingest/docs remain usable.
func (c *Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	check(strings.TrimSpace(c.DatabaseURL) != "", "DATABASE_URL is required")
	check(strings.TrimSpace(c.RedisURL) != "", "REDIS_URL is required")
	port, portErr := strconv.Atoi(c.Port)
	check(portErr == nil && port >= 1 && port <= 65535, "PORT must be an integer in [1, 65535], got %q", c.Port)

	check(c.WebhookTimeout > 0, "WEBHOOK_TIMEOUT must be > 0, got %s", c.WebhookTimeout)
	check(c.WorkerConcurrency >= 1, "WORKER_CONCURRENCY must be >= 1, got %d", c.WorkerConcurrency)
	check(c.PerSubConcurrency >= 1, "PER_SUB_CONCURRENCY must be >= 1, got %d", c.PerSubConcurrency)
	check(c.DrainTimeout > 0, "DRAIN_TIMEOUT must be > 0, got %s", c.DrainTimeout)
	check(c.DrainTimeout >= c.WebhookTimeout, "DRAIN_TIMEOUT must be >= WEBHOOK_TIMEOUT (%s), got %s", c.WebhookTimeout, c.DrainTimeout)

	check(c.MaxRetryAttempts >= 1, "MAX_RETRY_ATTEMPTS must be >= 1, got %d", c.MaxRetryAttempts)
	check(c.RetryBaseDelay > 0, "RETRY_BASE_DELAY must be > 0, got %s", c.RetryBaseDelay)
	check(c.RetryMaxDelay > 0, "RETRY_MAX_DELAY must be > 0, got %s", c.RetryMaxDelay)
	check(c.RetryMaxDelay >= c.RetryBaseDelay, "RETRY_MAX_DELAY must be >= RETRY_BASE_DELAY (%s), got %s", c.RetryBaseDelay, c.RetryMaxDelay)

	check(c.VisibilityTimeout > c.WebhookTimeout, "VISIBILITY_TIMEOUT must be > WEBHOOK_TIMEOUT (%s), got %s", c.WebhookTimeout, c.VisibilityTimeout)
	check(c.OrphanThreshold >= c.VisibilityTimeout, "ORPHAN_THRESHOLD must be >= VISIBILITY_TIMEOUT (%s), got %s", c.VisibilityTimeout, c.OrphanThreshold)
	check(c.RecoveryScanInterval > 0, "RECOVERY_SCAN_INTERVAL must be > 0, got %s", c.RecoveryScanInterval)

	check(c.LogRetention > 0, "LOG_RETENTION_HOURS must be > 0, got %s", c.LogRetention)
	check(c.DLQMaxAge > 0, "DLQ_MAX_AGE must be > 0, got %s", c.DLQMaxAge)
	check(c.IdempotencyTTL > 0, "IDEMPOTENCY_TTL must be > 0, got %s", c.IdempotencyTTL)
	check(c.IdempotencyTTL >= c.SignatureDriftWindow, "IDEMPOTENCY_TTL must be >= SIGNATURE_DRIFT_WINDOW (%s), got %s", c.SignatureDriftWindow, c.IdempotencyTTL)

	check(c.CacheTTL > 0, "CACHE_TTL must be > 0, got %s", c.CacheTTL)
	check(c.RegistryIdleTTL > c.WebhookTimeout, "REGISTRY_IDLE_TTL must be > WEBHOOK_TIMEOUT (%s), got %s", c.WebhookTimeout, c.RegistryIdleTTL)
	check(c.RateLimitPerSec > 0, "RATE_LIMIT_PER_SEC must be > 0, got %d", c.RateLimitPerSec)
	check(c.RateLimitBurst > 0, "RATE_LIMIT_BURST must be > 0, got %d", c.RateLimitBurst)

	check(c.BreakerThreshold > 0, "BREAKER_THRESHOLD must be > 0, got %d", c.BreakerThreshold)
	check(c.BreakerResetTimeout > 0, "BREAKER_RESET_TIMEOUT must be > 0, got %s", c.BreakerResetTimeout)
	check(c.AutoDisableThreshold > 0, "AUTO_DISABLE_THRESHOLD must be > 0, got %d", c.AutoDisableThreshold)

	check(c.SignatureDriftWindow > 0, "SIGNATURE_DRIFT_WINDOW must be > 0, got %s", c.SignatureDriftWindow)
	check(c.SecretGraceWindow >= 0, "SECRET_GRACE_WINDOW must be >= 0, got %s", c.SecretGraceWindow)

	if len(errs) > 0 {
		return fmt.Errorf("config: invalid values: %w", errors.Join(errs...))
	}
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseEnvInt returns def when key is unset, the parsed value when valid, or
// (def, error) when the var is set but unparseable so Load can fail fast.
func parseEnvInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q is not a valid integer", key, v)
	}
	return n, nil
}

func parseEnvBool(key string, def bool) (bool, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q is not a valid bool", key, v)
	}
	return b, nil
}

func parseEnvDuration(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q is not a valid duration (e.g. 30s, 5m, 2h)", key, v)
	}
	return d, nil
}

// parseEnvHours parses an integer number of hours into a Duration.
func parseEnvHours(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s=%q is not a valid integer number of hours", key, v)
	}
	return time.Duration(n) * time.Hour, nil
}

// getEnvCSV splits a comma-separated env var into a trimmed, non-empty slice.
// Returns nil when unset (so the CORS layer can distinguish "no origins set").
func getEnvCSV(key string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
