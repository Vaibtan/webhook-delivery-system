package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidateAcceptsSafeRelationships(t *testing.T) {
	require.NoError(t, validConfig().validate())
}

func TestConfigValidateRejectsUnsafeValues(t *testing.T) {
	tests := []struct {
		name string
		want string
		set  func(*Config)
	}{
		{"port", "PORT", func(c *Config) { c.Port = "70000" }},
		{"webhook timeout", "WEBHOOK_TIMEOUT", func(c *Config) { c.WebhookTimeout = 0 }},
		{"drain relation", "DRAIN_TIMEOUT", func(c *Config) { c.DrainTimeout = 5 * time.Second }},
		{"retry base", "RETRY_BASE_DELAY", func(c *Config) { c.RetryBaseDelay = 0 }},
		{"retry max relation", "RETRY_MAX_DELAY", func(c *Config) { c.RetryMaxDelay = time.Second }},
		{"visibility relation", "VISIBILITY_TIMEOUT", func(c *Config) { c.VisibilityTimeout = 10 * time.Second }},
		{"orphan relation", "ORPHAN_THRESHOLD", func(c *Config) { c.OrphanThreshold = 30 * time.Second }},
		{"recovery interval", "RECOVERY_SCAN_INTERVAL", func(c *Config) { c.RecoveryScanInterval = 0 }},
		{"retention", "LOG_RETENTION_HOURS", func(c *Config) { c.LogRetention = 0 }},
		{"dlq age", "DLQ_MAX_AGE", func(c *Config) { c.DLQMaxAge = 0 }},
		{"idempotency relation", "IDEMPOTENCY_TTL", func(c *Config) { c.IdempotencyTTL = time.Minute }},
		{"cache ttl", "CACHE_TTL", func(c *Config) { c.CacheTTL = 0 }},
		{"registry relation", "REGISTRY_IDLE_TTL", func(c *Config) { c.RegistryIdleTTL = 10 * time.Second }},
		{"rate", "RATE_LIMIT_PER_SEC", func(c *Config) { c.RateLimitPerSec = 0 }},
		{"burst", "RATE_LIMIT_BURST", func(c *Config) { c.RateLimitBurst = 0 }},
		{"breaker threshold", "BREAKER_THRESHOLD", func(c *Config) { c.BreakerThreshold = 0 }},
		{"breaker reset", "BREAKER_RESET_TIMEOUT", func(c *Config) { c.BreakerResetTimeout = 0 }},
		{"auto disable", "AUTO_DISABLE_THRESHOLD", func(c *Config) { c.AutoDisableThreshold = 0 }},
		{"signature drift", "SIGNATURE_DRIFT_WINDOW", func(c *Config) { c.SignatureDriftWindow = 0 }},
		{"secret grace", "SECRET_GRACE_WINDOW", func(c *Config) { c.SecretGraceWindow = -time.Second }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.set(cfg)
			err := cfg.validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func validConfig() *Config {
	return &Config{
		DatabaseURL: "postgres://example", RedisURL: "redis://example", Port: "8080",
		WebhookTimeout: 10 * time.Second, WorkerConcurrency: 10, PerSubConcurrency: 2,
		DrainTimeout: 30 * time.Second, MaxRetryAttempts: 5,
		RetryBaseDelay: 10 * time.Second, RetryMaxDelay: 15 * time.Minute,
		VisibilityTimeout: time.Minute, OrphanThreshold: 15 * time.Minute,
		RecoveryScanInterval: 5 * time.Minute, LogRetention: 72 * time.Hour,
		DLQMaxAge: 720 * time.Hour, IdempotencyTTL: 5 * time.Minute,
		CacheTTL: 5 * time.Minute, RegistryIdleTTL: time.Hour,
		RateLimitPerSec: 100, RateLimitBurst: 200,
		BreakerThreshold: 5, BreakerResetTimeout: 30 * time.Second,
		AutoDisableThreshold: 5, SignatureDriftWindow: 5 * time.Minute,
		SecretGraceWindow: 24 * time.Hour,
	}
}
