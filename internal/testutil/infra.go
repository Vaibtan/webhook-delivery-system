//go:build integration

// Package testutil provides shared helpers for integration tests that run
// against live Postgres + Redis (the docker/docker-compose.yml stack). It is
// compiled only under the `integration` build tag, so unit tests and the
// production binary never pull it in.
//
// Run integration tests with:
//
//	go test -tags=integration ./...
//
// reading DATABASE_URL/REDIS_URL (or TEST_DATABASE_URL/TEST_REDIS_URL).
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func dsn() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return os.Getenv("DATABASE_URL")
}

func redisURL() string {
	if v := os.Getenv("TEST_REDIS_URL"); v != "" {
		return v
	}
	return os.Getenv("REDIS_URL")
}

// Pool returns a pgx pool to the test database, skipping the test if no DSN is set.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	d := dsn()
	if d == "" {
		t.Skip("integration: set DATABASE_URL or TEST_DATABASE_URL")
	}
	pool, err := pgxpool.New(context.Background(), d)
	if err != nil {
		t.Fatalf("integration: connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Redis returns a go-redis client to the test instance, skipping if unset.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	u := redisURL()
	if u == "" {
		t.Skip("integration: set REDIS_URL or TEST_REDIS_URL")
	}
	opt, err := redis.ParseURL(u)
	if err != nil {
		t.Fatalf("integration: parse redis url: %v", err)
	}
	c := redis.NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Truncate wipes the application tables for a clean test slate.
func Truncate(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		"TRUNCATE delivery_logs, ingest_idempotency, subscriptions CASCADE"); err != nil {
		t.Fatalf("integration: truncate: %v", err)
	}
}

// FlushRedis clears the test Redis database.
func FlushRedis(t testing.TB, c *redis.Client) {
	t.Helper()
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("integration: flushdb: %v", err)
	}
}
