# Go Webhook Delivery System

[![CI](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/ci.yml/badge.svg)](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)

A production-oriented webhook ingestion and delivery service built with Go, PostgreSQL, and Redis. It authenticates inbound events, persists delivery state before enqueueing work, sends signed outbound webhooks, retries transient failures, and exposes replay, dead-letter, status, and operational APIs.

The implementation uses `net/http`, explicit dependency injection, consumer-owned interfaces, and native goroutine supervision. PostgreSQL is authoritative; Redis accelerates queueing, retry scheduling, dead-letter indexing, and subscription reads.

## System design

![Webhook delivery system design](docs/architecture/Screenshot%202026-07-18%20222303.png)

The editable source is [`docs/architecture/webhook-system-design.excalidraw`](docs/architecture/webhook-system-design.excalidraw). See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for delivery invariants, failure recovery, module boundaries, and scaling constraints.

## Product applications

Product teams can use this service as a shared event-delivery layer instead of rebuilding signing, retries, failure recovery, and delivery visibility for every integration. A producer publishes a domain event, subscriptions route it to customer or internal endpoints, and operators can inspect, replay, or acknowledge failed deliveries.

| Application | Example events | Product value |
|---|---|---|
| Payments and billing | `payment.succeeded`, `invoice.overdue`, `refund.created` | Keep merchant, accounting, and risk systems synchronized with replayable notifications. |
| Commerce and fulfillment | `order.created`, `shipment.dispatched`, `inventory.low` | Connect storefronts, warehouses, carriers, and customer-notification services without tight coupling. |
| SaaS integrations | `account.created`, `subscription.changed`, `record.updated` | Offer customer-configured integrations without maintaining a separate delivery worker for every destination. |
| Identity and security | `user.invited`, `role.changed`, `suspicious_login` | Send signed events to IAM, SIEM, audit, and compliance systems. |
| Developer tooling and operations | `build.failed`, `deployment.completed`, `incident.opened` | Trigger ChatOps, release automation, alert enrichment, and incident workflows. |
| Monitoring and connected devices | `threshold.exceeded`, `device.offline`, `maintenance.required` | Drive downstream alerts and automation while isolating slow or failing subscribers. |

The strongest fit is event-driven integration where consumers accept at-least-once delivery and deduplicate by webhook ID. Workflows requiring strict global ordering or exactly-once processing need additional coordination outside this service.

## Core behavior

- **At-least-once delivery, without ordering guarantees.** Consumers must deduplicate using the stable `X-Webhook-ID`. `X-Webhook-Delivery-ID` identifies one attempt.
- **Durable-before-async.** Ingest writes a `pending` attempt to PostgreSQL before its ID is pushed to Redis. Recovery re-enqueues due rows when a post-commit Redis write is lost.
- **Atomic retry chains.** A failed attempt and its successor `pending` attempt are committed in one PostgreSQL transaction.
- **CAS delivery lifecycle.** Visibility leases and conditional state transitions prevent duplicate queue entries from finalizing the same attempt twice.
- **Bounded retries.** Failures use capped ×3 backoff with ±20% jitter before the terminal attempt enters the DLQ.
- **Operational recovery.** Retry polling, orphan recovery, DLQ reconciliation, retention cleanup, and registry sweeping run continuously under one lifecycle supervisor.
- **Subscription isolation.** Each subscription has its own ingest token bucket and outbound concurrency semaphore. Circuit breakers are keyed by target URL.
- **Security at the boundary.** Inbound HMAC verification, timestamp replay protection, signed idempotency keys, secret rotation, request-size limits, fail-closed admin auth, and SSRF-safe outbound dialing are built in.

## Quick start

Requirements:

- Go 1.26
- Docker with Compose

Start PostgreSQL and Redis:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Set the required environment and run the server:

```bash
export DATABASE_URL="postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable"
export REDIS_URL="redis://localhost:6379/0"
export ADMIN_API_KEY="dev-admin-key"
export ALLOW_HTTP_URLS=true

go run ./cmd/server
```

PowerShell uses the same values with `$env:DATABASE_URL=...`, `$env:REDIS_URL=...`, and so on. The server applies embedded migrations during startup and listens on `:8080` by default.

Useful commands:

```bash
go run ./cmd/server -migrate=up    # apply all migrations and exit
go run ./cmd/server -migrate=down  # roll back one migration and exit
go test ./...                      # unit tests
go vet ./...
golangci-lint run
```

Fresh integration infrastructure needs the schema before the tagged suite runs:

```bash
go run ./cmd/server -migrate=up
go test -tags=integration ./...
```

Stop local infrastructure with:

```bash
docker compose -f docker/docker-compose.yml down
```

## Configuration

`internal/config` parses and validates the environment at startup. Unset values use the defaults below; malformed or unsafe values fail startup instead of silently falling back.

| Variable | Default | Purpose |
|---|---:|---|
| `DATABASE_URL` | required | PostgreSQL DSN. |
| `REDIS_URL` | required | Redis URL for queues, schedules, DLQ index, and subscription cache. |
| `PORT` | `8080` | HTTP listen port. |
| `WEBHOOK_TIMEOUT` | `10s` | Outbound request timeout. |
| `WORKER_CONCURRENCY` | `50` | Maximum concurrent worker tasks. |
| `PER_SUB_CONCURRENCY` | `5` | Maximum concurrent deliveries per subscription. |
| `DRAIN_TIMEOUT` | `30s` | Graceful shutdown deadline for in-flight deliveries. |
| `MAX_RETRY_ATTEMPTS` | `5` | Total attempts, including the initial attempt. |
| `RETRY_BASE_DELAY` | `10s` | Initial retry delay before jitter. |
| `RETRY_MAX_DELAY` | `15m` | Retry-delay cap. |
| `VISIBILITY_TIMEOUT` | `60s` | Lease applied when an attempt is claimed. |
| `ORPHAN_THRESHOLD` | `15m` | Minimum age before a lease-less pending row is rearmed. |
| `RECOVERY_SCAN_INTERVAL` | `5m` | Orphan recovery and DLQ reconciliation interval. |
| `LOG_RETENTION_HOURS` | `72` | Retention for terminal, acknowledged delivery rows. |
| `DLQ_MAX_AGE` | `720h` | Age at which unacknowledged DLQ rows are force-acknowledged. |
| `IDEMPOTENCY_TTL` | `5m` | Retention for inbound idempotency records. |
| `CACHE_TTL` | `5m` | Subscription read-through cache TTL. |
| `REGISTRY_IDLE_TTL` | `1h` | Idle TTL for in-memory bucket, semaphore, and breaker entries. |
| `RATE_LIMIT_PER_SEC` | `100` | Per-subscription token refill rate. |
| `RATE_LIMIT_BURST` | `200` | Per-subscription ingest burst capacity. |
| `BREAKER_THRESHOLD` | `5` | Consecutive failures before a target breaker opens. |
| `BREAKER_RESET_TIMEOUT` | `30s` | Delay before one half-open probe is admitted. |
| `AUTO_DISABLE_THRESHOLD` | `5` | Consecutive terminal failures before a subscription is disabled. |
| `SIGNATURE_DRIFT_WINDOW` | `5m` | Accepted inbound timestamp skew. |
| `SECRET_GRACE_WINDOW` | `24h` | Previous-secret acceptance window after rotation. |
| `ALLOW_HTTP_URLS` | `false` | Permit non-HTTPS targets; intended for local development. |
| `SYNC_DELIVERY` | `false` | Run only the first attempt in the ingest request. Background processing remains active. |
| `ENABLE_PPROF` | `false` | Mount admin-protected `/api/v1/debug/pprof/`. |
| `CORS_ALLOWED_ORIGINS` | empty | Comma-separated browser-origin allow-list. |
| `ADMIN_API_KEY` | empty | Bearer token for management APIs. Empty means those routes fail closed with `503`. |

Important validation relationships include:

- `VISIBILITY_TIMEOUT > WEBHOOK_TIMEOUT`
- `DRAIN_TIMEOUT >= WEBHOOK_TIMEOUT`
- `ORPHAN_THRESHOLD >= VISIBILITY_TIMEOUT`
- `RETRY_MAX_DELAY >= RETRY_BASE_DELAY`
- `IDEMPOTENCY_TTL >= SIGNATURE_DRIFT_WINDOW`

## API and authentication

All routes use the `/api/v1` prefix.

| Method and path | Auth | Purpose |
|---|---|---|
| `GET /health` | Public | PostgreSQL and Redis liveness. |
| `GET /ready` | Public | Dependency and worker-pool readiness. |
| `GET /docs` | Public | Interactive Swagger UI. |
| `GET /openapi.json` | Public | Embedded OpenAPI 3.1 specification. |
| `POST /ingest/{id}` | Subscription HMAC | Validate and persist an event. |
| `POST /subscriptions` | Admin bearer | Create a subscription and return its secret once. |
| `GET /subscriptions` | Admin bearer | Keyset-paginated subscription list. |
| `GET /subscriptions/{id}` | Admin bearer | Get a secret-redacted subscription. |
| `PUT /subscriptions/{id}` | Admin bearer | Update target, event filters, or active state. |
| `DELETE /subscriptions/{id}` | Admin bearer | Delete a subscription and cascade its delivery rows. |
| `GET /subscriptions/{id}/attempts` | Admin bearer | List recent attempts for a subscription. |
| `POST /subscriptions/{id}/rotate-secret` | Admin bearer | Rotate the secret and return the new value once. |
| `POST /subscriptions/{id}/replay/{webhook_id}` | Admin bearer | Atomically claim a DLQ row and start a replay chain. |
| `POST /subscriptions/{id}/dlq/{webhook_id}/ack` | Admin bearer | Acknowledge a DLQ row without replaying it. |
| `GET /status/{webhook_id}` | Admin bearer | Retrieve all attempt and replay chains for an event. |
| `GET /status/metrics/summary` | Admin bearer | Aggregate delivery statuses over a time window. |
| `GET /monitor` | Admin bearer | Queue, DLQ, breaker, status, uptime, and latency data. |
| `GET /debug/vars` | Admin bearer | `expvar` metrics. |
| `GET /debug/pprof/` | Admin bearer + flag | Runtime profiling when `ENABLE_PPROF=true`. |

The full request and response schemas are served by the running application at [`/api/v1/docs`](http://localhost:8080/api/v1/docs).

## Inbound signing protocol

Every ingest request requires:

- `Webhook-Timestamp: <unix-seconds>`
- `X-Hub-Signature-256: sha256=<hex-hmac>`
- optional `X-Idempotency-Key: [A-Za-z0-9_-]{1,128}`

The signed bytes are:

```text
timestamp + "." + idempotency_key + "." + raw_body
```

The idempotency key is an empty string when the header is absent. Including it in the signature prevents an intermediary from changing deduplication semantics without invalidating the request. The key and initial delivery row are committed together in PostgreSQL.

Generate headers with the included helper:

```bash
python scripts/sign_webhook.py \
  --secret "$SECRET" \
  --body '{"event":"order.created","order_id":42}' \
  --idempotency-key "order-42"
```

The service verifies the current subscription secret and, during `SECRET_GRACE_WINDOW`, the previous secret. Outbound requests use the same grammar with an empty idempotency-key field and include `X-Webhook-ID`, `X-Webhook-Delivery-ID`, and `X-Delivery-Attempt`.

## Minimal request flow

Create a subscription:

```bash
BASE="http://localhost:8080/api/v1"
ADMIN="Authorization: Bearer $ADMIN_API_KEY"

curl -sS -X POST "$BASE/subscriptions" \
  -H "$ADMIN" \
  -H 'Content-Type: application/json' \
  --data-raw '{"target_url":"https://example.com/webhooks","event_types":["order.created"]}'
```

Save the returned `id` and one-time `secret_key`, generate signing headers, then ingest JSON:

```bash
curl -sS -X POST "$BASE/ingest/$ID?event_type=order.created" \
  -H "Webhook-Timestamp: $TIMESTAMP" \
  -H "X-Hub-Signature-256: $SIGNATURE" \
  -H 'Content-Type: application/json' \
  --data-raw '{"event":"order.created","order_id":42}'
```

Async mode returns `202 Accepted`. A repeated idempotency key returns the original `webhook_id` with `200 OK` and does not enqueue another attempt. In sync mode, the first attempt runs in-request and returns `200`; retries still use the background pipeline.

## Observability and operations

- JSON logs use `log/slog`; API logs carry a generated request ID and delivery logs carry delivery/webhook IDs.
- `/monitor` combines PostgreSQL status counts with Redis queue/DLQ depths, circuit-breaker states, uptime, and a bounded latency histogram (`min`, `max`, `avg`, `p50`, `p95`, `p99`).
- `/debug/vars` publishes `expvar` delivery counters.
- `/debug/pprof/` is disabled by default and requires both the feature flag and admin authentication.
- `SIGINT` and `SIGTERM` stop new dequeue work, shut down HTTP with a bounded context, and wait for in-flight deliveries to drain.
- Cleanup never deletes `pending` or unacknowledged DLQ rows. Explicit subscription deletion is the one operation that cascades all delivery history for that subscription.

## Testing and CI

Unit tests cover configuration, validation, signing, middleware, lifecycle supervision, rate limiting, circuit breaking, worker behavior, and retry math. Integration tests use live PostgreSQL and Redis to exercise atomic ingest, duplicate dequeue claims, retry recovery, DLQ replay/ack, cache coherence, cleanup safety, and synchronous-to-background retry handoff.

GitHub Actions runs:

- `go vet`, `gofmt`, and `golangci-lint`
- build and dependency checks
- shuffled unit tests under the race detector
- integration tests under the race detector with PostgreSQL and Redis services

## Deployment

The root `Dockerfile` builds a static Go binary and runs it as a non-root user in a distroless image. `railway.toml` and `railway.json` configure Dockerfile builds and `/api/v1/health` checks; `Procfile` is available for buildpack-style platforms.

Production deployment requires PostgreSQL, Redis, `DATABASE_URL`, `REDIS_URL`, and a strong `ADMIN_API_KEY`. Migrations run during startup, so multiple replicas should not be introduced until the deployment has an explicit migration strategy and distributed rate/concurrency controls.

Current rate limits, concurrency semaphores, and circuit breakers are process-local. A single replica gives exact configured limits; with `N` replicas those effective limits multiply by `N`.

## Project layout

| Path | Responsibility |
|---|---|
| `cmd/server` | Configuration, migrations, dependency composition, and process registration. |
| `internal/api` | HTTP routing, middleware, handlers, DTOs, and embedded OpenAPI. |
| `internal/domain` | Entities, value types, validation, statuses, and sentinel errors. |
| `internal/store` | PostgreSQL repositories and transactional delivery lifecycle. |
| `internal/subscription` | Subscription CRUD, read-through cache coherence, delivery consequences, token buckets, and semaphores. |
| `internal/worker` | Delivery engine, worker pool, retry polling, recovery, cleanup, and DLQ reconciliation. |
| `internal/lifecycle` | Coordinated cancellation, HTTP shutdown, and process draining. |
| `internal/infra` | Redis adapters, circuit breaker, metrics, HMAC, SSRF-safe dialing, and concurrency primitives. |
| `migrations` | Embedded PostgreSQL schema and indexes. |
| `docker` | Local PostgreSQL and Redis Compose stack. |

## Runtime dependencies

- [`pgx/v5`](https://github.com/jackc/pgx) for PostgreSQL
- [`go-redis/v9`](https://github.com/redis/go-redis) for Redis
- [`golang-migrate`](https://github.com/golang-migrate/migrate) for embedded migrations
- [`golang.org/x/sync`](https://pkg.go.dev/golang.org/x/sync) for bounded goroutine groups

`testify` is test-only. The interactive API page loads Swagger UI from a CDN.
