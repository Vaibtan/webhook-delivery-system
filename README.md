# Go Webhook Delivery System

[![CI](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/ci.yml/badge.svg)](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/ci.yml)
[![Security](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/security.yml/badge.svg)](https://github.com/Vaibtan/webhook-delivery-system/actions/workflows/security.yml)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)

A production-grade webhook delivery service written in **Go 1.26** — a portfolio port of a Python FastAPI/Celery service. It accepts signed webhook events, persists every delivery attempt to PostgreSQL, and delivers them to subscriber endpoints with at-least-once semantics: exponential backoff with jitter, a per-URL circuit breaker, per-subscription rate limiting and concurrency control, secret rotation with a grace window, replay-attack protection, SSRF-safe outbound dialing, and a dead-letter queue with replay. Celery is replaced entirely by native Go concurrency (a bounded goroutine worker pool over a Redis queue + sorted-set retry scheduler). The runtime depends on **three third-party packages** — [`pgx/v5`](https://github.com/jackc/pgx), [`go-redis/v9`](https://github.com/redis/go-redis), [`golang-migrate`](https://github.com/golang-migrate/migrate) — **plus `golang.org/x/sync`** (the Go team's extended-stdlib `errgroup`); everything else is the standard library.

---

## Table of Contents

1. [Architecture (ADR)](#architecture-adr)
2. [Delivery Guarantee Statement](#delivery-guarantee-statement)
3. [Authentication & Authorization](#authentication--authorization)
4. [Signature Scheme & Client Migration Note](#signature-scheme--client-migration-note)
5. [Setup & Run](#setup--run)
6. [Configuration](#configuration)
7. [API Reference](#api-reference)
8. [curl Examples](#curl-examples)
9. [Signing Helper Script](#signing-helper-script)
10. [Observability](#observability)
11. [Testing](#testing)
12. [Architectural Considerations / Future Work](#architectural-considerations--future-work)
13. [Deployment](#deployment)
14. [Credits](#credits)

---

## Architecture (ADR)

The service is built as a **hexagonal (ports & adapters)** application. The dependency rule is strict and one-directional: **`internal/domain` imports nothing outside the standard library**, and every adapter depends on domain ports — never the reverse.

```
                        cmd/server/main.go
        parse config -> init adapters -> wire services -> run goroutines
                 |                                  |
        internal/api  (net/http ServeMux)   internal/worker  (pool, scheduler,
        handlers, middleware                  recovery, cleanup, dlq-reconciler)
                 \                                  /
                       internal/domain
            Subscription, DeliveryLog, enums, ports,
                 sentinel errors  (ZERO external imports)
                 /                                  \
        internal/store  (PostgreSQL/pgx)    internal/infra/*  (queue, scheduler,
        repository impls, raw SQL, txns       cache, breaker, ratelimit,
                                              safedial, signature, metrics)
```

| Layer | Responsibility |
|---|---|
| `cmd/server` | Composition root: parse config, run embedded migrations, wire adapters via DI, supervise goroutines with `errgroup`. |
| `internal/config` | Typed `Config` struct, stdlib `os.Getenv` parsing — no config framework. |
| `internal/domain` | Entities (`Subscription`, `DeliveryLog`), enums, repository/service **ports**, sentinel errors. Pure stdlib. |
| `internal/api` | `net/http` `ServeMux` (Go 1.22+ `{id}` path params), middleware chain, handlers. |
| `internal/store` | `pgx/v5` pool + raw-SQL repository implementations (cursor pagination, transactions). |
| `internal/infra/*` | Redis queue/scheduler/cache, circuit breaker, token-bucket rate limiter, SSRF dialer, HMAC signer, latency metrics. |
| `internal/worker` | Bounded worker pool, retry scheduler, orphan recovery, retention cleanup, DLQ reconciler — all goroutine-based. |

### Key design decisions (and why)

- **One row per attempt (immutable delivery log).** Each delivery attempt is its own `delivery_logs` row, mirroring the source system's `create_retry_log`. `delivery_logs.id` (`X-Webhook-Delivery-ID`) is unique per attempt; `webhook_id` (`X-Webhook-ID`) is shared across all attempts of one event. This single decision is load-bearing for the status endpoint, the metrics summary, the retry scheduler, and orphan recovery — a scheduled retry is a *new* `pending` row, so the recovery scan that looks for `status='pending'` provably catches lost retries, not just first attempts. (Plan §0.)
- **CAS + single-transaction + visibility-lease lifecycle.** Every status transition is a compare-and-swap (`UPDATE … WHERE id=$1 AND status='pending'`); a 0-row result means another worker already finalized it, so the loser aborts with no side effects. The `failed_attempt` update and its successor `INSERT` share **one transaction**, so a crash can never strand a `failed_attempt` row without a `pending` successor. When a worker claims a row it stamps a `VISIBILITY_TIMEOUT` lease (`next_retry_at = now()+lease WHERE … AND next_retry_at <= now()`), which is itself the mutual-exclusion point for duplicate dequeues. **Postgres is the source of truth; Redis is a derived index** healed by the recovery job and the DLQ reconciler. (Plan §0, §1.)
- **Redis sorted-set retry scheduler with an atomic Lua claim.** Due retries are moved from `webhook:retry:schedule` (ZSET, `score = next_retry_at`) into `webhook:queue` by a single Lua script (`ZRANGEBYSCORE` + `ZREM` + `LPUSH`), eliminating the race where two schedulers claim the same retry. (Plan §2.)
- **Per-URL circuit breaker with a generation-token authoritative probe.** A Closed→Open→Half-Open FSM built on `sync/atomic`. `Allow()` returns a generation token; only a result still carrying the current generation can change state, so a late-finishing request can't spuriously close the breaker. A breaker denial reschedules the `pending` row **without consuming a retry attempt**, so a down target never burns a healthy payload's 5-attempt budget. (Plan §3.)
- **Per-subscription token-bucket rate limiter + non-blocking concurrency semaphore.** A mutex-only, fixed-point token bucket per subscription enforces ingest rate. A per-subscription semaphore (acquired **non-blockingly**) caps in-flight deliveries; if a subscription is at its limit the worker reschedules with a short backoff (no attempt consumed) rather than parking, preventing head-of-line blocking. (Plan §4, Adv §3.)
- **SSRF-safe dialer.** The outbound `http.Transport` dials through a custom `SafeDialContext` built on `net/netip` that resolves the host and rejects loopback/private/link-local/CGNAT/reserved ranges (with `Unmap()` to defeat the `::ffff:` IPv4-mapped bypass), pins the vetted IP to defeat DNS rebinding, sets `Proxy: nil` so a proxy env var can't tunnel around the check, and enforces a redirect cap + HTTPS-downgrade rejection. (Plan §6.)
- **Postgres-authoritative idempotency (NOT Redis `SETNX`).** The optional `X-Idempotency-Key` is deduped via an `INSERT … ON CONFLICT DO NOTHING` into `ingest_idempotency` **in the same transaction** as the initial `pending` delivery row. A Redis-only `SETNX` would commit the dedupe marker before the DB write and could acknowledge a key whose webhook never durably landed — the atomic Postgres path makes that failure mode impossible. (Plan §5.)

---

## Delivery Guarantee Statement

**At-least-once delivery, with NO ordering guarantee.**

- Because each attempt is a distinct row, **`X-Webhook-Delivery-ID` is unique per attempt** while **`X-Webhook-ID` is stable across all attempts of one event**. Consumers **must dedupe on `X-Webhook-ID`** and may use `Webhook-Timestamp` for staleness/ordering decisions.
- The terminal-transition CAS and visibility lease make a rare double-delivery harmless at the DB layer, but a duplicate HTTP call of the same `X-Webhook-Delivery-ID` is bounded only by at-least-once — hence the dedupe requirement.
- **Scope of the guarantee.** The single deletion path *outside* `cleanup.go` is the explicit, operator-initiated subscription delete, which `ON DELETE CASCADE`s its `delivery_logs` (including still-`pending` and un-acked DLQ rows). The at-least-once guarantee therefore holds **for a subscription's lifetime, not past its deletion**. `cleanup.go` itself never reaps undelivered work (`status <> 'pending'` guard).
- **Limits are per-instance.** v1 runs a **single instance**, so the per-subscription rate limit, per-subscription concurrency cap, and per-URL circuit breakers live in process memory and the configured numbers are exact. With *N* replicas the effective limits would be *N×*; distributing them (Redis-backed) is documented future work.

---

## Authentication & Authorization

| Route group | Auth |
|---|---|
| `POST /api/v1/ingest/{id}` | **Per-subscription HMAC** — `X-Hub-Signature-256` + `Webhook-Timestamp`. No admin token. |
| `GET /api/v1/health`, `GET /api/v1/ready` | **Public** (platform liveness/readiness probes). |
| `GET /api/v1/docs`, `GET /api/v1/openapi.json` | **Public** (spec only, no secrets). |
| All other routes — subscription CRUD, rotate-secret, replay, dlq/ack, status, monitor, debug/vars, pprof | **Admin auth** — `Authorization: Bearer <ADMIN_API_KEY>`, **constant-time** compared. |

- The admin middleware **fails closed**: if `ADMIN_API_KEY` is unset the admin group returns **`503`**, so the service can never be deployed accidentally wide-open. A present-but-mismatched/absent token on a request returns **`401`**.
- **Secret redaction.** Subscription responses (create/get/list/update) never include `secret_key` or `previous_secret_key`. The plaintext secret is shown **once** — at create and at `rotate-secret` — and never again.
- A **single static admin credential is a deliberate v1 choice** for a portfolio build. Per-user tokens / JWT / mTLS (real per-subscription authorization isolation) is the documented production evolution.

---

## Signature Scheme & Client Migration Note

> **⚠ Behavioral divergence from the Python system — this breaks existing clients.**

The Go service signs **one canonical grammar in both directions**:

```
signed_message  = timestamp + "." + idempotency_key + "." + body
signature_header = "sha256=" + hex( HMAC-SHA256(secret, signed_message) )
```

- **`timestamp`** — the value of the now-**required** `Webhook-Timestamp` ingest header (unix seconds, digits only). Verified against a **5-minute drift window** (`SIGNATURE_DRIFT_WINDOW`) for replay protection.
- **`idempotency_key`** — the optional inbound `X-Idempotency-Key`, or the **empty string** when absent. **Always empty for outbound deliveries.** Charset is `[A-Za-z0-9_-]{1,128}`, which excludes the `.` delimiter so the field boundaries are unambiguous when the verifier reconstructs the message.
- **`body`** — the exact transmitted body bytes (inbound: the raw received body; outbound: the once-canonicalized JSON that is actually sent).
- **Header format** — `X-Hub-Signature-256: sha256=<hex>`.

**Why the key is folded into the signed bytes.** `X-Idempotency-Key` is part of the signed material (not a free, unsigned header). This stops an attacker from defeating dedupe by mutating only the key while a body+timestamp-only signature still verifies. The key is then deduped **Postgres-authoritatively** (`ingest_idempotency`, see ADR) — a repeat key is an idempotent no-op that returns the original `X-Webhook-ID` and never double-enqueues.

**Secret rotation.** Verification tries the current `secret_key`, then falls back to `previous_secret_key` if still within the `SECRET_GRACE_WINDOW` (24h) after rotation — zero-downtime rotation.

### Migration note (Python → Go)

The old Python clients signed **canonical JSON with no timestamp** and sent only `X-Hub-Signature-256`. They **will not verify against the Go service**. To migrate, a client must now:

1. Send a `Webhook-Timestamp: <unix_seconds>` header on every ingest (within ±5 minutes of server time).
2. Compute the HMAC over `timestamp + "." + idempotency_key + "." + body` (with `idempotency_key` empty unless an `X-Idempotency-Key` is also sent), **not** over canonical JSON.
3. Send the result as `X-Hub-Signature-256: sha256=<hex>`.

A request without `Webhook-Timestamp`, or outside the drift window, is rejected before signature comparison.

---

## Setup & Run

### Prerequisites

- **Go 1.26**
- **Docker** (for local Postgres + Redis)

### 1. Start infrastructure

Brings up `postgres:16-alpine` + `redis:7-alpine` (Redis with AOF persistence so the queue/schedule/DLQ survive a restart):

```bash
docker compose -f docker/docker-compose.yml up -d
```

### 2. Environment

```bash
export DATABASE_URL="postgres://webhook:webhook@localhost:5432/webhook?sslmode=disable"
export REDIS_URL="redis://localhost:6379/0"
export ADMIN_API_KEY="dev-admin-key"      # any non-empty value; the admin group fails closed without it
export ALLOW_HTTP_URLS=true               # local dev only — permit non-HTTPS target URLs
```

### 3. Run

Migrations run **embedded on startup** (golang-migrate as a library) — no separate migrate step is required.

```bash
make run            # equivalent to: go run ./cmd/server
# or, directly:
go run ./cmd/server
```

> **Windows note:** `make` is not installed by default. Use Git Bash + `choco install make`, or run the underlying `go` / `docker compose` commands shown in each recipe directly — every Makefile target is a thin wrapper over them.

### 4. Build & run the container

The Dockerfile is a multi-stage build (Go builder → `gcr.io/distroless/static:nonroot`):

```bash
docker build -t webhook-go .
docker run --rm -p 8080:8080 \
  -e DATABASE_URL="postgres://webhook:webhook@host.docker.internal:5432/webhook?sslmode=disable" \
  -e REDIS_URL="redis://host.docker.internal:6379/0" \
  -e ADMIN_API_KEY="dev-admin-key" \
  webhook-go
```

### 5. Test & lint

```bash
go test ./...                       # unit tests
go test -tags=integration ./...     # integration tests (require the infra up)
make lint                           # golangci-lint run ./...
```

---

## Configuration

All configuration is parsed from the environment into a typed `Config` struct (stdlib only). Defaults below are copied from `internal/config/config.go`.

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (**required**) | Postgres DSN for the `pgxpool`. |
| `REDIS_URL` | — (**required**) | Redis URL (queue, scheduler, DLQ, cache). |
| `PORT` | `8080` | HTTP listen port. |
| `WEBHOOK_TIMEOUT` | `10s` | Per-delivery HTTP client timeout. |
| `WORKER_CONCURRENCY` | `50` | `errgroup.SetLimit` for the worker pool. |
| `PER_SUB_CONCURRENCY` | `5` | Max in-flight deliveries per subscription. |
| `DRAIN_TIMEOUT` | `30s` | Graceful-shutdown in-flight drain deadline. |
| `MAX_RETRY_ATTEMPTS` | `5` | Total attempts (1 initial + 4 retries) before DLQ. |
| `RETRY_BASE_DELAY` | `10s` | Base of the ×3 backoff curve. |
| `RETRY_MAX_DELAY` | `15m` | Backoff cap guard. |
| `VISIBILITY_TIMEOUT` | `60s` | Lease stamped on a claimed `pending` row; bounds recovery latency & duplicate risk. |
| `ORPHAN_THRESHOLD` | `15m` | Coarse backstop for a `pending` row with no live lease. |
| `RECOVERY_SCAN_INTERVAL` | `5m` | Period of the orphan-recovery scan. |
| `LOG_RETENTION_HOURS` | `72` | Cleanup horizon for non-DLQ rows. |
| `DLQ_MAX_AGE` | `720h` (30d) | Force-ack age for un-acked DLQ entries. |
| `IDEMPOTENCY_TTL` | `5m` | Prune horizon for `ingest_idempotency`. |
| `CACHE_TTL` | `5m` | Subscription read-through cache TTL. |
| `REGISTRY_IDLE_TTL` | `1h` | Idle eviction TTL for in-memory bucket/semaphore/breaker registries. |
| `RATE_LIMIT_PER_SEC` | `100` | Token-bucket refill rate per subscription. |
| `RATE_LIMIT_BURST` | `200` | Token-bucket capacity per subscription. |
| `BREAKER_THRESHOLD` | `5` | Consecutive failures that trip a per-URL breaker Open. |
| `BREAKER_RESET_TIMEOUT` | `30s` | Open→Half-Open probe delay. |
| `AUTO_DISABLE_THRESHOLD` | `5` | Consecutive `final_failure`s that set `is_active=FALSE`. |
| `SIGNATURE_DRIFT_WINDOW` | `5m` | Allowed `Webhook-Timestamp` clock skew (replay window bound). |
| `SECRET_GRACE_WINDOW` | `24h` | Previous-secret acceptance window after rotation. |
| `ALLOW_HTTP_URLS` | `false` | Permit non-HTTPS target URLs (local dev). |
| `SYNC_DELIVERY` | `false` | Deliver synchronously in the ingest request instead of enqueuing. |
| `ENABLE_PPROF` | `false` | Mount `/api/v1/debug/pprof/`. |
| `CORS_ALLOWED_ORIGINS` | — | Explicit origin allow-list (never `*` combined with credentials). |
| `ADMIN_API_KEY` | — (required for admin routes) | Bearer token for management/operational endpoints; the admin group fails closed (`503`) if unset. |

---

## API Reference

All endpoints are versioned under `/api/v1`. Paths below match `internal/api/server.go` exactly.

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/api/v1/health` | Public | Liveness probe (pings DB & Redis). |
| `GET` | `/api/v1/ready` | Public | Readiness probe (worker pool running?). |
| `GET` | `/api/v1/openapi.json` | Public | OpenAPI 3.1 spec (served via `embed.FS`). |
| `GET` | `/api/v1/docs` | Public | **Interactive Swagger UI** (loads swagger-ui from CDN against `/openapi.json`). |
| `POST` | `/api/v1/ingest/{id}` | HMAC | Ingest a payload for subscription `{id}`. Requires `X-Hub-Signature-256` + `Webhook-Timestamp`; checks `is_active` + rate limit; event-type filter; body capped at 64 KB. |
| `POST` | `/api/v1/subscriptions` | Admin | Create a subscription (HTTPS target required unless `ALLOW_HTTP_URLS=true`). |
| `GET` | `/api/v1/subscriptions` | Admin | List subscriptions — keyset/cursor pagination (`?limit`, `?cursor`), returns `{items, next_cursor}`. |
| `GET` | `/api/v1/subscriptions/{id}` | Admin | Get a subscription. |
| `PUT` | `/api/v1/subscriptions/{id}` | Admin | Update a subscription. |
| `DELETE` | `/api/v1/subscriptions/{id}` | Admin | Delete a subscription (cascades its delivery logs). |
| `GET` | `/api/v1/subscriptions/{id}/attempts` | Admin | List recent delivery attempts for the subscription. |
| `POST` | `/api/v1/subscriptions/{id}/rotate-secret` | Admin | Rotate the signing secret (begins the 24h grace window; returns the new secret once). |
| `POST` | `/api/v1/subscriptions/{id}/replay/{webhook_id}` | Admin | Replay a failed/DLQ'd webhook (inserts a fresh `pending` chain + enqueues; acks the DLQ entry). |
| `POST` | `/api/v1/subscriptions/{id}/dlq/{webhook_id}/ack` | Admin | Discard/ack a DLQ entry without replaying (`in_dlq=FALSE`, `LREM` from list). |
| `GET` | `/api/v1/status/{webhook_id}` | Admin | Webhook status & full attempt history (`attempts[]` + `statistics`). |
| `GET` | `/api/v1/status/metrics/summary` | Admin | Delivery aggregation over the last `?hours` (default 24). |
| `GET` | `/api/v1/monitor` | Admin | System health, queue/DLQ depths, circuit-breaker states, latency distribution. |
| `GET` | `/api/v1/debug/vars` | Admin | `expvar` counters/gauges. |
| `GET` | `/api/v1/debug/pprof/` | Admin + `ENABLE_PPROF=true` | `net/http/pprof` profiling handler (index, cmdline, profile, symbol, trace). |

> Interactive API docs are served at **`GET /api/v1/docs`** (Swagger UI). The two-segment `/status/metrics/summary` path is more specific than `/status/{webhook_id}`, so `ServeMux` routes them unambiguously.

---

## curl Examples

These assume the server is on `http://localhost:8080` and `ADMIN_API_KEY` is exported. The `sign()` helper computes the ingest signature over the canonical grammar `timestamp + "." + idempotency_key + "." + body`.

```bash
export ADMIN_API_KEY="dev-admin-key"
ADMIN="Authorization: Bearer $ADMIN_API_KEY"
BASE="http://localhost:8080/api/v1"

sign() { # args: timestamp idempotency_key body secret  ->  prints "sha256=<hex>"
  printf '%s' "${1}.${2}.${3}" | openssl dgst -sha256 -hmac "$4" | awk '{print "sha256="$NF}'
}
```

### Create a subscription (capture `id` + `secret_key`)

```bash
RESP=$(curl -s -X POST "$BASE/subscriptions" -H "$ADMIN" -H 'Content-Type: application/json' \
  --data-raw '{"target_url":"https://example.com/webhook-receiver","event_types":["order.created"]}')
echo "$RESP"

ID=$(printf '%s' "$RESP"     | python -c 'import sys,json;print(json.load(sys.stdin)["id"])')
SECRET=$(printf '%s' "$RESP" | python -c 'import sys,json;print(json.load(sys.stdin)["secret_key"])')
echo "id=$ID secret=$SECRET"   # secret_key is shown ONCE — save it now
```

### List subscriptions (keyset pagination)

```bash
curl -s "$BASE/subscriptions?limit=20" -H "$ADMIN"
# follow the cursor returned as next_cursor:
curl -s "$BASE/subscriptions?limit=20&cursor=<next_cursor>" -H "$ADMIN"
```

### Get / update a subscription

```bash
curl -s "$BASE/subscriptions/$ID" -H "$ADMIN"

curl -s -X PUT "$BASE/subscriptions/$ID" -H "$ADMIN" -H 'Content-Type: application/json' \
  --data-raw '{"target_url":"https://example.com/webhook-receiver","event_types":["order.created","order.updated"],"is_active":true}'
```

### Rotate the signing secret

```bash
curl -s -X POST "$BASE/subscriptions/$ID/rotate-secret" -H "$ADMIN"
# response includes the new secret_key once; the old one stays valid for 24h
```

### List delivery attempts

```bash
curl -s "$BASE/subscriptions/$ID/attempts" -H "$ADMIN"
```

### Ingest a signed webhook

```bash
BODY='{"event":"order.created","order_id":42}'
TS=$(date +%s)
SIG=$(sign "$TS" "" "$BODY" "$SECRET")

curl -s -X POST "$BASE/ingest/$ID?event_type=order.created" \
  -H "X-Hub-Signature-256: $SIG" \
  -H "Webhook-Timestamp: $TS" \
  -H 'Content-Type: application/json' \
  --data-raw "$BODY"
# 202 Accepted — capture the returned webhook_id for status/replay
```

### Ingest with an idempotency key (folded into the signature)

```bash
BODY='{"event":"order.created","order_id":42}'
TS=$(date +%s)
KEY="order-42-attempt-1"
SIG=$(sign "$TS" "$KEY" "$BODY" "$SECRET")

curl -s -X POST "$BASE/ingest/$ID?event_type=order.created" \
  -H "X-Hub-Signature-256: $SIG" \
  -H "Webhook-Timestamp: $TS" \
  -H "X-Idempotency-Key: $KEY" \
  -H 'Content-Type: application/json' \
  --data-raw "$BODY"
# a repeat with the same KEY is an idempotent no-op returning the original webhook_id
```

### Status, metrics, replay, ack, monitor, delete

```bash
WID="<webhook_id-from-ingest>"

curl -s "$BASE/status/$WID" -H "$ADMIN"                       # full attempt history
curl -s "$BASE/status/metrics/summary?hours=24" -H "$ADMIN"   # aggregate metrics

curl -s -X POST "$BASE/subscriptions/$ID/replay/$WID" -H "$ADMIN"        # replay a DLQ'd webhook
curl -s -X POST "$BASE/subscriptions/$ID/dlq/$WID/ack" -H "$ADMIN"       # discard a DLQ entry

curl -s "$BASE/monitor" -H "$ADMIN"                           # system metrics JSON

curl -s -X DELETE "$BASE/subscriptions/$ID" -H "$ADMIN"       # delete (cascades delivery logs)
```

---

## Signing Helper Script

A Python helper, [`scripts/sign_webhook.py`](scripts/sign_webhook.py), computes the `X-Hub-Signature-256` and `Webhook-Timestamp` headers using the exact canonical grammar:

```bash
python scripts/sign_webhook.py --secret "$SECRET" --body '{"event":"order.created"}'
# prints:
#   Webhook-Timestamp: <unix_seconds>
#   X-Hub-Signature-256: sha256=<hex>

# with an idempotency key:
python scripts/sign_webhook.py --secret "$SECRET" --body '{"event":"order.created"}' \
  --idempotency-key "order-42-attempt-1"
```

---

## Observability

- **`GET /api/v1/monitor`** — JSON with `uptime_seconds`, `delivery_metrics` (total/success/failed_attempt/final_failure/pending), `queue_depth`, `dead_letter_queue_depth`, `circuit_breakers` (per-URL state), and `delivery_latency_ms` with `min/max/avg/p50/p95/p99`. Percentiles come from a small fixed-size, mutex-guarded **bounded histogram** (log-linear buckets) in the deliverer — stdlib only, O(1) per observation, no Prometheus dependency.
- **`GET /api/v1/debug/vars`** — `expvar` counters and gauges.
- **`GET /api/v1/debug/pprof/`** — `net/http/pprof` (index, cmdline, profile, symbol, trace), **env-gated** (`ENABLE_PPROF=true`) **and** admin-authed.
- **Structured logging** via `log/slog` with context-propagated request IDs throughout the middleware chain.

Example `/monitor` response:

```json
{
  "uptime_seconds": 86400,
  "current_time": "2026-06-18T22:00:00Z",
  "delivery_metrics": { "total": 1523, "success": 1400, "failed_attempt": 80, "final_failure": 3, "pending": 40 },
  "queue_depth": 12,
  "dead_letter_queue_depth": 1,
  "circuit_breakers": { "https://example.com/webhook-receiver": "closed" },
  "delivery_latency_ms": { "min": 12, "max": 2400, "avg": 84, "p50": 42, "p95": 195, "p99": 450 }
}
```

---

## Testing

```bash
go test ./...                       # unit tests
go test -tags=integration ./...     # integration tests (require docker infra up)
make lint                           # golangci-lint, clean
```

- **Unit tests** cover signature sign/verify, the SSRF `safedial` classifier, the circuit-breaker FSM (including stale-generation rejection), the token-bucket rate limiter, the in-memory registries, the latency metrics histogram, and the worker pool teardown semantics.
- **Integration tests** (behind the `integration` build tag) exercise the high-risk failure modes against real Postgres + Redis:
  - cleanup never deletes `pending` rows,
  - concurrent same-key ingest collapses to one row,
  - single-claim dequeue (no double-delivery under duplicate queue entries),
  - concurrent replay produces exactly one new chain,
  - recovery reclaims orphaned `pending` rows.
- The codebase is **golangci-lint clean**.
- **Continuous integration** ([`.github/workflows`](.github/workflows)): every push and pull request runs lint (`go vet` + `gofmt` + golangci-lint), the unit suite under the race detector (`go test -race -shuffle=on`), and the full integration suite against ephemeral Postgres + Redis service containers (migrated first). A `govulncheck` scan runs on push/PR and weekly, and Dependabot tracks module, Actions and Docker updates. The race detector runs on the Linux runners (CGO available) where it cannot on the Windows dev box.

---

## Architectural Considerations / Future Work

Evaluated during design, deferred for v1, documented for discussion (plan's Architectural Considerations table):

| Pattern | Why deferred / future path |
|---|---|
| **Transactional Outbox** | Orphan-recovery + the visibility lease keep the dual-write (DB insert + Redis push) correct and far simpler; add an `outbox` table + relay goroutine if lost-enqueue rates become measurable. |
| **Fair queue scheduling** | A single Redis LIST + per-sub concurrency semaphores gives adequate fairness; per-subscription LISTs with weighted round-robin is the next step. |
| **Distributed rate / concurrency limits** | In-memory limits are exact on a single instance; horizontal scale-out needs Redis token buckets (Lua) + leased per-subscription concurrency counters. |
| **Wildcard event matching** | Exact `slices.Contains` matching today; `order.*`-style prefix/glob matching behind a feature flag later. |
| **Per-user authorization** | Single static `ADMIN_API_KEY` in v1; scoped tokens / JWT / mTLS for real per-subscription isolation. |

---

## Deployment

Target: a low-cost PaaS (**Railway**) with managed Postgres + Redis, deployed from
the root `Dockerfile` (the multi-stage → distroless image). Migrations run embedded
on startup, so the deploy is self-provisioning. Build config: `railway.toml` /
`railway.json` (`DOCKERFILE` builder, healthcheck `/api/v1/health`); `Procfile` is a
buildpack fallback.

### Deploy steps (Railway)

1. Create a Railway project and add the **PostgreSQL** and **Redis** plugins.
2. Add a service from this repo (Railway detects the root `Dockerfile`).
3. Set service variables:
   - `DATABASE_URL` → reference the Postgres plugin's connection string.
   - `REDIS_URL` → reference the Redis plugin's URL.
   - `ADMIN_API_KEY` → a strong random secret (the admin group fails closed without it).
   - Optionally `CORS_ALLOWED_ORIGINS`, `ENABLE_PPROF`, etc.
4. Deploy. Railway injects `PORT`; the server binds it and migrates on boot.
5. Verify: `curl https://<your-app>.up.railway.app/api/v1/health` → `200`.

- **Deployed URL:** _set after deploying (e.g. `https://<your-app>.up.railway.app`)._

### Monthly cost estimate

Workload: 24×7 uptime + ~5,000 webhooks/day × ~1.2 attempts ≈ **6,000 outbound
attempts/day (~180k/month, ~0.07 req/s average)**. This is a *tiny* request rate —
cost is dominated by the 24×7 idle baseline of the three always-on components, not by
request volume. The Go service is a ~25 MB static binary that idles at low CPU and
~64–128 MB RAM.

| Component | Sizing | Est. / month |
|---|---|---|
| Go service (1 instance) | ~128 MB RAM, <0.1 vCPU avg | ~$3–5 |
| Managed PostgreSQL | small instance, ~0.5 GB RAM | ~$5–8 |
| Managed Redis | small instance, ~0.25 GB RAM | ~$3–5 |
| **Total** | | **~$11–18 / month** |

On Railway's **Hobby plan** ($5/month, includes $5 of usage) the workload's compute is
low enough that the bill is essentially the three components' idle baseline; expect
**~$10–18/month** in practice. The request volume itself adds negligible cost.

> **Why a single instance (v1):** the rate limiter, per-subscription concurrency
> semaphores, and circuit breakers live in process memory, so per-instance == global
> only at one replica. Horizontal scale-out would require moving that state to Redis
> (Lua token buckets + leased counters) — documented as future work above.

---

## Credits

Built with **Go**. Runtime dependencies:

- [`github.com/jackc/pgx/v5`](https://github.com/jackc/pgx) — PostgreSQL driver + connection pool.
- [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis) — Redis client (queue, scheduler, DLQ, cache, Lua).
- [`github.com/golang-migrate/migrate/v4`](https://github.com/golang-migrate/migrate) — embedded database migrations.
- [`golang.org/x/sync`](https://pkg.go.dev/golang.org/x/sync) — `errgroup` (Go-team extended stdlib).

Test-only: [`github.com/stretchr/testify`](https://github.com/stretchr/testify) (not in the production binary). The interactive docs page loads **Swagger UI** from a CDN.

Developed with AI assistance.
