# Go Webhook Delivery System — Implementation Plan (v4)

A production-grade, portfolio-quality port of the Python webhook delivery service to Go, incorporating all fixes from the plan review.
**Locked decisions**: Go 1.26 · 3 third-party runtime packages + `golang.org/x/sync` (Go-team extended stdlib, for `errgroup`); `testify` test-only · Redis sorted-set retry scheduler with atomic Lua scheduling · circuit breaker + rate limiter + secret rotation + advanced safety features (SSRF protection, DLQ, replay API, concurrency limits).

> [!IMPORTANT]
> **v4 review-driven decisions (ground truth checked against the Python `app/` source):**
> 1. **Delivery attempt model = one row per attempt** (immutable attempt log, matching Python's `create_retry_log`). See §0. This is the foundational choice the schema, status endpoint, `/metrics`, retry scheduler, and recovery job all depend on.
> 2. **Retry policy** = exponential ×3 (`10/30/90/270s`) + ±20% jitter, **5 total attempts** (1 initial + 4 retries) then DLQ. See §4b.
> 3. **List pagination** = keyset/cursor (`{items, next_cursor}`, ordered `created_at DESC, id DESC`). Drops the `total` field the Python API returned. See API surface.
> 4. **Honest parity labels**: `is_active`, secret-rotation, the `timestamp.payload` signature scheme, and the required `Webhook-Timestamp` ingest header are **NEW** features, not parity with Python. The Python `Subscription` has no `is_active`, and Python signs the canonical payload *without* a timestamp.
> 5. Correctness fixes folded in: worker-pool `errgroup` teardown + graceful-drain context split (§1), auto-disable `consecutive_failures` storage, cache invalidation on rotate/deactivate, SSRF range gaps (IPv4-mapped IPv6, CGNAT), mutex-only token bucket, latency percentiles from a histogram (not `expvar`), DLQ-vs-retention horizon, and an interactive Swagger UI (not raw JSON).

> [!NOTE]
> **IMPLEMENTATION STATUS (2026-06-21): COMPLETE.** All 13 slices in `docs/CHECKLIST.md` are built and
> verified — `go build`/`go vet`/`golangci-lint` v2 clean; 34 tests pass (unit + `-tags=integration`);
> Docker image (~25 MB distroless) builds and runs; two adversarial-review passes (foundation clean; final
> pass found 1 low-sev DLQ-reconciler race, fixed). The only HITL remainder is the **live Railway deploy**.
>
> **As-built deltas from this plan** (the design held; these are the refinements that emerged):
> 1. **Dockerfile is at repo root `./Dockerfile`** (not `docker/Dockerfile`) — Railway auto-detects a root
>    Dockerfile and it avoids clobbering the Python Dockerfile. The Go infra compose is `docker/docker-compose.yml`.
> 2. **Retry-scheduler ZSET scores are sub-second floats** (`UnixMilli/1000`), not integer `Unix()`. Integer
>    truncation moved rows up to ~1s early, so the §0 `next_retry_at <= now()` claim predicate rejected them
>    and retries stalled until the recovery scan. The deliverer also **reschedules-on-not-due** (re-ZADDs a
>    claim-rejected still-future pending row) to self-heal any residual app↔DB clock skew (`worker/delivery.go`).
> 3. **Auto-disable counter is folded into combined transactional store methods**: `MarkSuccess` (CAS success
>    + reset `consecutive_failures`, one tx) and `FinalizeFailure` (final_failure + `in_dlq` + counter++ +
>    auto-disable at threshold, one tx). `MarkFinalFailure` is retained only for the intentional
>    "subscription deactivated" drop (no counter). `internal/subscription.State` owns those semantic
>    delivery outcomes together with cache invalidation and derived rate/semaphore resets; API and worker callers
>    no longer coordinate those consequences through callbacks.
> 4. **DLQ reconciler re-push is symmetric** with its LREM branch: the flag→list re-push re-validates `IsInDLQ`
>    before pushing (closes a transient ack-race the final review found).
> 5. **Request-id correlation**: the API layer propagates a `request_id` context logger; async worker tasks
>    correlate via `delivery_id`/`webhook_id` (delivery is decoupled from the originating request).
> 6. **Tests**: integration tests are gated by the `//go:build integration` tag (`internal/testutil` helpers,
>    read `DATABASE_URL`/`REDIS_URL`). `-race` is unavailable on the Windows dev box (no CGO/gcc); concurrency
>    is proven via behavioral + integration tests instead (the high-risk failure tests).

---

## Philosophy: Why This Will Stand Out

The Python implementation leans heavily on frameworks (FastAPI, Celery, SQLAlchemy, Pydantic) that abstract away most of the distributed systems engineering. In Go, you **own every layer**. The result is a codebase that demonstrates:

- Native Go concurrency replacing Celery entirely (goroutines, channels, worker pools)
- `net/http` routing on Go 1.22+ `ServeMux` — zero framework, versioned under `/api/v1/`
- Direct PostgreSQL interaction via `pgx/v5` (no ORM, raw SQL)
- Durable retry scheduling via Redis Sorted Sets using atomic Lua scripts
- Circuit breaker per target endpoint, built with `sync/atomic` and a probing flag to prevent simultaneous race conditions
- Per-subscription token-bucket rate limiter, stdlib-only
- Webhook secret rotation with zero downtime (dual-secret grace window) + replay attack prevention (timestamp signature verification)
- Safe Dialing with SSRF prevention (DNS-lookup IP range checks)
- Proper `context` propagation, cancellation, and graceful shutdown throughout
- Full observability: `log/slog`, `expvar`, `/monitor` metrics endpoint, and `net/http/pprof` — all stdlib

---

## Technology Choices (Final)

> [!IMPORTANT]
> **3 third-party runtime packages** (`pgx/v5`, `golang-migrate`, `go-redis/v9`) **+ `golang.org/x/sync`** — the latter is the Go team's quasi-stdlib extension, pulled in for `errgroup` (used by the worker pool's bounded `SetLimit` backpressure in §1 and the top-level supervisor in `main.go`). It is called out explicitly so the "own every layer" story stays honest: it is a `go.mod` dependency, not stdlib. Test-only packages (`testify`) do not ship in the production binary. No other runtime dependencies.

| Concern | Python (current) | Go replacement | Justification |
|---|---|---|---|
| HTTP server + routing | FastAPI | `net/http` stdlib (Go 1.22+ `ServeMux` with `{id}` path params) | `r.PathValue("id")` makes `chi` unnecessary on Go 1.22+ |
| DB driver | SQLAlchemy + asyncpg | `pgx/v5` ← **package 1** | Best-in-class Postgres driver; connection pooling built-in |
| DB migrations | Alembic | `golang-migrate/migrate` ← **package 2** | Embedded as a library. Executed from `main.go` on startup |
| Redis client | celery broker | `go-redis/redis/v9` ← **package 3** | Official Redis Go client; required for BRPOP, ZADD, Lua script |
| Task queue / broker | Celery + RabbitMQ/Redis | Native worker pool + Redis LIST (`LPUSH`/`BRPOP`) | Eliminates Celery entirely |
| Retry scheduler | Celery Beat | Redis Sorted Set (`ZADD` + Lua `ZRANGEBYSCORE`/`ZREM` script) | Durable across restarts; same O(log N) guarantee |
| Config | pydantic-settings | `os.Getenv` + typed struct + manual parsing (stdlib) | 20 lines; no dep needed |
| Structured logging | Python logging | `log/slog` (stdlib Go 1.21+) | Zero dependency |
| HTTP client (outbound) | `httpx` | `net/http` (stdlib) | Custom transport tuning with Safe Dialing |
| Circuit breaker | — | `sync/atomic` + state machine (stdlib) | Built from first principles |
| Rate limiting | — | Token bucket with `time` + `sync.Mutex` (stdlib) | Textbook algorithm, no framework (mutex-only — see §4) |
| HMAC signing | `hmac` + `hashlib` | `crypto/hmac` + `crypto/sha256` (stdlib) | Signed `timestamp.payload` to prevent replays |
| Observability | — | `expvar` + `net/http/pprof` (stdlib) | Zero-cost metrics + profiling endpoint |
| Testing | pytest | `testing` (stdlib) + `testify` ← **test-only** | `assert` / `require` save enormous boilerplate |

**Total runtime packages: 3 third-party** (`pgx/v5`, `golang-migrate/migrate`, `go-redis/v9`) **+ `golang.org/x/sync`** (Go-team extended stdlib, for `errgroup`).  
*Note: `github.com/stretchr/testify` is used exclusively for unit tests and is not in the production binary.*

---

## Architecture: Hexagonal (Ports & Adapters)

**The dependency rule**: arrows always point inward. `domain` has zero imports outside stdlib. Shared capabilities live in `domain`; workflow-specific persistence seams are narrow and owned by their consumers. Infrastructure adapters never leak into API or worker code.

```
┌──────────────────────────────────────────────────────────────────────┐
│                            cmd/server/main.go                        │
│   Parse config → init adapters → wire services → start goroutines   │
└───────────┬──────────────────────────────────────┬───────────────────┘
            │                                      │
┌───────────▼──────────────┐          ┌────────────▼──────────────────┐
│       internal/api        │          │       internal/worker          │
│  net/http ServeMux        │          │  Pool, Deliverer, Scheduler    │
│  Handlers, Middleware     │          │  CleanupJob, RecoveryJob       │
│  RequestID, Recovery      │          │  (all goroutine-based)         │
└───────────┬──────────────┘          └────────────┬──────────────────┘
            │                                      │
            └───────────────┬──────────────────────┘
                            │
          ┌─────────────────▼────────────────────┐
          │            internal/domain            │
          │  Subscription, DeliveryLog, enums     │
          │  Pagination values + delivery port    │
          │  Sentinel errors                      │
          │  ← ZERO external imports →            │
          └──────┬──────────────────┬─────────────┘
                 │                  │
    ┌─────────────▼──┐    ┌──────────▼───────────────────────────────┐
    │ internal/store  │    │            internal/infra                │
    │ PostgreSQL/pgx  │    │  queue/     (Redis LIST + Sorted Set)    │
    │ Repository impl │    │  breaker/   (per-URL circuit breaker)    │
    │ Raw SQL, txns   │    │  ratelimit/ (per-sub token bucket)       │
    └─────────────────┘    │  safedial/  (SSRF prevention dialer)     │
                           │  signature/ (HMAC sign/verify)           │
                           └──────────────────────────────────────────┘
      internal/subscription                 internal/lifecycle
    (Redis read-through + policy state)   (HTTP + process supervision)
```

---

## Project Directory Layout

```
webhook-delivery-system/                # module github.com/Vaibtan/webhook-delivery-system
├── cmd/
│   └── server/
│       └── main.go                   # Wire everything; one file, pure DI, runs migrations
│
├── internal/
│   ├── config/
│   │   └── config.go                 # Typed Config struct, os.Getenv parsing
│   │
│   ├── domain/                       # ← ZERO external imports in this directory
│   │   ├── subscription.go           # Subscription type + validation (HTTPS-only check, ALLOW_HTTP_URLS override)
│   │   ├── delivery.go               # DeliveryLog type, DeliveryStatus enum
│   │   ├── ports.go                  # Pagination values + delivery service port
│   │   └── errors.go                 # Sentinel errors (ErrNotFound, ErrConflict…)
│   │
│   ├── api/
│   │   ├── server.go                 # net/http ServeMux, middleware chain, v1 prefix
│   │   ├── middleware.go             # RequestID, structured logging, recovery, CORS, body limits
│   │   ├── handler_subscription.go   # CRUD + secret rotation + replay endpoints
│   │   ├── handler_ingest.go         # POST /ingest/{id} — size validation, is_active check, sig verify, event filter, enqueue
│   │   ├── handler_status.go         # GET /status/{webhook_id}, GET /status/metrics/summary
│   │   ├── handler_health.go         # GET /health, GET /ready
│   │   ├── handler_docs.go           # GET /docs (interactive Swagger UI) + /openapi.json (//go:embed spec)
│   │   └── handler_monitor.go        # GET /monitor — detailed JSON metrics & latency
│   │
│   ├── store/
│   │   ├── db.go                     # pgxpool setup, embedded migrate runner
│   │   ├── subscription_repo.go      # PostgreSQL subscription adapter (cursor-pagination, ORDER BY)
│   │   └── delivery_log_repo.go      # PostgreSQL delivery-log adapter; consumers own narrow seams
│   │
│   ├── infra/
│   │   ├── queue/
│   │   │   └── redis_queue.go        # LPUSH/BRPOP for immediate delivery tasks
│   │   ├── scheduler/
│   │   │   └── redis_scheduler.go    # ZADD/Lua script ZRANGEBYSCORE+ZREM retry sorted set
│   │   ├── breaker/
│   │   │   └── circuit_breaker.go    # Per-URL CB: Closed→Open→Half-Open FSM with probing flag
│   │   ├── ratelimit/
│   │   │   └── token_bucket.go       # Per-subscription token bucket
│   │   ├── safedial/
│   │   │   └── dialer.go             # Custom Resolver & dialer for SSRF prevention
│   │   └── signature/
│   │       └── hmac.go               # Sign, Verify, CanonicalJSON (json.Number safe)
│   │
│   ├── subscription/
│   │   └── state.go                  # CRUD, cache coherence, delivery outcomes, rate/concurrency state
│   ├── lifecycle/
│   │   └── supervisor.go             # Coordinated cancellation, HTTP shutdown, worker drain
│   └── worker/
│       ├── pool.go                   # Goroutine pool, errgroup, graceful stop, BRPOP timeout
│       ├── delivery.go               # HTTP delivery: re-check is_active → sign → check CB/RL/SSRF → send → drain
│       ├── retry.go                  # retryDelay backoff + RetrySchedulerWorker (polls sorted set with Lua, re-enqueues) + RecoveryWorker (startup + periodic scan for orphaned tasks)
│       └── cleanup.go                # CleanupWorker (Ticker): the only retention-driven deleter — DELETE … WHERE created_at < threshold AND NOT in_dlq AND status <> 'pending' (+ prunes ingest_idempotency); the lone exception is the explicit subscription-delete cascade. Also DLQReconciler (Ticker): LIST↔in_dlq reconciliation + DLQ_MAX_AGE force-ack (mutates flag only, no deletes)
│
├── migrations/
│   ├── 000001_initial_schema.up.sql  # Consolidated schema: subscriptions (with rotation) + delivery_logs
│   ├── 000001_initial_schema.down.sql
│   ├── 000002_indexes.up.sql         # Covering and performance indexes
│   └── 000002_indexes.down.sql
│
├── docker/
│   └── docker-compose.yml            # postgres + redis (with AOF persistence)
│
├── Dockerfile                        # Multi-stage: builder + distroless static runner (repo root — Railway auto-detect)
├── .golangci.yml
├── Makefile                          # run, test, lint, migrate-up, migrate-down
├── go.mod                            # module github.com/Vaibtan/webhook-delivery-system
└── go.sum
```

---

## Core System Design Decisions (Updated)

### 0. Delivery Attempt Model: One Row Per Attempt (Foundational)

**Decision:** each delivery attempt is its own immutable `delivery_logs` row, mirroring the Python system (`app/db/repositories/delivery_log_repository.py::create_retry_log`, which inserts a new row with `attempt_number + 1`, a new `id`, and the *same* `webhook_id`). We do **not** mutate a single row in place across retries.

This single decision is load-bearing for four subsystems, so it is stated first:

| Subsystem | Consequence of one-row-per-attempt |
|---|---|
| **`id` vs `webhook_id`** | `delivery_logs.id` (= `X-Webhook-Delivery-ID`) is **unique per attempt**. `webhook_id` (= `X-Webhook-ID`) is **shared across all attempts** of one ingested event. This matches the header docs in §8. |
| **`GET /status/{webhook_id}`** | Fetches **all** rows for the `webhook_id` ordered by `(replay_number, attempt_number)`, returns the full `attempts[]` history + a `statistics` block. The **current** delivery outcome is the latest chain (`MAX(replay_number)`), so a replay's progress is never masked by the original chain's old `final_failure`. Parity-plus with `app/services/webhook_service.py::get_webhook_status`. |
| **`/status/metrics/summary`** | Counts **rows** by status over the window (`total_attempts` = number of attempt rows), exactly like `get_metrics_since`. |
| **Retry scheduler + recovery** | A scheduled retry is a **new row inserted with `status = pending`** and `next_retry_at` set; the previous row transitions to `failed_attempt`. Because every not-yet-delivered attempt is `pending`, the lease-based recovery scan (`WHERE status = 'pending' AND next_retry_at <= now()`, §1) provably catches lost retries too — not just first attempts. |

**Lifecycle of one attempt row** — every status transition is a **compare-and-swap** (`UPDATE … WHERE id=$1 AND status='pending'`); if it affects 0 rows the attempt was already finalized by another worker and we abort with no side effects:
```
INSERT (status=pending) ──▶ worker delivers (CAS-guarded transitions)
   ├─ 2xx ........................▶ UPDATE status=success         WHERE id=$1 AND status='pending'
   ├─ fail & attempt < 5 ........▶ ┌─ BEGIN (one DB transaction) ───────────────────────────────┐
   │                               │ UPDATE status=failed_attempt WHERE id=$1 AND status='pending'│
   │                               │   (0 rows ⇒ ROLLBACK & abort — already handled elsewhere)    │
   │                               │ INSERT new row (attempt_number+1, status=pending, next_retry)│
   │                               └─ COMMIT ────────────────────────────────────────────────────┘
   │                               then ZADD webhook:retry:schedule <next_retry_unix> <new_row_id>   (after commit)
   └─ fail & attempt == 5 .......▶ BEGIN; UPDATE status=final_failure, in_dlq=TRUE WHERE id=$1 AND status='pending'; COMMIT
                                   then LPUSH webhook:dlq <row_id>   (after commit)
```
Two correctness guarantees fall out of this shape:

- **No lost webhook on crash.** The `failed_attempt` update and the successor `INSERT` share **one transaction**, so a crash can never leave a `failed_attempt` row with no `pending` successor — a state orphan-recovery (§1) could not see, since it only scans `pending`. The post-commit `ZADD`/`LPUSH` *can* be lost on a crash, but the successor/final row is already durable: recovery re-enqueues the lost `pending` successor, and DLQ reconciliation (Adv §1) re-`LPUSH`es the lost DLQ id. Postgres is the source of truth; Redis is a derived index.
- **No corrupt attempt chain on double-delivery.** Because dequeue is at-most-once and recovery may re-enqueue a still-in-flight row, the same attempt-row id can be delivered twice. The CAS predicate (`AND status='pending'`) means only the **first** finisher transitions the row — the loser's `UPDATE` affects 0 rows and it aborts, so there is never a duplicate successor or duplicate DLQ push. A duplicate *HTTP call* of the same `X-Webhook-Delivery-ID` is bounded by the stated at-least-once guarantee, and consumers dedupe on `X-Webhook-ID` (Delivery Guarantee Statement).
- **Visibility lease bounds recovery latency *and* duplicate risk.** When a worker dequeues a row it first **claims** it with a conditional CAS: `UPDATE … SET next_retry_at = now() + VISIBILITY_TIMEOUT WHERE id=$1 AND status='pending' AND next_retry_at <= now()` (0 rows ⇒ the row is not due: either it is already terminal, or its lease is live because another worker is mid-delivery — so this dequeue is a duplicate and the worker skips it). The `next_retry_at <= now()` predicate is **load-bearing**: it makes the claim itself the mutual-exclusion point, so two workers draining duplicate `webhook:queue` entries for the *same* still-pending row cannot both pass the claim and both fire the HTTP call — the loser sees the just-stamped future lease and gets 0 rows. (Without the predicate the "actively in-flight row is hidden behind its lease" guarantee would not hold for *concurrent* duplicate dequeues, only for ones arriving after lease expiry.) For a `pending` row, `next_retry_at` doubles as a "visible-at" deadline (a first attempt is created with `next_retry_at = created_at` = immediately visible; a retry with its backoff due time). Recovery (§1) reclaims any `status='pending' AND next_retry_at <= now()` row — i.e. one that is genuinely due but whose `ZADD`/`LPUSH` was lost, **or** one whose worker crashed mid-delivery (its lease has expired). An actively in-flight row is hidden behind its lease, so recovery does **not** re-enqueue it. This recovers a lost enqueue within ~`VISIBILITY_TIMEOUT` of its due time (not the old flat 1h), while avoiding the duplicate-delivery storm a naïve short horizon would cause.

The ZSET member is the **new pending attempt row's id**, and `webhook:queue` likewise carries attempt-row ids. There is never an in-place `attempt_number` increment. (`idx_dl_webhook_attempt` is intentionally **non-unique**: a replay restarts the attempt chain — see Adv §1 — so `(webhook_id, attempt_number)` can legitimately repeat; duplicate successors are prevented by the CAS above, not by a unique index.)

> Rejected alternatives: (a) *in-place update of one row* — loses per-attempt history and breaks the status/metrics contract, and makes recovery miss orphaned retries (they'd be `failed_attempt`, not `pending`); (b) *in-place + `delivery_attempts` child table* — restores history but adds a table and forces recovery to scan overdue `failed_attempt` rows absent from the ZSET. One-row-per-attempt is simpler **and** higher-fidelity to the source system.

---

### 1. Replacing Celery: Native Worker Pool (BRPOP Timeout Fix)

```
Python path:                          Go path:
─────────────────────────             ───────────────────────────────────────
POST /ingest →                        POST /ingest →
  Validate signature                    Validate signature
  INSERT delivery_log (pending)         INSERT delivery_log (pending)
  celery.send_task(log_id)   ──redis──▶  LPUSH webhook:queue log_id
       ↓                                       ↓
  Celery worker process                  goroutine in pool does BRPOP
  (separate binary, heavy)              (same binary, lightweight goroutine)
```

**Worker Pool Queue Tradeoff (acks_late)**: 
Redis `BRPOP` is at-most-once for dequeue. If a worker crashes after `BRPOP` but before completing the delivery, the task is lost. To ensure at-least-once reliability without the complex overhead of `RPOPLPUSH` (or Redis Streams), we run an **Orphaned Task Recovery Job** at startup and periodically (`RECOVERY_SCAN_INTERVAL`). It uses the **visibility lease** from §0: it re-enqueues every `status='pending' AND next_retry_at <= now()` row — a row that is genuinely *due* (its `ZADD`/`LPUSH` was lost) or whose claiming worker crashed (lease expired). Because an actively in-flight row is hidden behind its `VISIBILITY_TIMEOUT` lease, recovery reclaims lost work within ~`VISIBILITY_TIMEOUT` of its due time **without** re-enqueuing tasks that are merely in progress. `ORPHAN_THRESHOLD` is a coarse backstop for any `pending` row that somehow never got a lease stamp. Per §0, **both** lost first-attempts and lost retries are `pending`, so a single scan covers both; the terminal-transition CAS makes a rare double-delivery idempotent at the DB layer.

**is_active re-check at delivery time** *(NEW — Python has no `is_active`; see §0/schema)*: Because there can be a delay between ingestion (enqueue) and delivery (dequeue), the delivery worker **re-checks `sub.IsActive`** before the HTTP call. If the subscription was deactivated in the interim, the delivery is skipped and the row is marked `final_failure` with `error_details = "subscription deactivated"` (no DLQ push — this is an intentional drop, not a failure to inspect).

**Worker Pool Dequeue and Run Loop (two fixes baked in):**

Two correctness rules the snippet enforces, both of which the naïve `errgroup.WithContext` pattern gets wrong:

1. **A single failed delivery must not tear down the pool.** `Process` **never returns a non-nil error for a delivery failure** — failures are persisted to the DB and logged. A non-nil return is reserved for truly unexpected panics-as-errors and must not propagate, because under `errgroup.WithContext` the first non-nil error cancels the shared context and kills every other in-flight worker.
2. **Graceful drain ≠ cancellation.** SIGTERM cancels `ctx` to **stop dequeuing**, but in-flight deliveries get their own context derived via `context.WithoutCancel` + a hard drain deadline, so they finish (up to the deadline) instead of being aborted the instant `ctx` is cancelled.

```go
// internal/worker/pool.go
func (p *Pool) Start(ctx context.Context) error {
    g := new(errgroup.Group)             // plain Group: no derived ctx to cancel on error
    g.SetLimit(p.concurrency)            // bounded + backpressure — no unbounded spawning

    for ctx.Err() == nil {               // ctx cancelled = "stop accepting new work"
        // 2s BRPOP timeout lets us re-check ctx.Done() for prompt shutdown
        logID, err := p.queue.Dequeue(ctx, 2*time.Second)
        if err != nil {
            if errors.Is(err, ErrQueueEmpty) || ctx.Err() != nil {
                continue
            }
            slog.Error("dequeue failed", "error", err)
            continue
        }

        g.Go(func() error {              // blocks here when concurrency limit is hit
            // Drain context: survives SIGTERM until a hard deadline rather than
            // being cancelled with ctx. context.WithoutCancel detaches cancellation
            // while preserving values (e.g. request-scoped logger).
            dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.deliverTimeout)
            defer cancel()
            if err := p.deliverer.Process(dctx, logID); err != nil {
                // Must stay nil-returning: see rule (1). Log + move on.
                slog.Error("deliver: unexpected error", "log_id", logID, "error", err)
            }
            return nil
        })
    }
    return g.Wait()                      // drains all in-flight within the deadline
}
```
*(`errgroup` also powers `internal/lifecycle`, where the supervisor coordinates the HTTP server, pool, scheduler, recovery, cleanup, and DLQ reconciler and propagates the first fatal error.)*

---

### 2. Atomic Retry Scheduler: Lua Script ZSet

To resolve the race condition where multiple scheduler replicas query and claim the same retry events, we execute an atomic Lua script for `ZRANGEBYSCORE` + `ZREM` + `LPUSH`.

> Per §0, each ZSET member is the **id of an already-inserted `pending` attempt row** (the retry row created when the previous attempt failed), with `score = next_retry_at` as a unix timestamp. The script moves due rows from `webhook:retry:schedule` into `webhook:queue`; a worker then `BRPOP`s and delivers. If the worker crashes between `LPUSH` and delivery, the row is still `pending` and the recovery job reclaims it.

```lua
-- Lua script for atomic retrieval, removal, and enqueuing of due retries
local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1], 'LIMIT', 0, 100)
for _, task in ipairs(tasks) do
    redis.call('ZREM', KEYS[1], task)
    redis.call('LPUSH', KEYS[2], task)
end
return #tasks
```

**Implementation in the Scheduler**:
```go
// internal/infra/scheduler/redis_scheduler.go
var claimDueScript = redis.NewScript(`
    local tasks = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1], 'LIMIT', 0, 100)
    for _, task in ipairs(tasks) do
        redis.call('ZREM', KEYS[1], task)
        redis.call('LPUSH', KEYS[2], task)
    end
    return #tasks
`)

func (s *RetryScheduler) processDue(ctx context.Context) {
    now := strconv.FormatFloat(float64(time.Now().UnixMilli())/1000.0, 'f', 3, 64)
    keys := []string{"webhook:retry:schedule", "webhook:queue"}
    
    count, err := claimDueScript.Run(ctx, s.redis, keys, now).Int()
    if err != nil {
        slog.Error("failed to process due retries via Lua", "error", err)
        return
    }
    if count > 0 {
        slog.Debug("successfully scheduled due retries", "count", count)
    }
}
```

---

### 3. Circuit Breaker per Target URL (Half-Open Probing Flag Fix)

To prevent multiple concurrent goroutines from simultaneously entering the `Half-Open` state and probing a down server during transition, we use a dedicated `atomic.Bool` probing flag **plus a generation counter**. The generation makes the probe *authoritative*: `Allow()` returns a token, and only a result still carrying the current generation can change state — so a request that began while Closed and finishes late (after the breaker has Opened, or while a real probe is in flight) is recognized as **stale** and cannot spuriously close the breaker.

```go
// internal/infra/breaker/circuit_breaker.go
type state int32
const (
    stateClosed   state = iota // 0
    stateOpen                  // 1
    stateHalfOpen              // 2
)

type CircuitBreaker struct {
    state        atomic.Int32
    failures     atomic.Int32
    lastOpenedAt atomic.Int64 // UnixNano
    generation   atomic.Int64 // bumped on every Open and every Half-Open probe issue
    probing      atomic.Bool  // a probe is in flight
    threshold    int32
    resetTimeout time.Duration
}

// Allow reports whether a request may proceed and returns a generation TOKEN the
// caller must hand back to RecordSuccess/RecordFailure. A result whose token no
// longer matches the current generation is stale (the state moved on while the
// request was in flight) and is ignored — this is what makes the probe authoritative.
func (cb *CircuitBreaker) Allow() (bool, int64) {
    switch state(cb.state.Load()) {
    case stateClosed:
        return true, cb.generation.Load()
    case stateOpen:
        elapsed := time.Since(time.Unix(0, cb.lastOpenedAt.Load()))
        if elapsed > cb.resetTimeout && cb.probing.CompareAndSwap(false, true) {
            cb.state.Store(int32(stateHalfOpen))
            return true, cb.generation.Add(1) // exactly one probe, on a fresh generation
        }
        return false, 0
    case stateHalfOpen:
        return false, 0 // block everyone but the single in-flight probe
    }
    return false, 0
}

func (cb *CircuitBreaker) RecordSuccess(token int64) {
    if cb.generation.Load() != token {
        return // stale result from a previous generation — ignore
    }
    if state(cb.state.Load()) == stateHalfOpen { // the probe succeeded
        cb.state.CompareAndSwap(int32(stateHalfOpen), int32(stateClosed))
        cb.probing.Store(false)
    }
    cb.failures.Store(0)
}

func (cb *CircuitBreaker) RecordFailure(token int64) {
    if cb.generation.Load() != token {
        return // stale result — ignore
    }
    if state(cb.state.Load()) == stateHalfOpen { // the probe failed: reopen, fresh cooldown
        cb.lastOpenedAt.Store(time.Now().UnixNano())
        cb.state.Store(int32(stateOpen))
        cb.generation.Add(1)
        cb.probing.Store(false)
        return
    }
    if cb.failures.Add(1) >= cb.threshold {
        cb.lastOpenedAt.Store(time.Now().UnixNano())
        if cb.state.CompareAndSwap(int32(stateClosed), int32(stateOpen)) {
            cb.generation.Add(1) // invalidate all closed-era in-flight tokens
        }
    }
}
```

**Breaker denial has a delivery-log lifecycle (no attempt consumed).** When `Allow()` returns `false` (Open, or Half-Open with the single probe already in flight), the deliverer treats it exactly like the per-subscription concurrency rejection in Adv §3: it makes **no** HTTP call and records **no** failed attempt. An Open breaker must never burn the row's 5-attempt budget — that would DLQ a healthy payload for a problem with the *target*, not the payload, and a chronically-open endpoint would exhaust every attempt without a single real send. Instead the deliverer **reschedules the same `pending` row without consuming an attempt**: `UPDATE … SET next_retry_at = now() + BREAKER_RESET_TIMEOUT WHERE id=$1 AND status='pending'`, then post-commit `ZADD webhook:retry:schedule <next_retry_unix> <row_id>`. The row stays `pending`, `attempt_number` is unchanged, and it becomes due right around when the breaker would admit its next probe. The terminal CAS, the §0 claim predicate, and recovery all still apply, so a lost post-commit `ZADD` is healed exactly as any other.

---

### 4. Per-Subscription Rate Limiter: Token Bucket

Enforced locally in-memory per subscription using a token bucket. **Mutex-only** — since every read/write happens under `Allow()`'s lock, `sync/atomic` would be redundant overhead (and muddies the "textbook algorithm" story). Fixed-point (`×1000`) avoids float drift in the refill math.

```go
// internal/infra/ratelimit/token_bucket.go
type TokenBucket struct {
    mu         sync.Mutex
    tokens     int64 // current tokens × 1000 (fixed-point)
    maxTokens  int64 // capacity × 1000
    refillRate int64 // tokens/sec × 1000
    lastRefill time.Time
}

func (tb *TokenBucket) Allow() bool {
    tb.mu.Lock()
    defer tb.mu.Unlock()
    tb.refill()
    if tb.tokens >= 1000 {
        tb.tokens -= 1000
        return true
    }
    return false
}

func (tb *TokenBucket) refill() {
    now := time.Now()
    elapsed := now.Sub(tb.lastRefill).Seconds()
    tb.tokens = min(tb.maxTokens, tb.tokens+int64(elapsed*float64(tb.refillRate)))
    tb.lastRefill = now
}
```

A `sync.Map` of `*TokenBucket` keyed by subscription id holds the buckets. Memory is bounded by the shared registry-eviction rule (idle-TTL sweep + evict-on-subscription-delete) that also governs the per-subscription concurrency semaphores and the per-URL circuit breakers — see Advanced Features §3.

---

### 4b. Exponential Backoff with Jitter

**5 total attempts** (1 initial + 4 retries), then `final_failure` → DLQ — matching Python's count semantics (`should_retry = current_attempt < MAX_RETRY_ATTEMPTS`, `MAX_RETRY_ATTEMPTS=5`). This **intentionally diverges** from the assignment's example curve (`10/30/60/300/900s`); the assignment phrases it as "e.g.", and a clean geometric ×3 with jitter is the better resilience story.

`retryDelay(attempt)` takes the attempt number that **just failed** (`1..4`) and returns the delay before the next attempt. With 5 total attempts the delays actually exercised are `retryDelay(1..4) = 10s, 30s, 90s, 270s`. The 15-minute cap is a guard for `RETRY_DELAYS`-style config overrides, not hit by the default curve.

```go
// internal/worker/retry.go — uses math/rand/v2 (rand.Int64N), Go 1.22+
// base/max come from RETRY_BASE_DELAY / RETRY_MAX_DELAY (defaults 10s / 15m).
func retryDelay(attempt int, base, max time.Duration) time.Duration {
    if base <= 0 {
        base = 10 * time.Second
    }
    delay := time.Duration(float64(base) * math.Pow(3, float64(attempt-1))) // 10,30,90,270,810…
    if max > 0 && delay > max {
        delay = max // cap (guard for config overrides)
    }
    // ±20% jitter
    jitter := delay / 5
    if jitter <= 0 {
        return delay
    }
    return delay - jitter + time.Duration(rand.Int64N(int64(2*jitter)))
}
```

Without jitter, 1,000 simultaneous failures would all retry at exactly the same wall-clock time, creating a self-inflicted burst. (`math/rand/v2` top-level funcs are safe for concurrent use, so no per-goroutine seeding is needed.)

---

### 5. Secret Rotation & Replay Attack Prevention *(NEW — not Python parity)*

> [!WARNING]
> **Behavioral divergence from Python.** The Python system signs `HMAC(canonical_json(payload))` with **no timestamp** (`app/utils/signature.py`), and the ingest endpoint requires only `X-Hub-Signature-256`. The Go system signs `HMAC(timestamp + "." + idempotency_key + "." + payload)` (the canonical grammar defined just below; `idempotency_key` is empty when the optional header is absent, and always empty outbound) and makes **`Webhook-Timestamp` a required ingest header** (5-minute drift window). This is a deliberate security upgrade (replay protection + dual-key rotation), but it **breaks existing Python clients** — call it out in the README migration notes. The scheme is applied **symmetrically**: inbound verification (below) and outbound signing (§8) use the same construction (outbound with an empty key field).

We include a `Webhook-Timestamp` header in all deliveries.

**Canonical signing grammar (one construction, used in both directions):**
```
signed_message = timestamp + "." + idempotency_key + "." + body
```
- `timestamp` — `Webhook-Timestamp`, unix seconds (digits only).
- `idempotency_key` — the optional inbound `X-Idempotency-Key` value, or the **empty string** when absent (always empty for *outbound* deliveries, which have no idempotency concept). Constrained to `[A-Za-z0-9_-]{1,128}`, so it can never contain the `.` delimiter.
- `body` — the exact transmitted body bytes (see §8 for the inbound-raw vs. outbound-canonical resolution).

The signature is `sha256=<hmac>` of `signed_message`. Because the verifier *reconstructs* the message from three independently-known fields (it never parses the wire bytes back apart) and the key field cannot contain the delimiter, the field boundaries are unambiguous. **Folding `idempotency_key` into the signed bytes** (rather than leaving it an unsigned header) is what stops an attacker from defeating dedupe by mutating only the key while the body+timestamp signature still verifies. This bounded, timestamped construction prevents replay of captured webhook events.

**Verification with Grace Period Window**:
```go
// internal/api/handler_ingest.go
func verifySignature(sub *domain.Subscription, payload []byte, timestamp, idempotencyKey, sig string) bool {
    // 1. Enforce 5-minute clock drift tolerance
    ts, err := strconv.ParseInt(timestamp, 10, 64)
    if err != nil || math.Abs(float64(time.Now().Unix()-ts)) > 300 {
        return false
    }

    // Canonical grammar (§5): idempotencyKey is "" when the optional header is absent.
    message := timestamp + "." + idempotencyKey + "." + string(payload)

    // 2. Try current secret
    if signature.Verify([]byte(message), sub.SecretKey, sig) {
        return true
    }
    
    // 3. Fall back to previous secret if within the 24-hour grace window
    if sub.PreviousSecretKey != "" && time.Since(sub.SecretRotatedAt) < 24*time.Hour {
        return signature.Verify([]byte(message), sub.PreviousSecretKey, sig)
    }
    return false
}
```

> **Replay vs. legitimate duplicates — an idempotency key, not the signature.** The 5-minute drift check only *bounds* the replay window. Using the (deterministic) HMAC as a nonce would be wrong: two legitimately-identical events in the same window, or a client's good-faith retry after a lost response, share a signature and would be **falsely rejected**. Instead the contract is an **optional** `X-Idempotency-Key` header (high-entropy, client-chosen, **folded into the signed material** per the §5 grammar so it can't be tampered). Dedupe is **Postgres-authoritative, not Redis**: within the *same transaction* as the initial `pending` `delivery_logs` insert, ingest does `INSERT INTO ingest_idempotency (subscription_id, idempotency_key, webhook_id) VALUES (…) ON CONFLICT (subscription_id, idempotency_key) DO NOTHING`. A **first** key takes the row → the webhook is accepted and (post-commit) enqueued; a **repeat** key conflicts → the transaction instead reads back the **stored** `webhook_id` and returns it as an idempotent no-op (a retry after a dropped response is safe and never double-enqueues). Because the dedupe record and the delivery row **commit atomically**, a crash can never leave a key acknowledged with no durable webhook behind it — the silent-data-loss failure mode a Redis-only `SETNX` would have (it commits the dedupe marker *before* the DB write, so a crash in between fakes a successful prior delivery). `IDEMPOTENCY_TTL` is the prune horizon `cleanup.go` applies to `ingest_idempotency`. With no key supplied, the scheme degrades to honest replay-*window limiting* (the timestamp bound) rather than a false single-use reject. This is the accurate basis for the "replay protection" claim.

---

### 6. SSRF Prevention: Custom Safe Resolver & Dialer

To protect our internal network from Server-Side Request Forgery (SSRF), the outbound HTTP client resolves destination hostnames and checks them against blocked private/loopback IP ranges before dialing.

Built on `net/netip` (Go 1.18+) rather than `net.IP`, so the classifier is exhaustive and the **IPv4-mapped IPv6 bypass** (`::ffff:169.254.169.254`) is collapsed via `Unmap()` before classification. `Addr.IsPrivate()` already folds in `10/8`, `172.16/12`, `192.168/16` **and** `fc00::/7` (IPv6 ULA, incl. some cloud-metadata addresses); we add the ranges it omits.

```go
// internal/infra/safedial/dialer.go
func isBlockedIP(addr netip.Addr) bool {
    addr = addr.Unmap() // collapse ::ffff:a.b.c.d — classic SSRF bypass
    if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() ||
        addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
        return true // IsLinkLocalUnicast covers 169.254.0.0/16 (incl. 169.254.169.254)
    }
    for _, p := range []netip.Prefix{
        netip.MustParsePrefix("0.0.0.0/8"),     // "this host on this network"
        netip.MustParsePrefix("100.64.0.0/10"), // CGNAT (RFC 6598)
        netip.MustParsePrefix("240.0.0.0/4"),   // reserved
    } {
        if p.Contains(addr) {
            return true
        }
    }
    return false
}

func SafeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
    host, port, err := net.SplitHostPort(addr)
    if err != nil {
        return nil, err
    }
    ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
    if err != nil {
        return nil, err
    }
    for _, ip := range ips {
        if isBlockedIP(ip) {
            return nil, fmt.Errorf("ssrf: connection to %s blocked", ip)
        }
    }
    // Pin to the first vetted IP so a second (re-resolved) DNS answer can't
    // swap in a private address between check and dial (DNS rebinding).
    dialer := &net.Dialer{Timeout: 5 * time.Second}
    return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
}
```

> **Transport hardening (3 layers, not just dial-time):**
> 1. **`Proxy: nil`** on the `http.Transport` — do **not** inherit `http.DefaultTransport`'s `ProxyFromEnvironment`, or a `HTTP(S)_PROXY` env var would tunnel deliveries *around* `SafeDialContext` (the dialer never sees the real destination IP).
> 2. **`CheckRedirect`** that (a) caps the chain at ≤ 3 hops (prevents redirect-loop resource exhaustion) **and** (b) rejects any redirect target whose scheme is not `https` (unless `ALLOW_HTTP_URLS`), so a `30x` HTTPS→HTTP downgrade can't leak the signed payload in cleartext.
> 3. Because each hop re-dials through `SafeDialContext`, a `30x` to an internal host is still blocked at dial time. `SafeDialContext` also **fails closed on an empty DNS answer** (no IPs ⇒ error, never dial).

---

### 7. HMAC + Canonical JSON: UseNumber() Fix

To preserve original numeric representations during canonicalization and sign verification, we use `json.Decoder` with `UseNumber()`.

```go
// internal/infra/signature/hmac.go
func CanonicalJSON(raw []byte) ([]byte, error) {
    dec := json.NewDecoder(bytes.NewReader(raw))
    dec.UseNumber() // Enforce preserving numbers instead of mapping to float64

    var tmp any
    if err := dec.Decode(&tmp); err != nil {
        return nil, err
    }
    // Require EOF after the first value. dec.More() is NOT a correct top-level
    // check (it reports array/object continuation, not stream end); a second
    // Decode that returns io.EOF is the well-defined way to reject trailing
    // tokens or garbage (`{...}{...}`, `{} junk`).
    var rest json.RawMessage
    if err := dec.Decode(&rest); err != io.EOF {
        return nil, errors.New("unexpected trailing data after JSON value")
    }
    return json.Marshal(tmp) // encoding/json marshals object keys in sorted order
}
```

---

### 8. Outbound HTTP Headers

Outbound delivery attempts send the following headers:
- `Content-Type`: `application/json`
- `User-Agent`: `webhook-delivery-service/1.0`
- `X-Webhook-ID`: `<webhook_uuid>` (persistent ingestion event ID)
- `X-Webhook-Delivery-ID`: `<delivery_log_uuid>` (unique ID for this run, used as idempotency key)
- `X-Delivery-Attempt`: `<attempt_number>`
- `Webhook-Timestamp`: `<unix_seconds>`
- `X-Hub-Signature-256`: `sha256=<hmac>` of the §5 canonical grammar `timestamp + "." + idempotency_key + "." + <the exact bytes in this request body>`. Outbound deliveries carry no idempotency key, so that field is the **empty string** (i.e. `timestamp + "." + "" + "." + body`).

> **Signing input is always the §5 grammar over the literal transmitted body** (resolving the §5/§7/§8 ambiguity): the HMAC covers `timestamp + "." + idempotency_key + "." + body`, where `body` is the exact byte sequence of *that* HTTP request and `idempotency_key` is empty for all outbound deliveries. **Inbound** verification (§5) signs the **raw received body** with the inbound `X-Idempotency-Key` (or empty) in the key field. **Outbound** delivery serializes the stored JSONB payload **once** via `CanonicalJSON` (§7) to get deterministic bytes, then signs and sends *those* bytes with an empty key field. Canonicalization exists only to make the outbound body deterministic — it is never a separate signing transform that could differ from what is sent. Signer and verifier therefore always agree on the bytes.

When processing responses, we truncate error response bodies to `512` characters (`maxErrorDetailLen = 512`) before updating the DB's `error_details` field. Furthermore, we always drain the response body (`io.Copy(io.Discard, resp.Body)` followed by `resp.Body.Close()`) to enable HTTP connection reuse in the pooled Transport.

---

### 9. Dev Mode Sync Delivery Option

In development, the first delivery can run synchronously within the ingest handler request lifecycle. Redis remains required because retries, recovery, maintenance, and subscription caching stay active in both modes. This is controlled via the `SYNC_DELIVERY` environment variable:
- `SYNC_DELIVERY=true`: Make the first delivery attempt synchronously (blocks the ingest HTTP call); any persisted successors are retried by the always-on background pipeline.
- `SYNC_DELIVERY=false` (default): Enqueue task to Redis and return `202 Accepted` immediately.

---

## Configuration (Environment Variables)

All config is parsed from env into a typed `Config` struct (stdlib `os.Getenv` + manual parsing — no dependency). Exact names and defaults (the single source of truth for `internal/config` and CHECKLIST Slice 1):

| Env var | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN for the `pgxpool` |
| `REDIS_URL` | — (required) | Redis URL (queue, scheduler, DLQ, cache) — idempotency dedupe is Postgres, not Redis (§5) |
| `PORT` | `8080` | HTTP listen port |
| `WEBHOOK_TIMEOUT` | `10s` | Per-delivery HTTP client timeout (Python parity) |
| `WORKER_CONCURRENCY` | `50` | `errgroup.SetLimit` for the worker pool |
| `PER_SUB_CONCURRENCY` | `5` | Max in-flight deliveries per subscription (noisy-neighbor, Adv §3) |
| `MAX_RETRY_ATTEMPTS` | `5` | Total attempts (1 initial + 4 retries) before DLQ (§4b) |
| `RETRY_BASE_DELAY` | `10s` | Base of the ×3 backoff curve (§4b) |
| `RETRY_MAX_DELAY` | `15m` | Backoff cap guard (§4b) |
| `ORPHAN_THRESHOLD` | `15m` | Coarse backstop: max age of a `pending` row with no live lease before recovery force-reclaims it (§1); the lease (`VISIBILITY_TIMEOUT`) is the primary, fast path |
| `RECOVERY_SCAN_INTERVAL` | `5m` | Period of the orphan-recovery scan |
| `LOG_RETENTION_HOURS` | `72` | Cleanup horizon for non-DLQ rows (Python parity) |
| `DLQ_MAX_AGE` | `720h` (30d) | Force-ack age for un-acked DLQ entries (Adv §1) |
| `CACHE_TTL` | `5m` | Subscription read-through cache TTL (Adv §5) |
| `REGISTRY_IDLE_TTL` | `1h` | Idle eviction TTL for in-memory bucket/semaphore/breaker registries (Adv §3) |
| `RATE_LIMIT_PER_SEC` | `100` | Token-bucket refill rate per subscription (§4) |
| `RATE_LIMIT_BURST` | `200` | Token-bucket capacity per subscription (§4) |
| `BREAKER_THRESHOLD` | `5` | Consecutive failures that trip a per-URL breaker Open (§3) |
| `BREAKER_RESET_TIMEOUT` | `30s` | Open→Half-Open probe delay (§3) |
| `AUTO_DISABLE_THRESHOLD` | `5` | Consecutive `final_failure`s that set `is_active=FALSE` (Adv §2) |
| `SIGNATURE_DRIFT_WINDOW` | `5m` | Allowed `Webhook-Timestamp` clock skew; bounds the replay window (§5) |
| `SECRET_GRACE_WINDOW` | `24h` | Previous-secret acceptance window after rotation (§5) |
| `DRAIN_TIMEOUT` | `30s` | Graceful-shutdown in-flight drain deadline (§1) |
| `ALLOW_HTTP_URLS` | `false` | Permit non-HTTPS target URLs (local dev) |
| `SYNC_DELIVERY` | `false` | Make the first attempt synchronously instead of enqueuing it; retries remain asynchronous (§9) |
| `ENABLE_PPROF` | `false` | Mount `/api/v1/debug/pprof/` |
| `CORS_ALLOWED_ORIGINS` | — | Explicit origin allow-list (never `*` combined with credentials) |
| `ADMIN_API_KEY` | — (required for admin routes) | Bearer token for management/operational endpoints; if unset the admin group fails closed (Authentication & Authorization) |
| `VISIBILITY_TIMEOUT` | `60s` | Lease applied to a `pending` row when a worker claims it; recovery reclaims rows whose lease/`next_retry_at` is past due (§1) |
| `IDEMPOTENCY_TTL` | `5m` | Dedup window for the optional `X-Idempotency-Key`; `cleanup.go` prunes `ingest_idempotency` rows older than this (§5) |

> Defaults are the design intent; Phase 1 confirms each against the Python `config.py` where parity applies (`WEBHOOK_TIMEOUT`, `MAX_RETRY_ATTEMPTS`, `LOG_RETENTION_HOURS`, cache TTL).

---

## Database Schema (Consolidated Initial Migration)

We consolidate our schema into a single initial migration script. The unused Python `webhooks` table is removed since Webhook IDs are generated dynamically at ingestion.

```sql
-- migrations/000001_initial_schema.up.sql
CREATE TYPE delivery_status AS ENUM (
    'pending', 'success', 'failed_attempt', 'final_failure'
);

CREATE TABLE subscriptions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    target_url           VARCHAR(2048) NOT NULL,        -- widened from Python's VARCHAR(255)
    secret_key           VARCHAR(255)  NOT NULL,
    previous_secret_key  VARCHAR(255),                  -- NEW: dual-secret rotation key
    secret_rotated_at    TIMESTAMPTZ,                   -- NEW: grace period start
    event_types          TEXT[]        NOT NULL DEFAULT '{}',
    is_active            BOOLEAN       NOT NULL DEFAULT TRUE,  -- NEW (Python has no is_active)
    consecutive_failures INT           NOT NULL DEFAULT 0,    -- NEW: drives auto-disable (§ Advanced Features 2)
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- X-Webhook-Delivery-ID
    webhook_id      UUID            NOT NULL,                    -- X-Webhook-ID
    subscription_id UUID            NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE, -- the ONE deletion path outside cleanup.go: an explicit, operator-initiated subscription delete intentionally discards that sub's rows (see Adv §1 + Delivery Guarantee Statement)
    target_url      VARCHAR(2048)   NOT NULL,
    payload         JSONB           NOT NULL,
    event_type      VARCHAR(100),
    attempt_number  INT             NOT NULL DEFAULT 1,        -- per-chain (resets to 1 on replay)
    replay_number   INT             NOT NULL DEFAULT 0,        -- NEW: 0 = original chain; +1 per replay. Current chain = MAX(replay_number) per webhook_id
    status          delivery_status NOT NULL DEFAULT 'pending',
    http_status     INT,
    error_details   TEXT,
    next_retry_at   TIMESTAMPTZ,                               -- retry due time AND visibility-lease deadline for pending rows (§0/§1)
    in_dlq          BOOLEAN         NOT NULL DEFAULT FALSE,  -- NEW: TRUE between final_failure and DLQ ack; gates cleanup
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Idempotency authority for the OPTIONAL X-Idempotency-Key (Postgres, not Redis — see §5).
-- Written in the SAME transaction as the initial pending delivery_logs row, so a crash can
-- never acknowledge a key whose webhook row was not durably inserted (the classic dual-write
-- bug a Redis-only SETNX would have). cleanup.go also prunes rows here older than IDEMPOTENCY_TTL.
CREATE TABLE ingest_idempotency (
    subscription_id UUID         NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128) NOT NULL,                  -- client-chosen token; charset excludes the '.' signing delimiter
    webhook_id      UUID         NOT NULL,                  -- the X-Webhook-ID minted for the first accepted request
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subscription_id, idempotency_key)          -- the atomic dedupe constraint (ON CONFLICT → return stored webhook_id)
);

-- migrations/000002_indexes.up.sql
-- Keyset pagination for GET /subscriptions (ORDER BY created_at DESC, id DESC):
CREATE INDEX idx_sub_created_id     ON subscriptions(created_at DESC, id DESC);
-- Status endpoint orders all attempts of a webhook by (replay_number, attempt_number) (§0);
-- the index column order matches that sort so the status query needs no extra in-memory ordering:
CREATE INDEX idx_dl_webhook_attempt ON delivery_logs(webhook_id, replay_number, attempt_number);
CREATE INDEX idx_dl_subscription_id ON delivery_logs(subscription_id);
CREATE INDEX idx_dl_status          ON delivery_logs(status);
CREATE INDEX idx_dl_next_retry_at   ON delivery_logs(next_retry_at) WHERE next_retry_at IS NOT NULL;
-- Orphan-recovery / lease scan: due pending rows (next_retry_at <= now()). Partial index keeps it tiny:
CREATE INDEX idx_dl_pending_recovery ON delivery_logs(next_retry_at) WHERE status = 'pending';
CREATE INDEX idx_dl_created_at      ON delivery_logs(created_at);
```

---

## Complete API Surface

All API endpoints are versioned under prefix `/api/v1`.

| Method | Path | Description | Parity/New? |
|---|---|---|---|
| `POST` | `/api/v1/subscriptions` | Create subscription (requires HTTPS; overridable via `ALLOW_HTTP_URLS=true` for local dev) | — |
| `GET` | `/api/v1/subscriptions` | List subscriptions — **keyset/cursor** pagination (`ORDER BY created_at DESC, id DESC`), returns `{items, next_cursor}` | Ordering fix + **contract change** (drops Python's `total`) |
| `GET` | `/api/v1/subscriptions/{id}` | Get subscription details | — |
| `PUT` | `/api/v1/subscriptions/{id}` | Update subscription details | — |
| `DELETE` | `/api/v1/subscriptions/{id}` | Delete subscription | — |
| `GET` | `/api/v1/subscriptions/{id}/attempts` | Get recent delivery attempts | — |
| `POST` | `/api/v1/subscriptions/{id}/rotate-secret` | Rotate secret (generates key, begins 24h grace window) | **NEW** |
| `POST` | `/api/v1/subscriptions/{id}/replay/{webhook_id}`| Replays a failed/DLQ'd webhook (inserts fresh `pending` attempt + enqueues; **acks** the DLQ entry → `in_dlq=FALSE`) | **NEW** |
| `POST` | `/api/v1/subscriptions/{id}/dlq/{webhook_id}/ack` | Discards/acks a DLQ entry without replaying (`in_dlq=FALSE`, `LREM` from list) so cleanup can reap it | **NEW** |
| `POST` | `/api/v1/ingest/{subscription_id}` | Ingest payload. Requires `X-Hub-Signature-256` **and** `Webhook-Timestamp` (NEW); checks `is_active` (NEW) + rate-limit (NEW); event-type filter (parity). Body capped at 64KB | Parity + **NEW** |
| `GET` | `/api/v1/status/{webhook_id}` | Retrieve webhook status & attempt history | — |
| `GET` | `/api/v1/status/metrics/summary` | Delivery aggregation statistics summary over last `hours` (default: 24) | Python Parity |
| `GET` | `/api/v1/health` | Liveness probe (ping DB & Redis) | — |
| `GET` | `/api/v1/ready` | Readiness probe (workers running?) | **NEW** |
| `GET` | `/api/v1/monitor` | System health, queue depths, CB states, & latency distributions | Enhanced |
| `GET` | `/api/v1/openapi.json` | Serves the generated OpenAPI 3.1 spec (hand-authored, embedded via `//go:embed`) | **NEW** |
| `GET` | `/api/v1/docs` | Serves an **interactive Swagger UI** (static HTML loading swagger-ui from CDN, pointed at `/api/v1/openapi.json`) — satisfies the assignment's "minimal UI" requirement. Raw JSON alone is *not* a UI | **NEW** |
| `GET` | `/api/v1/debug/vars` | expvar runtime variables (admin auth) | **NEW** |
| `GET` | `/api/v1/debug/pprof/` | Profiling handler (gated by `ENABLE_PPROF=true`) | **NEW** |

### `/api/v1/monitor` JSON Response Shape:
```json
{
  "uptime_seconds": 86400,
  "current_time": "2026-06-18T22:00:00Z",
  "delivery_metrics": {
    "total": 1523,
    "success": 1400,
    "failed_attempt": 80,
    "final_failure": 3,
    "pending": 40
  },
  "queue_depth": 12,
  "dead_letter_queue_depth": 1,
  "circuit_breakers": {
    "https://example.com/webhook-receiver": "closed"
  },
  "delivery_latency_ms": {
    "min": 12,
    "max": 2400,
    "avg": 84,
    "p50": 42,
    "p95": 195,
    "p99": 450
  }
}
```

> **Implementation note:** `expvar` provides the counters/gauges (`delivery_metrics`, `queue_depth`), but **percentiles are not free from `expvar`** — they require a latency sketch. We keep a small fixed-size, mutex-guarded **bounded histogram** (log-linear buckets) in the deliverer and compute `p50/p95/p99` from bucket counts at scrape time. This stays stdlib-only and O(1) per observation; no `metrics`/Prometheus dependency.

---

## Authentication & Authorization

The HMAC signature on `/ingest` authenticates **payload senders**, not operators — so every other route needs its own gate. Without this, subscription CRUD, secret rotation, DLQ replay/ack, payload-bearing status responses, `/monitor`, and `pprof` are world-open (arbitrary secret rotation/deletion/replay and payload/secret exfiltration).

| Route group | Auth |
|---|---|
| `POST /api/v1/ingest/{id}` | Per-subscription **HMAC** (`X-Hub-Signature-256` + `Webhook-Timestamp`); no admin token |
| `GET /api/v1/health`, `GET /api/v1/ready` | **Public** (platform liveness/readiness probes) |
| All `/api/v1/subscriptions*` (CRUD, rotate-secret, replay, dlq/ack), `/api/v1/status*`, `/api/v1/monitor` | **Admin auth**: `Authorization: Bearer <ADMIN_API_KEY>`, constant-time compared; `401` if absent/mismatched |
| `/api/v1/debug/pprof/*` | Admin auth **and** `ENABLE_PPROF=true` (defense in depth — profiles are sensitive) |
| `/api/v1/docs`, `/api/v1/openapi.json` | Public (spec only — no secrets) |

- An **`AdminAuth` middleware** wraps the admin route group; `health`/`ready`/`ingest`/`docs` are mounted outside it.
- **Secret redaction:** subscription responses (create/get/list/update) **never** include `secret_key` or `previous_secret_key`. The plaintext secret is shown **once** — at create and at `rotate-secret` — and never again.
- `ADMIN_API_KEY` has **no default**: if unset, the admin group **fails closed** (`503`), so the service can never be deployed accidentally wide-open.
- Scope note: a single static admin credential is deliberate for this portfolio build; the README ADR records per-user tokens / JWT / mTLS as the production evolution.

---

## Redis Key Space

| Key | Type | Purpose |
|---|---|---|
| `webhook:queue` | LIST | Immediate delivery tasks (`LPUSH`/`BRPOP`) |
| `webhook:retry:schedule` | ZSET | Retry schedule (`score=unix_ts, member=log_id`) |
| `webhook:dlq` | LIST | Dead letter queue of permanently failed attempt-row `id`s; `LREM` on replay/ack, reconciled against the `in_dlq` flag (DB is source of truth) |
| `subscription:{uuid}` | STRING | Read-through cache (JSON serialization, TTL=5min) |

> Idempotency dedupe for the optional `X-Idempotency-Key` is **Postgres-authoritative** (the `ingest_idempotency` table, §5), *not* a Redis key — a Redis-only `SETNX` would commit the dedupe marker before the DB write and could acknowledge a key whose webhook never durably landed. Redis here holds only derived indexes (queue, schedule, DLQ) and the subscription cache.

---

## Advanced Production Features

### 1. Dead Letter Queue (DLQ) with ACK-Gated Cleanup
When a webhook reaches `final_failure` (the 5th and last attempt failed — see §4b), the delivery engine sets `in_dlq = TRUE` **in the same DB transaction** as the `final_failure` status update, and then — **after that transaction commits** — best-effort `LPUSH`es the attempt-row `id` onto the `webhook:dlq` Redis LIST. (Postgres and Redis cannot share a transaction, so the durable signal is the `in_dlq` flag; the LIST is a fast operator-facing index that the reconciler repairs if the post-commit `LPUSH` is lost.) This is the buffer engineering teams inspect, replay, or discard.

**Addressing & atomic claim.** The endpoints carry both `{subscription_id}` and `{webhook_id}` and operate on the **single row where `subscription_id = $1 AND webhook_id = $2 AND in_dlq = TRUE`** (at most one exists at any instant — `final_failure` is the only thing that sets the flag, and both ack paths clear it before a new chain can fail again). Clearing the flag is an **atomic claim** — `UPDATE delivery_logs SET in_dlq = FALSE WHERE subscription_id=$1 AND webhook_id=$2 AND in_dlq=TRUE RETURNING id, replay_number`. Exactly one concurrent caller gets a row back and proceeds; the rest get 0 rows → `404`. Two simultaneous replays therefore cannot both spawn a fresh chain. Scoping by `subscription_id` is a **data-integrity / addressing** check (the webhook must actually belong to the named subscription, and the claim targets exactly the right row) — it is **not** a tenant/authz boundary: under a single global `ADMIN_API_KEY` (see Authentication & Authorization) every authenticated operator can name any `subscription_id`. Real per-operator authorization (scoped tokens / JWT) is the documented production evolution; v1's single admin credential has no per-subscription isolation to enforce.

**ACK-gated retention (durable, bounded).** Postgres remains the source of truth via the `in_dlq` flag; the Redis LIST is just the fast operator-facing index. A DLQ entry is **acknowledged** in two operator-initiated ways (plus an **automatic force-ack** after `DLQ_MAX_AGE` — see the reconciler below). Each operator path runs the atomic claim in a DB transaction and then (post-commit, best-effort) `LREM`s the id from the LIST:

| Action | Effect |
|---|---|
| **Replay** (`POST .../replay/{webhook_id}`) | In the claim's transaction (only if a row was claimed): insert a fresh `pending` row that **starts a new chain** — `replay_number = claimed.replay_number + 1`, `attempt_number = 1`, `next_retry_at = now()` — under the **same** `webhook_id` (a replay gets a full new retry budget, not `attempt 6`, which would instantly exhaust `attempt < 5`). After commit: enqueue + `LREM`. The status endpoint identifies the *current* chain by the **highest `replay_number`**, so a stale old `final_failure` never masks a successful replay. `(webhook_id, attempt_number)` stays non-unique (§0); consumers dedupe on `X-Webhook-ID`. |
| **Discard/ack** (`POST .../dlq/{webhook_id}/ack`) | The atomic claim *is* the ack (`in_dlq = FALSE`); after commit `LREM`. No new row. |

**Cleanup then becomes:** `DELETE FROM delivery_logs WHERE created_at < threshold AND NOT in_dlq AND status <> 'pending'`. The `status <> 'pending'` guard is **load-bearing for at-least-once**: a `pending` row is undelivered work (its enqueue may have been lost and recovery hasn't reclaimed it yet), so retention must never reap it — only terminal rows (`success`, `failed_attempt`, acked `final_failure`) are eligible. So a `final_failure` row survives retention **only until it is acked**, after which the next cleanup cycle reaps it. This bounds growth (a chronically-dead endpoint no longer accumulates rows forever once triaged) while guaranteeing replay/inspection is possible for any *un-acked* entry regardless of age. The `replay`/`ack` endpoints return `404` if the row was already reaped.

**Separation of concerns (two goroutines).** All of this is split so that exactly one component deletes rows:
- **`cleanup.go`** is the *only* deleter: `DELETE … WHERE created_at < threshold AND NOT in_dlq AND status <> 'pending'` (never reaps undelivered work). It also prunes `ingest_idempotency` rows older than `IDEMPOTENCY_TTL` — the only other table it touches, and still the sole deletion path, so the "one deleter" invariant holds. (The one deletion *outside* `cleanup.go` is the explicit operator-initiated subscription delete, which `ON DELETE CASCADE`s — see the Delivery Guarantee Statement.)
- **`DLQReconciler`** (in `cleanup.go`) never deletes — it only mutates the `in_dlq` flag and syncs the Redis LIST:
  - **Reconciliation (flag is authoritative):** the LIST and flag can momentarily disagree (e.g. a crash between `LPUSH` and commit, or a lost post-ack `LREM`). The reconciler re-`LPUSH`es ids of rows where `in_dlq = TRUE` but absent from the LIST, and `LREM`s any LIST id whose row **fails `WHERE id=$1 AND in_dlq = TRUE`** — i.e. the row is gone *or* it is still present but already acked (`in_dlq = FALSE`). Checking only "row is gone" would leave an acked-but-not-`LREM`'d entry visible in the LIST and in `dead_letter_queue_depth` until retention finally deleted the row, and a replay/ack on it would `404` for an entry operators can still see. Same "DB is source of truth" principle as orphan recovery (§1).
  - **`DLQ_MAX_AGE` safety valve (default 30d):** for `in_dlq` entries older than the limit that no operator ever acked, the reconciler **force-acks** them (`in_dlq = FALSE` + `LREM` + a logged warning). It does *not* delete — the next `cleanup` cycle then reaps them via its `AND NOT in_dlq` predicate. This keeps every row deletion in one place.

### 2. Automatic Endpoint Disablement *(NEW)*
To avoid burning worker cycles on dead URLs, the `subscriptions.consecutive_failures` counter (see schema) is maintained transactionally with each terminal outcome:
- **On `final_failure`:** `UPDATE subscriptions SET consecutive_failures = consecutive_failures + 1`. If it reaches **5**, also set `is_active = FALSE` in the same statement and **evict the subscription cache key**.
- **On `success`:** reset `consecutive_failures = 0` (only if non-zero, to avoid needless writes).

This makes "5 consecutive final failures" a durable, crash-safe count rather than in-memory state. (Note: 5 *consecutive final failures* ≈ 25 delivery attempts to a dead endpoint before disablement — tune the threshold via config if that's too lenient.)

### 3. HTTP Client Transport Tuning & Noisy Neighbor Protection
Outbound webhook deliveries are run through a tuned, shared `http.Client`:
- `MaxIdleConns`: 100
- `MaxIdleConnsPerHost`: 10
- `IdleConnTimeout`: 90s
- `TLSHandshakeTimeout`: 10s
- `DialContext`: `SafeDialContext` (SSRF prevention)
- `Proxy`: `nil` (never route around `SafeDialContext` via proxy env vars — see §6)

To prevent a single slow or misbehaved subscriber (noisy neighbor) from consuming all pool workers, the worker limits in-flight concurrent requests per subscription using a semaphore map (`sync.Map` of bounded channels). The permit is acquired **non-blockingly** (`select { case sem <- token: deliver; default: requeue }`): if the subscription is already at its concurrency limit the worker does **not** park on the permit — parking would let one hot subscription pin every worker and starve all others (head-of-line blocking). Instead it re-schedules the task with a small back-off (`ZADD webhook:retry:schedule` a few seconds out, **without** consuming a retry attempt) and moves to the next task, keeping workers available to other subscriptions.

**Bounded in-memory registries (explicit eviction, not just asserted).** Three per-key registries live in memory: token-bucket rate limiters (keyed by subscription id), per-subscription concurrency semaphores (subscription id), and circuit breakers (target URL). All three share one discipline: each entry stamps a `lastUsed` time; a periodic in-memory sweep (the same cleanup ticker, distinct from `cleanup.go`'s DB-row deletion) evicts any entry idle longer than `REGISTRY_IDLE_TTL` (default 1h), and subscription-keyed entries are evicted immediately on subscription delete. This caps memory under subscription/URL churn instead of growing unbounded.

> **Scope: these limits are per-instance.** The token buckets, per-subscription semaphores, and circuit breakers live in process memory, so with *N* replicas the *effective* per-subscription rate/concurrency is *N×* the configured value. v1 deploys a **single instance** (Phase 6 / Railway free tier), where per-instance == global and the configured numbers are exact. Horizontal scale-out would require either dividing the configured limits by the replica count or moving rate/concurrency state to Redis (token buckets via Lua, concurrency via leased counters) — deferred, see Architectural Considerations.

### 4. Global & Payload Body Size Limits
To prevent Out of Memory (OOM) and DoS attacks:
- We wrap incoming HTTP request bodies in a global size limiter.
- Webhook payloads to `/api/v1/ingest/{id}` are strictly capped at 64KB using `http.MaxBytesReader`. Payloads exceeding 64KB return `413 Payload Too Large`.

### 5. Subscription State and Cache Coherency
`internal/subscription.State` is the mutation boundary for durable subscription changes, delivery-outcome counter changes, Redis coherence, and derived rate/concurrency state. The `subscription:{uuid}` cache (TTL 5min) is read-through on ingest/delivery and **must be evicted on every state-changing write path**, or a worker could sign with a stale secret or keep delivering to a deactivated subscription. Reads and writes for the same subscription are serialized with bounded lock striping, preventing an in-flight miss from back-filling stale state after a committed write. A failed Redis invalidation records a local dirty marker so reads bypass the stale key until repair or TTL expiry.

| Write path | Cache action |
|---|---|
| `PUT /subscriptions/{id}` (update) | evict `subscription:{id}` |
| `DELETE /subscriptions/{id}` | evict `subscription:{id}` |
| `POST .../rotate-secret` (NEW) | evict `subscription:{id}` — else outbound signing uses the pre-rotation secret |
| Auto-disable on 5 consecutive `final_failure` (NEW) | evict `subscription:{id}` — else ingest/delivery keeps treating it as active |

Eviction (delete-on-write), not write-through, keeps the secret out of any window where a partial update could publish an inconsistent record.

---

## Architectural Considerations (Documented for ADR / README)

The following patterns were evaluated during design and are documented here for interview discussion and future extensibility, even though they are not implemented in v1:

| Pattern | Why Considered | Why Deferred | Future Path |
|---|---|---|---|
| **Transactional Outbox** | Eliminates dual-write risk (DB INSERT + Redis LPUSH can partially fail) | Orphaned task recovery job is simpler and sufficient for current scale | Add an `outbox` table + relay goroutine if dual-write failures become measurable |
| **Fair Queue Scheduling** | Round-robin across subscriptions prevents high-volume subs from starving others | Single Redis LIST with per-sub concurrency semaphores provides adequate fairness | Use per-subscription Redis LISTs with weighted round-robin dequeue |
| **Wildcard Event Type Matching** | `order.*` matching `order.created`, `order.updated` is common in Stripe/GitHub | Exact-match with `slices.Contains` covers the most common use cases | Add `strings.HasPrefix` or glob matching behind a feature flag |
| **Event Ordering Guarantees** | Some consumers expect events to arrive in order | Ordering requires per-subscription partitioning which conflicts with simple LIST-based queuing | N/A — explicitly document the guarantee instead |
| **Distributed rate/concurrency limits** | In-memory buckets/semaphores multiply by replica count under horizontal scale-out | v1 runs a single instance, so per-instance == global — correct and simplest at current scale | Redis token buckets (Lua) + leased per-subscription concurrency counters in Redis |
| **Visibility lease vs. transactional outbox** | The post-commit `ZADD`/`LPUSH` can still be lost; the lease only bounds recovery latency, not the dual-write itself | The lease + CAS keeps it correct (at-least-once, no corruption) and far simpler than an outbox relay | Add an `outbox` table + relay goroutine if lost-enqueue rates become measurable |

> [!NOTE]
> **Delivery Guarantee Statement**: This system provides **at-least-once** delivery with **no ordering guarantee**. Because each attempt is a distinct row (§0), `X-Webhook-Delivery-ID` is **unique per attempt** (per-attempt idempotency), while `X-Webhook-ID` is **stable across all attempts of one event** — consumers should **dedupe on `X-Webhook-ID`** and use `Webhook-Timestamp` for staleness/ordering decisions. **Scope of the guarantee:** deleting a subscription (`DELETE /subscriptions/{id}`) is an explicit, destructive operator action that `ON DELETE CASCADE`s its `delivery_logs` — including any still-`pending` or un-acked DLQ rows. The at-least-once guarantee therefore holds for a subscription's lifetime, *not* past its deletion; this cascade is the single deletion path outside `cleanup.go` (which otherwise never reaps undelivered work).

---

## Senior-Level Portfolio Signals

| Signal | Manifests as |
|---|---|
| Distributed systems literacy | Lua-script based atomic scheduling of retries, eliminating task race conditions |
| Concurrency mastery | Bounded worker pool (`errgroup.SetLimit` backpressure) with a `context.WithoutCancel` drain that survives SIGTERM instead of aborting in-flight work; deliveries never tear down the pool; per-subscriber concurrency semaphores |
| Resilience engineering | Circuit breaker FSM per URL with `atomic` probing flags, exponential backoff with ±20% jitter |
| Security depth | SSRF-preventing DNS dialer, constant-time signature evaluation, 5-minute replay window, dual-key rotation |
| Rate limiting | Bounded Token Bucket implemented on the API layer without external dependencies |
| Observability | `slog` with context-propagated Request IDs, expvar stats, custom latency tracking, pprof gate |
| Database craft | Raw SQL with pgx, cursor-based pagination, covering indexes, embedded migrations |
| Operational maturity | Clean 30s graceful drain, Docker multi-stage + distroless, DLQ, and crash-recovery job |

---

## Implementation Phases

### Phase 1 — Foundation
- `go.mod` init, directory scaffold, `Makefile`
- `internal/config` — env parsing
- `internal/domain` — all types, interfaces, sentinel errors (incl. one-row-per-attempt invariants, §0)
- `migrations/` — initial schema (incl. the `ingest_idempotency` table, §5) with `is_active` + `consecutive_failures` + rotation cols, and the §0-aware indexes (keyset, `(webhook_id, replay_number, attempt_number)` status-ordering, partial pending-recovery)
- `internal/store` — pgx pool + subscription repo (**keyset/cursor** pagination, `ORDER BY created_at DESC, id DESC`)
- `docker-compose.yml` — postgres + redis with AOF

### Phase 2 — Core Delivery Pipeline & Safedial
- `internal/infra/signature` — HMAC sign/verify/canonical JSON (using `json.Number`)
- `internal/infra/safedial` — SSRF DialContext and resolver
- `internal/infra/queue` — Redis LIST queue + DLQ
- `internal/infra/scheduler` — Redis ZSET retry scheduler (Lua script claims)
- `internal/subscription` — subscription read-through cache and derived runtime state
- `internal/worker/pool` + `internal/worker/delivery` — end-to-end delivery with response body drain
- `internal/worker/retry` (RetrySchedulerWorker) — scheduler goroutine with Lua script polling
- `internal/worker/retry` (RecoveryWorker) — orphaned task recovery goroutine
- `internal/worker/cleanup` — retention ticker (sole retention-driven row deleter; also prunes `ingest_idempotency`; the one deletion outside it is the explicit subscription-delete cascade)
- `internal/worker/cleanup` (DLQReconciler) — DLQ LIST↔`in_dlq` reconciliation + `DLQ_MAX_AGE` force-ack (added in Phase 4 once the DLQ exists; scaffolded here)

### Phase 3 — API Layer
- `internal/api/server` — ServeMux with CORS middleware (correct constraints: no `*` origin with `allow_credentials`, unlike the Python `main.py`), v1 prefix
- All handlers: subscription CRUD (keyset list returning `{items, next_cursor}`), ingest (signature **+ `Webhook-Timestamp`**, `is_active`, rate-limit, 64KB cap, event filter), status (**aggregates all attempt rows by `webhook_id`** → `attempts[]` + `statistics`, §0), `/metrics/summary`, health, ready, monitor
- `internal/api/handler_docs` — **interactive Swagger UI** at `/docs` + `//go:embed` OpenAPI 3.1 spec at `/openapi.json`

### Phase 4 — Advanced Features
- `internal/infra/breaker` — circuit breaker with probing flag + integration with deliverer
- `internal/infra/ratelimit` — mutex-only token bucket (bounded `sync.Map`, idle reaping) + ingest integration
- Secret rotation endpoint + dual-secret & timestamp verification logic + **cache eviction on rotate**
- Auto-disable: `consecutive_failures` increment/reset (transactional) + `is_active=false` at threshold + cache eviction
- DLQ: `in_dlq` flag set transactionally on `final_failure` + `LPUSH`; replay & ack endpoints clear the flag (`LREM`); **ACK-gated cleanup** in `cleanup.go` (`... AND NOT in_dlq`)
- `internal/worker/cleanup` (DLQReconciler) — dedicated goroutine: LIST↔`in_dlq` reconciliation + `DLQ_MAX_AGE` force-ack (flag-only, no deletes — keeps `cleanup.go` the sole retention-driven deleter)
- Per-subscription concurrency limits in worker delivery
- `pprof` endpoint (env-gated)

### Phase 5 — Polish & Testing
- Structured logging throughout (`slog` with request ID context)
- `expvar` counters + **bounded latency histogram** (p50/p95/p99) wired to deliverer, pool, scheduler
- `Dockerfile` multi-stage (Go builder → `gcr.io/distroless/static`)
- `README.md` — ADR, setup guide, **client migration note** for the new `timestamp.payload` signature + required `Webhook-Timestamp` header
- Unit tests using `testify` for signature, safedial, breaker, ratelimit, worker pool; plus a recovery-job test asserting orphaned `pending` retries are reclaimed (§0)

### Phase 6 — Deployment & Cost *(assignment deliverable — was missing)*
- Deploy to a free-tier PaaS using the existing `railway.toml` / `railway.json` / `Procfile` (postgres + redis add-ons), with `migrate`-on-startup
- README: **monthly cost estimate** for 24×7 + ~5,000 webhooks/day @ 1.2 attempts (assignment requirement), and the deployed URL
- README: assumptions, library/AI-tool credits, and sample `curl` commands per endpoint (including how to compute the new `timestamp.payload` HMAC — port `generate_signature.py`)
