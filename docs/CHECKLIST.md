# Go Webhook Delivery System — Implementation Checklist

Vertical **tracer-bullet** slices derived from [`implementation_plan.md`](./implementation_plan.md) (v4).
The plan is the single source of truth for design; the Python `app/` tree is the read-only parity
reference. This file tracks *what to build, in what order* — it does not restate the design.

**How to use:** each slice is a thin path through every layer it touches and is independently
verifiable. Work top-to-bottom (slices are listed in dependency order). Check criteria as you go;
a slice is done when all its criteria are checked, `go build ./...` + `go vet ./...` are clean, and
its verification step passes. `AFK` = implementable/mergeable without human input. `HITL` = needs a
human (decision, credentials, or a live deploy).

---

## Standing invariants (apply to every slice — never violate)

- [x] **Runtime deps**: 3 third-party packages (`pgx/v5`, `golang-migrate`, `go-redis/v9`) **+ `golang.org/x/sync`** (Go-team extended stdlib, for `errgroup`). `testify` is test-only. Nothing else ships in the binary. _(go.mod direct requires = those 3 + `golang.org/x/sync` (test-only `testify` aside); migrate uses its pgx/v5 driver so no lib/pq; binary import graph verified clean.)_
- [x] **`internal/domain` has zero non-stdlib imports** (the dependency rule — arrows point inward). _(domain imports only fmt/net/url/strings/time/context/encoding/json/errors.)_
- [x] **One row per delivery attempt** (plan §0): `delivery_logs.id` = `X-Webhook-Delivery-ID`; `webhook_id` = `X-Webhook-ID` (stable). `attempt_number` is per-chain; `replay_number` identifies the chain (current chain = `MAX(replay_number)`). `(webhook_id, attempt_number)` is **non-unique**; never mutate `attempt_number` in place.
- [x] **CAS + single-txn + visibility-lease lifecycle** (plan §0/§1): every status change is `UPDATE … WHERE id=$1 AND status='pending'` (0 rows ⇒ abort). The `failed_attempt` update + `pending` successor INSERT are **one DB transaction**. A worker **claims** a pending row with a conditional CAS — `next_retry_at = now()+VISIBILITY_TIMEOUT WHERE id=$1 AND status='pending' AND next_retry_at <= now()` (the `<= now()` predicate makes the claim the mutual-exclusion point; 0 rows ⇒ not due / lease live / already handled ⇒ duplicate dequeue, skip); recovery reclaims `pending AND next_retry_at <= now()`. Redis `ZADD`/`LPUSH` happen **after commit** (Postgres is source of truth; recovery + reconciler heal lost post-commit ops).
- [x] **Signature** = `HMAC(timestamp + "." + idempotency_key + "." + <exact transmitted body bytes>)` — **one** canonical grammar both directions (plan §5); `idempotency_key` is empty when absent and always empty outbound; the key charset excludes `.`. Inbound signs the **raw received body** (+ inbound key); outbound signs the **canonical bytes it emits** (empty key). `Webhook-Timestamp` required. Replay is bounded by the timestamp window **plus an optional signed `X-Idempotency-Key`** whose dedupe is **Postgres-authoritative** (`ingest_idempotency`, same txn as the pending insert) — never a Redis `SETNX`, never a deterministic signature-as-nonce.
- [x] **`cleanup.go` is the only retention-driven row deleter**, and it **never deletes `pending`** (`… AND NOT in_dlq AND status <> 'pending'`); it also prunes `ingest_idempotency` past `IDEMPOTENCY_TTL`. The **one** deletion outside it is the explicit operator-initiated `DELETE /subscriptions/{id}`, which `ON DELETE CASCADE`s that sub's rows by design (plan Delivery Guarantee Statement). Reconciler/auto-disable mutate flags only.
- [x] **Auth**: `ingest` = per-sub HMAC; `health`/`ready`/`docs`/`openapi.json` = public; **all other routes** (subscription CRUD, rotate-secret, replay, dlq/ack, status, monitor, pprof) require `Authorization: Bearer <ADMIN_API_KEY>` (constant-time, fails closed if unset). Subscription responses **never** return `secret_key`/`previous_secret_key` (shown once at create + rotate).
- [x] **Cache coherency** (Adv §5): every subscription mutation is serialized with the same key's read-through path and evicts `subscription:{id}` before releasing the guard; invalidation failures install a local bypass marker. This includes delivery transactions that reset/increment counters or auto-disable.
- [x] Use `find-docs` / `ctx7` for `pgx/v5`, `go-redis/v9`, `golang-migrate` APIs — do not trust training-data signatures. Invoke the `golang-how-to` orchestrator before writing Go.
- [x] Dev env is Windows 11 / PowerShell — `Makefile` + scripts must run there (note bash/WSL alternative where needed).

---

## Slice map

| # | Slice | Type | Blocked by | Plan refs |
|---|---|---|---|---|
| 1 | Foundation & walking skeleton | AFK | — | Phase 1, §schema, §config |
| 2 | Subscription CRUD + admin auth + middleware | AFK | 1 | Phase 3, API surface, §auth |
| 3 | Core secure tracer bullet: sync ingest → sign → SSRF-safe deliver → status | AFK | 2 | Phase 2/3, §0, §5, §6, §7, §8, §9 |
| 4 | Async pipeline: Redis queue + leased worker pool + graceful shutdown | AFK | 3 | Phase 2, §0, §1 |
| 5 | Durable pending-row lifecycle: retry scheduler + lease-based recovery | AFK | 4 | Phase 2, §0, §1, §2, §4b |
| 6 | DLQ + atomic replay/ack + ACK-gated cleanup + reconciler | AFK | 5 | Phase 4, Adv. §1 |
| 7 | Resilience: circuit breaker + rate limiter + per-sub concurrency | AFK | 5 | Phase 4, §3, §4, Adv. §3 |
| 8 | Cache coherency: read-through + secret rotation + auto-disable | AFK | 5 | Phase 2/4, §5, Adv. §2, Adv. §5 |
| 9 | Observability: /monitor, expvar, latency histogram, pprof | AFK | 7 | Phase 5, §monitor |
| 10 | OpenAPI 3.1 spec + interactive Swagger UI | AFK | 8 | Phase 3, API surface |
| 11 | Test & lint hardening (incl. high-risk failure tests) | AFK | 9 | Phase 5 |
| 12 | Dockerfile + README/ADR + curl samples | AFK | 10 | Phase 5/6 |
| 13 | Deployment + cost estimate | HITL | 12 | Phase 6 |

---

## Slice 1 — Foundation & walking skeleton  `AFK`
**Blocked by:** none · **Plan refs:** Phase 1, Database Schema, Configuration

The empty-but-running skeleton: config → store → migrations → health endpoint → `main.go` wiring. Proves the whole stack boots and talks to Postgres + Redis.

- [x] **Module path confirmed** with the user: `github.com/Vaibtan/webhook-delivery-system` → `go.mod` (Go 1.26).
- [x] Hexagonal scaffold per the plan's directory tree: `cmd/server`, `internal/{config,domain,api,store,infra/*,worker}`. _(api/store/config/domain created; infra/* + worker created per-slice as they're built.)_
- [x] `internal/config`: typed `Config` from `os.Getenv`, matching the plan's **Configuration** table exactly (incl. `ADMIN_API_KEY`, `VISIBILITY_TIMEOUT`, `IDEMPOTENCY_TTL`, `ORPHAN_THRESHOLD`).
- [x] `internal/domain`: `Subscription`, `DeliveryLog` (incl. `replay_number`), `DeliveryStatus` enum, repository + service ports, sentinel errors. _(ports grow per slice.)_
- [x] `migrations/000001` (subscriptions w/ `is_active` + `consecutive_failures` + rotation cols; delivery_logs w/ `replay_number` + `in_dlq`; **`ingest_idempotency`** table w/ `PRIMARY KEY (subscription_id, idempotency_key)`, §5) + `000002` indexes (keyset, **non-unique** status-ordering `(webhook_id, replay_number, attempt_number)`, partial `idx_dl_pending_recovery ON (next_retry_at) WHERE status='pending'`, etc.).
- [x] `internal/store/db.go`: `pgxpool` + embedded `golang-migrate` runner invoked on startup. _(iofs + pgx5:// driver; verified migration version=2.)_
- [x] `docker-compose.yml`: postgres + redis (AOF persistence). _(at docker/docker-compose.yml to avoid clobbering the Python root compose.)_
- [x] `main.go` boots, runs migrations, serves `GET /api/v1/health` (pings DB + Redis) and `GET /api/v1/ready` (basic readiness **stub** — deps reachable; worker-aware readiness added in Slice 4).
- [x] `Makefile` (`run`/`test`/`lint`/`migrate-up`/`migrate-down`). _(make absent on Windows; recipes runnable directly as go/docker compose; Windows note in header.)_
- [x] **Verify:** `docker-compose up` + `make run`; `curl /api/v1/health` → 200 with DB + Redis OK. _(verified: /health & /ready both 200 with database+redis ok.)_

## Slice 2 — Subscription CRUD + admin auth + middleware  `AFK`
**Blocked by:** 1 · **Plan refs:** Phase 3 (CORS middleware), API surface, Authentication & Authorization, Configuration (`CORS_ALLOWED_ORIGINS`)

- [x] `store/subscription_repo.go` implements `SubscriptionRepository` in raw SQL; **keyset pagination** `ORDER BY created_at DESC, id DESC`. _(tuple `(created_at,id) < ($1,$2)`; fetch limit+1 for next_cursor.)_
- [x] `api/server.go` ServeMux under `/api/v1`; `middleware.go` (RequestID, slog logging, panic recovery, CORS with correct constraints — no `*` + credentials; `CORS_ALLOWED_ORIGINS`, body size limit).
- [x] **`AdminAuth` middleware**: `Authorization: Bearer <ADMIN_API_KEY>` (constant-time), wrapping the management route group; **fails closed** (`503`) if `ADMIN_API_KEY` unset. `health`/`ready` mounted outside it. _(verified: 503 fail-closed; constant-time via sha256+subtle.)_
- [x] **Secret redaction**: CRUD responses never include `secret_key`/`previous_secret_key`; plaintext returned only at create. _(verified: GET/LIST carry no secret.)_
- [x] `POST /api/v1/subscriptions` — HTTPS-only validation, overridable via `ALLOW_HTTP_URLS=true`. _(verified: http→400 when disallowed.)_
- [x] `GET /api/v1/subscriptions` returns `{items, next_cursor}` (no `total`); `GET`/`PUT`/`DELETE /api/v1/subscriptions/{id}` with `ErrNotFound` → 404; `GET /api/v1/subscriptions/{id}/attempts`.
- [x] **Verify:** full CRUD via curl with bearer token; requests without/with wrong token → 401; responses carry no secret material. _(all 11 checks + 503 fail-closed passed.)_

## Slice 3 — Core secure tracer bullet: sync ingest → sign → SSRF-safe deliver → status  `AFK`
**Blocked by:** 2 · **Plan refs:** §0, §5, §6, §7, §8, §9, Phase 2/3

The keystone slice — proves the whole webhook concept end-to-end with an in-request first attempt (`SYNC_DELIVERY=true`). The final system keeps the queue/scheduler workers alive in this mode so persisted retry successors cannot be stranded. **SSRF protection ships here** with the first real outbound call. (Rate-limit + per-sub concurrency are added in Slice 7.)

**Signature + ingest:**
- [x] `infra/signature`: `Sign`, `Verify` (constant-time), `CanonicalJSON` (`UseNumber()`; **rejects trailing tokens via a second `Decode` expecting `io.EOF`**, not `dec.More()`). Signing input is always the canonical grammar `timestamp + "." + idempotency_key + "." + <exact bytes of that request body>` (empty key field when absent/outbound; §5). _(unit-tested incl. key-is-signed, large-number preservation.)_
- [x] `handler_ingest`: verify `X-Hub-Signature-256` over the **raw received body** (with the inbound `X-Idempotency-Key`, or empty, in the key field) + required `Webhook-Timestamp` (drift window); **optional `X-Idempotency-Key`** folded into the signed material → **Postgres dedupe**: `INSERT INTO ingest_idempotency … ON CONFLICT (subscription_id, idempotency_key) DO NOTHING` in the **same txn** as the pending insert (repeat = idempotent no-op returning the **stored** `webhook_id`; never a Redis `SETNX`); `is_active` check; 64KB `MaxBytesReader` (→413); event-type filter; insert `pending` log with `next_retry_at = now()` (immediately visible). _(verified: dup key → same webhook_id, no re-deliver.)_

**SSRF-safe delivery:**
- [x] `infra/safedial`: `isBlockedIP` on `net/netip` with `Unmap()`; blocks loopback/private/link-local (uni+multi)/multicast/unspecified + `0.0.0.0/8`, `100.64.0.0/10`, `240.0.0.0/4`. `SafeDialContext` vets all resolved IPs, **pins to the first vetted IP**, **fails closed on empty DNS**. _(verified: 169.254.169.254, localhost→::1, 10.0.0.1 all blocked at dial.)_
- [x] Shared `http.Client`: `MaxIdleConns 100`/`MaxIdleConnsPerHost 10`/`IdleConnTimeout 90s`/`TLSHandshakeTimeout 10s`/`DialContext=SafeDialContext`/**`Proxy: nil`**/`CheckRedirect` (≤3 hops **and** reject non-`https` unless `ALLOW_HTTP_URLS`; each hop re-dials through `SafeDialContext`). _(NewHardenedClient; redirect guard unit-tested.)_
- [x] `worker/delivery` (sync path): outbound headers (§8), sign the **emitted** `timestamp + "." + canonical body`, POST, drain+close body, truncate `error_details` to 512, terminal CAS (`WHERE id=$1 AND status='pending'`). _(verified delivery to postman-echo → success.)_

**Status + persistence:**
- [x] `store/delivery_log_repo.go`: insert `pending`, CAS status updates, `createRetryLog` (new row, same `webhook_id`, in the failed→successor transaction), fetch all rows by `webhook_id`. _(IngestPending + MarkSuccess/MarkFinalFailure CAS; createRetryLog (failed→successor) lands in Slice 5.)_
- [x] `GET /api/v1/status/{webhook_id}` (admin auth) → `attempts[]` ordered by `(replay_number, attempt_number)` + `statistics`; current outcome = `MAX(replay_number)` chain. `GET /api/v1/status/metrics/summary` counts rows by status over `hours` (default 24). _(verified shapes.)_
- [x] testify test: safedial blocks `127.0.0.1`, `169.254.169.254`, `10.x`, `::ffff:169.254.169.254`; CanonicalJSON rejects trailing tokens. _(both green.)_
- [x] **Verify:** signed ingest → delivered; status shows the attempt; 413 oversize; 401/400 bad sig/timestamp; duplicate `X-Idempotency-Key` returns the same `webhook_id` without re-delivering; internal-IP target blocked. _(all verified.)_

## Slice 4 — Async pipeline: Redis queue + leased worker pool + graceful shutdown  `AFK`
**Blocked by:** 3 · **Plan refs:** §0, §1

- [x] `infra/queue`: `LPUSH`/`BRPOP` on `webhook:queue`; `ErrQueueEmpty` on timeout.
- [x] `worker/pool`: `errgroup.Group` + `SetLimit(WORKER_CONCURRENCY)`; 2s BRPOP; `Process` **never returns non-nil on delivery failure** (§1 rule 1); drain ctx via `context.WithoutCancel` + `DRAIN_TIMEOUT` (§1 rule 2).
- [x] **Visibility-lease claim**: on dequeue the worker stamps `next_retry_at = now()+VISIBILITY_TIMEOUT WHERE id=$1 AND status='pending' AND next_retry_at <= now()` before the HTTP call. _(in Deliverer.Process via ClaimPending; 0 rows ⇒ skip duplicate dequeue.)_
- [x] Delivery **re-checks `sub.IsActive`** at delivery time → terminal `final_failure` `"subscription deactivated"` with `in_dlq=FALSE` if deactivated, so neither the direct path nor reconciler publishes an intentional drop.
- [x] `SYNC_DELIVERY=false` (default): ingest `LPUSH`es + returns `202 Accepted`. _(verified: 202 → async delivery → success.)_
- [x] `internal/lifecycle` supervisor over HTTP and background processes; SIGTERM stops dequeue, drains in-flight within `DRAIN_TIMEOUT`. _(Any process exit cancels peers; bounded HTTP shutdown and pool drain are unit-tested.)_
- [x] `GET /api/v1/ready` upgraded to **worker-aware** readiness (pool running). _(verified: worker:ok.)_
- [x] **Verify:** ingest → 202 → async delivery; SIGTERM drains in-flight instead of aborting. _(async verified; drain pattern unit-tested in Slice 11.)_

## Slice 5 — Durable pending-row lifecycle: retry scheduler + lease-based recovery  `AFK`
**Blocked by:** 4 · **Plan refs:** §0, §1, §2, §4b

*(Merged: retry scheduler + orphan recovery — both govern the `pending`-row lifecycle; the recovery test needs retry rows to exercise the §0 "recovery catches lost retries too" guarantee.)*

**Retry scheduler:**
- [x] `infra/scheduler`: `ZADD webhook:retry:schedule`; atomic Lua claim script (`ZRANGEBYSCORE` + `ZREM` + `LPUSH`, `LIMIT 0 100`). _(sub-second float scores — integer `Unix()` truncation moved rows ~1s early and the claim predicate then rejected them, stalling retries until recovery; fixed.)_
- [x] `worker/retry` (RetrySchedulerWorker) goroutine polls due entries via the Lua script and re-enqueues. _(1s tick.)_
- [x] `retryDelay(attempt)`: `RETRY_BASE_DELAY` × 3^(n-1), ±20% jitter (`math/rand/v2`), `RETRY_MAX_DELAY` cap. Default 10/30/90/270s.
- [x] Failure lifecycle is **one DB transaction**: CAS prev row → `failed_attempt` (0 rows ⇒ abort), INSERT new `pending` row (`attempt+1`, `next_retry_at`), COMMIT, **then** `ZADD` the new row's id (post-commit). _(FailAndScheduleRetry + INSERT…SELECT successor.)_
- [x] **5 total attempts** (1 + 4) then `final_failure`. _(verified: 4 failed_attempt + final_failure.)_

**Lease-based orphan recovery:**
- [x] `worker/recovery`: startup + periodic (`RECOVERY_SCAN_INTERVAL`) scan drains keyset pages until fewer than the configured batch remain; it re-enqueues due `pending` rows while live leases stay hidden. A `next_retry_at IS NULL` row older than `ORPHAN_THRESHOLD` is conditionally rearmed first, so concurrent scanners cannot both claim it. _(plus deliverer self-heals an early-moved row via reschedule-on-not-due.)_
- [x] testify tests assert an orphaned `pending` **retry** row is reclaimed, more than one batch drains in one scan, and only threshold-old lease-less rows are rearmed. _(integration tests; leased in-flight row excluded.)_

- [x] **Verify:** failing target → `failed_attempt` rows + scheduled retries + `final_failure` after the 5th; a lost-enqueue pending row is reclaimed. _(retry chain + recovery both verified; concurrent-CAS single-transition test is a Slice 11 high-risk test.)_

## Slice 6 — DLQ + atomic replay/ack + ACK-gated cleanup + reconciler  `AFK`
**Blocked by:** 5 · **Plan refs:** Advanced Features §1

- [x] On `final_failure`: set `in_dlq = TRUE` **in the same DB transaction** as the status update; **after commit** best-effort `LPUSH webhook:dlq <row id>`. _(single UPDATE sets status+in_dlq atomically; post-commit LPUSH.)_
- [x] **Atomic claim** for both ack paths: `UPDATE … SET in_dlq=FALSE WHERE subscription_id=$1 AND webhook_id=$2 AND in_dlq=TRUE RETURNING id, replay_number` — one concurrent caller wins; others get 0 rows → 404. _(verified: concurrent replay → one 202/one 404.)_
- [x] `POST /api/v1/subscriptions/{id}/replay/{webhook_id}` (admin auth): in the claim's txn, insert a fresh `pending` row **starting a new chain** (`replay_number = claimed+1`, `attempt_number = 1`, `next_retry_at = now()`) under the same `webhook_id`; after commit enqueue + `LREM`. _(verified: new chain replay_number=1, old LREM'd.)_
- [x] `POST /api/v1/subscriptions/{id}/dlq/{webhook_id}/ack` (admin auth): the atomic claim *is* the ack; after commit `LREM`; 404 if already reaped. _(verified: 200 then 404.)_
- [x] `worker/cleanup`: **sole retention-driven row deleter** (also prunes `ingest_idempotency` past `IDEMPOTENCY_TTL`; the lone exception is the explicit subscription-delete cascade) — `DELETE … WHERE created_at < retention AND NOT in_dlq AND status <> 'pending'`. _(never-deletes-pending is a Slice 11 high-risk integration test.)_
- [x] `worker/cleanup` (DLQReconciler, flag-only, no deletes): re-`LPUSH` `in_dlq=TRUE` rows missing from LIST; `LREM` any LIST id failing `WHERE id=$1 AND in_dlq=TRUE` (row gone **or** already acked); `DLQ_MAX_AGE` force-ack with logged warning. _(verified: bogus LIST id healed.)_
- [x] **Verify:** `final_failure` → DLQ; replay starts a new chain (attempt 1) + clears flag; concurrent replays produce exactly one new chain; ack clears flag; reconciler heals a manual LIST↔flag divergence. _(all verified; cleanup-never-pending → Slice 11.)_

## Slice 7 — Resilience: circuit breaker + rate limiter + per-sub concurrency  `AFK`
**Blocked by:** 5 · **Plan refs:** §3, §4, Advanced Features §3

*(Merged: all three protect the deliverer/ingest from a misbehaving target or noisy neighbor. Blocked by 5 because the non-blocking concurrency requeue uses `ZADD webhook:retry:schedule`, introduced in Slice 5.)*

**Circuit breaker (per target URL):**
- [x] `infra/breaker`: Closed→Open→Half-Open FSM with atomic `state`/`failures`/`lastOpenedAt` + `probing atomic.Bool` + **`generation` counter**.
- [x] `Allow()` returns `(bool, token)`; admits **exactly one** probe after `BREAKER_RESET_TIMEOUT`; `RecordSuccess(token)`/`RecordFailure(token)` **ignore stale tokens** and only the half-open probe closes/reopens.
- [x] Per-URL breaker registry; deliverer passes the token back. _(breakerReg + SetBreakerRegistry; token threaded through Process.)_
- [x] **Breaker-denial lifecycle** (plan §3): on `Allow()`=false the deliverer makes **no HTTP call and consumes no attempt** — reschedules the **same `pending` row** (`RescheduleSamePending` + post-commit `ZADD`). Acquired AFTER the concurrency permit so a half-open probe token is never abandoned.
- [x] testify test: threshold opens; single probe in half-open; probe success closes; **a stale closed-era success does not close an Open breaker**. _(+ stale-failure-ignored.)_

**Rate limiter + concurrency:**
- [x] `infra/ratelimit`: **mutex-only** token bucket, fixed-point (×1000) refill; keyed by sub id; `RATE_LIMIT_PER_SEC`/`RATE_LIMIT_BURST` → `429` when exhausted. _(verified: burst 2 → 202×2 then 429, refills.)_
- [x] Per-subscription concurrency semaphore (`PER_SUB_CONCURRENCY`) acquired **non-blockingly**: at limit ⇒ **requeue with small back-off (no attempt consumed)** rather than parking the worker.
- [x] **Bounded registries**: buckets, semaphores, breakers share one eviction rule — idle-`REGISTRY_IDLE_TTL` sweep + evict-on-subscription-delete. _(generic registry.Registry[T] + sweep goroutine; Remove on delete.)_
- [x] testify test: bucket allows a burst then blocks, and refills over time. _(+ registry get-once/remove/sweep tests.)_

## Slice 8 — Cache coherency: read-through + secret rotation + auto-disable  `AFK`
**Blocked by:** 5 · **Plan refs:** §5, Advanced Features §2, Advanced Features §5

*(Merged: the cache is the prerequisite for rotation + auto-disable, and the Adv §5 eviction matrix is one coherent unit. Build the cache, then wire every write path — including the Slice 2 CRUD paths — to evict it.)*

**Read-through cache:**
- [x] `internal/subscription`: read-through `subscription:{uuid}` (JSON, `CACHE_TTL`); ingest/delivery read through the state boundary, which degrades to PostgreSQL on Redis error.
- [x] Subscription CRUD and delivery outcomes own cache invalidation; delete and auto-disable retire rate/semaphore state. HTTP handlers and workers do not coordinate those mechanisms.

**Secret rotation + dual-key grace:**
- [x] `POST /api/v1/subscriptions/{id}/rotate-secret` (admin auth): new key, current → `previous_secret_key`, set `secret_rotated_at`; **evict cache**; return the new plaintext once. _(verified: new secret active immediately.)_
- [x] Ingest verification tries current then previous within `SECRET_GRACE_WINDOW`; outbound signing always uses the current (post-eviction) secret. _(verified: old secret accepted within grace; wrong → 401.)_

**Automatic endpoint disablement:**
- [x] On `final_failure`: `consecutive_failures + 1` (txn); at `AUTO_DISABLE_THRESHOLD`, also `is_active = FALSE` same statement + **evict cache**. _(FinalizeFailure: final_failure+in_dlq+counter in one tx; verified disable at 2.)_
- [x] On `success`: reset `consecutive_failures = 0` (only when non-zero). _(folded into MarkSuccess tx.)_
- [x] **Verify:** post-rotation old-secret sigs accepted within grace; threshold consecutive `final_failure`s disable the sub and ingest refuses it (403); no stale-TTL window after any write. _(all verified.)_

## Slice 9 — Observability: /monitor, expvar, latency histogram, pprof  `AFK`
**Blocked by:** 7 · **Plan refs:** §monitor response shape, Phase 5

- [x] `expvar` counters/gauges: `delivery_metrics` by status, `queue_depth`, `dead_letter_queue_depth`. _(webhook_delivery published at /debug/vars; depths + status counts in /monitor.)_
- [x] Bounded, mutex-guarded log-linear latency histogram → `p50`/`p95`/`p99` at scrape (no Prometheus dep). _(fixed Fibonacci-spaced ms buckets; unit-tested.)_
- [x] `GET /api/v1/monitor` (admin auth) returns the plan's JSON shape (uptime, delivery_metrics, queue depths, `circuit_breakers` states, delivery_latency_ms). _(verified shape.)_
- [x] `net/http/pprof` at `/api/v1/debug/pprof/` gated by `ENABLE_PPROF=true` **and** admin auth. _(verified: 200 auth+flag / 401 no-auth / 404 no-flag.)_
- [x] slog request-id propagated via context across API + worker layers. _(API: request_id context logger; worker: delivery_id/webhook_id correlation on async tasks.)_

## Slice 10 — OpenAPI 3.1 spec + interactive Swagger UI  `AFK`
**Blocked by:** 8 · **Plan refs:** Phase 3, API surface

- [x] Hand-authored OpenAPI 3.1 spec served at `GET /api/v1/openapi.json` (public). _(46KB, 14 paths/17 ops, embedded via //go:embed; Subscription schema secret-redacted.)_
- [x] `GET /api/v1/docs` — interactive Swagger UI (CDN swagger-ui pointed at the spec). _(swagger-ui-dist@5; verified loads, public.)_
- [x] Spec covers every `/api/v1` endpoint + the `Webhook-Timestamp` / optional `X-Idempotency-Key` headers, the `Bearer` admin scheme, rotate/replay/ack, and cursor params. _(all present; bearerAuth scheme + HMAC header params.)_

## Slice 11 — Test & lint hardening (incl. high-risk failure tests)  `AFK`
**Blocked by:** 9 · **Plan refs:** Phase 5

- [x] `.golangci.yml`; `make lint` and `go vet ./...` clean. _(golangci-lint v2.12.2: 0 issues with & without integration tag; go vet clean.)_
- [x] testify unit tests: signature (trailing-token reject; **idempotency-key-bound message** verifies, and a request with a mutated key fails), safedial, breaker (incl. stale-success), ratelimit, worker pool. _(+ registry, metrics/histogram.)_
- [x] **High-risk failure tests** (the claims most likely to be wrong): cleanup **never deletes `pending`**; concurrent replay/ack yields exactly one new chain; **concurrent same-key ingest inserts exactly one delivery row**; **concurrent duplicate dequeue claims the row only once**; crash-after-commit-before-enqueue is reclaimed by the lease scan (first attempt **and** retry); breaker stale-token success is ignored. _(all green as integration tests; pool also: processes-all / failure-doesn't-tear-down / drains-in-flight.)_
- [x] `go build ./...` + `make test` green. _(build OK; all unit + integration tests pass.)_

## Slice 12 — Dockerfile + README/ADR + curl samples  `AFK`
**Blocked by:** 10 · **Plan refs:** Phase 5/6

- [x] `Dockerfile` multi-stage: Go builder → `gcr.io/distroless/static:nonroot`. _(at repo root; built + ran — 24.8MB, migrates on startup, /health 200; .dockerignore keeps context small.)_
- [x] README: ADR / architecture, setup, **delivery-guarantee statement** (at-least-once, no ordering, dedupe on `X-Webhook-ID`), auth model + per-instance-limits note. _(454-line README; routes/config verified against code.)_
- [x] README: **client migration note** for the signature + required `Webhook-Timestamp` (+ optional `X-Idempotency-Key`).
- [x] README: per-endpoint curl samples (incl. `Bearer` admin token); port `generate_signature.py` to compute the new HMAC. _(scripts/sign_webhook.py — verified byte-identical to the openssl/Go scheme.)_

## Slice 13 — Deployment + cost estimate  `HITL`
**Blocked by:** 12 · **Plan refs:** Phase 6

- [x] Deploy config ready: `railway.toml` / `railway.json` (`DOCKERFILE` builder, healthcheck `/api/v1/health`) + `Procfile`; migrate-on-startup verified in-container; deploy guide in README. **HITL:** the live deploy + `ADMIN_API_KEY` set + connecting the repo needs the user's Railway account.
- [x] README: monthly **cost estimate** for 24×7 + ~5,000 webhooks/day @ 1.2 attempts. _(~$11–18/mo breakdown; request volume negligible vs idle baseline.)_
- [x] README: assumptions + library/AI-tool credits. _(live URL is a placeholder for the user to fill post-deploy — the one genuinely HITL item.)_
