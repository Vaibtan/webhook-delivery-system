# Webhook Delivery System Architecture

This document explains the implemented runtime and the invariants that keep delivery state recoverable. Configuration and endpoint usage live in the root [`README.md`](../README.md); the API schema is served by the application at `/api/v1/openapi.json`.

The editable system diagram is [`architecture/webhook-system-design.excalidraw`](architecture/webhook-system-design.excalidraw). The reviewed render embedded in the README is [`architecture/Screenshot 2026-07-18 222303.png`](architecture/Screenshot%202026-07-18%20222303.png).

## System boundary

One process runs the HTTP API and six supervised background processes:

1. Worker pool
2. Retry scheduler poller
3. Orphan recovery scanner
4. Retention and idempotency cleanup
5. DLQ reconciler
6. In-memory registry sweeper

`cmd/server` is the composition root. It loads validated configuration, applies migrations, connects to PostgreSQL and Redis, constructs concrete adapters, and registers already-built processes with `internal/lifecycle.Supervisor`.

## Module ownership

| Module | Owns | Does not own |
|---|---|---|
| `internal/api` | HTTP contracts, authentication middleware, request validation, response projection | SQL, Redis commands, worker lifecycle |
| `internal/domain` | Entities, statuses, value validation, sentinel errors | Infrastructure interfaces or orchestration |
| `internal/store` | PostgreSQL queries and transaction boundaries | Cache invalidation, queue polling, HTTP delivery |
| `internal/subscription` | Subscription reads/writes, Redis cache coherence, delivery counter consequences, rate buckets, semaphores | HTTP routing or delivery transport |
| `internal/worker` | Claims, outbound attempts, retry decisions, recovery, cleanup, DLQ repair | Global process supervision |
| `internal/lifecycle` | Peer cancellation, HTTP shutdown, worker draining, fatal-error attribution | Construction of application dependencies |
| `internal/infra` | Redis queue/scheduler/DLQ adapters, breaker, HMAC, metrics, SSRF-safe transport | Business transactions |

Interfaces are declared by the consuming package and contain only the methods that consumer needs. Concrete stores and Redis adapters satisfy those interfaces implicitly.

## Authoritative and derived state

| State | Authority | Derived copy/index | Repair mechanism |
|---|---|---|---|
| Subscriptions | PostgreSQL `subscriptions` | Redis `subscription:{id}` | Read-through refill; dirty-key bypass after failed invalidation |
| Delivery attempts | PostgreSQL `delivery_logs` | Redis `webhook:queue` | Recovery scans due `pending` rows |
| Retry due times | PostgreSQL `next_retry_at` | Redis `webhook:retry:schedule` | Recovery re-enqueues rows whose due time or lease has expired |
| Dead-letter membership | PostgreSQL `in_dlq` | Redis `webhook:dlq` | DLQ reconciler repairs both missing and stale list entries |
| Inbound idempotency | PostgreSQL `ingest_idempotency` | None | Unique `(subscription_id, idempotency_key)` constraint |

Redis loss can delay work but does not erase accepted delivery state. PostgreSQL loss is outside the guarantees of the application and must be addressed with database backups and managed-service durability.

## Ingest flow

1. Load the subscription through `subscription.State`.
2. Reject inactive subscriptions.
3. Require `Webhook-Timestamp` and `X-Hub-Signature-256`; validate the optional idempotency key grammar.
4. Read at most 64 KiB and verify the timestamp window plus HMAC over the raw body.
5. Charge the per-subscription token bucket only after authentication succeeds.
6. Validate JSON and apply the subscription's exact event-type filter.
7. In one PostgreSQL transaction, claim the optional idempotency key and insert the initial `pending` delivery row.
8. In async mode, push the delivery-row ID to `webhook:queue`. A failed push is logged; recovery later finds the durable row.
9. In sync mode, run only the first attempt in-request. Successors and maintenance still use the background pipeline.

Repeated idempotency keys return the previously stored `webhook_id` and do not create or enqueue another attempt.

## Attempt and retry lifecycle

Each HTTP attempt has its own `delivery_logs` row. Lifecycle fields on that row are mutable, but retries never reuse it: a retry creates a new row with the same `webhook_id` and an incremented `attempt_number`.

| Status | Meaning | Next transition |
|---|---|---|
| `pending` | Durable work that is due, scheduled, or protected by a visibility lease | `success`, `failed_attempt`, or `final_failure` |
| `success` | Target returned a 2xx response | Terminal |
| `failed_attempt` | A real HTTP attempt failed and a successor row was inserted | Terminal for this row |
| `final_failure` | Attempt budget exhausted or delivery was intentionally dropped | DLQ when `in_dlq=true`; otherwise terminal |

### Claiming

Before making an HTTP request, a worker conditionally updates a due `pending` row and stamps `next_retry_at = NOW() + VISIBILITY_TIMEOUT`. That PostgreSQL CAS is the mutual-exclusion point. Duplicate Redis queue entries are harmless because only one worker can claim the due row.

### Retrying

When attempts remain, `FailAndScheduleRetry` performs two operations in one transaction:

1. CAS the current row from `pending` to `failed_attempt`.
2. Insert a successor `pending` row with the next attempt number and due time.

After commit, the successor ID is added to the retry ZSET. The poller uses one Lua script to remove due ZSET members and push them into the immediate queue atomically. Backoff is capped ×3 with ±20% jitter.

Breaker denial and semaphore saturation do not consume an attempt. They reschedule the same `pending` row because no outbound call occurred.

### Terminal failure and DLQ

The final failure transaction CASes the attempt to `final_failure`, sets `in_dlq=true`, increments the subscription's consecutive-failure counter, and disables the subscription when the threshold is reached. The post-commit Redis DLQ push is repairable from PostgreSQL.

Replay and acknowledgement atomically clear `in_dlq`. Replay also inserts a fresh `pending` row with `replay_number + 1` and `attempt_number = 1` in the same transaction.

## Subscription state consistency

`internal/subscription.State` is the semantic boundary for subscription data and its derived state:

- Redis reads and PostgreSQL mutations for the same subscription share a bounded lock stripe.
- A committed mutation invalidates Redis before releasing that stripe, closing the commit-to-evict race.
- If invalidation fails, the process marks the key dirty and bypasses Redis until a later repair or the old TTL expires.
- Successful deliveries evict only when they reset a non-zero failure counter.
- Terminal failures evict only when their CAS wins.
- Delete and auto-disable retire rate/concurrency state. Ordinary updates and secret rotation preserve active semaphore permits and token-bucket history.

The last rule prevents a management update from granting a fresh rate-limit burst or replacing a semaphore while deliveries still hold permits.

## Outbound delivery boundary

The deliverer:

1. Rechecks subscription activity after claiming the row.
2. Acquires a non-blocking per-subscription semaphore.
3. Consults the per-target circuit breaker.
4. Canonicalizes the stored JSON once and signs the exact bytes sent.
5. Sends through the hardened HTTP client.
6. Drains and closes the response body for connection reuse.
7. Records metrics and applies the PostgreSQL lifecycle transition.

The custom dialer rejects loopback, private, link-local, CGNAT, unspecified, multicast, and reserved addresses. It resolves and pins the vetted IP to prevent DNS rebinding, disables environment proxies, limits redirects, and rejects HTTPS-to-HTTP downgrade redirects.

## Shutdown and process ownership

`internal/lifecycle.Supervisor` runs HTTP and all background processes under derived cancellation. Any long-lived component exit cancels its peers, including an unexpected clean exit that would otherwise leave a partial service running.

On `SIGINT` or `SIGTERM`:

1. The shared context stops dequeue and periodic loops.
2. HTTP shutdown receives a detached context bounded by `DRAIN_TIMEOUT`.
3. The worker pool stops accepting queue entries.
4. In-flight deliveries use detached, individually bounded contexts and are awaited.
5. The supervisor returns only after every registered process exits.

## Security model

- Ingest is authenticated with each subscription's HMAC secret.
- Management, status, debug, and replay APIs use one static admin bearer token.
- An unset admin token fails closed with `503`; incorrect credentials return `401`.
- Subscription reads redact current and previous secret values. Create and rotate return the new plaintext once.
- Timestamp validation bounds replay of otherwise valid signed bodies.
- The optional idempotency key is signed and stored transactionally with the initial delivery.
- Inbound requests have a 1 MiB global cap and a 64 KiB ingest cap.
- Target URLs require HTTPS unless explicitly relaxed for local development.

The static admin key is an operational credential, not tenant isolation. A multi-tenant deployment needs scoped identity and authorization.

## Scaling constraints

The durable database and Redis protocols support multiple workers, but three controls are process-local:

- per-subscription token buckets
- per-subscription concurrency semaphores
- per-target circuit breakers

With `N` replicas, their effective limits multiply by `N`. Exact global limits require Redis-backed token buckets, leased distributed semaphores, and shared breaker state—or explicit per-replica partitioning.

The current deployment should also avoid concurrent migrate-on-startup replicas until migration ownership is separated into a deployment step.

## Deliberately deferred

| Concern | Current choice | Evolution trigger |
|---|---|---|
| Transactional outbox | Recovery repairs post-commit Redis write loss | Add an outbox relay if recovery latency or dual-write telemetry becomes unacceptable |
| Global fair scheduling | One Redis LIST plus per-subscription semaphores | Introduce per-subscription queues and weighted scheduling for stronger fairness |
| Distributed limits | Process-local controls | Move state to Redis before horizontal scaling |
| Tenant authorization | One admin bearer credential | Add scoped tokens, mTLS, or an identity provider for multi-tenant use |
| Wildcard event filters | Exact event-type matching | Add explicit prefix/glob semantics when required by clients |
