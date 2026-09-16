# Feature Design: Reconciler + Duplicate Suppression

Built and merged (commit `0f9b40e`). This file is the design record.

## Problem

The async path made a promise the code did not keep.

`TaskService.Create` commits to Postgres, then publishes `task.created`. If the
publish fails, the task is still persisted and the error is only logged — the
deliberate choice that losing a summary beats losing a write. The README then
claimed *"a reconciliation sweep (or the caller's retry)"* would catch these.

**No sweep existed.** A lost publish meant the task sat in `pending` forever,
silently. Nothing detected it, nothing retried it, nothing alerted.

Two adjacent gaps:

- **Duplicate processing.** RabbitMQ is at-least-once. A retried message carried
  the same body and no dedup key, so a transient failure after a successful LLM
  call burned a second paid call.
- **Dead vocabulary.** `TaskStatusProcessing` was defined but never written, so
  nothing could distinguish queued / in-flight / abandoned.

## Solution

### 1. Reconciler (`internal/reconciler`)

A ticker sweeps for abandoned work and republishes it:

| Condition | Meaning |
|---|---|
| `status = 'pending' AND created_at < now - PendingGrace` | publish was lost |
| `status = 'processing' AND updated_at < now - ProcessingTimeout` | worker died mid-flight |

Sweeps once immediately on start (not just on the first tick), so a restart does
not wait a full interval. Per-row republish failures are logged and skipped
rather than aborting the batch. Query failures log and retry next tick instead
of killing the loop.

**The subtle part:** a task found in `processing` must be reset to `pending`
*before* its event is republished. Otherwise the redelivered message reaches
`BeginProcessing`, fails its `status = 'pending'` condition, and is silently
skipped — leaving the task stuck forever, which is the exact bug the reconciler
exists to fix. `RepublishCreated` does the reset.

### 2. Duplicate suppression (`broker.Dedupe`)

Redis `SETNX processed:event:<envelope-id>` with a TTL. Duplicates are dropped.

**This is best-effort, not durable.** A Redis flush, eviction, or failover
silently restores double-processing. The guard also **fails open** when Redis is
unreachable — a cache outage must not stop the queue draining, even though that
re-exposes the risk. Both behaviours are documented in the package, the README,
and `.env.example`, with a note that a Postgres `processed_events` table is the
durable alternative and touches one call site.

### 3. Two guards, not one

These solve different problems and neither subsumes the other:

- `Dedupe` — stops the same **event** running twice.
- `BeginProcessing` — stops two **workers** owning the same task, via a
  conditional `UPDATE ... WHERE status = 'pending'`.

If a duplicate slips past the dedupe guard (Redis was down), the conditional
update still means exactly one worker wins.

## Configuration

| Var | Default | Note |
|---|---|---|
| `RECONCILER_ENABLED` | `true` | |
| `RECONCILER_INTERVAL` | `1m` | |
| `RECONCILER_PENDING_GRACE` | `5m` | must be `>= INTERVAL` |
| `RECONCILER_PROCESSING_TIMEOUT` | `10m` | |
| `RECONCILER_BATCH_SIZE` | `100` | bounds the republish burst |
| `DEDUPE_TTL` | `24h` | must exceed the longest retry window |

Startup fails if `PENDING_GRACE < INTERVAL`: the sweep would outrun normal queue
latency and republish work that is merely in flight, duplicating LLM calls.

## Tests

- Reconciler against a fake store with an injected clock — including the
  immediate-first-sweep case and per-row-error-continues.
- `Dedupe` fail-open behaviour against an unreachable Redis (a `127.0.0.1:0`
  client), plus the empty-ID case.
- Service paths: claim-once, reject-completed, reset-to-pending, and
  republish-resets-processing.
- A dry-run GORM test pinning that `FindStale`'s nested `Or` is grouped in
  parentheses. Verified SQL:
  ```sql
  SELECT * FROM "tasks"
   WHERE ((status = $1 AND created_at < $2) OR (status = $3 AND updated_at < $4))
     AND "tasks"."deleted_at" IS NULL
   ORDER BY created_at ASC LIMIT $5
  ```
  If GORM's grouping ever changed, the `OR` could bind wide enough to match
  every row.

## Known limitations

- The dedupe guard's durability is bounded by Redis, as above.
- The reconciler is per-replica. With N replicas, N sweeps may race to republish
  the same task; the dedupe guard and conditional claim absorb the duplicates,
  but a `SELECT ... FOR UPDATE SKIP LOCKED` claim would be cleaner at scale.
- No metrics yet: `METRICS_ENABLED` is parsed but unused, so rescue counts and
  DLQ depth are only visible in logs. `Reconciler.OnSweep` is the hook for
  wiring a counter when Prometheus is added.
