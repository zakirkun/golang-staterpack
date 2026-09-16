# Golang Starter Pack

Production-shaped Go service boilerplate: **Fiber** HTTP, **GORM/Postgres**,
**Redis**, **RabbitMQ**, and **LangChain Go** for LLM-backed features —
containerised with **Docker Compose** and deployable to **Kubernetes**.

The example domain is a `Task` that gets summarised asynchronously by an LLM,
which exercises every layer end to end: HTTP → service → repository → broker →
worker → LLM → cache invalidation.

## Stack

| Concern | Choice |
|---|---|
| HTTP | [Fiber v2](https://gofiber.io) |
| ORM / DB | [GORM](https://gorm.io) + Postgres 17 |
| Cache / rate limit | [go-redis v9](https://github.com/redis/go-redis) |
| Messaging | [amqp091-go](https://github.com/rabbitmq/amqp091-go) (RabbitMQ 4) |
| LLM | [LangChain Go](https://github.com/tmc/langchaingo) → DeepSeek V4.1 Flash (OpenAI-compatible) |
| Logging | stdlib `log/slog` (JSON in prod, text in dev) |

## Layout

```
cmd/api/                     entrypoint: flag parsing, wiring, graceful shutdown
internal/
  config/                    env-driven config with defaults + validation
  logger/                    slog facade
  model/                     GORM entities
  repository/                persistence (interface + GORM impl), migrations
  service/                   use cases; depends on interfaces, not infra
  broker/                    RabbitMQ publisher + consumer (retry + DLQ) + dedupe
  event/                     message contract shared by producers/consumers
  llm/                       LangChain Go client behind a small interface
  worker/                    background consumers (LLM summariser)
  reconciler/                background sweep rescuing stranded tasks
  storage/                   Postgres + Redis constructors
  transport/http/            Fiber router, middleware, handlers
deploy/k8s/                  namespace, configmap, secret, deployment, svc, hpa, pdb, ingress, job
scripts/                     manifest + config validation helpers
```

## Quick start (Docker Compose)

```bash
cp .env.example .env          # then set LLM_API_KEY, or leave blank to disable summaries
make up                       # build and start api + postgres + redis + rabbitmq
make logs
```

The API listens on `http://localhost:8080`.

```bash
# liveness / readiness
curl localhost:8080/health/live
curl localhost:8080/health/ready

# create a task — queues async LLM summarisation
curl -X POST localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"title":"Ship the starter pack","description":"Wire Fiber, GORM, Redis, RabbitMQ and LangChain Go."}'

# after a moment the worker fills in `summary` and flips status to completed
curl localhost:8080/api/v1/tasks/<id>

curl "localhost:8080/api/v1/tasks?limit=10&offset=0"
curl -X DELETE localhost:8080/api/v1/tasks/<id>
```

RabbitMQ management UI: <http://localhost:15672> (`guest` / `guest`).

## Local development (no containers for the app)

```bash
cp .env.example .env
docker compose up -d postgres redis rabbitmq   # infra only
make run                                       # or: go run ./cmd/api
```

```bash
make check        # gofmt + go vet + go test -race
make test-cover   # writes coverage.txt
```

## API

| Method | Path | Description |
|---|---|---|
| `GET` | `/health/live` | Process is up. Never touches backends. |
| `GET` | `/health/ready` | Pings Postgres and Redis; `503` when degraded. |
| `POST` | `/api/v1/tasks` | Create a task, publishes `task.created`. |
| `GET` | `/api/v1/tasks` | Paginated list (`limit`, `offset`). |
| `GET` | `/api/v1/tasks/:id` | Fetch one (Redis-cached, 5 min TTL). |
| `DELETE` | `/api/v1/tasks/:id` | Delete and invalidate cache. |

Errors use one envelope:

```json
{ "error": "resource not found", "code": "not_found", "trace_id": "..." }
```

A task moves through `pending` → `processing` → `completed`, or `pending` →
`processing` → `pending` on a transient failure (eligible for retry). A task
whose processing fails terminally can be marked `failed` via
`TaskService.MarkFailed`.

## Design decisions worth knowing

**Liveness vs readiness are separated on purpose.** `/health/live` touches
nothing, so a slow database can never cause the kubelet to restart a healthy
pod. `/health/ready` checks Postgres and Redis, so traffic only arrives once the
dependency set is actually usable.

**The cache fails open; the rate limiter fails open.** If Redis is down,
`Get` falls through to Postgres and the limiter allows the request. A cache
outage should degrade latency, not availability.

**Publishing does not fail the request.** `Create` commits to Postgres *then*
publishes. If the publish fails the task still exists and the error is logged —
losing a summary is better than losing a write. The reconciler (below) is what
rescues those tasks; without it they would sit in `pending` forever, silently.

**Consumers retry with backoff, then dead-letter.** A failed message is
republished with an incremented `x-retry-count` header (1s, 2s, 4s) up to
`MaxRetries`, then nacked to the DLQ. Undecodable payloads are acked and dropped
rather than retried, since they can never succeed.

**The reconciler rescues stranded work.** `internal/reconciler` runs a ticker
that finds tasks stuck in `pending` longer than `RECONCILER_PENDING_GRACE` or in
`processing` longer than `RECONCILER_PROCESSING_TIMEOUT`, and republishes
`task.created` for each. A task found in `processing` is reset to `pending`
first — otherwise the redelivered event would fail the pending-only claim and
the task would stay stuck, which is the bug the sweep exists to fix.

**Duplicate suppression is Redis-backed and therefore best-effort.** RabbitMQ is
at-least-once, so a redelivered message carries the same envelope ID; the worker
claims it with `SETNX processed:event:<id>` (TTL `DEDUPE_TTL`) and drops the
duplicate. This is *not* a durable guarantee: a Redis flush, eviction, or
failover silently restores the ability to double-process, and each duplicate is
a second paid LLM call. `broker.Dedupe` fails open (processes anyway, with a
warning) when Redis is unreachable. **If duplicate side effects are
unacceptable, replace it with a `processed_events` table in Postgres** — the
interface is deliberately narrow so that swap touches one call site.

**Two guards, not one.** Dedupe stops the same *event* running twice;
`BeginProcessing` stops two *workers* owning the same task. The latter is a
conditional `UPDATE ... WHERE status = 'pending'`, so exactly one winner is
possible even if two deliveries slip past the dedupe guard.

**Migrations run as a Job, not at boot.** The Deployment sets
`DB_AUTO_MIGRATE=false` so replicas never race each other applying DDL;
`api-migrate` runs the same binary with `--migrate-only`:

```bash
kubectl apply -f deploy/k8s/00-namespace.yaml
kubectl apply -f deploy/k8s/01-configmap.yaml
kubectl apply -f deploy/k8s/02-secret.yaml
make k8s-migrate      # applies the Job and waits for completion
kubectl apply -f deploy/k8s/10-deployment.yaml
kubectl apply -f deploy/k8s/11-service.yaml
kubectl apply -f deploy/k8s/12-hpa.yaml -f deploy/k8s/14-pdb.yaml
```

`make k8s-apply` does the common subset in one go.

**The LLM degrades gracefully.** With no `LLM_API_KEY` the app still boots:
CRUD works, and the summariser becomes a no-op instead of flooding the DLQ.
- **The reconciler refuses a misconfiguration.** `RECONCILER_PENDING_GRACE` must
  be `>= RECONCILER_INTERVAL`; otherwise the sweep could outrun normal queue
  latency and republish work that is merely in flight, causing duplicate LLM
  calls. The app fails to start rather than do that.


## Configuration

All configuration is environment-driven with working defaults; see
`.env.example` for the full list with comments. In production `LLM_API_KEY` is
required, and `RECONCILER_PENDING_GRACE` must be at least
`RECONCILER_INTERVAL` — both validated at boot.

## Notes on the LLM client

DeepSeek V4.1 Flash is served over an **OpenAI-compatible** endpoint, so the
boilerplate uses `langchaingo/llms/openai` with `WithBaseURL` and
`WithModel` pointed at `https://api.deepseek.com/v1` and `deepseek-v4.1-flash`
rather than a DeepSeek-specific SDK. Swapping providers means changing
`LLM_BASE_URL` / `LLM_MODEL`; swapping to a non-OpenAI-compatible provider means
implementing `llm.Client` (`Complete` + `Summarize` + `Model`) — three methods.

## Adding a feature

1. `internal/model/` — add the entity and register it in `repository.AutoMigrate`.
2. `internal/repository/` — add an interface and GORM implementation.
3. `internal/service/` — add the use case, depending on the repository interface.
4. `internal/transport/http/` — add a handler, register routes in `router.go`.
5. If it is async: add the payload to `internal/event`, a handler in
   `internal/worker`, and register the consumer in `cmd/api/main.go`. Make the
   handler idempotent (claim via `broker.Dedupe`) and give the entity a status
   the reconciler can query, or a lost message will strand it silently.

## Verification status

`go build ./...`, `go vet ./...` and `go test -race ./...` pass. Kubernetes and
Compose manifests are validated for parsing and for config-key consistency
against the code (`scripts/`). A live `docker compose up` end-to-end run was
**not** executed in the authoring environment because the Docker daemon was
unavailable — build the image and bring the stack up once before trusting it.
