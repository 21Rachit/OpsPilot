# OpsPilot

An incident-tracking REST API written in Go, built step by step as a learning project.

OpsPilot will eventually become an AI-powered incident response copilot, with Kafka, RAG and an MCP server. It currently covers **Step 3**:

- CRUD operations on incidents, stored in **PostgreSQL**
- A **Redis** cache in front of `GET /incidents/{id}`
- An **alert webhook** (`POST /alerts`) that de-duplicates repeated alerts with Redis
- Structured logging and graceful shutdown

Both backing services are optional. Without `DATABASE_URL` the server uses an in-memory store, and without `REDIS_URL` caching and de-duplication are switched off.

## Prerequisites

- Go 1.26.5 or newer (see `go.mod`)
- Docker, to run PostgreSQL and Redis
- `curl`, for calling the API

## Quick start

```bash
# 1. start PostgreSQL 17 (data kept in the opspilot-pgdata volume)
docker run -d --name opspilot-postgres \
  -e POSTGRES_USER=opspilot \
  -e POSTGRES_PASSWORD=opspilot \
  -e POSTGRES_DB=opspilot \
  -p 5432:5432 \
  -v opspilot-pgdata:/var/lib/postgresql/data \
  postgres:17

# 2. start Redis 7
docker run -d --name opspilot-redis -p 6379:6379 redis:7

# 3. create the incidents table (run once)
docker exec -i opspilot-postgres psql -U opspilot -d opspilot < migrations/001_create_incidents.sql

# 4. point the server at both
export DATABASE_URL="postgres://opspilot:opspilot@localhost:5432/opspilot?sslmode=disable"
export REDIS_URL="redis://localhost:6379/0"

# 5. run the server
go run ./cmd/server
```

The server listens on **http://localhost:8087**. On startup you should see:

```
level=INFO msg="connected to postgres" host=localhost port=5432 database=opspilot user=opspilot max_conns=14
level=INFO msg="connected to redis" addr=localhost:6379 db=0 pool_size=140 dial_timeout=500ms read_timeout=200ms
level=INFO msg="config loaded" port=8087 log_level=INFO postgres_enabled=true redis_enabled=true cache_ttl=1m0s dedup_window=5m0s
level=INFO msg="OpsPilot starting" addr=http://localhost:8087
```

To manage the containers afterwards:

```bash
docker stop opspilot-postgres opspilot-redis    # stop (Postgres data is kept)
docker start opspilot-postgres opspilot-redis   # start again
```

## Configuration

All settings come from environment variables.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(empty)* | PostgreSQL connection string, `postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=disable`. Empty means use the in-memory store |
| `REDIS_URL` | *(empty)* | Redis connection string, `redis://HOST:PORT/DB`. Empty means no caching and no alert de-duplication |
| `PORT` | `8087` | HTTP listen port |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. `debug` also prints SQL queries, cache hits and misses, and dedup steps |
| `CACHE_TTL` | `60s` | How long an incident stays in the Redis cache |
| `DEDUP_WINDOW` | `5m` | How long repeats of the same alert are treated as duplicates |

Durations use Go syntax: `30s`, `5m`, `1h`. An invalid value (for example `CACHE_TTL=abc`) stops the server at startup with an `invalid config` error.

## Build and checks

```bash
go mod download   # download dependencies (gorilla/mux, pgx, go-redis)
go build ./...    # compile every package
go vet ./...      # static checks
```

## Testing

There are no automated tests yet. Use the curl commands below to test by hand.

To see every command OpsPilot sends to Redis while you test, run this in a separate terminal:

```bash
docker exec -it opspilot-redis redis-cli MONITOR
```

## API

### Incidents

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| `POST` | `/incidents` | `title`, `severity` | `201` created incident | `400` |
| `GET` | `/incidents` | none | `200` list of incidents, sorted by ID | |
| `GET` | `/incidents/{id}` | none | `200` the incident (served from Redis when cached) | `404` |
| `PUT` | `/incidents/{id}` | `title`, `severity`, `status` | `200` updated incident | `400`, `404` |
| `DELETE` | `/incidents/{id}` | none | `204` no content | `404` |

### Alerts (for monitoring systems)

| Method | Path | Body | New problem | Repeat within the window | Errors |
|---|---|---|---|---|---|
| `POST` | `/alerts` | `fingerprint`, `title`, `severity` | `201` `{"duplicate":false,"incident_id":N}` | `200` `{"duplicate":true,"incident_id":N}` | `400`, `500` |

`/alerts` is a webhook for tools like Prometheus Alertmanager, not for people. Monitoring tools re-send the same alert for as long as a problem lasts. The `fingerprint` identifies "the same problem" (for example `dns-latency-us-east`), so repeats within `DEDUP_WINDOW` return the existing incident instead of creating a new one.

A duplicate can come back without `incident_id` if the first alert is still being created at that moment.

The payload is a simplified format for learning. Real Alertmanager payloads are not supported yet.

**Validation rules** (incidents are also checked by `CHECK` constraints in Postgres)

- `title`: required
- `severity`: one of `low`, `medium`, `high`, `critical`
- `status`: one of `open`, `acknowledged`, `resolved`. New incidents always start as `open`.
- `fingerprint` (alerts only): required
- `{id}` must be numeric. Non-numeric IDs never reach a handler: gorilla/mux answers with a plain-text `404`.

Errors come back as JSON: `{"error": "title is required"}`. A database failure returns `500`.

### Example requests

```bash
BASE=localhost:8087

# create
curl -i -X POST $BASE/incidents -H "Content-Type: application/json" \
  -d '{"title":"DNS latency high in us-east","severity":"high"}'

# list
curl -i $BASE/incidents

# get one (run twice in a row: the second is a cache hit)
curl -i $BASE/incidents/1

# update
curl -i -X PUT $BASE/incidents/1 -H "Content-Type: application/json" \
  -d '{"title":"DNS latency high in us-east","severity":"critical","status":"resolved"}'

# delete
curl -i -X DELETE $BASE/incidents/1

# send the same alert 3 times: 1 incident, 2 duplicates
for i in 1 2 3; do
  curl -s -X POST $BASE/alerts -H "Content-Type: application/json" \
    -d '{"fingerprint":"dns-latency-us-east","title":"DNS latency high","severity":"high"}'; echo
done
```

Example incident response:

```json
{
  "id": 1,
  "title": "DNS latency high in us-east",
  "severity": "high",
  "status": "open",
  "created_at": "2026-11-01T12:31:59.254+05:30"
}
```

## Caching

`CachedStore` (`internal/store/cached.go`) wraps the real store and has the same methods. This is the decorator pattern: the handlers don't know the cache exists.

| Operation | Cache behaviour |
|---|---|
| `GET /incidents/{id}` | **Cache-aside:** try Redis key `incident:{id}`. On a miss, read from the store and save a copy for `CACHE_TTL` |
| `PUT` / `DELETE` | Delete the cached copy after the database write, so readers never see stale data |
| `GET /incidents` | Not cached. Any create, update or delete would make a cached list stale |
| `POST /incidents` | Not cached |

**Failure behaviour:** if Redis is down or slow, requests skip the cache and go to the database (fail open). Short timeouts (500 ms to connect, 200 ms to read or write, 1 retry) keep a broken Redis from slowing requests down.

> Every write must go through the API. If you change a row directly in Postgres, the API keeps returning the old cached copy until `CACHE_TTL` expires. To clear it right away: `docker exec opspilot-redis redis-cli DEL incident:<id>`

## Alert de-duplication

`Deduper` (`internal/cache/dedup.go`) stores one Redis key per fingerprint: `alert:dedup:<fingerprint>`.

```
POST /alerts
  │
  ├─ Claim:  SET alert:dedup:<fp> 0 NX EX <window>
  │     ├─ key already exists → duplicate → 200 + incident ID stored in the key
  │     └─ key created        → this alert is first
  │
  ├─ Create the incident in the store
  │     └─ failed? → Release: DEL the key, so the sender's retry isn't treated as a duplicate → 500
  │
  └─ Attach: SET alert:dedup:<fp> <incident id> KEEPTTL → 201
```

- **`NX` makes the claim atomic.** If several identical alerts arrive at the same moment, exactly one creates an incident.
- **The value `0`** means "claimed, incident being created". `Attach` replaces it with the real ID.
- **`KEEPTTL`** changes the value without resetting the window.
- **When the window expires**, Redis deletes the key. The next alert for that fingerprint creates a new incident.
- **If Redis is down**, the alert still creates an incident (fail open): a duplicate is better than a lost alert.

Inspect a key:

```bash
docker exec opspilot-redis redis-cli GET alert:dedup:dns-latency-us-east   # incident ID
docker exec opspilot-redis redis-cli TTL alert:dedup:dns-latency-us-east   # seconds left (-2 = gone)
```

## Database

The schema lives in `migrations/001_create_incidents.sql`:

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | Primary key, generated by Postgres. Deleted IDs are never reused |
| `title` | `TEXT` | Required |
| `severity` | `TEXT` | `CHECK` limits it to the allowed values |
| `status` | `TEXT` | Defaults to `open`. `CHECK` limits it to the allowed values |
| `created_at` | `TIMESTAMPTZ` | Defaults to `now()` |

Open a SQL shell:

```bash
docker exec -it opspilot-postgres psql -U opspilot -d opspilot
```

```sql
SELECT * FROM incidents;
\d incidents   -- show the table definition
\q             -- quit
```

## Logging

The server uses Go's structured logger (`log/slog`) and writes text logs to stdout. Set the level with `LOG_LEVEL`.

| Source | Level | What it logs |
|---|---|---|
| `internal/middleware/logging.go` | INFO / WARN (4xx) / ERROR (5xx) | One line per request: method, path, status, duration. Includes 404 and 405 responses from gorilla/mux |
| `internal/handler/incident.go` | INFO / WARN / ERROR | Incident created, updated or deleted; why a request was rejected; database errors |
| `internal/handler/alert.go` | INFO / WARN / ERROR | Alert created an incident or was de-duplicated (with the fingerprint and incident ID); dedup unavailable; create failed |
| `internal/store/postgres.go` | INFO | Connected to Postgres (host, port, database, user, pool size) and pool closed. The password is never logged |
| `internal/store/postgres.go` | DEBUG | Every SQL statement with its arguments, duration and rows affected |
| `internal/store/cached.go` | DEBUG | `cache hit`, `cache miss`, `cache set` (with TTL), `cache invalidated` |
| `internal/store/cached.go` | WARN | Redis failed and the request fell back to the database |
| `internal/cache/dedup.go` | DEBUG | Fingerprint claimed, already claimed, incident attached, fingerprint released |
| `internal/cache/redis.go` | INFO | Connected to Redis (address, DB, pool size, timeouts). The URL is never logged, as it can contain a password |
| `internal/cache/redis.go` | WARN | go-redis's own messages (for example `connection refused`), prefixed with `redis client:` |
| `cmd/server/main.go` | INFO | Config loaded (including which backends are enabled), server start, graceful shutdown and how long it took |

Example with `LOG_LEVEL=debug`, for a cache miss followed by a cache hit:

```
level=DEBUG msg="cache miss" key=incident:3
level=DEBUG msg="sql Query" args=[3] commandTag="SELECT 1" sql="SELECT id, title, severity, status, created_at FROM incidents WHERE id = $1" time=1.376875ms
level=DEBUG msg="cache set" key=incident:3 ttl=1m0s
level=INFO msg=request method=GET path=/incidents/3 status=200 duration=6.237083ms
level=DEBUG msg="cache hit" key=incident:3
level=INFO msg=request method=GET path=/incidents/3 status=200 duration=...
```

The hit has no `sql Query` line, because Postgres isn't touched.

For a new alert followed by a repeat:

```
level=DEBUG msg="dedup: fingerprint claimed" key=alert:dedup:fp1 window=5m0s
level=DEBUG msg="dedup: incident attached" key=alert:dedup:fp1 incident_id=1
level=INFO msg="alert created incident" fingerprint=fp1 incident_id=1 severity=low
level=DEBUG msg="dedup: fingerprint already claimed" key=alert:dedup:fp1 incident_id=1
level=INFO msg="alert deduplicated" fingerprint=fp1 incident_id=1
```

`sql Prepare` lines show pgx preparing each statement the first time it runs on a connection. After that, pgx reuses the prepared statement.

> Debug logs include query arguments, which can contain user data. Don't use `LOG_LEVEL=debug` in production.

## Graceful shutdown

On Ctrl+C (`SIGINT`) or `SIGTERM` (sent by Docker or Kubernetes), the server stops accepting new connections, waits up to 10 seconds for in-flight requests to finish, then closes Redis and the database pool:

```
level=INFO msg="shutting down, finishing in-flight requests"
level=INFO msg="server stopped" took=62.208µs
level=INFO msg="redis connection closed"
level=INFO msg="postgres connection pool closed"
```

## Failure behaviour

| Situation | Result |
|---|---|
| Postgres or Redis unreachable **at startup** | Server logs an error and exits (fail fast) |
| Invalid `CACHE_TTL` or `DEDUP_WINDOW` | Server logs `invalid config` and exits |
| Redis goes down **while running** | Reads come from Postgres; alerts create incidents without de-duplication. Recovers by itself when Redis is back |
| Postgres goes down **while running** | Requests that need the database return `500`. Recovers by itself when Postgres is back |

## Project structure

```
.
├── cmd/server/main.go               # entry point: config, logger, store choice, Redis, HTTP server, graceful shutdown
├── migrations/
│   └── 001_create_incidents.sql     # incidents table
└── internal/
    ├── config/config.go             # reads PORT, DATABASE_URL, REDIS_URL, LOG_LEVEL, CACHE_TTL, DEDUP_WINDOW
    ├── model/incident.go            # Incident struct and allowed severity/status values
    ├── store/
    │   ├── postgres.go              # PostgreSQL store (pgx connection pool, SQL query logging)
    │   ├── memory.go                # in-memory store, used when DATABASE_URL is empty
    │   └── cached.go                # Redis cache decorator around any store
    ├── cache/
    │   ├── redis.go                 # Redis client setup (timeouts, logging)
    │   └── dedup.go                 # alert de-duplication with SET NX
    ├── handler/
    │   ├── incident.go              # /incidents routes, validation, status codes
    │   ├── alert.go                 # /alerts webhook
    │   └── response.go              # writeJSON / writeError helpers
    └── middleware/logging.go        # per-request logging middleware
```

**How a request flows:**

```
GET /incidents/7 → logging middleware → router → IncidentHandler → CachedStore ──hit──▶ Redis
                                                                       └─miss─▶ PostgresStore → Postgres

POST /alerts     → logging middleware → router → AlertHandler → Deduper (Redis)
                                                              → CachedStore → PostgresStore → Postgres
```

The handlers depend on the `IncidentStore` interface, not on a concrete store. `main.go` builds the chain: first the base store (`PostgresStore` or `MemoryStore`), then, if `REDIS_URL` is set, it wraps it in `CachedStore`. The handlers pass the request's `context.Context` down, so if a client disconnects, its Redis and database calls are cancelled.

## Roadmap

1. ✅ CRUD API (in-memory) + logging
2. ✅ PostgreSQL persistence, config from env vars, graceful shutdown
3. ✅ Redis caching and alert de-duplication
4. Kafka / NATS events with outbox and dead-letter queue
5. API gateway: JWT auth, RBAC, rate limiting, gRPC
6. pgvector embeddings and document ingestion
7. RAG question answering with Claude
8. MCP server exposing OpsPilot tools to AI agents
9. Observability (OpenTelemetry, Prometheus, Grafana), Docker and Kubernetes
