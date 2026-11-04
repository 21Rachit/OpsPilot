# OpsPilot

An incident-tracking REST API written in Go, built step by step as a learning project.

OpsPilot will eventually become an AI-powered incident response copilot, with RAG and an MCP server. It currently covers **Step 4a**:

- CRUD operations on incidents, stored in **PostgreSQL**
- A **Redis** cache in front of `GET /incidents/{id}`
- An **alert webhook** (`POST /alerts`) that de-duplicates repeated alerts with Redis
- **Kafka events** for every incident change, published reliably with the **transactional outbox** pattern
- Structured logging and graceful shutdown

All backing services are optional. Without `DATABASE_URL` the server uses an in-memory store. Without `REDIS_URL`, caching and de-duplication are switched off. Without `KAFKA_BROKERS`, events are still recorded in the outbox table but aren't published.

## Prerequisites

- Go 1.26.5 or newer (see `go.mod`)
- Docker, to run PostgreSQL, Redis and Kafka
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

# 3. start Kafka 3.9 (single node in KRaft mode, no ZooKeeper)
docker run -d --name opspilot-kafka -p 9092:9092 apache/kafka:3.9.1

# 4. create the topic, about 10s after Kafka starts (run once)
#    a WARNING about '.' and '_' in topic names is harmless
docker exec opspilot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic incident.events --partitions 3 --replication-factor 1

# 5. create the tables (run once, in order)
docker exec -i opspilot-postgres psql -U opspilot -d opspilot < migrations/001_create_incidents.sql
docker exec -i opspilot-postgres psql -U opspilot -d opspilot < migrations/002_create_outbox.sql

# 6. point the server at all three
export DATABASE_URL="postgres://opspilot:opspilot@localhost:5432/opspilot?sslmode=disable"
export REDIS_URL="redis://localhost:6379/0"
export KAFKA_BROKERS="localhost:9092"

# 7. run the server
go run ./cmd/server
```

The server listens on **http://localhost:8087**. On startup you should see:

```
level=INFO msg="connected to postgres" host=localhost port=5432 database=opspilot user=opspilot max_conns=14
level=INFO msg="connected to redis" addr=localhost:6379 db=0 pool_size=140 dial_timeout=500ms read_timeout=200ms
level=INFO msg="connected to kafka" brokers=[localhost:9092] topic=incident.events partitions=3
level=INFO msg="config loaded" port=8087 log_level=INFO postgres_enabled=true redis_enabled=true cache_ttl=1m0s kafka_enabled=true dedup_window=5m0s
level=INFO msg="outbox relay started" interval=1s batch_size=100
level=INFO msg="OpsPilot starting" addr=http://localhost:8087
```

To manage the containers afterwards:

```bash
docker stop opspilot-postgres opspilot-redis opspilot-kafka    # stop (Postgres data is kept)
docker start opspilot-postgres opspilot-redis opspilot-kafka   # start again
```

The Kafka container has no volume, so `docker rm opspilot-kafka` deletes the topic and its messages. If you recreate the container, run step 4 again.

## Configuration

All settings come from environment variables.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(empty)* | PostgreSQL connection string, `postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=disable`. Empty means use the in-memory store |
| `REDIS_URL` | *(empty)* | Redis connection string, `redis://HOST:PORT/DB`. Empty means no caching and no alert de-duplication |
| `PORT` | `8087` | HTTP listen port |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. `debug` also prints cache hits and misses, and dedup steps |
| `CACHE_TTL` | `60s` | How long an incident stays in the Redis cache |
| `DEDUP_WINDOW` | `5m` | How long repeats of the same alert are treated as duplicates |
| `KAFKA_BROKERS` | *(empty)* | Comma-separated Kafka brokers, for example `localhost:9092` or `k1:9092,k2:9092`. Empty means the outbox relay is off. It also needs `DATABASE_URL` |
| `OUTBOX_POLL_INTERVAL` | `1s` | How often the relay checks the outbox table for new events |

Durations use Go syntax: `30s`, `5m`, `1h`. An invalid value (for example `CACHE_TTL=abc`) stops the server at startup with an `invalid config` error.

## Build and checks

```bash
go mod download   # download dependencies (gorilla/mux, pgx, go-redis, kafka-go)
go build ./...    # compile every package
go vet ./...      # static checks
```

## Testing

There are no automated tests yet. Use the curl commands below to test by hand.

To see every command OpsPilot sends to Redis while you test, run this in a separate terminal:

```bash
docker exec -it opspilot-redis redis-cli MONITOR
```

To watch incident events arrive in Kafka, run this in another terminal:

```bash
docker exec -it opspilot-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic incident.events --from-beginning \
  --property print.key=true --property print.partition=true --property print.headers=true
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

## Events (Kafka + transactional outbox)

Every incident change is announced on the Kafka topic **`incident.events`**, so other services (a notifier, analytics, and later the AI ingest worker) can react without OpsPilot knowing about them.

| Change | Event type | `data` |
|---|---|---|
| `POST /incidents`, or a new alert | `incident.created` | the incident |
| `PUT /incidents/{id}` | `incident.updated` | the incident after the change |
| `DELETE /incidents/{id}` | `incident.deleted` | `{"id": N}` |

Example message (key `7`, header `event_type: incident.created`):

```json
{
  "event_id": "1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed",
  "type": "incident.created",
  "incident_id": 7,
  "occurred_at": "2026-11-04T18:40:12.31+05:30",
  "data": {"id": 7, "title": "DNS latency high", "severity": "critical", "status": "open", "created_at": "2026-11-04T18:40:12.31+05:30"}
}
```

### Why an outbox?

Saving to Postgres and then publishing to Kafka are two separate systems, and you can't write to both atomically (the **dual-write problem**). If Kafka is down, or the server crashes between the two steps, the event is lost. So OpsPilot does this instead:

```
┌──────────── ONE Postgres transaction ────────────┐
│ INSERT / UPDATE / DELETE incidents ...           │   both saved, or neither
│ INSERT INTO outbox (event) ...                   │
└──────────────────────────────────────────────────┘
                        │
  Relay (goroutine in the server, every OUTBOX_POLL_INTERVAL):
  SELECT unpublished rows FOR UPDATE SKIP LOCKED → send to Kafka → SET published_at = now()
                        │
                        ▼
               Kafka: incident.events
```

- **An event is saved if and only if its change is saved.** A failed or rolled-back request (for example a `404` update) leaves no event behind.
- **Kafka being down doesn't affect the API.** Requests still succeed, and events wait in the outbox until Kafka is back.
- **`FOR UPDATE SKIP LOCKED`** lets several OpsPilot servers run a relay at the same time, each taking different rows.
- **Delivery is at-least-once.** If the relay crashes after sending but before marking rows published, those events are sent again. Consumers must ignore duplicates using `event_id`.
- **Ordering:** the message key is the incident ID, so all events for one incident go to the same partition and arrive in order. Different incidents are spread across the 3 partitions.
- The in-memory store doesn't record events. The outbox needs Postgres.

Check the outbox:

```bash
docker exec opspilot-postgres psql -U opspilot -d opspilot -c \
  "SELECT id, event_type, incident_id, created_at, published_at FROM outbox ORDER BY id DESC LIMIT 10;"

# events still waiting to be published
docker exec opspilot-postgres psql -U opspilot -d opspilot -c \
  "SELECT count(*) FROM outbox WHERE published_at IS NULL;"
```

Published rows are kept, which makes testing easier. A production system would delete old published rows on a schedule.

## Database

The schema lives in `migrations/`. `001_create_incidents.sql` creates **incidents**:

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | Primary key, generated by Postgres. Deleted IDs are never reused |
| `title` | `TEXT` | Required |
| `severity` | `TEXT` | `CHECK` limits it to the allowed values |
| `status` | `TEXT` | Defaults to `open`. `CHECK` limits it to the allowed values |
| `created_at` | `TIMESTAMPTZ` | Defaults to `now()` |

`002_create_outbox.sql` creates **outbox**:

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | Primary key. The relay publishes in `id` order |
| `event_id` | `UUID` | Unique per event, generated by Postgres (`gen_random_uuid()`) |
| `event_type` | `TEXT` | `incident.created`, `incident.updated` or `incident.deleted` |
| `incident_id` | `BIGINT` | No foreign key, because `deleted` events outlive their incident |
| `payload` | `JSONB` | Becomes the event's `data` |
| `created_at` | `TIMESTAMPTZ` | Defaults to `now()` |
| `published_at` | `TIMESTAMPTZ` | `NULL` until the relay has sent it to Kafka |

A partial index (`WHERE published_at IS NULL`) keeps the relay's query fast as published rows pile up.

Open a SQL shell:

```bash
docker exec -it opspilot-postgres psql -U opspilot -d opspilot
```

```sql
SELECT * FROM incidents;
SELECT * FROM outbox;
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
| `internal/store/cached.go` | DEBUG | `cache hit`, `cache miss`, `cache set` (with TTL), `cache invalidated` |
| `internal/store/cached.go` | WARN | Redis failed and the request fell back to the database |
| `internal/cache/dedup.go` | DEBUG | Fingerprint claimed, already claimed, incident attached, fingerprint released |
| `internal/cache/redis.go` | INFO | Connected to Redis (address, DB, pool size, timeouts). The URL is never logged, as it can contain a password |
| `internal/cache/redis.go` | WARN | go-redis's own messages (for example `connection refused`), prefixed with `redis client:` |
| `internal/outbox/kafka.go` | INFO | Connected to Kafka (brokers, topic, number of partitions) |
| `internal/outbox/relay.go` | INFO | Relay started and stopped; `events published` with a count, only when there was something to send |
| `internal/outbox/relay.go` | WARN | A publish failed (for example Kafka unreachable). The relay retries on the next tick |
| `cmd/server/main.go` | INFO | Config loaded (including which backends are enabled), server start, graceful shutdown and how long it took |
| `cmd/server/main.go` | WARN | A backend is switched off: no `DATABASE_URL`, `REDIS_URL` or `KAFKA_BROKERS` |

SQL queries aren't logged. The relay polls every second, so logging them would flood the output. To see what's in the database, query it directly (see [Database](#database)).

Example with `LOG_LEVEL=debug`, for a cache miss followed by a cache hit:

```
level=DEBUG msg="cache miss" key=incident:3
level=DEBUG msg="cache set" key=incident:3 ttl=1m0s
level=INFO msg=request method=GET path=/incidents/3 status=200 duration=6.237083ms
level=DEBUG msg="cache hit" key=incident:3
level=INFO msg=request method=GET path=/incidents/3 status=200 duration=...
```

On a miss the store reads from Postgres and caches the result; the hit is served from Redis without touching Postgres.

For a new alert followed by a repeat:

```
level=DEBUG msg="dedup: fingerprint claimed" key=alert:dedup:fp1 window=5m0s
level=DEBUG msg="dedup: incident attached" key=alert:dedup:fp1 incident_id=1
level=INFO msg="alert created incident" fingerprint=fp1 incident_id=1 severity=low
level=DEBUG msg="dedup: fingerprint already claimed" key=alert:dedup:fp1 incident_id=1
level=INFO msg="alert deduplicated" fingerprint=fp1 incident_id=1
```

For an incident created and then published by the relay:

```
level=INFO msg="incident created" id=12 severity=critical
level=INFO msg=request method=POST path=/incidents status=201 duration=4.1ms
level=INFO msg="outbox relay: events published" count=1
```

## Graceful shutdown

On Ctrl+C (`SIGINT`) or `SIGTERM` (sent by Docker or Kubernetes), the server stops accepting new connections and waits up to 10 seconds for in-flight requests to finish. It then waits for the outbox relay to stop, and closes the Kafka writer, Redis and the database pool, in that order:

```
level=INFO msg="shutting down, finishing in-flight requests"
level=INFO msg="outbox relay stopped"
level=INFO msg="server stopped" took=1.02ms
level=INFO msg="kafka writer closed"
level=INFO msg="redis connection closed"
level=INFO msg="postgres connection pool closed"
```

The relay must stop before the pool and writer are closed, because it uses both. If shutdown interrupts a batch, the batch is rolled back and sent on the next start.

## Failure behaviour

| Situation | Result |
|---|---|
| Postgres, Redis or Kafka unreachable **at startup** | Server logs an error and exits (fail fast) |
| Topic `incident.events` doesn't exist | Server logs `could not connect to kafka ... (did you create it?)` and exits |
| Invalid `CACHE_TTL`, `DEDUP_WINDOW` or `OUTBOX_POLL_INTERVAL` | Server logs `invalid config` and exits |
| Redis goes down **while running** | Reads come from Postgres; alerts create incidents without de-duplication. Recovers by itself when Redis is back |
| Kafka goes down **while running** | API requests are unaffected. Events wait in the outbox and are published when Kafka is back |
| Postgres goes down **while running** | Requests that need the database return `500`. Recovers by itself when Postgres is back |

## Project structure

```
.
├── cmd/server/main.go               # entry point: config, logger, store choice, Redis, Kafka relay, HTTP server, graceful shutdown
├── migrations/
│   ├── 001_create_incidents.sql     # incidents table
│   └── 002_create_outbox.sql        # outbox table for events waiting to go to Kafka
└── internal/
    ├── config/config.go             # reads all env vars (see Configuration)
    ├── model/incident.go            # Incident struct and allowed severity/status values
    ├── event/event.go               # event types, topic name and the Kafka message format
    ├── outbox/
    │   ├── kafka.go                 # Kafka producer and startup check
    │   └── relay.go                 # moves outbox rows to Kafka
    ├── store/
    │   ├── postgres.go              # PostgreSQL store; writes the incident and its outbox event in one transaction
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

PUT /incidents/7 → ... → PostgresStore ─┬─ UPDATE incidents   ┐ one transaction
                                        └─ INSERT INTO outbox ┘
                         Relay (background) → outbox → Kafka incident.events
```

The handlers depend on the `IncidentStore` interface, not on a concrete store. `main.go` builds the chain: first the base store (`PostgresStore` or `MemoryStore`), then, if `REDIS_URL` is set, it wraps it in `CachedStore`. The handlers pass the request's `context.Context` down, so if a client disconnects, its Redis and database calls are cancelled.

## Roadmap

1. ✅ CRUD API (in-memory) + logging
2. ✅ PostgreSQL persistence, config from env vars, graceful shutdown
3. ✅ Redis caching and alert de-duplication
4. Kafka events
   - 4a. ✅ Transactional outbox and relay publishing to Kafka
   - 4b. Consumer service with retries, a dead-letter queue and idempotency
5. API gateway: JWT auth, RBAC, rate limiting, gRPC
6. pgvector embeddings and document ingestion
7. RAG question answering with Claude
8. MCP server exposing OpsPilot tools to AI agents
9. Observability (OpenTelemetry, Prometheus, Grafana), Docker and Kubernetes
