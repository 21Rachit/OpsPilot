# OpsPilot

An incident-tracking REST API written in Go, built step by step as a learning project.

OpsPilot will eventually become an AI-powered incident response copilot, with RAG and an MCP server. It currently covers **Step 6**:

- CRUD operations on incidents, stored in **PostgreSQL**
- A **Redis** cache in front of `GET /incidents/{id}`
- An **alert webhook** (`POST /alerts`) that de-duplicates repeated alerts with Redis
- **Kafka events** for every incident change, published reliably with the **transactional outbox** pattern
- A separate **notifier** service that consumes those events with a consumer group, retries, a dead-letter queue and duplicate detection
- An **API gateway** in front of the server, with JWT login, role-based access and Redis rate limiting
- A **gRPC API** for incidents, next to the HTTP one, defined by a `.proto` contract
- **Semantic search** (`GET /search`) over incidents and Markdown runbooks, using embeddings from a local model (**Ollama**) stored in **pgvector**
- Structured logging and graceful shutdown

The repo builds five programs:

| Program | Command | What it does |
|---|---|---|
| Server | `go run ./cmd/server` | The REST API on `:8087` and the gRPC API on `:9090`, plus the outbox relay that publishes events to Kafka (the **producer**) |
| Notifier | `go run ./cmd/notifier` | Reads the events from Kafka and writes one line per event to a file (a **consumer**) |
| Embedder | `go run ./cmd/embedder` | Reads the same events and stores a vector for each incident in `incident_embeddings` (another consumer) |
| Ingest | `go run ./cmd/ingest` | A command you run, not a service: splits `runbooks/*.md` into sections and stores a vector for each in `runbook_chunks` |
| Gateway | `go run ./cmd/gateway` | The front door on `:8082`: checks the token, role and rate limit, then forwards the request to the server |

A separate repo, **`opspilot-client`**, is a command-line client for the gRPC API. It shares only a copy of the `.proto` file with this repo. See [gRPC API](#grpc-api).

All of the server's backing services are optional. Without `DATABASE_URL` the server uses an in-memory store. Without `REDIS_URL`, caching and de-duplication are switched off. Without `KAFKA_BROKERS`, events are still recorded in the outbox table but aren't published. `/search` needs `DATABASE_URL`, and Ollama only when a search runs. The notifier and the embedder need both `DATABASE_URL` and `KAFKA_BROKERS`, and the embedder and ingest also need Ollama.

## Prerequisites

- Go 1.26.5 or newer (see `go.mod`)
- Docker, to run PostgreSQL (with the pgvector extension), Redis and Kafka
- [Ollama](https://ollama.com) (`brew install ollama`), to run the embedding model `nomic-embed-text` locally. Only for the embedder, ingest and `/search`
- `curl`, for calling the API
- Only if you change `proto/`: `protoc` and the Go plugins, to regenerate `gen/` (see [Changing the contract](#changing-the-contract))
- Optional: `grpcurl` (`brew install grpcurl`), to call the gRPC API by hand

## Quick start

```bash
# 1. start PostgreSQL 17 with pgvector (data kept in the opspilot-pgdata volume)
docker run -d --name opspilot-postgres \
  -e POSTGRES_USER=opspilot \
  -e POSTGRES_PASSWORD=opspilot \
  -e POSTGRES_DB=opspilot \
  -p 5432:5432 \
  -v opspilot-pgdata:/var/lib/postgresql/data \
  pgvector/pgvector:0.8.7-pg17-trixie

# 2. start Redis 7
docker run -d --name opspilot-redis -p 6379:6379 redis:7

# 3. start Kafka 3.9 (single node in KRaft mode, no ZooKeeper)
docker run -d --name opspilot-kafka -p 9092:9092 apache/kafka:3.9.1

# 4. create the topics, about 10s after Kafka starts (run once)
#    a WARNING about '.' and '_' in topic names is harmless
docker exec opspilot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic incident.events --partitions 3 --replication-factor 1
docker exec opspilot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic incident.events.dlq --partitions 1 --replication-factor 1

# 5. create the tables (safe to run again: every migration uses IF NOT EXISTS)
for f in migrations/*.sql; do
  docker exec -i opspilot-postgres psql -U opspilot -d opspilot < $f
done

# 6. point the programs at all three (in every terminal you use)
export DATABASE_URL="postgres://opspilot:opspilot@localhost:5432/opspilot?sslmode=disable"
export REDIS_URL="redis://localhost:6379/0"
export KAFKA_BROKERS="localhost:9092"

# 7. run the server (terminal 1)
go run ./cmd/server

# 8. run the notifier (terminal 2)
go run ./cmd/notifier

# 9. run the gateway (terminal 3); the secret signs login tokens
export JWT_SECRET=my-local-dev-secret-at-least-32-chars-long
go run ./cmd/gateway

# 10. start Ollama and download the embedding model (once, about 270 MB)
brew services start ollama
ollama pull nomic-embed-text

# 11. run the embedder (terminal 4)
go run ./cmd/embedder

# 12. load the runbooks (run again whenever a file in runbooks/ changes)
go run ./cmd/ingest
```

Steps 10–12 are only needed for `/search`. If you created `opspilot-postgres` from the plain `postgres:17` image in an earlier step, swap the image and keep the data: `docker rm -f opspilot-postgres`, run step 1 again (same volume), then step 5.

The server listens on **http://localhost:8087** (HTTP) and **localhost:9090** (gRPC). On startup you should see:

```
level=INFO msg="connected to postgres" host=localhost port=5432 database=opspilot user=opspilot max_conns=14
level=INFO msg="connected to redis" addr=localhost:6379 db=0 pool_size=140 dial_timeout=500ms read_timeout=200ms
level=INFO msg="connected to kafka" brokers=[localhost:9092] topic=incident.events partitions=3
level=INFO msg="config loaded" port=8087 log_level=INFO postgres_enabled=true redis_enabled=true cache_ttl=1m0s kafka_enabled=true dedup_window=5m0s
level=INFO msg="outbox relay started" interval=1s batch_size=100
level=INFO msg="gRPC server starting" addr=[::]:9090
level=INFO msg="OpsPilot starting" addr=http://localhost:8087
```

The gateway prints:

```
level=INFO msg="connected to redis" addr=localhost:6379 ...
level=INFO msg="rate limiting enabled" per_user=60 login_per_ip=5 window=1m0s
level=INFO msg="gateway starting" addr=http://localhost:8082 upstream=http://localhost:8087 token_ttl=15m0s
```

The notifier prints:

```
level=INFO msg="connected to postgres" ...
level=INFO msg="connected to kafka" brokers=[localhost:9092] topic=incident.events partitions=3
level=INFO msg="connected to kafka" brokers=[localhost:9092] topic=incident.events.dlq partitions=1
level=INFO msg="notifier config loaded" log_level=INFO consumers=3
level=INFO msg="notifier consumer started" consumer_id=1 topic=incident.events group=opspilot-notifier
level=INFO msg="notifier consumer started" consumer_id=2 topic=incident.events group=opspilot-notifier
level=INFO msg="notifier consumer started" consumer_id=3 topic=incident.events group=opspilot-notifier
```

The first time it runs, it also writes every event already in the topic, because a new consumer group starts from the oldest message.

The embedder prints the same kind of lines, with its own group:

```
level=INFO msg="embedder config loaded" log_level=INFO consumers=1 ollama_url=http://localhost:11434 model=nomic-embed-text dims=768
level=INFO msg="embedder consumer started" consumer_id=1 topic=incident.events group=opspilot-embedder
```

To manage the containers afterwards:

```bash
docker stop opspilot-postgres opspilot-redis opspilot-kafka    # stop (Postgres data is kept)
docker start opspilot-postgres opspilot-redis opspilot-kafka   # start again
```

The Kafka container has no volume, so `docker rm opspilot-kafka` deletes the topics, their messages and the notifier's saved position. If you recreate the container, run step 4 again.

## Configuration

All settings come from environment variables.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(empty)* | PostgreSQL connection string, `postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=disable`. Empty means use the in-memory store |
| `REDIS_URL` | *(empty)* | Redis connection string, `redis://HOST:PORT/DB`. Empty means no caching and no alert de-duplication |
| `PORT` | `8087` | Server HTTP listen port |
| `GRPC_PORT` | `9090` | Server gRPC listen port |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. `debug` also prints cache hits and misses, and dedup steps |
| `CACHE_TTL` | `60s` | How long an incident stays in the Redis cache |
| `DEDUP_WINDOW` | `5m` | How long repeats of the same alert are treated as duplicates |
| `KAFKA_BROKERS` | *(empty)* | Comma-separated Kafka brokers, for example `localhost:9092` or `k1:9092,k2:9092`. Empty means the outbox relay is off. It also needs `DATABASE_URL` |
| `OUTBOX_POLL_INTERVAL` | `1s` | How often the relay checks the outbox table for new events |
| `NOTIFIER_CONSUMERS` | `3` | Notifier only. How many consumers it runs. Each one writes `out/consumer_<n>_hello.txt` |
| `OLLAMA_URL` | `http://localhost:11434` | Server, embedder and ingest. Where Ollama listens |
| `EMBEDDING_MODEL` | `nomic-embed-text` | Server, embedder and ingest. The model that turns text into vectors. It must return 768 numbers, to fit the `vector(768)` columns. All three programs must use the same model, because vectors from different models can't be compared |
| `EMBEDDER_CONSUMERS` | `1` | Embedder only. How many consumers it runs. Ollama embeds one text at a time, so more consumers rarely help |
| `GATEWAY_PORT` | `8082` | Gateway only. Where the gateway listens |
| `UPSTREAM_URL` | `http://localhost:8087` | Gateway only. The server the gateway forwards requests to |
| `JWT_SECRET` | *(empty)* | Gateway only. **Required**, at least 32 characters. Signs and verifies login tokens. Every gateway copy must use the same value, and changing it logs everyone out |
| `TOKEN_TTL` | `15m` | Gateway only. How long a login token stays valid |
| `RATE_LIMIT` | `60` | Gateway only. Requests each user may make per `RATE_LIMIT_WINDOW` |
| `LOGIN_RATE_LIMIT` | `5` | Gateway only. Login attempts each client IP may make per `RATE_LIMIT_WINDOW` |
| `RATE_LIMIT_WINDOW` | `1m` | Gateway only. The time window both limits count over |

The notifier reads `DATABASE_URL`, `KAFKA_BROKERS`, `LOG_LEVEL` and `NOTIFIER_CONSUMERS`, and ignores the rest. The embedder reads the same plus `OLLAMA_URL`, `EMBEDDING_MODEL` and `EMBEDDER_CONSUMERS`. Ingest reads `DATABASE_URL`, `LOG_LEVEL`, `OLLAMA_URL` and `EMBEDDING_MODEL`. The gateway reads `REDIS_URL` (for rate limits; without it, limits are off), `LOG_LEVEL` and the `GATEWAY_PORT` … `RATE_LIMIT_WINDOW` settings.

Durations use Go syntax: `30s`, `5m`, `1h`. An invalid value (for example `CACHE_TTL=abc`, `NOTIFIER_CONSUMERS=0` or a short `JWT_SECRET`) stops the program at startup with an `invalid config` error.

## Build and checks

```bash
go mod download   # download dependencies (gorilla/mux, pgx, go-redis, kafka-go, golang-jwt, bcrypt, grpc)
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

To test the notifier, see [Notifier → Checking it works](#checking-it-works). To test the gateway, see [Gateway → Trying it](#trying-it). To test search, see [Semantic search → Trying it](#trying-search). To test the gRPC API, see [gRPC API → Calling it](#calling-it).

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

### Search

| Method | Path | Query | Success | Errors |
|---|---|---|---|---|
| `GET` | `/search` | `q` (required, at most 500 characters), `limit` (1–20, default 5) | `200` `{"query","model","incidents":[...],"runbooks":[...]}`, best match first | `400`, `503` when the embedding model is unreachable |

Each hit has a `score` from 0 to 1 (higher means closer in meaning). See [Semantic search](#semantic-search-embeddings--pgvector).

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
- **Ordering:** the relay sets the message key to the incident ID (`internal/outbox/relay.go`), and the writer's `kafka.Hash` balancer (`internal/outbox/kafka.go`) picks the partition as `hash(key) % 3`. So all events for one incident go to the same partition and arrive in order. Different incidents are spread across the 3 partitions. Adding partitions later changes `% 3`, so an incident's new events could move to another partition.
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

## Notifier (Kafka consumer)

The notifier (`cmd/notifier`) is a separate program that reacts to incident events. The server doesn't know it exists. The consumer loop described below (consumer group, duplicate check, retries, DLQ, commit) lives in `internal/consumer` and is shared with the [embedder](#embedder): each service only supplies the function that handles one event. For each event it appends a line to a file, standing in for a real action such as sending a Slack message:

```
hello i am a file and consumes from partition 1 with event id 1b9d6bcd-bbfd-4b2d-9b5d-ab8dfbbd4bed
```

```
                         Kafka: incident.events (3 partitions)
                     partition 0      partition 1      partition 2
                          │                │                │
 ┌──────── notifier process, group "opspilot-notifier" ─────┼────────┐
 │                  consumer 3        consumer 2       consumer 1    │
 │                  (goroutine)       (goroutine)      (goroutine)   │
 └──────────────────────┼────────────────┼────────────────┼──────────┘
                        ▼                ▼                ▼
          out/consumer_3_hello.txt  consumer_2_...   consumer_1_...
```

### Consumer group

- One process starts `NOTIFIER_CONSUMERS` goroutines. Each has its own `kafka.Reader`, and all of them use the group ID **`opspilot-notifier`**.
- Kafka gives each member of the group its own partitions. Which consumer gets which partition is decided by Kafka and can change between runs.
- **Partitions limit parallelism.** With 2 consumers, one of them reads 2 partitions. With 4 consumers, one sits idle, because a partition is never shared within a group.
- If a consumer leaves (for example the process stops), Kafka **rebalances** and gives its partitions to the remaining members. This takes a few seconds.
- The group's position (its **offset** in each partition) is stored in Kafka. After a restart, the notifier continues where it stopped. A brand-new group starts from the oldest message (`StartOffset: FirstOffset`).

### Handling one message

```
FetchMessage (doesn't commit)
  │
  ├─ not valid JSON, or no event_id → DLQ right away (attempts=0), it would never succeed
  ├─ event_id already in processed_events → skip, it's a duplicate
  ├─ write the line, up to 3 attempts, waiting 500ms then 1s between them
  │     ├─ success → INSERT INTO processed_events
  │     └─ all 3 failed → copy the message to the DLQ
  │
  └─ CommitMessages (tell Kafka "done up to here")
```

- **At-least-once:** the offset is committed only after the message is fully handled. If the notifier crashes in between, Kafka delivers the message again.
- **Idempotency:** because of that, the same event can arrive twice. The `processed_events` table records each `event_id` once handled, so a redelivered event is skipped instead of written twice.
- **Dead-letter queue (DLQ):** a message that keeps failing is copied to **`incident.events.dlq`** and then committed, so one bad message doesn't block its partition forever. The DLQ copy keeps the original key and value, and adds headers explaining what went wrong:

  | Header | Example |
  |---|---|
  | `dlq_error` | `append line: open out/consumer_2_hello.txt: permission denied` |
  | `dlq_attempts` | `3` (`0` for a message that couldn't be decoded) |
  | `dlq_consumer` | `notifier` or `embedder`: which service gave up on it (both share one DLQ topic) |
  | `original_topic` | `incident.events` |
  | `original_partition` | `1` |
  | `original_offset` | `42` |

- **Shutdown during a retry:** the message isn't committed or sent to the DLQ. It's delivered again on the next start.

### Output files

Files go in `out/` (ignored by git) and are named `consumer_<n>_hello.txt`. Each write opens and closes the file, so you can delete or `chmod` it while the notifier runs.

### Checking it works

```bash
# shortcuts used below
alias kgroup='docker exec opspilot-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group opspilot-notifier'
alias sql='docker exec opspilot-postgres psql -U opspilot -d opspilot -c'

# which consumer owns which partition, and how many messages are waiting (LAG)
kgroup --describe

# what the consumers wrote, and which partitions each file contains
cat out/*.txt
for f in out/*.txt; do echo "$f: $(grep -o 'partition [0-9]' $f | sort | uniq -c | tr '\n' ' ')"; done

# events handled so far
sql "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

# messages in the dead-letter queue, with their headers (Ctrl+C to exit)
docker exec -it opspilot-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic incident.events.dlq --from-beginning --property print.headers=true
```

Scenarios to try:

| Scenario | How | What you should see |
|---|---|---|
| Follow one event | Create an incident | The same `event_id` in the outbox row, the notifier log, the file and `processed_events` |
| Partition split | Create 9 incidents | Each file contains only one partition number |
| Ordering | Update and delete one incident | All its events come from the same consumer and partition, with increasing offsets |
| Catch up | Stop the notifier, create incidents, start it again | `LAG` grows while it's stopped, then the events are written straight after the restart |
| Kafka outage | `docker stop opspilot-kafka`, create an incident, `docker start opspilot-kafka` | The API still returns `201`. The notifier logs `fetch failed, retrying`, and after about 30–60s the event is published and written |
| Duplicates | Stop the notifier, `kgroup --reset-offsets --to-earliest --topic incident.events --execute`, start it | `duplicate event skipped` for every event, and the files don't grow |
| Retries then DLQ | `chmod 444 out/*.txt`, create an incident | 2 `attempt failed, retrying` lines, then `event moved to DLQ ... attempts=3`. Undo with `chmod 644 out/*.txt` |
| Poison message | `echo 'not json' \| docker exec -i opspilot-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic incident.events` | `event moved to DLQ ... attempts=0 permanent=true`, with no retries |
| Scaling | `NOTIFIER_CONSUMERS=2 go run ./cmd/notifier`, or `=4` | With 2, one file has two partitions. With 4, only 3 files are written |

`--reset-offsets` only works while no consumer in the group is running.

## Semantic search (embeddings + pgvector)

Keyword search fails when people use different words: "login is slow in Europe" shares no words with an incident titled "DNS latency high in eu-west". Semantic search compares **meaning** instead:

1. An **embedding model** turns a text into a list of 768 numbers (a **vector**). Texts with similar meaning get vectors that point in similar directions. OpsPilot uses `nomic-embed-text`, running locally in Ollama, so no data leaves your machine.
2. The vectors are stored in Postgres with the **pgvector** extension (column type `vector(768)`).
3. A search embeds the question and asks Postgres for the nearest vectors with the `<=>` (cosine distance) operator. An **HNSW index** makes this fast without comparing against every row.

`nomic-embed-text` expects a prefix that says what a text is for: stored texts are embedded as `search_document: ...` and questions as `search_query: ...`. Both are added in `internal/embedding`, so callers never write them.

This is the retrieval half of RAG. Step 7 will pass the search results to Claude to write an answer.

### Embedder

The embedder (`cmd/embedder`) is a Kafka consumer in group **`opspilot-embedder`**, using the same consumer loop as the notifier. For each event:

| Event | What it does |
|---|---|
| `incident.created`, `incident.updated` | Builds the text `Incident: <title>\nSeverity: ...\nStatus: ...`, embeds it and upserts the row in `incident_embeddings`. If the text and model haven't changed, the existing vector is kept and Ollama isn't called |
| `incident.deleted` | Deletes the row (usually already gone, through `ON DELETE CASCADE`) |
| `created` for an incident that was deleted since | The insert hits the foreign key (`23503`), and the event is skipped |

At startup it embeds the sentence `startup check`, so a stopped Ollama or a missing model stops it right away with a clear message, instead of sending every event to the DLQ. Ollama being down while it runs counts as a temporary error: the event is retried, then goes to the DLQ.

Incidents created before Kafka existed (in Steps 2–3) have no event, so they have no vector until they're updated.

### Runbook ingest

Runbooks are Markdown files in `runbooks/`. They change rarely, so `cmd/ingest` is a command you run (by hand or from CI), not a service:

```bash
go run ./cmd/ingest            # ingest ./runbooks
go run ./cmd/ingest -dry-run   # only show how each file is split; needs no database or Ollama
go run ./cmd/ingest -force     # re-embed every file, even unchanged ones (after changing the chunking)
go run ./cmd/ingest -dir path  # another folder
```

- **Chunking.** A whole runbook as one vector would blur several topics together. Each file is split into sections at its `##` (and deeper) headings, and a section longer than about 1000 characters is split again at blank lines. Lines inside ```` ``` ```` code blocks are never treated as headings, so a `# comment` in a bash block stays in its section.
- **Context.** A chunk like `1. Run df -h` means little on its own, so the text that gets embedded starts with the runbook title and section: `Runbook: Disk full\nSection: Fix > Free space quickly\n\n...`.
- **Unchanged files are skipped.** Each row stores the file's SHA-256 hash and the model. A file with the same hash and model as last time isn't embedded again.
- **A changed file is replaced in one transaction**: its old chunks are deleted and the new ones inserted together, so a search never sees half of each, and a file that shrank doesn't leave old chunks behind. All chunks are embedded before the transaction starts.
- **Deleted files are removed.** Chunks of files that are no longer in the folder are deleted, but only for that folder. An empty or missing folder is refused, because otherwise every runbook would count as deleted.
- One failing file is logged and skipped, and its old chunks stay. The exit code is `1` if anything failed.

### Search

`GET /search?q=...` (see [API → Search](#search)) embeds the question once, then runs two queries: the nearest incidents (joined to `incidents`, so titles and statuses are current) and the nearest runbook chunks. Only vectors made by the current `EMBEDDING_MODEL` are compared. Each hit's `score` is `1 - cosine distance`.

Vector search always returns the nearest rows, even when nothing is actually close. A question about a full disk still returns some DNS incidents if there are no disk incidents, just with lower scores (around 0.55 instead of 0.7).

The server has no Ollama startup check: search is one feature among many, so if Ollama is down, `/search` returns `503` and everything else keeps working. Through the gateway, `/search` needs the `viewer` role, like every `GET`. The query text itself isn't logged, only its length.

### Trying search

```bash
alias sql='docker exec opspilot-postgres psql -U opspilot -d opspilot -c'

# log in through the gateway as a viewer and keep the token (see Gateway → Trying it)
TOKEN=...
s() { curl -s -G localhost:8082/search -H "Authorization: Bearer $TOKEN" --data-urlencode "q=$1" --data-urlencode "limit=${2:-3}"; }

s "login is slow in Europe" | python3 -m json.tool

# what's stored
sql "SELECT incident_id, model, vector_dims(embedding), content FROM incident_embeddings ORDER BY 1;"
sql "SELECT source, chunk_index, heading, vector_dims(embedding) FROM runbook_chunks ORDER BY 1, 2;"
```

| Scenario | How | What you should see |
|---|---|---|
| Different words, same meaning | `s "login is slow in Europe"` | A DNS incident first, and `dns-latency.md` sections in `runbooks` |
| The right section | `s "browser says connection not private"` | `tls-certificate-expired.md`, heading `Symptoms`, first |
| New incident becomes searchable | Create an incident, wait a few seconds, search for it in other words | The embedder logs `embedding saved`, and the incident is the top hit |
| Unchanged update | `PUT` an incident with the same title, severity and status | `text unchanged, embedding kept` |
| Edit a runbook | Change one file in `runbooks/`, run `go run ./cmd/ingest` | Only that file is `ingested`; the others are `unchanged, skipped` |
| Delete a runbook | Move a file out of `runbooks/`, run ingest | `removed chunks of runbooks that no longer exist` |
| Ollama down | `brew services stop ollama`, then search | `503 search is unavailable right now`, while `GET /incidents` still works. The embedder and ingest refuse to start |
| Bad input | No `q`, or `limit=50` | `400` |

## Gateway

The gateway (`cmd/gateway`, port `8082`) is the single entry point for clients. It decides **who** you are, **what** you may do and **how often**, then forwards the request to the server with `httputil.ReverseProxy`. The server itself has no security code.

```
POST /login      → [login limit per IP] → check password → JWT
everything else  → RequireAuth (401) → [user limit] (429) → RequireRole (403) → proxy → server :8087
```

### Login and tokens (JWT)

`POST /login {"username":"bob","password":"password"}` returns `{"access_token":"eyJ...","token_type":"Bearer","expires_in":900}`. Every other request must send `Authorization: Bearer <token>`.

- Passwords are checked with **bcrypt**. An unknown username still runs bcrypt against a dummy hash, so both kinds of failed login take the same time and don't reveal which usernames exist.
- The token is signed with `JWT_SECRET` using **HS256**. The gateway checks the signature, the issuer and the expiry, and only accepts HS256, so a token with `"alg":"none"` is rejected.
- The payload is only base64-encoded, not encrypted. It holds `sub` (username), `role`, `iss`, `iat` and `exp`.
- The gateway removes any `X-User` and `X-User-Role` headers the client sent, then sets them from the token before forwarding.

Demo users, all with the password `password`. They live in memory in `internal/auth/users.go`:

| User | Role | GET | POST / PUT / PATCH | DELETE |
|---|---|---|---|---|
| `alice` | `viewer` | ✅ | ❌ 403 | ❌ 403 |
| `bob` | `responder` | ✅ | ✅ | ❌ 403 |
| `carol` | `admin` | ✅ | ✅ | ✅ |

The role needed depends only on the HTTP method (`internal/gateway/rbac.go`). Any other method, such as `OPTIONS`, needs `admin`, so anything unplanned is denied by default.

### Rate limits

Fixed-window counters in Redis: `INCR` and `EXPIRE` run together in `MULTI/EXEC`, on the key `ratelimit:<limiter>:<who>:<window start>`.

| Limiter | Counts per | Default |
|---|---|---|
| `user` | logged-in user | `RATE_LIMIT=60` per `RATE_LIMIT_WINDOW=1m` |
| `login` | client IP (`RemoteAddr`; `X-Forwarded-For` is ignored, as clients can fake it) | `LOGIN_RATE_LIMIT=5` per window |

Every response carries `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`. Over the limit, the answer is `429` with `Retry-After`. If Redis goes down, requests are allowed (fail open).

### Status codes

| Code | Meaning |
|---|---|
| `401` | No token, a bad or expired token, or a wrong username or password |
| `403` | Valid token, but the role isn't allowed to do this |
| `429` | Over the rate limit |
| `502` | The server is down (`{"error":"upstream unavailable"}`) |
| anything else | Whatever the server answered |

### Trying it

```bash
G=localhost:8082
login() { curl -s -X POST $G/login -d "{\"username\":\"$1\",\"password\":\"password\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])'; }
BOB=$(login bob); ALICE=$(login alice)

curl -i $G/incidents                                                    # 401, no token
curl -i $G/incidents -H "Authorization: Bearer $BOB"                     # 200
curl -i -X POST $G/incidents -H "Authorization: Bearer $ALICE" \
  -d '{"title":"x","severity":"low"}'                                    # 403, viewer
echo $BOB | cut -d. -f2 | base64 -d 2>/dev/null; echo                    # the token's payload
docker exec opspilot-redis redis-cli --scan --pattern 'ratelimit:*'     # the counters
```

To hit a limit quickly, start the gateway with `RATE_LIMIT=5 RATE_LIMIT_WINDOW=30s`.

Port `8087` still works without a token. In production, only the gateway would be reachable from outside.

## gRPC API

The server also serves incidents over **gRPC** on port `9090`, from the same store as the HTTP API. An incident created over gRPC goes through the same cache, outbox and Kafka events.

```
                    ┌── HTTP :8087 ── internal/handler/incident.go ────┐
OpsPilot server ────┤                                                  ├── store (cache → Postgres + outbox)
                    └── gRPC :9090 ── internal/grpcserver/incident.go ─┘
```

Both APIs check input with the same `model.Validate`, so the rules can't drift apart.

### The contract

`proto/incident/v1/incident.proto` defines the service:

| RPC | Request | Response | HTTP equivalent |
|---|---|---|---|
| `CreateIncident` | `title`, `severity` | `Incident` | `POST /incidents` |
| `GetIncident` | `id` | `Incident` | `GET /incidents/{id}` |
| `ListIncidents` | *(none)* | `incidents` (a list) | `GET /incidents` |
| `UpdateIncident` | `id`, `title`, `severity`, `status` | `Incident` | `PUT /incidents/{id}` |
| `DeleteIncident` | `id` | *(empty)* | `DELETE /incidents/{id}` |

`Incident` has `id` (int64), `title`, `severity`, `status` and `created_at` (`google.protobuf.Timestamp`).

Errors use gRPC status codes instead of HTTP ones:

| Situation | gRPC code | HTTP API |
|---|---|---|
| Bad input (missing title, bad severity or status, `id <= 0`) | `InvalidArgument` | `400` |
| Incident doesn't exist | `NotFound` | `404` |
| Unexpected error (details are logged, not returned) | `Internal` | `500` |

The server registers **reflection**, so tools like `grpcurl` can list the services and methods without the `.proto` file.

### Calling it

```bash
A=localhost:9090
S=opspilot.incident.v1.IncidentService

grpcurl -plaintext $A list                      # services on the server
grpcurl -plaintext $A describe $S               # the 5 methods
grpcurl -plaintext -d '{"title":"DNS latency high","severity":"high"}' $A $S/CreateIncident
grpcurl -plaintext -d '{"id":1}' $A $S/GetIncident
grpcurl -plaintext $A $S/ListIncidents
grpcurl -plaintext -d '{"id":1,"title":"DNS latency high","severity":"critical","status":"resolved"}' $A $S/UpdateIncident
grpcurl -plaintext -d '{"id":1}' $A $S/DeleteIncident
```

`-plaintext` means no TLS, which is fine on one machine. grpcurl prints int64 values as strings (`"id": "1"`) and times in UTC.

Or use the separate **`opspilot-client`** repo, a command-line client with its own copy of the `.proto`:

```bash
cd ../opspilot-client
OPSPILOT_USER=rachit go run . create "DNS latency high" high
go run . list
go run . get 1
```

The client sends its user as gRPC metadata (`x-user`). The server's log shows it as `user=rachit`, or `user=-` when there's none, as with grpcurl.

### Changing the contract

`gen/incident/v1/` is generated from the `.proto`. Never edit it by hand: change the `.proto`, then regenerate:

```bash
# once: brew install protobuf
#       go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#       go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc --go_out=. --go_opt=module=opspilot \
       --go-grpc_out=. --go-grpc_opt=module=opspilot \
       proto/incident/v1/incident.proto
```

- Never change a field's number (`= 1`, `= 2`…) once it's in use. Protobuf sends the number, not the name, so changing it breaks every client.
- A new RPC returns `Unimplemented` until you write it, because `IncidentServer` embeds `UnimplementedIncidentServiceServer`.
- Copy the changed `.proto` to `opspilot-client` and regenerate there too.

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

`003_create_processed_events.sql` creates **processed_events**, which consumers use to skip duplicates:

| Column | Type | Notes |
|---|---|---|
| `consumer` | `TEXT` | Which service handled the event, for example `notifier` |
| `event_id` | `UUID` | The event's `event_id` |
| `processed_at` | `TIMESTAMPTZ` | Defaults to `now()` |

The primary key is `(consumer, event_id)`, so each service keeps its own record and a future consumer can process the same events independently.

`004_create_incident_embeddings.sql` enables the `vector` extension (so Postgres must run the pgvector image) and creates **incident_embeddings**, one row per incident:

| Column | Type | Notes |
|---|---|---|
| `incident_id` | `BIGINT` | Primary key, and a foreign key to `incidents` with `ON DELETE CASCADE` |
| `content` | `TEXT` | The exact text that was embedded |
| `model` | `TEXT` | Which model made the vector, for example `nomic-embed-text` |
| `embedding` | `vector(768)` | The vector |
| `updated_at` | `TIMESTAMPTZ` | Defaults to `now()` |

It's a separate table rather than a column on `incidents`, so the API works the same whether or not a vector exists yet.

`005_create_runbook_chunks.sql` creates **runbook_chunks**, one row per runbook section:

| Column | Type | Notes |
|---|---|---|
| `source` | `TEXT` | The file, for example `runbooks/disk-full.md` |
| `chunk_index` | `INT` | Position in the file, from 0. The primary key is `(source, chunk_index)` |
| `title` | `TEXT` | The runbook's `# ` heading |
| `heading` | `TEXT` | The section, for example `Fix > Free space quickly` |
| `content` | `TEXT` | The exact text that was embedded, returned in search results |
| `file_hash` | `TEXT` | SHA-256 of the whole file, to skip unchanged files |
| `model` | `TEXT` | Which model made the vector |
| `embedding` | `vector(768)` | The vector |
| `updated_at` | `TIMESTAMPTZ` | Defaults to `now()` |

Both tables have an HNSW index on `embedding` with `vector_cosine_ops`, which serves `ORDER BY embedding <=> $1`.

Open a SQL shell:

```bash
docker exec -it opspilot-postgres psql -U opspilot -d opspilot
```

```sql
SELECT * FROM incidents;
SELECT * FROM outbox;
SELECT * FROM processed_events;
SELECT incident_id, model, content FROM incident_embeddings;   -- leave out embedding: 768 numbers per row
SELECT source, chunk_index, heading FROM runbook_chunks ORDER BY 1, 2;
\d incidents   -- show the table definition
\q             -- quit
```

## Logging

All programs use Go's structured logger (`log/slog`) and write text logs to stdout. Set the level with `LOG_LEVEL`.

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
| `internal/outbox/kafka.go` | INFO | Connected to Kafka (brokers, topic, number of partitions), once per topic |
| `internal/outbox/relay.go` | INFO | Relay started and stopped; `events published` with a count, only when there was something to send |
| `internal/outbox/relay.go` | WARN | A publish failed (for example Kafka unreachable). The relay retries on the next tick |
| `cmd/server/main.go` | INFO | Config loaded (including which backends are enabled), server start, graceful shutdown and how long it took |
| `cmd/server/main.go` | WARN | A backend is switched off: no `DATABASE_URL`, `REDIS_URL` or `KAFKA_BROKERS` |
| `internal/consumer/consumer.go` | INFO | Consumer started and stopped; `duplicate event skipped`. Messages start with the service name, for example `notifier consumer started` or `embedder: duplicate event skipped` |
| `internal/consumer/consumer.go` | WARN | `attempt failed, retrying`; `fetch failed, retrying` (Kafka unreachable); commit failed; Postgres unavailable for the duplicate check |
| `internal/consumer/consumer.go` | ERROR | `event moved to DLQ` (attempts, whether the error was permanent, the cause) |
| `internal/consumer/group.go` | INFO / ERROR | Readers closed; a consumer failed and the service is stopping |
| `internal/notifier/process.go` | INFO | `notifier: event written` (event ID, type, incident, partition, offset, file, attempt) |
| `cmd/notifier/main.go` | INFO | Config loaded (number of consumers), DLQ writer closed |
| `internal/embedder/process.go` | INFO | `embedding saved` (model, dims), `text unchanged, embedding kept`, `embedding deleted`, `incident no longer exists, skipped` |
| `cmd/embedder/main.go` | INFO / ERROR | Config loaded (Ollama URL, model, dims); `startup failed: embedding model not ready` |
| `cmd/ingest/main.go` | INFO / ERROR | Per file: `runbook ingested` (chunks, time), `runbook unchanged, skipped` or `runbook failed`; `removed chunks of runbooks that no longer exist`; an `ingest finished` summary |
| `internal/handler/search.go` | INFO / ERROR | `search done` (query length, number of hits, time). The query text isn't logged. `search failed` (usually Ollama down) |
| `internal/grpcserver/logging.go` | INFO / WARN / ERROR | One `grpc request` line per call: method, user, code, duration. WARN for caller errors (`NotFound`, `InvalidArgument`), ERROR for ours (`Internal`) |
| `internal/grpcserver/incident.go` | INFO / ERROR | Incident created, updated or deleted, with `api=grpc`; unexpected store errors |
| `internal/gateway/auth.go` | INFO / WARN | `login succeeded` (username, role), `login failed`, `token rejected` (with the reason: invalid or expired) |
| `internal/gateway/rbac.go` | WARN | `access denied` (user, role, the role needed, method, path) |
| `internal/gateway/ratelimit.go` | WARN | `rate limit exceeded` (limiter, who, limit, retry after); `rate limiter unavailable, allowing request` (Redis down) |
| `internal/gateway/proxy.go` | ERROR | `upstream unreachable`: the server is down |
| `cmd/gateway/main.go` | INFO / WARN | Gateway start (port, upstream, token TTL), rate limiting enabled or disabled, shutdown |

Every notifier log line carries `consumer_id`, so you can tell the consumers apart.

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

The notifier then handles the same event:

```
level=INFO msg="notifier: event written" consumer_id=2 event_id=1b9d6bcd-... type=incident.created incident_id=12 partition=1 offset=40 file=out/consumer_2_hello.txt attempt=1
```

When the file can't be written, after all retries:

```
level=WARN msg="notifier: attempt failed, retrying" consumer_id=2 ... attempt=1 max_attempts=3 retry_in=500ms error="append line: open out/consumer_2_hello.txt: permission denied"
level=WARN msg="notifier: attempt failed, retrying" consumer_id=2 ... attempt=2 max_attempts=3 retry_in=1s error="..."
level=ERROR msg="notifier: event moved to DLQ" consumer_id=2 partition=1 offset=41 attempts=3 permanent=false error="..."
```

A gRPC call that succeeds, then one for an incident that doesn't exist:

```
level=INFO msg="incident created" id=42 severity=high api=grpc
level=INFO msg="grpc request" method=/opspilot.incident.v1.IncidentService/CreateIncident user=rachit code=OK duration=14.78ms
level=WARN msg="grpc request" method=/opspilot.incident.v1.IncidentService/GetIncident user=rachit code=NotFound duration=39µs
```

`method` is the call's full name, `/<proto package>.<service>/<rpc>`. It's also the HTTP/2 path the call is sent to.

In the gateway, a viewer trying to create an incident:

```
level=WARN msg="gateway: access denied" user=alice role=viewer required_role=responder method=POST path=/incidents
level=WARN msg=request method=POST path=/incidents status=403 duration=...
```

## Graceful shutdown

On Ctrl+C (`SIGINT`) or `SIGTERM` (sent by Docker or Kubernetes), the server stops accepting new connections and waits up to 10 seconds for in-flight requests to finish. It then stops the gRPC server, waits for the outbox relay to stop, and closes the Kafka writer, Redis and the database pool, in that order:

```
level=INFO msg="shutting down, finishing in-flight requests"
level=INFO msg="grpc server stopped"
level=INFO msg="outbox relay stopped"
level=INFO msg="server stopped" took=1.02ms
level=INFO msg="kafka writer closed"
level=INFO msg="redis connection closed"
level=INFO msg="postgres connection pool closed"
```

The relay must stop before the pool and writer are closed, because it uses both. If shutdown interrupts a batch, the batch is rolled back and sent on the next start. gRPC's `GracefulStop` has no timeout of its own, so if in-flight calls take longer than the 10 seconds, the server force-stops them (`grpc server forced to stop`).

The gateway stops accepting requests, waits for in-flight ones, then closes Redis:

```
level=INFO msg="gateway shutting down"
level=INFO msg="gateway stopped"
level=INFO msg="redis connection closed"
```

The notifier stops all its consumers, then closes the readers (which leaves the consumer group), the DLQ writer and the database pool:

```
level=INFO msg="notifier consumer stopped" consumer_id=1     (one line per consumer)
level=INFO msg="kafka reader closed" consumer_id=3           (one line per consumer)
level=INFO msg="dlq writer closed"
level=INFO msg="postgres connection pool closed"
```

A message being retried at that moment isn't committed (`message will be redelivered`), so it's handled on the next start.

## Failure behaviour

| Situation | Result |
|---|---|
| Postgres, Redis or Kafka unreachable **at startup** | Server logs an error and exits (fail fast) |
| Topic `incident.events` doesn't exist | Server logs `could not connect to kafka ... (did you create it?)` and exits |
| Invalid `CACHE_TTL`, `DEDUP_WINDOW` or `OUTBOX_POLL_INTERVAL` | Server logs `invalid config` and exits |
| Redis goes down **while running** | Reads come from Postgres; alerts create incidents without de-duplication. Recovers by itself when Redis is back |
| Kafka goes down **while running** | API requests are unaffected. Events wait in the outbox and are published when Kafka is back |
| Postgres goes down **while running** | Requests that need the database return `500`. Recovers by itself when Postgres is back |
| **Notifier:** Postgres or Kafka unreachable at startup, or a topic missing | Notifier logs `startup failed` and exits |
| **Notifier:** Kafka goes down while running | Logs `fetch failed, retrying` every 2s and stays up. Continues from its saved offset when Kafka is back |
| **Notifier:** a write keeps failing | 3 attempts, then the message goes to the DLQ and the consumer moves on |
| **Notifier:** a message isn't valid JSON | Goes straight to the DLQ, without retries |
| **Notifier:** Postgres goes down while running | Events are still written, but duplicates can't be detected (fail open: a duplicate line is better than a missed one) |
| **Notifier:** the DLQ can't be written | The message isn't committed, and the whole notifier exits so it can be restarted. Nothing is lost |
| **Server:** the gRPC port is already in use | Logs `could not listen for grpc ... address already in use` and exits, before the HTTP server starts |
| **Gateway:** `JWT_SECRET` missing or shorter than 32 characters | Logs `invalid config` and exits |
| **Gateway:** Redis unreachable at startup | Logs `could not connect to redis` and exits. Without `REDIS_URL`, it starts with rate limits off |
| **Gateway:** Redis goes down while running | Requests are allowed without limits (fail open). Limits come back by themselves |
| **Gateway:** the server is down | `502 upstream unavailable`. Login still works, because the gateway handles `/login` itself |
| **Server:** Ollama down | The server starts and works normally; only `/search` returns `503` |
| **Embedder / ingest:** Ollama down or the model not pulled at startup | Logs `startup failed: embedding model not ready (is Ollama running? ...)` and exits |
| **Embedder:** Ollama goes down while running | Each event is retried, then goes to the DLQ with `dlq_consumer=embedder` |
| **Embedder:** the model returns a vector of the wrong size | Permanent error: the event goes straight to the DLQ |
| **Ingest:** one file fails | Logged as `runbook failed`; its old chunks stay, the other files continue, exit code `1` |
| **Ingest:** the folder is empty or missing | Refuses to run, so it doesn't delete every stored runbook |

## Project structure

```
.
├── cmd/
│   ├── server/main.go               # API entry point: config, logger, store choice, Redis, Kafka relay, HTTP and gRPC servers, graceful shutdown
│   ├── notifier/main.go             # consumer entry point: starts NOTIFIER_CONSUMERS consumers in one group, shared DLQ writer
│   ├── embedder/main.go             # embedder entry point: Ollama startup check, then a consumer group that stores incident vectors
│   ├── ingest/main.go               # runbook ingest command: -dir, -dry-run, -force
│   └── gateway/main.go              # gateway entry point: builds the /login and API handler chains
├── runbooks/                        # sample Markdown runbooks (DNS latency, disk full, TLS certificate expired)
├── proto/incident/v1/incident.proto # the gRPC contract (source of truth)
├── gen/incident/v1/                 # Go code generated from the .proto (don't edit by hand)
│   ├── incident.pb.go               # message types: Incident, CreateIncidentRequest...
│   └── incident_grpc.pb.go          # IncidentServiceServer interface, client, method routing
├── migrations/
│   ├── 001_create_incidents.sql     # incidents table
│   ├── 002_create_outbox.sql        # outbox table for events waiting to go to Kafka
│   ├── 003_create_processed_events.sql  # events a consumer has already handled
│   ├── 004_create_incident_embeddings.sql  # pgvector extension + one vector per incident
│   └── 005_create_runbook_chunks.sql   # one vector per runbook section
├── out/                             # notifier output files (created at runtime, ignored by git)
└── internal/
    ├── config/config.go             # reads all env vars (see Configuration)
    ├── model/incident.go            # Incident struct, allowed severity/status values, Validate (shared by HTTP and gRPC)
    ├── event/event.go               # event types, topic and DLQ topic names, the Kafka message format
    ├── outbox/
    │   ├── kafka.go                 # Kafka producer and startup check (also used by the notifier)
    │   └── relay.go                 # moves outbox rows to Kafka
    ├── consumer/
    │   ├── consumer.go              # shared Kafka loop: fetch, duplicate check, retries, DLQ, commit
    │   └── group.go                 # runs N consumers in one group; DLQ writer
    ├── notifier/
    │   ├── process.go               # handles one event: write a line
    │   └── writer.go                # appends a line to out/consumer_<n>_hello.txt
    ├── embedding/
    │   ├── embedding.go             # Embedder interface, 768 dimensions, vector → pgvector text
    │   └── ollama.go                # Ollama client, adds the search_document / search_query prefixes
    ├── embedder/process.go          # handles one event: embed and upsert, or delete
    ├── runbook/
    │   ├── chunk.go                 # splits Markdown into sections (code-block aware)
    │   └── ingest.go                # hash check, embed, replace a file's chunks in one transaction; remove deleted files
    ├── search/search.go             # embeds the question, nearest incidents and runbook chunks
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
    │   ├── search.go                # /search: q and limit checks, 503 when search fails
    │   └── response.go              # writeJSON / writeError helpers
    ├── grpcserver/
    │   ├── incident.go              # implements IncidentService on the same store; errors → gRPC codes
    │   └── logging.go               # interceptor: one log line per gRPC call
    ├── auth/
    │   ├── users.go                 # demo users, roles, bcrypt password check
    │   ├── token.go                 # issue and verify HS256 JWTs
    │   └── roles.go                 # role ranking (viewer < responder < admin)
    ├── gateway/
    │   ├── proxy.go                 # reverse proxy to the server, 502 when it's down
    │   ├── auth.go                  # POST /login, RequireAuth middleware, claims in the context
    │   ├── rbac.go                  # RequireRole: which role each HTTP method needs
    │   └── ratelimit.go             # fixed-window rate limiter in Redis, per user or per IP
    └── middleware/logging.go        # per-request logging middleware (server and gateway)
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
                                                              │
                         Notifier (separate process) ◀────────┘ → out/consumer_<n>_hello.txt
                                                                → processed_events (Postgres)
                                                                → incident.events.dlq (on failure)
                         Embedder (separate process) ◀────────┘ → Ollama → incident_embeddings (Postgres)

go run ./cmd/ingest → runbooks/*.md → split into sections → Ollama → runbook_chunks (Postgres)

GET /search?q=... → SearchHandler → Ollama (embed the question once) ─┬─ nearest incident_embeddings
                                                                      └─ nearest runbook_chunks

Through the gateway:
DELETE /incidents/7 → gateway :8082 → Logging → RequireAuth → user limit → RequireRole (admin?) → proxy → server :8087

Over gRPC:
grpcurl / opspilot-client → :9090 → LoggingInterceptor → IncidentServer.DeleteIncident → CachedStore → PostgresStore
```

The handlers depend on the `IncidentStore` interface, not on a concrete store. `main.go` builds the chain: first the base store (`PostgresStore` or `MemoryStore`), then, if `REDIS_URL` is set, it wraps it in `CachedStore`. The handlers pass the request's `context.Context` down, so if a client disconnects, its Redis and database calls are cancelled.

## Roadmap

1. ✅ CRUD API (in-memory) + logging
2. ✅ PostgreSQL persistence, config from env vars, graceful shutdown
3. ✅ Redis caching and alert de-duplication
4. ✅ Kafka events
   - 4a. ✅ Transactional outbox and relay publishing to Kafka
   - 4b. ✅ Consumer service with retries, a dead-letter queue and idempotency
5. ✅ API gateway and gRPC
   - 5a. ✅ Gateway as a reverse proxy
   - 5b. ✅ Login and JWT tokens
   - 5c. ✅ Role-based access (RBAC)
   - 5d. ✅ Rate limiting with Redis
   - 5e. ✅ gRPC API (`.proto` contract, generated code, gRPC server) and a separate client repo
6. ✅ pgvector embeddings and document ingestion
   - 6a. ✅ pgvector in Postgres, `incident_embeddings` table
   - 6b. ✅ Embedder service with Ollama, sharing the consumer loop with the notifier
   - 6c. ✅ Runbook chunking and ingest
   - 6d. ✅ `GET /search` over incidents and runbooks
7. RAG question answering with Claude
8. MCP server exposing OpsPilot tools to AI agents
9. Observability (OpenTelemetry, Prometheus, Grafana), Docker and Kubernetes
