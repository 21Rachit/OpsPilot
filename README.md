# OpsPilot

An incident-tracking REST API written in Go, built step by step as a learning project.

OpsPilot will eventually become an AI-powered incident response copilot, with PostgreSQL, Redis, Kafka, RAG and an MCP server. Today it covers **Step 1**: CRUD operations on incidents, stored in memory, with structured request logging.

> ⚠️ Data is kept in memory only. Restarting the server deletes every incident.

## Prerequisites

- Go 1.26.5 or newer (see `go.mod`)
- `curl`, for calling the API

## Build and run

```bash
# download dependencies (gorilla/mux)
go mod download

# run the server
go run ./cmd/server

# or build every package
go build ./...

# static checks
go vet ./...
```

The server listens on **http://localhost:8087**.

## Testing

There are no automated tests yet. Use the curl commands in the [API](#api) section to test by hand.

## API

| Method | Path | Body | Success | Errors |
|---|---|---|---|---|
| `POST` | `/incidents` | `title`, `severity` | `201` created incident | `400` |
| `GET` | `/incidents` | none | `200` list of incidents, sorted by ID | |
| `GET` | `/incidents/{id}` | none | `200` the incident | `404` |
| `PUT` | `/incidents/{id}` | `title`, `severity`, `status` | `200` updated incident | `400`, `404` |
| `DELETE` | `/incidents/{id}` | none | `204` no content | `404` |

**Validation rules**

- `title`: required
- `severity`: one of `low`, `medium`, `high`, `critical`
- `status`: one of `open`, `acknowledged`, `resolved`. New incidents always start as `open`.
- `{id}` must be numeric. Non-numeric IDs never reach a handler: gorilla/mux answers with a plain-text `404`.

Errors come back as JSON: `{"error": "title is required"}`.

### Example requests

```bash
BASE=localhost:8087

# create
curl -i -X POST $BASE/incidents -H "Content-Type: application/json" \
  -d '{"title":"DNS latency high in us-east","severity":"high"}'

# list
curl -i $BASE/incidents

# get one
curl -i $BASE/incidents/1

# update
curl -i -X PUT $BASE/incidents/1 -H "Content-Type: application/json" \
  -d '{"title":"DNS latency high in us-east","severity":"critical","status":"resolved"}'

# delete
curl -i -X DELETE $BASE/incidents/1
```

Example response:

```json
{
  "id": 1,
  "title": "DNS latency high in us-east",
  "severity": "high",
  "status": "open",
  "created_at": "2026-11-01T12:31:59.254+05:30"
}
```

## Logging

The server uses Go's built-in structured logger (`log/slog`) and writes text logs to stdout.

- **Request log** (`internal/middleware/logging.go`): one line per request, with method, path, status and duration. Status 4xx logs at `WARN` and 5xx at `ERROR`. The middleware wraps the whole router, so 404 and 405 responses from gorilla/mux are logged too.
- **Event logs** (`internal/handler/incident.go`): incident created, updated or deleted, plus the reason a request was rejected.

```
level=INFO msg="incident created" id=1 severity=high
level=INFO msg=request method=POST path=/incidents status=201 duration=131.917µs remote=[::1]:53802
level=WARN msg="create: validation failed" error="severity must be low, medium, high or critical"
level=WARN msg=request method=POST path=/incidents status=400 duration=88.5µs remote=[::1]:53803
```

## Project structure

```
.
├── cmd/server/main.go            # entry point: logger setup, wiring, starts HTTP server
└── internal/
    ├── model/incident.go         # Incident struct and allowed severity/status values
    ├── store/memory.go           # in-memory store (map + mutex), ErrNotFound
    ├── handler/
    │   ├── incident.go           # routes, JSON decoding, validation, status codes
    │   └── response.go           # writeJSON / writeError helpers
    └── middleware/logging.go     # per-request logging middleware
```

**How a request flows:** `main.go` → logging middleware → gorilla/mux router → handler (validates input) → store (reads or writes data) → handler writes the JSON response → middleware logs the result.

The handler depends on an `IncidentStore` interface, not on the in-memory store directly. That lets a PostgreSQL store replace it later without changing any handler code.

## Configuration

There are no environment variables yet. The listen address (`:8087`) and log level (`INFO`) are set in `cmd/server/main.go`.

## Roadmap

1. ✅ CRUD API (in-memory) + logging
2. PostgreSQL persistence
3. Redis caching and alert de-duplication
4. Kafka / NATS events with outbox and dead-letter queue
5. API gateway: JWT auth, RBAC, rate limiting, gRPC
6. pgvector embeddings and document ingestion
7. RAG question answering with Claude
8. MCP server exposing OpsPilot tools to AI agents
9. Observability (OpenTelemetry, Prometheus, Grafana), Docker and Kubernetes
