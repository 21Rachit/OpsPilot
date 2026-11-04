package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"opspilot/internal/event"
)

// Relay moves events from the outbox table to Kafka.
type Relay struct {
	pool      *pgxpool.Pool
	writer    *kafka.Writer
	interval  time.Duration
	batchSize int
}

func NewRelay(pool *pgxpool.Pool, writer *kafka.Writer, interval time.Duration) *Relay {
	return &Relay{pool: pool, writer: writer, interval: interval, batchSize: 100}
}

// Run checks the outbox every interval until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	slog.Info("outbox relay started", "interval", r.interval, "batch_size", r.batchSize)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("outbox relay stopped")
			return
		case <-ticker.C:
			n, err := r.publishBatch(ctx)
			if err != nil {
				if ctx.Err() == nil { // don't warn about errors caused by shutting down
					slog.Warn("outbox relay: publish failed, will retry", "error", err)
				}
				continue
			}
			if n > 0 {
				slog.Info("outbox relay: events published", "count", n)
			}
		}
	}
}

// publishBatch sends up to batchSize unpublished events to Kafka and marks them published.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// FOR UPDATE SKIP LOCKED: if several OpsPilot servers run a relay, each one
	// locks and takes different rows instead of publishing the same events twice.
	rows, err := tx.Query(ctx,
		`SELECT id, event_id::text, event_type, incident_id, payload, created_at
		 FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}

	var ids []int64
	var msgs []kafka.Message
	for rows.Next() {
		var id int64
		var ev event.Event
		if err := rows.Scan(&id, &ev.ID, &ev.Type, &ev.IncidentID, &ev.Data, &ev.OccurredAt); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		value, err := json.Marshal(ev)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("marshal event %s: %w", ev.ID, err)
		}
		ids = append(ids, id)
		msgs = append(msgs, kafka.Message{
			Key:     []byte(strconv.Itoa(ev.IncidentID)),
			Value:   value,
			Headers: []kafka.Header{{Key: "event_type", Value: []byte(ev.Type)}},
		})
	}
	rows.Close() // must close before running another query on the same transaction
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
		return 0, fmt.Errorf("write to kafka: %w", err)
	}

	// If we crash right here, Kafka has the events but they're still unpublished in the
	// outbox, so they'll be sent again. That's "at-least-once" delivery: consumers must
	// handle duplicates, which is what Event.ID is for.
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(msgs), nil
}
