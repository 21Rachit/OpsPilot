package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"opspilot/internal/event"
)

// consumerName is this service's row key in processed_events.
const consumerName = "notifier"

// permanentError marks a failure that retrying can't fix, such as a malformed message.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func permanent(err error) error { return permanentError{err} }

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Consumer reads incident events, writes a line per event to its file, and moves failures to the DLQ.
type Consumer struct {
	log         *slog.Logger // tagged with consumer_id, so you can tell the consumers apart
	reader      *kafka.Reader
	dlq         *kafka.Writer
	pool        *pgxpool.Pool
	writer      *FileWriter
	maxAttempts int
	backoff     time.Duration // wait before the 2nd attempt; doubles each time
}

func NewConsumer(id int, reader *kafka.Reader, dlq *kafka.Writer, pool *pgxpool.Pool, writer *FileWriter) *Consumer {
	return &Consumer{
		log:    slog.With("consumer_id", id),
		reader: reader, dlq: dlq, pool: pool, writer: writer,
		maxAttempts: 3, backoff: 500 * time.Millisecond,
	}
}

// fetchRetryDelay is how long to wait before fetching again after Kafka was unreachable.
const fetchRetryDelay = 2 * time.Second

// Run processes messages until ctx is cancelled. It returns an error only if it can't go on
// safely (a message failed and the DLQ is unreachable), so the service should stop and be restarted.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("notifier consumer started", "topic", event.Topic, "group", c.reader.Config().GroupID)
	for {
		// FetchMessage doesn't commit: we commit only after the message is fully handled,
		// so a crash in between means Kafka delivers it again (at-least-once).
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("notifier consumer stopped")
				return nil
			}
			// Usually Kafka is unreachable. Wait and try again instead of exiting: our offset
			// is stored in Kafka, so we resume exactly where we left off once it's back.
			c.log.Warn("notifier: fetch failed, retrying", "retry_in", fetchRetryDelay, "error", err)
			select {
			case <-time.After(fetchRetryDelay):
				continue
			case <-ctx.Done():
				c.log.Info("notifier consumer stopped")
				return nil
			}
		}

		if err := c.handle(ctx, msg); err != nil {
			if ctx.Err() != nil {
				// Shutting down mid-retry: don't commit, the message is redelivered on restart.
				c.log.Info("notifier consumer stopped, message will be redelivered", "partition", msg.Partition, "offset", msg.Offset)
				return nil
			}
			return err
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				c.log.Info("notifier consumer stopped")
				return nil
			}
			// The message was handled. If the commit is lost, Kafka redelivers it later and
			// processed_events skips it as a duplicate, so carry on.
			c.log.Warn("notifier: commit failed, message may be redelivered", "partition", msg.Partition, "offset", msg.Offset, "error", err)
		}
	}
}

// handle processes one message. A nil error means "done with it, commit the offset",
// including when the message went to the DLQ.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var ev event.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil || ev.ID == "" {
		if err == nil {
			err = errors.New("missing event_id")
		}
		return c.sendToDLQ(ctx, msg, permanent(fmt.Errorf("decode event: %w", err)), 0)
	}
	log := c.log.With("event_id", ev.ID, "type", ev.Type, "incident_id", ev.IncidentID, "partition", msg.Partition, "offset", msg.Offset)

	done, err := c.isProcessed(ctx, ev.ID)
	if err != nil {
		// Postgres is down: a duplicate notification is better than a missed one (fail open).
		log.Warn("notifier: could not check for duplicates, processing anyway", "error", err)
	}
	if done {
		log.Info("notifier: duplicate event skipped")
		return nil
	}

	attempts, err := c.processWithRetry(ctx, log, ev, msg.Partition)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		return c.sendToDLQ(ctx, msg, err, attempts)
	}

	if err := c.markProcessed(ctx, ev.ID); err != nil {
		log.Warn("notifier: could not record event as processed", "error", err)
	}
	return nil
}

// processWithRetry tries to process the event up to maxAttempts times, waiting longer after
// each failure (exponential backoff). It returns how many attempts were made.
func (c *Consumer) processWithRetry(ctx context.Context, log *slog.Logger, ev event.Event, partition int) (int, error) {
	wait := c.backoff
	for attempt := 1; ; attempt++ {
		err := c.process(ev, partition)
		if err == nil {
			log.Info("notifier: event written", "file", c.writer.Path(), "attempt", attempt)
			return attempt, nil
		}
		if isPermanent(err) || attempt == c.maxAttempts {
			return attempt, err
		}
		log.Warn("notifier: attempt failed, retrying", "attempt", attempt, "max_attempts", c.maxAttempts, "retry_in", wait, "error", err)

		select {
		case <-time.After(wait):
			wait *= 2
		case <-ctx.Done():
			return attempt, ctx.Err()
		}
	}
}

// process writes this event's line to the consumer's file.
func (c *Consumer) process(ev event.Event, partition int) error {
	line := fmt.Sprintf("hello i am a file and consumes from partition %d with event id %s", partition, ev.ID)
	return c.writer.Write(line)
}

// sendToDLQ copies the message to the dead-letter topic with headers explaining why.
func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, cause error, attempts int) error {
	headers := append([]kafka.Header{}, msg.Headers...) // copy, so the original message isn't changed
	headers = append(headers,
		kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())},
		kafka.Header{Key: "dlq_attempts", Value: []byte(strconv.Itoa(attempts))},
		kafka.Header{Key: "original_topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "original_partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafka.Header{Key: "original_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
	)
	err := c.dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
	if err != nil {
		// Without the DLQ we can neither process nor park the message. Stop instead of
		// committing past it, so it isn't lost.
		return fmt.Errorf("write to dlq: %w", err)
	}
	c.log.Error("notifier: event moved to DLQ", "partition", msg.Partition, "offset", msg.Offset,
		"attempts", attempts, "permanent", isPermanent(cause), "error", cause)
	return nil
}

func (c *Consumer) isProcessed(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer = $1 AND event_id = $2)`,
		consumerName, eventID).Scan(&exists)
	return exists, err
}

func (c *Consumer) markProcessed(ctx context.Context, eventID string) error {
	_, err := c.pool.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumerName, eventID)
	return err
}
