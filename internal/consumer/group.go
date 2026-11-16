package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"opspilot/internal/event"
)

// Group describes one service's consumer group.
type Group struct {
	Name    string // e.g. "notifier"; the Kafka group ID is "opspilot-<Name>"
	Brokers []string
	Count   int           // how many consumers to run in this process
	DLQ     *kafka.Writer // shared: safe to use from many goroutines
	Pool    *pgxpool.Pool // shared, for the duplicate check
	// NewProcess builds consumer id's ProcessFunc. It's called once per consumer, so each
	// one can have its own resources (the notifier gives each consumer its own file).
	NewProcess func(id int) (ProcessFunc, error)
}

// GroupID is the Kafka consumer group this service's consumers join.
func (g Group) GroupID() string { return "opspilot-" + g.Name }

// Run starts Count consumers and waits until ctx is cancelled or one of them fails.
// It returns false if a consumer failed (or couldn't start), so the service should exit
// non-zero and be restarted instead of running half-broken.
func (g Group) Run(ctx context.Context) bool {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := true
	fail := func() {
		mu.Lock()
		ok = false
		mu.Unlock()
		cancelRun() // stop the other consumers too
	}

	for i := 1; i <= g.Count; i++ {
		process, err := g.NewProcess(i)
		if err != nil {
			slog.Error("startup failed", "error", err)
			fail()
			break
		}

		// Each consumer has its own reader, and all readers share one GroupID. Kafka sees them
		// as separate group members and gives each its own partitions, exactly as if they were
		// separate processes.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     g.Brokers,
			GroupID:     g.GroupID(),
			Topic:       event.Topic,
			StartOffset: kafka.FirstOffset, // a brand-new group starts from the oldest message
			MaxWait:     500 * time.Millisecond,
		})
		defer func() {
			reader.Close()
			slog.Info("kafka reader closed", "consumer_id", i)
		}()

		c := New(g.Name, i, reader, g.DLQ, g.Pool, process)
		wg.Go(func() {
			if err := c.Run(runCtx); err != nil {
				slog.Error("consumer failed, stopping the "+g.Name, "consumer_id", i, "error", err)
				fail()
			}
		})
	}

	wg.Wait() // until Ctrl+C, or until one consumer fails
	// deferred reader.Close() calls run now
	return ok
}

// NewDLQWriter creates the writer every consumer uses to park failed messages.
func NewDLQWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        event.DLQTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}
