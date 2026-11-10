package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"opspilot/internal/config"
	"opspilot/internal/event"
	"opspilot/internal/notifier"
	"opspilot/internal/outbox"
	"opspilot/internal/store"
)

// The notifier is a separate service: it reads incident events from Kafka and writes a line
// per event to out/consumer_<n>_hello.txt. OpsPilot's API doesn't know it exists.
// It runs NOTIFIER_CONSUMERS consumers in one group, and Kafka splits the partitions between them.
func main() {
	os.Exit(run()) // run's deferred Close() calls finish before os.Exit ends the process
}

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "error", err)
		return 1
	}

	var level slog.Level
	levelErr := level.UnmarshalText([]byte(cfg.LogLevel))
	if levelErr != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	if levelErr != nil {
		slog.Warn("invalid LOG_LEVEL, using info", "value", cfg.LogLevel)
	}

	if cfg.DatabaseURL == "" || len(cfg.KafkaBrokers) == 0 {
		slog.Error("the notifier needs DATABASE_URL (to skip duplicates) and KAFKA_BROKERS")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pg, err := store.NewPostgresStore(connectCtx, cfg.DatabaseURL)
	if err == nil {
		err = outbox.CheckKafka(connectCtx, cfg.KafkaBrokers, event.Topic, event.DLQTopic)
	}
	cancel()
	if err != nil {
		slog.Error("startup failed", "error", err)
		return 1
	}
	defer pg.Close()

	// One DLQ writer and one Postgres pool are shared: both are safe to use from many goroutines.
	dlq := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        event.DLQTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer func() {
		dlq.Close()
		slog.Info("dlq writer closed")
	}()

	slog.Info("notifier config loaded", "log_level", level.String(), "consumers", cfg.NotifierConsumers)

	// If one consumer fails (e.g. the DLQ is unreachable), cancel runCtx to stop the others too,
	// so the whole service exits and gets restarted instead of running half-broken.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var wg sync.WaitGroup
	var mu sync.Mutex
	failed := false

	for i := 1; i <= cfg.NotifierConsumers; i++ {
		writer, err := notifier.NewFileWriter("out", strconv.Itoa(i))
		if err != nil {
			slog.Error("startup failed", "error", err)
			return 1
		}

		// Each consumer has its own reader, and all readers share one GroupID. Kafka sees them
		// as separate group members and gives each its own partitions, exactly as if they were
		// separate processes.
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.KafkaBrokers,
			GroupID:     "opspilot-notifier",
			Topic:       event.Topic,
			StartOffset: kafka.FirstOffset, // a brand-new group starts from the oldest message
			MaxWait:     500 * time.Millisecond,
		})
		defer func() {
			reader.Close()
			slog.Info("kafka reader closed", "consumer_id", i)
		}()

		consumer := notifier.NewConsumer(i, reader, dlq, pg.Pool(), writer)
		wg.Go(func() {
			if err := consumer.Run(runCtx); err != nil {
				slog.Error("consumer failed, stopping the notifier", "consumer_id", i, "error", err)
				mu.Lock()
				failed = true
				mu.Unlock()
				cancelRun()
			}
		})
	}

	wg.Wait() // until Ctrl+C, or until one consumer fails
	// deferred Close() calls run now in reverse order: readers, DLQ writer, then Postgres
	if failed {
		return 1
	}
	return 0
}
