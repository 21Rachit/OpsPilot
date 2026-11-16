package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"opspilot/internal/config"
	"opspilot/internal/consumer"
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
	dlq := consumer.NewDLQWriter(cfg.KafkaBrokers)
	defer func() {
		dlq.Close()
		slog.Info("dlq writer closed")
	}()

	slog.Info("notifier config loaded", "log_level", level.String(), "consumers", cfg.NotifierConsumers)

	group := consumer.Group{
		Name:    "notifier",
		Brokers: cfg.KafkaBrokers,
		Count:   cfg.NotifierConsumers,
		DLQ:     dlq,
		Pool:    pg.Pool(),
		// Each consumer writes its own file, out/consumer_<id>_hello.txt.
		NewProcess: func(id int) (consumer.ProcessFunc, error) {
			writer, err := notifier.NewFileWriter("out", strconv.Itoa(id))
			if err != nil {
				return nil, err
			}
			return notifier.NewProcess(writer), nil
		},
	}
	if !group.Run(ctx) { // until Ctrl+C, or until one consumer fails
		return 1
	}
	// deferred Close() calls run now in reverse order: DLQ writer, then Postgres
	return 0
}
