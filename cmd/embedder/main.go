package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"opspilot/internal/config"
	"opspilot/internal/consumer"
	"opspilot/internal/embedder"
	"opspilot/internal/embedding"
	"opspilot/internal/event"
	"opspilot/internal/outbox"
	"opspilot/internal/store"
)

// The embedder is a separate service, like the notifier: it reads incident events from Kafka,
// turns each incident's text into a vector with the embedding model (Ollama), and stores it in
// incident_embeddings, ready for semantic search. OpsPilot's API doesn't know it exists, so
// a slow or stopped model never slows down creating incidents.
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
		slog.Error("the embedder needs DATABASE_URL (to store vectors) and KAFKA_BROKERS")
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

	// Embed one test sentence now, so a missing model or a stopped Ollama shows up at startup
	// with a clear message, instead of as every event failing into the DLQ.
	// 60s, because the first call loads the model into memory.
	emb := embedding.NewOllama(cfg.OllamaURL, cfg.EmbeddingModel)
	checkCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	_, err = emb.EmbedDocument(checkCtx, "startup check")
	cancel()
	if err != nil {
		slog.Error("startup failed: embedding model not ready (is Ollama running? did you run 'ollama pull "+cfg.EmbeddingModel+"'?)",
			"ollama_url", cfg.OllamaURL, "model", cfg.EmbeddingModel, "error", err)
		return 1
	}

	dlq := consumer.NewDLQWriter(cfg.KafkaBrokers)
	defer func() {
		dlq.Close()
		slog.Info("dlq writer closed")
	}()

	slog.Info("embedder config loaded", "log_level", level.String(), "consumers", cfg.EmbedderConsumers,
		"ollama_url", cfg.OllamaURL, "model", cfg.EmbeddingModel, "dims", embedding.Dimensions)

	// One Processor is shared by all consumers: it holds nothing but the model client and the pool.
	processor := embedder.NewProcessor(emb, pg.Pool())
	group := consumer.Group{
		Name:    "embedder",
		Brokers: cfg.KafkaBrokers,
		Count:   cfg.EmbedderConsumers,
		DLQ:     dlq,
		Pool:    pg.Pool(),
		NewProcess: func(int) (consumer.ProcessFunc, error) {
			return processor.Process, nil
		},
	}
	if !group.Run(ctx) { // until Ctrl+C, or until one consumer fails
		return 1
	}
	// deferred Close() calls run now in reverse order: DLQ writer, then Postgres
	return 0
}
