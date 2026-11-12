package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	incidentv1 "opspilot/gen/incident/v1"
	"opspilot/internal/cache"
	"opspilot/internal/config"
	"opspilot/internal/event"
	"opspilot/internal/grpcserver"
	"opspilot/internal/handler"
	"opspilot/internal/middleware"
	"opspilot/internal/outbox"
	"opspilot/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	// LOG_LEVEL accepts debug, info, warn or error. An unknown value falls back to info.
	var level slog.Level
	levelErr := level.UnmarshalText([]byte(cfg.LogLevel))
	if levelErr != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	if levelErr != nil {
		slog.Warn("invalid LOG_LEVEL, using info", "value", cfg.LogLevel)
	}

	// ctx is cancelled when you press Ctrl+C (SIGINT) or Docker/K8s sends SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Choose the store: Postgres if DATABASE_URL is set, otherwise in-memory.
	var incidentStore handler.IncidentStore
	var pg *store.PostgresStore
	if cfg.DatabaseURL == "" {
		slog.Warn("DATABASE_URL not set, using in-memory store (data is lost on restart)")
		incidentStore = store.NewMemoryStore()
	} else {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pg, err = store.NewPostgresStore(connectCtx, cfg.DatabaseURL)
		cancel()
		if err != nil {
			slog.Error("could not connect to postgres", "error", err)
			os.Exit(1)
		}
		defer pg.Close() // never log DATABASE_URL: it contains the password
		incidentStore = pg
	}

	// Redis is optional: with REDIS_URL set we get caching and alert de-duplication.
	var dedup *cache.Deduper
	if cfg.RedisURL == "" {
		slog.Warn("REDIS_URL not set, caching and alert de-duplication are disabled")
	} else {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rdb, err := cache.NewRedisClient(connectCtx, cfg.RedisURL)
		cancel()
		if err != nil {
			slog.Error("could not connect to redis", "error", err)
			os.Exit(1)
		}
		defer func() {
			rdb.Close()
			slog.Info("redis connection closed")
		}()
		incidentStore = store.NewCachedStore(incidentStore, rdb, cfg.CacheTTL)
		dedup = cache.NewDeduper(rdb, cfg.DedupWindow)
	}

	// The outbox relay needs Postgres (where events are saved) and Kafka (where they go).
	var relayDone chan struct{}
	switch {
	case pg == nil:
		slog.Warn("outbox relay disabled: needs postgres (the in-memory store doesn't record events)")
	case len(cfg.KafkaBrokers) == 0:
		slog.Warn("KAFKA_BROKERS not set, outbox relay disabled (events wait in the outbox table)")
	default:
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := outbox.CheckKafka(checkCtx, cfg.KafkaBrokers, event.Topic)
		cancel()
		if err != nil {
			slog.Error("could not connect to kafka", "error", err)
			os.Exit(1)
		}
		writer := outbox.NewKafkaWriter(cfg.KafkaBrokers)
		defer func() {
			writer.Close()
			slog.Info("kafka writer closed")
		}()

		relay := outbox.NewRelay(pg.Pool(), writer, cfg.OutboxPollInterval)
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			relay.Run(ctx)
		}()
	}

	slog.Info("config loaded",
		"port", cfg.Port,
		"log_level", level.String(),
		"postgres_enabled", cfg.DatabaseURL != "",
		"redis_enabled", cfg.RedisURL != "",
		"cache_ttl", cfg.CacheTTL,
		"kafka_enabled", len(cfg.KafkaBrokers) > 0,
		"dedup_window", cfg.DedupWindow,
	)

	incidents := handler.NewIncidentHandler(incidentStore)
	alerts := handler.NewAlertHandler(incidentStore, dedup)

	r := mux.NewRouter()
	incidents.RegisterRoutes(r)
	alerts.RegisterRoutes(r)

	// The gRPC API serves the same incidents from the same store. The gateway uses it.
	// Listen before starting the goroutine, so a port that's already in use stops the
	// server right away instead of failing in the background.
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		slog.Error("could not listen for grpc", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(grpcserver.LoggingInterceptor))
	incidentv1.RegisterIncidentServiceServer(grpcSrv, grpcserver.NewIncidentServer(incidentStore))
	reflection.Register(grpcSrv) // lets tools like grpcurl list the services without the .proto file

	go func() {
		slog.Info("gRPC server starting", "addr", lis.Addr().String())
		if err := grpcSrv.Serve(lis); err != nil {
			slog.Error("grpc server failed", "error", err)
			stop()
		}
	}()

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: middleware.Logging(r), // wrap the whole router so 404/405 get logged too
	}

	// Run the server in a goroutine so main can wait for the shutdown signal.
	go func() {
		slog.Info("OpsPilot starting", "addr", "http://localhost:"+cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done() // block here until Ctrl+C

	slog.Info("shutting down, finishing in-flight requests")
	start := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
	// GracefulStop waits for in-flight gRPC calls but has no timeout of its own,
	// so force-stop if it takes longer than the HTTP shutdown deadline.
	grpcStopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
		slog.Info("grpc server stopped")
	case <-shutdownCtx.Done():
		grpcSrv.Stop()
		slog.Warn("grpc server forced to stop")
	}
	if relayDone != nil {
		<-relayDone // ctx is already cancelled, so the relay is finishing its current batch
	}
	slog.Info("server stopped", "took", time.Since(start))
	// deferred Close() calls run here: Redis first, then Postgres (reverse order of defer)
}
