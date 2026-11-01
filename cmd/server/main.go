package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"opspilot/internal/config"
	"opspilot/internal/handler"
	"opspilot/internal/middleware"
	"opspilot/internal/store"
)

func main() {
	cfg := config.Load()

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
	if cfg.DatabaseURL == "" {
		slog.Warn("DATABASE_URL not set, using in-memory store (data is lost on restart)")
		incidentStore = store.NewMemoryStore()
	} else {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pg, err := store.NewPostgresStore(connectCtx, cfg.DatabaseURL)
		cancel()
		if err != nil {
			slog.Error("could not connect to postgres", "error", err)
			os.Exit(1)
		}
		defer pg.Close() // never log DATABASE_URL: it contains the password
		incidentStore = pg
	}

	slog.Info("config loaded", "port", cfg.Port, "log_level", level.String())

	h := handler.NewIncidentHandler(incidentStore)

	r := mux.NewRouter()
	h.RegisterRoutes(r)

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
	slog.Info("server stopped", "took", time.Since(start))
	// deferred pg.Close() runs here and closes database connections
}
