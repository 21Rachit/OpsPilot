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

	"opspilot/internal/auth"
	"opspilot/internal/cache"
	"opspilot/internal/config"
	"opspilot/internal/gateway"
	"opspilot/internal/middleware"
)

// The gateway is the single entry point for clients. It checks who the user is (JWT),
// what they may do (roles) and how often (rate limits), then forwards the request to the
// OpsPilot server, so the server itself doesn't have to deal with any of that.
func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
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

	proxy, err := gateway.NewProxy(cfg.UpstreamURL)
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	tokens, err := auth.NewTokenService(cfg.JWTSecret, cfg.TokenTTL)
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	users, err := auth.NewDemoUsers()
	if err != nil {
		slog.Error("could not create users", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build the two handler chains. Each wrapper runs before the handler it wraps.
	login := http.Handler(gateway.LoginHandler(users, tokens))
	api := gateway.RequireRole(proxy)

	// Rate limits need Redis, where the counters live. Without REDIS_URL they're off.
	if cfg.RedisURL == "" {
		slog.Warn("REDIS_URL not set, rate limiting is disabled")
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

		// Per user for the API. Per client IP for /login, because there's no user yet,
		// and a low limit there makes guessing passwords very slow.
		userLimit := gateway.NewRateLimiter(rdb, "user", cfg.RateLimit, cfg.RateLimitWindow)
		loginLimit := gateway.NewRateLimiter(rdb, "login", cfg.LoginRateLimit, cfg.RateLimitWindow)
		api = userLimit.Middleware(gateway.UserKey, api)
		login = loginLimit.Middleware(gateway.ClientIP, login)

		slog.Info("rate limiting enabled", "per_user", cfg.RateLimit, "login_per_ip", cfg.LoginRateLimit, "window", cfg.RateLimitWindow)
	}

	// POST /login:  [login limit] → LoginHandler
	// everything else: RequireAuth → [user limit] → RequireRole → proxy
	mux := http.NewServeMux()
	mux.Handle("POST /login", login)
	mux.Handle("/", gateway.RequireAuth(tokens, api))

	srv := &http.Server{
		Addr:              ":" + cfg.GatewayPort,
		Handler:           middleware.Logging(mux),
		ReadHeaderTimeout: 5 * time.Second, // drop clients that open a connection and never send headers
	}

	go func() {
		slog.Info("gateway starting", "addr", "http://localhost:"+cfg.GatewayPort, "upstream", cfg.UpstreamURL, "token_ttl", cfg.TokenTTL)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("gateway failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()

	slog.Info("gateway shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
	slog.Info("gateway stopped")
	// deferred Close() runs here: Redis
}
