package cache

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient connects to Redis and pings it, so we fail fast if it's unreachable.
func NewRedisClient(ctx context.Context, redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	// A cache must be fast or get out of the way. Short timeouts and few retries mean
	// a broken Redis adds milliseconds to a request, not seconds.
	opts.DialTimeout = 500 * time.Millisecond
	opts.ReadTimeout = 200 * time.Millisecond
	opts.WriteTimeout = 200 * time.Millisecond
	opts.MaxRetries = 1
	opts.DialerRetries = 1

	// Route go-redis's own internal messages (e.g. "connection refused") through slog,
	// so they match the rest of our logs instead of printing in their own format.
	redis.SetLogger(slogAdapter{})

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	// Log the address only: REDIS_URL can contain a password.
	slog.Info("connected to redis",
		"addr", opts.Addr,
		"db", opts.DB,
		"pool_size", client.Options().PoolSize, // read from the client: go-redis fills in the default
		"dial_timeout", opts.DialTimeout,
		"read_timeout", opts.ReadTimeout,
	)
	return client, nil
}

// slogAdapter satisfies go-redis's logger interface. go-redis only logs problems, so use WARN.
type slogAdapter struct{}

func (slogAdapter) Printf(ctx context.Context, format string, v ...any) {
	slog.WarnContext(ctx, "redis client: "+fmt.Sprintf(format, v...))
}
