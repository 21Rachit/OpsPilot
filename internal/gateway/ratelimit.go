package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter allows at most limit requests per key in each window, for example 60 requests
// per user per minute. It uses a fixed window: one Redis counter per key per window.
type RateLimiter struct {
	rdb    *redis.Client
	name   string // part of the Redis key, so different limiters don't share counters
	limit  int
	window time.Duration
}

func NewRateLimiter(rdb *redis.Client, name string, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{rdb: rdb, name: name, limit: limit, window: window}
}

// Allow counts one request for key. It reports whether the request is within the limit,
// how many are left in this window, and how long until the window resets.
func (l *RateLimiter) Allow(ctx context.Context, key string) (allowed bool, remaining int, resetIn time.Duration, err error) {
	now := time.Now()
	windowStart := now.Truncate(l.window) // e.g. 10:42:37 → 10:42:00 for a 1m window
	redisKey := fmt.Sprintf("ratelimit:%s:%s:%d", l.name, key, windowStart.Unix())

	// MULTI/EXEC runs both commands together. INCR creates the key at 1 if it doesn't
	// exist. EXPIRE makes Redis delete it after the window, so old counters don't pile up.
	var count *redis.IntCmd
	_, err = l.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		count = pipe.Incr(ctx, redisKey)
		pipe.Expire(ctx, redisKey, l.window)
		return nil
	})
	if err != nil {
		return false, 0, 0, fmt.Errorf("count request: %w", err)
	}

	n := int(count.Val())
	return n <= l.limit, max(l.limit-n, 0), windowStart.Add(l.window).Sub(now), nil
}

// Middleware rejects requests over the limit with 429. keyFunc decides who is counted:
// UserKey counts per logged-in user, ClientIP per client address.
func (l *RateLimiter) Middleware(keyFunc func(*http.Request) string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := keyFunc(r)
		allowed, remaining, resetIn, err := l.Allow(r.Context(), key)
		if err != nil {
			// Redis is down: let the request through (fail open), as the cache does.
			// Being down for everyone would be worse than being briefly unlimited.
			slog.Warn("gateway: rate limiter unavailable, allowing request", "limiter", l.name, "error", err)
			next.ServeHTTP(w, r)
			return
		}

		// Tell the client where it stands, on every response.
		resetSecs := int(math.Ceil(resetIn.Seconds()))
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(l.limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSecs))

		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(resetSecs))
			slog.Warn("gateway: rate limit exceeded", "limiter", l.name, "key", key,
				"limit", l.limit, "window", l.window, "retry_after", resetSecs)
			writeError(w, http.StatusTooManyRequests, fmt.Sprintf("rate limit exceeded, try again in %ds", resetSecs))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// UserKey counts requests per user. It must run after RequireAuth.
func UserKey(r *http.Request) string {
	if claims, ok := ClaimsFrom(r.Context()); ok {
		return claims.Subject
	}
	return "unknown"
}

// ClientIP counts requests per client address. The gateway is the first thing clients
// reach, so RemoteAddr is the real client. X-Forwarded-For is ignored: a client can set
// it to anything and get a fresh limit every time.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr) // "[::1]:53621" → "::1"
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
