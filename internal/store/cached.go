package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"opspilot/internal/model"
)

// Store is the method set every incident store has. CachedStore wraps any Store.
type Store interface {
	Create(ctx context.Context, title, severity string) (model.Incident, error)
	Get(ctx context.Context, id int) (model.Incident, error)
	List(ctx context.Context) ([]model.Incident, error)
	Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error)
	Delete(ctx context.Context, id int) error
}

// CachedStore puts a Redis cache in front of another store (the decorator pattern).
// Only Get is cached. Update and Delete remove the cached copy so readers never see stale data.
type CachedStore struct {
	inner Store
	redis *redis.Client
	ttl   time.Duration
}

func NewCachedStore(inner Store, client *redis.Client, ttl time.Duration) *CachedStore {
	return &CachedStore{inner: inner, redis: client, ttl: ttl}
}

func incidentKey(id int) string {
	return fmt.Sprintf("incident:%d", id)
}

// Get uses the cache-aside pattern: try Redis, fall back to the inner store, then fill the cache.
func (s *CachedStore) Get(ctx context.Context, id int) (model.Incident, error) {
	key := incidentKey(id)

	data, err := s.redis.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		var inc model.Incident
		if err := json.Unmarshal(data, &inc); err == nil {
			slog.DebugContext(ctx, "cache hit", "key", key)
			return inc, nil
		}
		slog.WarnContext(ctx, "cache: unreadable value, ignoring", "key", key)
	case errors.Is(err, redis.Nil):
		slog.DebugContext(ctx, "cache miss", "key", key)
	default:
		// Redis is down or slow: carry on with the database instead of failing the request.
		slog.WarnContext(ctx, "cache: get failed, using database", "key", key, "error", err)
	}

	inc, err := s.inner.Get(ctx, id)
	if err != nil {
		return model.Incident{}, err
	}
	s.set(ctx, key, inc)
	return inc, nil
}

func (s *CachedStore) Create(ctx context.Context, title, severity string) (model.Incident, error) {
	return s.inner.Create(ctx, title, severity)
}

// List is not cached: any create, update or delete would make a cached list stale.
func (s *CachedStore) List(ctx context.Context) ([]model.Incident, error) {
	return s.inner.List(ctx)
}

func (s *CachedStore) Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error) {
	inc, err := s.inner.Update(ctx, id, title, severity, status)
	if err != nil {
		return model.Incident{}, err
	}
	s.invalidate(ctx, id)
	return inc, nil
}

func (s *CachedStore) Delete(ctx context.Context, id int) error {
	if err := s.inner.Delete(ctx, id); err != nil {
		return err
	}
	s.invalidate(ctx, id)
	return nil
}

// set saves an incident in Redis. A failure is only logged: the cache is optional.
func (s *CachedStore) set(ctx context.Context, key string, inc model.Incident) {
	data, err := json.Marshal(inc)
	if err != nil {
		slog.WarnContext(ctx, "cache: marshal failed", "key", key, "error", err)
		return
	}
	if err := s.redis.Set(ctx, key, data, s.ttl).Err(); err != nil {
		slog.WarnContext(ctx, "cache: set failed", "key", key, "error", err)
		return
	}
	slog.DebugContext(ctx, "cache set", "key", key, "ttl", s.ttl)
}

func (s *CachedStore) invalidate(ctx context.Context, id int) {
	key := incidentKey(id)
	if err := s.redis.Del(ctx, key).Err(); err != nil {
		slog.WarnContext(ctx, "cache: delete failed", "key", key, "error", err)
		return
	}
	slog.DebugContext(ctx, "cache invalidated", "key", key)
}
