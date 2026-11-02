package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduper drops repeated alerts. The first alert with a fingerprint claims a Redis key
// for the dedup window. Repeats inside that window get the incident ID the first one created.
type Deduper struct {
	redis  *redis.Client
	window time.Duration
}

func NewDeduper(client *redis.Client, window time.Duration) *Deduper {
	return &Deduper{redis: client, window: window}
}

func dedupKey(fingerprint string) string {
	return "alert:dedup:" + fingerprint
}

// Claim returns claimed=true if this is the first alert with this fingerprint in the window.
// Otherwise it returns the incident ID created by the first alert (0 if still being created).
func (d *Deduper) Claim(ctx context.Context, fingerprint string) (claimed bool, incidentID int, err error) {
	key := dedupKey(fingerprint)

	// SET key 0 NX EX <window>: only succeeds if the key doesn't exist yet.
	// Redis runs it atomically, so two identical alerts arriving together can't both win.
	ok, err := d.redis.SetNX(ctx, key, 0, d.window).Result()
	if err != nil {
		return false, 0, fmt.Errorf("claim fingerprint: %w", err)
	}
	if ok {
		slog.DebugContext(ctx, "dedup: fingerprint claimed", "key", key, "window", d.window)
		return true, 0, nil
	}

	id, err := d.redis.Get(ctx, key).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, 0, fmt.Errorf("read fingerprint: %w", err)
	}
	slog.DebugContext(ctx, "dedup: fingerprint already claimed", "key", key, "incident_id", id)
	return false, id, nil
}

// Attach stores the created incident ID under the fingerprint, keeping the original expiry.
func (d *Deduper) Attach(ctx context.Context, fingerprint string, incidentID int) error {
	key := dedupKey(fingerprint)
	if err := d.redis.Set(ctx, key, incidentID, redis.KeepTTL).Err(); err != nil {
		return fmt.Errorf("attach incident to fingerprint: %w", err)
	}
	slog.DebugContext(ctx, "dedup: incident attached", "key", key, "incident_id", incidentID)
	return nil
}

// Release frees the fingerprint, so a retry of a failed alert isn't treated as a duplicate.
func (d *Deduper) Release(ctx context.Context, fingerprint string) error {
	key := dedupKey(fingerprint)
	if err := d.redis.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("release fingerprint: %w", err)
	}
	slog.DebugContext(ctx, "dedup: fingerprint released", "key", key)
	return nil
}
