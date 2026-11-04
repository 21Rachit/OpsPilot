package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	Port        string
	DatabaseURL string
	LogLevel    string // debug, info, warn, error
	RedisURL    string
	CacheTTL    time.Duration
	DedupWindow time.Duration

	KafkaBrokers       []string
	OutboxPollInterval time.Duration
}

func Load() (Config, error) {
	cacheTTL, err := getDuration("CACHE_TTL", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	dedupWindow, err := getDuration("DEDUP_WINDOW", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := getDuration("OUTBOX_POLL_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Port:        getEnv("PORT", "8087"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		RedisURL:    os.Getenv("REDIS_URL"),
		CacheTTL:    cacheTTL,
		DedupWindow: dedupWindow,

		KafkaBrokers:       getList("KAFKA_BROKERS"),
		OutboxPollInterval: pollInterval,
	}, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getDuration reads values like "30s", "5m" or "1h".
func getDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a valid duration (use e.g. 30s, 5m): %w", key, v, err)
	}
	return d, nil
}

// getList reads a comma-separated value like "host1:9092,host2:9092".
func getList(key string) []string {
	var list []string
	for _, item := range strings.Split(os.Getenv(key), ",") {
		if item = strings.TrimSpace(item); item != "" {
			list = append(list, item)
		}
	}
	return list
}
