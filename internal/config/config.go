package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port        string
	GRPCPort    string // where the server accepts gRPC calls (the gateway uses it)
	DatabaseURL string
	LogLevel    string // debug, info, warn, error
	RedisURL    string
	CacheTTL    time.Duration
	DedupWindow time.Duration

	KafkaBrokers       []string
	OutboxPollInterval time.Duration

	NotifierConsumers int           // how many consumers the notifier runs; each writes consumer_<n>_hello.txt
	GatewayPort       string        // where the gateway listens
	UpstreamURL       string        // where the gateway forwards requests (the OpsPilot server)
	JWTSecret         string        // signs and verifies tokens; keep it secret, at least 32 characters
	TokenTTL          time.Duration // how long a token stays valid
	RateLimit         int           // requests each user may make per RateLimitWindow
	LoginRateLimit    int           // login attempts each client IP may make per RateLimitWindow
	RateLimitWindow   time.Duration
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
	consumers, err := getInt("NOTIFIER_CONSUMERS", 3)
	if err != nil {
		return Config{}, err
	}
	tokenTTL, err := getDuration("TOKEN_TTL", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	rateLimit, err := getInt("RATE_LIMIT", 60)
	if err != nil {
		return Config{}, err
	}
	loginRateLimit, err := getInt("LOGIN_RATE_LIMIT", 5)
	if err != nil {
		return Config{}, err
	}
	rateLimitWindow, err := getDuration("RATE_LIMIT_WINDOW", time.Minute)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Port:        getEnv("PORT", "8087"),
		GRPCPort:    getEnv("GRPC_PORT", "9090"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		RedisURL:    os.Getenv("REDIS_URL"),
		CacheTTL:    cacheTTL,
		DedupWindow: dedupWindow,

		KafkaBrokers:       getList("KAFKA_BROKERS"),
		OutboxPollInterval: pollInterval,

		NotifierConsumers: consumers,
		GatewayPort:       getEnv("GATEWAY_PORT", "8082"),
		UpstreamURL:       getEnv("UPSTREAM_URL", "http://localhost:8087"),
		JWTSecret:         os.Getenv("JWT_SECRET"),
		TokenTTL:          tokenTTL,
		RateLimit:         rateLimit,
		LoginRateLimit:    loginRateLimit,
		RateLimitWindow:   rateLimitWindow,
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

// getInt reads a positive whole number.
func getInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s=%q must be a whole number of at least 1", key, v)
	}
	return n, nil
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
