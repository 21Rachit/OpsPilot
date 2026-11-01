package config

import "os"

type Config struct {
	Port        string
	DatabaseURL string
	LogLevel    string // debug, info, warn, error
}

func Load() Config {
	return Config{
		Port:        getEnv("PORT", "8087"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
