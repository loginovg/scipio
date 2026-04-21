package config

import (
	"os"
	"strconv"
	"time"
)

type Runtime struct {
	GRPCPort                 int
	HTTPPort                 int
	StepWorkers              int
	StepPollInterval         time.Duration
	StepStaleTimeout         time.Duration
	LockTTL                  time.Duration
	LockRetryInterval        time.Duration
	PostgresConnectionString string
	RedisConnectionString    string
	MigrationsPath           string
}

func Load() Runtime {
	return Runtime{
		GRPCPort:                 envInt("SCIPIO_GRPC_PORT", 9090),
		HTTPPort:                 envInt("SCIPIO_HTTP_PORT", 8080),
		StepWorkers:              envInt("SCIPIO_STEP_WORKERS", 8),
		StepPollInterval:         envDuration("SCIPIO_STEP_POLL_INTERVAL", 25*time.Millisecond),
		StepStaleTimeout:         envDuration("SCIPIO_STEP_STALE_TIMEOUT", 5*time.Second),
		LockTTL:                  envDuration("SCIPIO_LOCK_TTL", 5*time.Second),
		LockRetryInterval:        envDuration("SCIPIO_LOCK_RETRY_INTERVAL", 25*time.Millisecond),
		PostgresConnectionString: envString("PG_CONN", "postgresql://scipio:scipio@127.0.0.1:5432/scipio?sslmode=disable"),
		RedisConnectionString:    envString("REDIS_CONN", "redis://127.0.0.1:6380/0"),
		MigrationsPath:           envString("SCIPIO_MIGRATIONS_PATH", "migrations"),
	}
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}

	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}

	return parsed
}

func envString(name string, fallback string) string {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	return raw
}
