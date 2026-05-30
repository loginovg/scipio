package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

var ErrEnvVarIsAbsent = errors.New("env var is absent or empty")

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

func Load() (Runtime, error) {
	var err error = nil
	GRPCPort, err := envOrErr(err, "SCIPIO_GRPC_PORT", 9090)
	HTTPPort, err := envOrErr(err, "SCIPIO_HTTP_PORT", 8080)
	StepWorkers, err := envOrErr(err, "SCIPIO_STEP_WORKERS", 8)
	StepPollInterval, err := envOrErr(err, "SCIPIO_STEP_POLL_INTERVAL", 25*time.Millisecond)
	StepStaleTimeout, err := envOrErr(err, "SCIPIO_STEP_STALE_TIMEOUT", 5*time.Second)
	LockTTL, err := envOrErr(err, "SCIPIO_LOCK_TTL", 5*time.Second)
	LockRetryInterval, err := envOrErr(err, "SCIPIO_LOCK_RETRY_INTERVAL", 25*time.Millisecond)
	PostgresConnectionString, err := envOrErr(err, "PG_CONN", "postgresql://scipio:scipio@127.0.0.1:5432/scipio?sslmode=disable")
	RedisConnectionString, err := envOrErr(err, "REDIS_CONN", "redis://127.0.0.1:6380/0")
	MigrationsPath, err := envOrErr(err, "SCIPIO_MIGRATIONS_PATH", "migrations")

	if err != nil {
		return Runtime{}, err
	}

	return Runtime{
		GRPCPort:                 GRPCPort,
		HTTPPort:                 HTTPPort,
		StepWorkers:              StepWorkers,
		StepPollInterval:         StepPollInterval,
		StepStaleTimeout:         StepStaleTimeout,
		LockTTL:                  LockTTL,
		LockRetryInterval:        LockRetryInterval,
		PostgresConnectionString: PostgresConnectionString,
		RedisConnectionString:    RedisConnectionString,
		MigrationsPath:           MigrationsPath,
	}, nil
}

func envOrErr[T any](err error, name string, fallback T) (T, error) {
	if err != nil {
		return fallback, err
	}

	parse := func() any {
		switch any(fallback).(type) {
		case int:
			return strconv.Atoi
		case time.Duration:
			return time.ParseDuration
		case string:
			return func(str string) (string, error) {
				return str, nil
			}
		default:
			panic("unsupported environment variable type")
		}
	}()

	return envVar(name, parse.(func(string) (T, error)), fallback)
}

func envVar[T any](name string, parse func(string) (T, error), fallback T) (T, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, fmt.Errorf("env %s: %w", name, ErrEnvVarIsAbsent)
	}

	parsed, err := parse(raw)
	if err != nil {
		return fallback, fmt.Errorf("env %s: invalid value %q: %w", name, raw, err)
	}

	return parsed, nil
}
