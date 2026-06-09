package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGRPCPort                 = 9090
	defaultHTTPPort                 = 8080
	defaultStepWorkers              = 8
	defaultStepPollInterval         = 25 * time.Millisecond
	defaultStepStaleTimeout         = 5 * time.Second
	defaultLockTTL                  = 5 * time.Second
	defaultLockRetryInterval        = 25 * time.Millisecond
	defaultPostgresConnectionString = "postgresql://scipio:scipio@127.0.0.1:5432/scipio?sslmode=disable"
	defaultRedisConnectionString    = "redis://127.0.0.1:6380/0"
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
}

func Load() (Runtime, error) {
	grpcPort, err := envVar("SCIPIO_GRPC_PORT", strconv.Atoi, defaultGRPCPort)
	if err != nil {
		return Runtime{}, err
	}

	httpPort, err := envVar("SCIPIO_HTTP_PORT", strconv.Atoi, defaultHTTPPort)
	if err != nil {
		return Runtime{}, err
	}

	stepWorkers, err := envVar("SCIPIO_STEP_WORKERS", strconv.Atoi, defaultStepWorkers)
	if err != nil {
		return Runtime{}, err
	}

	stepPollInterval, err := envVar("SCIPIO_STEP_POLL_INTERVAL", time.ParseDuration, defaultStepPollInterval)
	if err != nil {
		return Runtime{}, err
	}

	stepStaleTimeout, err := envVar("SCIPIO_STEP_STALE_TIMEOUT", time.ParseDuration, defaultStepStaleTimeout)
	if err != nil {
		return Runtime{}, err
	}

	lockTTL, err := envVar("SCIPIO_LOCK_TTL", time.ParseDuration, defaultLockTTL)
	if err != nil {
		return Runtime{}, err
	}

	lockRetryInterval, err := envVar("SCIPIO_LOCK_RETRY_INTERVAL", time.ParseDuration, defaultLockRetryInterval)
	if err != nil {
		return Runtime{}, err
	}

	postgresConnectionString, err := envVar("PG_CONN", asString, defaultPostgresConnectionString)
	if err != nil {
		return Runtime{}, err
	}

	redisConnectionString, err := envVar("REDIS_CONN", asString, defaultRedisConnectionString)
	if err != nil {
		return Runtime{}, err
	}

	return Runtime{
		GRPCPort:                 grpcPort,
		HTTPPort:                 httpPort,
		StepWorkers:              stepWorkers,
		StepPollInterval:         stepPollInterval,
		StepStaleTimeout:         stepStaleTimeout,
		LockTTL:                  lockTTL,
		LockRetryInterval:        lockRetryInterval,
		PostgresConnectionString: postgresConnectionString,
		RedisConnectionString:    redisConnectionString,
	}, nil
}

func envVar[T any](name string, parse func(string) (T, error), fallback T) (T, error) {
	raw := os.Getenv(name)
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}

	parsed, err := parse(raw)
	if err != nil {
		return fallback, fmt.Errorf("env %s: invalid value %q: %w", name, raw, err)
	}

	return parsed, nil
}

func asString(raw string) (string, error) {
	return raw, nil
}
