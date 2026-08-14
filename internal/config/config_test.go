package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Load_ApplyDefaultsWhenEnvironmentVariablesAreAbsent(t *testing.T) {
	resetConfigEnv(t)

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, Runtime{
		GRPCPort:                 defaultGRPCPort,
		HTTPPort:                 defaultHTTPPort,
		StepWorkers:              defaultStepWorkers,
		StepPollInterval:         defaultStepPollInterval,
		StepStaleTimeout:         defaultStepStaleTimeout,
		LockTTL:                  defaultLockTTL,
		LockRetryInterval:        defaultLockRetryInterval,
		PostgresConnectionString: defaultPostgresConnectionString,
		RedisConnectionString:    defaultRedisConnectionString,
	}, cfg)
}

func Test_Load_ParseConfiguredValuesWhenEnvironmentVariablesArePresent(t *testing.T) {
	resetConfigEnv(t)

	setEnv(t, "SCIPIO_GRPC_PORT", "19090")
	setEnv(t, "SCIPIO_HTTP_PORT", "18080")
	setEnv(t, "SCIPIO_STEP_WORKERS", "16")
	setEnv(t, "SCIPIO_STEP_POLL_INTERVAL", "150ms")
	setEnv(t, "SCIPIO_STEP_STALE_TIMEOUT", "12s")
	setEnv(t, "SCIPIO_LOCK_TTL", "3s")
	setEnv(t, "SCIPIO_LOCK_RETRY_INTERVAL", "10ms")
	setEnv(t, "PG_CONN", "postgresql://example")
	setEnv(t, "REDIS_CONN", "redis://example:6379/2")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, Runtime{
		GRPCPort:                 19090,
		HTTPPort:                 18080,
		StepWorkers:              16,
		StepPollInterval:         150 * time.Millisecond,
		StepStaleTimeout:         12 * time.Second,
		LockTTL:                  3 * time.Second,
		LockRetryInterval:        10 * time.Millisecond,
		PostgresConnectionString: "postgresql://example",
		RedisConnectionString:    "redis://example:6379/2",
	}, cfg)
}

func Test_Load_ReturnErrorWhenEnvironmentVariableValueIsInvalid(t *testing.T) {
	resetConfigEnv(t)

	setEnv(t, "SCIPIO_STEP_WORKERS", "not-a-number")

	_, err := Load()
	require.Error(t, err)
	require.ErrorContains(t, err, "env SCIPIO_STEP_WORKERS")
}

func setEnv(t *testing.T, key string, value string) {
	t.Helper()
	t.Setenv(key, value)
}

func resetConfigEnv(t *testing.T) {
	t.Helper()

	setEnv(t, "SCIPIO_GRPC_PORT", "")
	setEnv(t, "SCIPIO_HTTP_PORT", "")
	setEnv(t, "SCIPIO_STEP_WORKERS", "")
	setEnv(t, "SCIPIO_STEP_POLL_INTERVAL", "")
	setEnv(t, "SCIPIO_STEP_STALE_TIMEOUT", "")
	setEnv(t, "SCIPIO_LOCK_TTL", "")
	setEnv(t, "SCIPIO_LOCK_RETRY_INTERVAL", "")
	setEnv(t, "PG_CONN", "")
	setEnv(t, "REDIS_CONN", "")
}
