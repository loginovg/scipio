//go:build integration

package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldSatisfyClaimNextStepContractWhenUsingPostgresStore(t *testing.T) {
	t.Parallel()

	// given
	newStore := newPostgresClaimNextStepStoreForContract

	// when / then
	runClaimNextStepContractTests(t, newStore)
}

func newPostgresClaimNextStepStoreForContract(t *testing.T) claimNextStepStore {
	t.Helper()

	connectionString := strings.TrimSpace(os.Getenv("PG_CONN"))
	if connectionString == "" {
		t.Skip("PG_CONN is required for integration contract tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	postgresStore, err := NewPostgres(ctx, connectionString)
	require.NoError(t, err)
	t.Cleanup(postgresStore.Close)

	require.NoError(t, applyPostgresSchemaForContract(ctx, postgresStore))
	require.NoError(t, clearPostgresDataForContract(ctx, postgresStore))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = clearPostgresDataForContract(cleanupCtx, postgresStore)
	})

	return postgresStore
}

func applyPostgresSchemaForContract(ctx context.Context, postgresStore *Postgres) error {
	schemaFile := filepath.Join("..", "..", "sql", "schema", "sagas.sql")
	schemaSQL, err := os.ReadFile(schemaFile)
	if err != nil {
		return err
	}

	return postgresStore.Migrate(ctx, string(schemaSQL))
}

func clearPostgresDataForContract(ctx context.Context, postgresStore *Postgres) error {
	_, err := postgresStore.pool.Exec(ctx, "TRUNCATE TABLE saga_steps, sagas RESTART IDENTITY CASCADE")
	return err
}
