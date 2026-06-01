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

	runClaimNextStepContractTests(t, newPostgresClaimNextStepStoreForContract)
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

	require.NoError(t, applyPostgresMigrationsForContract(ctx, postgresStore))
	require.NoError(t, clearPostgresDataForContract(ctx, postgresStore))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = clearPostgresDataForContract(cleanupCtx, postgresStore)
	})

	return postgresStore
}

func applyPostgresMigrationsForContract(ctx context.Context, postgresStore *Postgres) error {
	migrationFiles, err := filepath.Glob(filepath.Join("..", "..", "migrations", "psql", "V*.sql"))
	if err != nil {
		return err
	}
	if len(migrationFiles) == 0 {
		return os.ErrNotExist
	}

	for _, migrationFile := range migrationFiles {
		migrationSQL, err := os.ReadFile(migrationFile)
		if err != nil {
			return err
		}

		if err := postgresStore.Migrate(ctx, string(migrationSQL)); err != nil {
			return err
		}
	}

	return nil
}

func clearPostgresDataForContract(ctx context.Context, postgresStore *Postgres) error {
	_, err := postgresStore.pool.Exec(ctx, "TRUNCATE TABLE saga_steps, sagas RESTART IDENTITY CASCADE")
	return err
}
