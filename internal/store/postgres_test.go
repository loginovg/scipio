package store

import (
	"testing"
	"time"

	storesqlc "scipio/internal/store/sqlc"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnContextMapWhenRawContextIsJSONObject(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"amount":42,"currency":"USD"}`)

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.NoError(t, err)
	require.Equal(t, map[string]any{"amount": float64(42), "currency": "USD"}, parsed)
}

func TestShouldReturnErrInvalidSagaContextWhenRawContextIsNullLiteral(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`null`)

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func TestShouldReturnErrInvalidSagaContextWhenRawContextIsEmpty(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte{}

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func TestShouldMapStepRowsWhenStatusesAreSupported(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	rows := []storesqlc.GetSagaStepsRow{
		{
			Name:       "charge",
			GrpcTarget: "billing:9000",
			Status:     "PENDING",
			Attempt:    3,
			StartedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			Error:      pgtype.Text{String: "boom", Valid: true},
		},
	}

	steps, err := mapStepRows(rows)

	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, rows[0].Name, steps[0].Name)
	require.Equal(t, rows[0].GrpcTarget, steps[0].GRPCTarget)
	require.Equal(t, uint32(rows[0].Attempt), steps[0].Attempt)
	require.NotNil(t, steps[0].StartedAt)
	require.Equal(t, now, steps[0].StartedAt.UTC())
	require.Equal(t, "boom", steps[0].Error)
}

func TestShouldMapStepRowsForUpdateWhenStatusesAreSupported(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	rows := []storesqlc.GetSagaStepsForUpdateRow{
		{
			Name:       "reserve",
			GrpcTarget: "inventory:9000",
			Status:     "RUNNING",
			Attempt:    2,
			FinishedAt: pgtype.Timestamptz{Time: now, Valid: true},
		},
	}

	steps, err := mapStepRowsForUpdate(rows)

	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, rows[0].Name, steps[0].Name)
	require.Equal(t, rows[0].GrpcTarget, steps[0].GRPCTarget)
	require.Equal(t, uint32(rows[0].Attempt), steps[0].Attempt)
	require.NotNil(t, steps[0].FinishedAt)
	require.Equal(t, now, steps[0].FinishedAt.UTC())
}

func TestShouldReturnErrorWhenStepRowsContainUnsupportedStatus(t *testing.T) {
	t.Parallel()

	rows := []storesqlc.GetSagaStepsRow{
		{
			Name:       "charge",
			GrpcTarget: "billing:9000",
			Status:     "UNKNOWN",
		},
	}

	steps, err := mapStepRows(rows)

	require.Nil(t, steps)
	require.Error(t, err)
}

func TestShouldReturnErrorWhenStepRowsForUpdateContainUnsupportedStatus(t *testing.T) {
	t.Parallel()

	rows := []storesqlc.GetSagaStepsForUpdateRow{
		{
			Name:       "reserve",
			GrpcTarget: "inventory:9000",
			Status:     "UNKNOWN",
		},
	}

	steps, err := mapStepRowsForUpdate(rows)

	require.Nil(t, steps)
	require.Error(t, err)
}
