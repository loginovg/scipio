package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"scipio/internal/domain"
	"scipio/internal/store/sqlc"

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

func TestShouldReturnErrInvalidSagaContextWhenRawContextExceedsSizeLimit(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"payload":"` + strings.Repeat("a", domain.MaxSagaContextBytes) + `"}`)

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func TestShouldMapStepRowsWhenStatusesAreSupported(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	rows := []sqlc.GetSagaStepsRow{
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
	rows := []sqlc.GetSagaStepsForUpdateRow{
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

	rows := []sqlc.GetSagaStepsRow{
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

	rows := []sqlc.GetSagaStepsForUpdateRow{
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

func TestShouldUpsertEachStepAndDeleteTailWhenReplacingSteps(t *testing.T) {
	t.Parallel()

	// given
	writer := &capturingStepWriter{}
	now := time.Now().UTC()
	steps := []domain.SagaStep{
		{
			Name:       "charge",
			GRPCTarget: "billing:9000",
			Status:     domain.SagaStepStatusRunning,
			Attempt:    2,
			StartedAt:  &now,
			Error:      "temporary",
		},
		{
			Name:       "reserve",
			GRPCTarget: "inventory:9000",
			Status:     domain.SagaStepStatusPending,
		},
	}

	// when
	err := replaceSteps(context.Background(), writer, "saga-1", steps)

	// then
	require.NoError(t, err)
	require.Len(t, writer.upserts, 2)
	require.Equal(t, "saga-1", writer.upserts[0].SagaID)
	require.Equal(t, int32(0), writer.upserts[0].StepIndex)
	require.Equal(t, "charge", writer.upserts[0].Name)
	require.Equal(t, int32(2), writer.upserts[0].Attempt)
	require.Equal(t, "saga-1", writer.deletedFromIndex.SagaID)
	require.Equal(t, int32(2), writer.deletedFromIndex.StepIndex)
}

func TestShouldReturnUpsertErrorWhenUpsertSagaStepFailsWhileReplacingSteps(t *testing.T) {
	t.Parallel()

	// given
	expectedErr := errors.New("upsert failed")
	writer := &capturingStepWriter{
		upsertErrAtIndex: map[int]error{
			1: expectedErr,
		},
	}
	steps := []domain.SagaStep{
		{Name: "first", GRPCTarget: "first:9000", Status: domain.SagaStepStatusPending},
		{Name: "second", GRPCTarget: "second:9000", Status: domain.SagaStepStatusPending},
	}

	// when
	err := replaceSteps(context.Background(), writer, "saga-1", steps)

	// then
	require.ErrorIs(t, err, expectedErr)
	require.Len(t, writer.upserts, 2)
	require.Equal(t, sqlc.DeleteSagaStepsFromIndexParams{}, writer.deletedFromIndex)
}

func TestShouldReturnDeleteErrorWhenDeleteSagaStepsFromIndexFailsWhileReplacingSteps(t *testing.T) {
	t.Parallel()

	// given
	expectedErr := errors.New("delete failed")
	writer := &capturingStepWriter{
		deleteErr: expectedErr,
	}
	steps := []domain.SagaStep{
		{Name: "first", GRPCTarget: "first:9000", Status: domain.SagaStepStatusPending},
	}

	// when
	err := replaceSteps(context.Background(), writer, "saga-1", steps)

	// then
	require.ErrorIs(t, err, expectedErr)
	require.Len(t, writer.upserts, 1)
	require.Equal(t, "saga-1", writer.deletedFromIndex.SagaID)
	require.Equal(t, int32(1), writer.deletedFromIndex.StepIndex)
}

type capturingStepWriter struct {
	upserts          []sqlc.UpsertSagaStepParams
	upsertErrAtIndex map[int]error
	deletedFromIndex sqlc.DeleteSagaStepsFromIndexParams
	deleteErr        error
}

func (w *capturingStepWriter) UpsertSagaStep(_ context.Context, arg sqlc.UpsertSagaStepParams) error {
	w.upserts = append(w.upserts, arg)
	if w.upsertErrAtIndex == nil {
		return nil
	}

	if err, ok := w.upsertErrAtIndex[len(w.upserts)-1]; ok {
		return err
	}

	return nil
}

func (w *capturingStepWriter) DeleteSagaStepsFromIndex(_ context.Context, arg sqlc.DeleteSagaStepsFromIndexParams) error {
	w.deletedFromIndex = arg
	return w.deleteErr
}
