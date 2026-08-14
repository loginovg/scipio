package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"scipio/internal/domain"

	"github.com/stretchr/testify/require"
)

type claimNextStepStore interface {
	Create(ctx context.Context, saga domain.Saga) error
	Get(ctx context.Context, id string) (domain.Saga, error)
	ClaimNextStep(ctx context.Context, staleAfter time.Duration) (domain.ClaimedSagaStep, bool, error)
}

type claimNextStepStoreFactory func(t *testing.T) claimNextStepStore

func Test_MemoryClaimNextStep_SatisfyContract(t *testing.T) {
	t.Parallel()

	runClaimNextStepContractTests(t, func(_ *testing.T) claimNextStepStore {
		return NewMemory()
	})
}

func runClaimNextStepContractTests(t *testing.T, newStore claimNextStepStoreFactory) {
	t.Helper()

	t.Run("TestShouldReturnNoClaimWhenStoreHasNoSagas", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.False(t, found)
		require.Equal(t, domain.ClaimedSagaStep{}, claimed)
	})

	t.Run("TestShouldClaimPendingStepWhenSagaHasPendingStep", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		sagaID := fmt.Sprintf("contract-pending-%d", time.Now().UnixNano())
		now := time.Now().UTC()
		err := store.Create(context.Background(), domain.Saga{
			ID:       sagaID,
			Workflow: "order_flow",
			Status:   domain.SagaStatusCreated,
			Context:  map[string]any{"kind": "pending"},
			Steps: []domain.SagaStep{
				{
					Name:       "charge",
					GRPCTarget: "billing:9000",
					Status:     domain.SagaStepStatusPending,
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		})
		require.NoError(t, err)

		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, sagaID, claimed.SagaID)
		require.Equal(t, 0, claimed.StepIndex)
		require.Equal(t, "charge", claimed.Name)
		require.Equal(t, uint32(1), claimed.Attempt)

		saga, err := store.Get(context.Background(), sagaID)
		require.NoError(t, err)
		require.Len(t, saga.Steps, 1)
		require.Equal(t, domain.SagaStepStatusRunning, saga.Steps[0].Status)
		require.Equal(t, uint32(1), saga.Steps[0].Attempt)
		require.NotNil(t, saga.Steps[0].StartedAt)
		require.Nil(t, saga.Steps[0].FinishedAt)
		require.Equal(t, "", saga.Steps[0].Error)
	})

	t.Run("TestShouldReclaimStaleRunningStepWhenStepBecameStale", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		sagaID := fmt.Sprintf("contract-stale-%d", time.Now().UnixNano())
		now := time.Now().UTC()
		startedAt := now.Add(-2 * time.Second)
		err := store.Create(context.Background(), domain.Saga{
			ID:       sagaID,
			Workflow: "order_flow",
			Status:   domain.SagaStatusRunning,
			Context:  map[string]any{"kind": "stale"},
			Steps: []domain.SagaStep{
				{
					Name:       "charge",
					GRPCTarget: "billing:9000",
					Status:     domain.SagaStepStatusRunning,
					Attempt:    1,
					StartedAt:  &startedAt,
				},
			},
			CreatedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		})
		require.NoError(t, err)

		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, sagaID, claimed.SagaID)
		require.Equal(t, 0, claimed.StepIndex)
		require.Equal(t, uint32(2), claimed.Attempt)

		saga, err := store.Get(context.Background(), sagaID)
		require.NoError(t, err)
		require.Len(t, saga.Steps, 1)
		require.Equal(t, domain.SagaStepStatusRunning, saga.Steps[0].Status)
		require.Equal(t, uint32(2), saga.Steps[0].Attempt)
		require.NotNil(t, saga.Steps[0].StartedAt)
	})

	t.Run("TestShouldClaimCompletedStepForCompensationWhenSagaIsCanceling", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		sagaID := fmt.Sprintf("contract-canceling-completed-%d", time.Now().UnixNano())
		now := time.Now().UTC()
		finishedAt := now.Add(-time.Second)
		err := store.Create(context.Background(), domain.Saga{
			ID:       sagaID,
			Workflow: "order_flow",
			Status:   domain.SagaStatusCanceling,
			Context:  map[string]any{"kind": "canceling"},
			Steps: []domain.SagaStep{
				{
					Name:       "charge",
					GRPCTarget: "billing:9000",
					Status:     domain.SagaStepStatusCompleted,
					Attempt:    1,
					FinishedAt: &finishedAt,
				},
			},
			CreatedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		})
		require.NoError(t, err)

		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, sagaID, claimed.SagaID)
		require.Equal(t, 0, claimed.StepIndex)
		require.Equal(t, uint32(2), claimed.Attempt)

		saga, err := store.Get(context.Background(), sagaID)
		require.NoError(t, err)
		require.Len(t, saga.Steps, 1)
		require.Equal(t, domain.SagaStepStatusCompensating, saga.Steps[0].Status)
		require.Equal(t, uint32(2), saga.Steps[0].Attempt)
		require.NotNil(t, saga.Steps[0].StartedAt)
		require.Nil(t, saga.Steps[0].FinishedAt)
		require.Equal(t, "", saga.Steps[0].Error)
	})

	t.Run("TestShouldClaimLastCompletedStepFirstWhenSagaIsCanceling", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		sagaID := fmt.Sprintf("contract-canceling-order-%d", time.Now().UnixNano())
		now := time.Now().UTC()
		finishedFirst := now.Add(-3 * time.Second)
		finishedSecond := now.Add(-2 * time.Second)
		err := store.Create(context.Background(), domain.Saga{
			ID:       sagaID,
			Workflow: "order_flow",
			Status:   domain.SagaStatusCanceling,
			Context:  map[string]any{"kind": "reverse"},
			Steps: []domain.SagaStep{
				{
					Name:       "charge",
					GRPCTarget: "billing:9000",
					Status:     domain.SagaStepStatusCompleted,
					Attempt:    1,
					FinishedAt: &finishedFirst,
				},
				{
					Name:       "reserve",
					GRPCTarget: "inventory:9000",
					Status:     domain.SagaStepStatusCompleted,
					Attempt:    1,
					FinishedAt: &finishedSecond,
				},
			},
			CreatedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		})
		require.NoError(t, err)

		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, sagaID, claimed.SagaID)
		require.Equal(t, 1, claimed.StepIndex)
		require.Equal(t, "reserve", claimed.Name)
		require.Equal(t, uint32(2), claimed.Attempt)

		saga, err := store.Get(context.Background(), sagaID)
		require.NoError(t, err)
		require.Len(t, saga.Steps, 2)
		require.Equal(t, domain.SagaStepStatusCompleted, saga.Steps[0].Status)
		require.Equal(t, domain.SagaStepStatusCompensating, saga.Steps[1].Status)
	})

	t.Run("TestShouldReclaimStaleCompensatingStepWhenCompensationBecameStale", func(t *testing.T) {
		t.Parallel()

		store := newStore(t)
		sagaID := fmt.Sprintf("contract-canceling-stale-%d", time.Now().UnixNano())
		now := time.Now().UTC()
		startedAt := now.Add(-2 * time.Second)
		err := store.Create(context.Background(), domain.Saga{
			ID:       sagaID,
			Workflow: "order_flow",
			Status:   domain.SagaStatusCanceling,
			Context:  map[string]any{"kind": "stale-compensation"},
			Steps: []domain.SagaStep{
				{
					Name:       "charge",
					GRPCTarget: "billing:9000",
					Status:     domain.SagaStepStatusCompensating,
					Attempt:    2,
					StartedAt:  &startedAt,
				},
			},
			CreatedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		})
		require.NoError(t, err)

		claimed, found, err := store.ClaimNextStep(context.Background(), time.Second)

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, sagaID, claimed.SagaID)
		require.Equal(t, 0, claimed.StepIndex)
		require.Equal(t, uint32(3), claimed.Attempt)

		saga, err := store.Get(context.Background(), sagaID)
		require.NoError(t, err)
		require.Len(t, saga.Steps, 1)
		require.Equal(t, domain.SagaStepStatusCompensating, saga.Steps[0].Status)
		require.Equal(t, uint32(3), saga.Steps[0].Attempt)
		require.NotNil(t, saga.Steps[0].StartedAt)
	})
}
