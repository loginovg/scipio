package service

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"scipio/internal/domain"
	"scipio/internal/lock"
	"scipio/internal/store"

	"github.com/stretchr/testify/require"
)

func TestShouldCompleteSagaWhenRunnerDrainsPendingStep(t *testing.T) {
	t.Parallel()

	// given
	queueStore := newMemoryStepQueueStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := NewWithLock(queueStore, lock.NewNoop(), time.Second, logger)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount": 42}`))
	require.NoError(t, err)

	runner := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 2, time.Millisecond, time.Second, logger)
	runnerCtx, cancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	runnerErr := make(chan error, 1)
	go func() {
		defer close(runnerDone)
		runnerErr <- runner.Run(runnerCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-runnerDone
		require.NoError(t, <-runnerErr)
	})

	// when
	// then
	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		if saga.Status != domain.SagaStatusCompleted || len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		return step.Status == domain.SagaStepStatusCompleted && step.Attempt == 1 && step.FinishedAt != nil
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldRecoverRunningStepWhenRunnerDetectsStaleExecution(t *testing.T) {
	t.Parallel()

	// given
	queueStore := newMemoryStepQueueStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	createErr := queueStore.Create(context.Background(), domain.Saga{
		ID:        "saga-stale-step",
		Workflow:  "recover_flow",
		Status:    domain.SagaStatusRunning,
		Context:   map[string]any{},
		Steps:     []domain.SagaStep{{Name: "recover_flow", Status: domain.SagaStepStatusRunning, Attempt: 1, StartedAt: &startedAt}},
		CreatedAt: now,
		UpdatedAt: now.Add(-time.Minute),
	})
	require.NoError(t, createErr)

	runner := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Millisecond, logger)
	runnerCtx, cancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	runnerErr := make(chan error, 1)
	go func() {
		defer close(runnerDone)
		runnerErr <- runner.Run(runnerCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-runnerDone
		require.NoError(t, <-runnerErr)
	})

	// when
	// then
	require.Eventually(t, func() bool {
		saga, getErr := queueStore.Get(context.Background(), "saga-stale-step")
		if getErr != nil {
			return false
		}

		if saga.Status != domain.SagaStatusCompleted || len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		return step.Status == domain.SagaStepStatusCompleted && step.Attempt >= 2 && step.FinishedAt != nil
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldReturnErrStoreNotConfiguredWhenStepRunnerStoreIsNil(t *testing.T) {
	t.Parallel()

	// given
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := NewStepRunner(nil, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, logger)

	// when
	err := runner.Run(context.Background())

	// then
	require.ErrorIs(t, err, ErrStoreNotConfigured)
}

type memoryStepQueueStore struct {
	inner *store.Memory
}

func newMemoryStepQueueStore() *memoryStepQueueStore {
	return &memoryStepQueueStore{inner: store.NewMemory()}
}

func (m *memoryStepQueueStore) Create(ctx context.Context, saga domain.Saga) error {
	return m.inner.Create(ctx, saga)
}

func (m *memoryStepQueueStore) Get(ctx context.Context, id string) (domain.Saga, error) {
	return m.inner.Get(ctx, id)
}

func (m *memoryStepQueueStore) List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error) {
	return m.inner.List(ctx, status, limit, offset)
}

func (m *memoryStepQueueStore) Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error) {
	return m.inner.Update(ctx, id, fn)
}

func (m *memoryStepQueueStore) ClaimNextStep(ctx context.Context, staleAfter time.Duration) (domain.ClaimedSagaStep, bool, error) {
	sagas, err := m.inner.List(ctx, nil, 1000, 0)
	if err != nil {
		return domain.ClaimedSagaStep{}, false, err
	}

	for _, saga := range sagas {
		if saga.Status != domain.SagaStatusCreated && saga.Status != domain.SagaStatusRunning {
			continue
		}

		for index, step := range saga.Steps {
			isPending := step.Status == domain.SagaStepStatusPending
			isRunningAndStale := step.Status == domain.SagaStepStatusRunning &&
				staleAfter > 0 &&
				step.StartedAt != nil &&
				time.Since(step.StartedAt.UTC()) >= staleAfter
			if !isPending && !isRunningAndStale {
				continue
			}

			claimed := domain.ClaimedSagaStep{
				SagaID:    saga.ID,
				StepIndex: index,
				Name:      step.Name,
				Attempt:   step.Attempt + 1,
			}

			_, updateErr := m.inner.Update(ctx, saga.ID, func(candidate *domain.Saga) error {
				if index < 0 || index >= len(candidate.Steps) {
					return nil
				}

				candidateStep := &candidate.Steps[index]
				if candidateStep.Status != domain.SagaStepStatusPending && candidateStep.Status != domain.SagaStepStatusRunning {
					return nil
				}

				now := time.Now().UTC()
				candidateStep.Status = domain.SagaStepStatusRunning
				candidateStep.Attempt++
				candidateStep.StartedAt = &now
				candidateStep.FinishedAt = nil
				candidateStep.Error = ""
				return nil
			})
			if updateErr != nil {
				return domain.ClaimedSagaStep{}, false, updateErr
			}

			return claimed, true, nil
		}
	}

	return domain.ClaimedSagaStep{}, false, nil
}
