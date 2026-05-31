package service

import (
	"context"
	"errors"
	"strings"
	"sync"
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
	dispatcher := &capturingStepDispatcher{}
	svc, newErr := New(queueStore, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(
		context.Background(),
		"order_flow",
		[]byte(`{"amount": 42}`),
		[]StartSagaStep{{Name: "order_flow", GRPCTarget: "billing:9000"}},
	)
	require.NoError(t, err)

	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 2, time.Millisecond, time.Second, dispatcher)
	require.NoError(t, newRunnerErr)
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
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	createErr := queueStore.Create(context.Background(), domain.Saga{
		ID:       "saga-stale-step",
		Workflow: "recover_flow",
		Status:   domain.SagaStatusRunning,
		Context:  map[string]any{},
		Steps: []domain.SagaStep{{
			Name:       "recover_flow",
			GRPCTarget: "billing:9000",
			Status:     domain.SagaStepStatusRunning,
			Attempt:    1,
			StartedAt:  &startedAt,
		}},
		CreatedAt: now,
		UpdatedAt: now.Add(-time.Minute),
	})
	require.NoError(t, createErr)

	dispatcher := &capturingStepDispatcher{}
	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Millisecond, dispatcher)
	require.NoError(t, newRunnerErr)
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

func TestShouldDispatchSagaContextWhenRunnerExecutesConfiguredStep(t *testing.T) {
	t.Parallel()

	queueStore := newMemoryStepQueueStore()
	dispatcher := &capturingStepDispatcher{}
	svc, newErr := New(queueStore, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(
		context.Background(),
		"order_flow",
		[]byte(`{"amount": 42, "currency": "USD"}`),
		[]StartSagaStep{{Name: "charge", GRPCTarget: "billing:9000"}},
	)
	require.NoError(t, err)

	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, dispatcher)
	require.NoError(t, newRunnerErr)
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

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}
		if saga.Status != domain.SagaStatusCompleted {
			return false
		}

		calls := dispatcher.callsSnapshot()
		if len(calls) != 1 {
			return false
		}

		call := calls[0]
		amount, ok := call.Saga.Context["amount"].(float64)
		if !ok || amount != 42 {
			return false
		}

		currency, ok := call.Saga.Context["currency"].(string)
		if !ok || currency != "USD" {
			return false
		}

		return call.Saga.ID == sagaID && call.Step.Name == "charge" && call.Step.GRPCTarget == "billing:9000"
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldFailSagaWhenStepDispatchReturnsError(t *testing.T) {
	t.Parallel()

	queueStore := newMemoryStepQueueStore()
	dispatcher := &capturingStepDispatcher{err: errors.New("participant unavailable")}
	svc, newErr := New(queueStore, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(
		context.Background(),
		"order_flow",
		[]byte(`{"amount": 42}`),
		[]StartSagaStep{{Name: "charge", GRPCTarget: "billing:9000"}},
	)
	require.NoError(t, err)

	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, dispatcher)
	require.NoError(t, newRunnerErr)
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

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}
		if saga.Status != domain.SagaStatusFailed || len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		if step.Status != domain.SagaStepStatusFailed {
			return false
		}

		return strings.Contains(step.Error, "participant unavailable")
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldReturnErrStoreNotConfiguredWhenStepRunnerStoreIsNil(t *testing.T) {
	t.Parallel()

	// given
	runner, newRunnerErr := NewStepRunner(nil, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, nil)
	require.NoError(t, newRunnerErr)

	// when
	err := runner.Run(context.Background())

	// then
	require.ErrorIs(t, err, ErrStoreNotConfigured)
}

func TestShouldReturnErrCompensationNotImplementedWhenRunnerProcessesCancelingSaga(t *testing.T) {
	t.Parallel()

	queueStore := newMemoryStepQueueStore()
	now := time.Now().UTC()
	createErr := queueStore.Create(context.Background(), domain.Saga{
		ID:       "saga-canceling-step",
		Workflow: "cancel_flow",
		Status:   domain.SagaStatusCanceling,
		Context:  map[string]any{},
		Steps: []domain.SagaStep{{
			Name:       "cancel_flow",
			GRPCTarget: "billing:9000",
			Status:     domain.SagaStepStatusRunning,
			Attempt:    1,
			StartedAt:  &now,
		}},
		CreatedAt: now,
		UpdatedAt: now,
	})
	require.NoError(t, createErr)

	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, nil)
	require.NoError(t, newRunnerErr)

	err := runner.processClaimedStep(context.Background(), domain.ClaimedSagaStep{
		SagaID:    "saga-canceling-step",
		StepIndex: 0,
		Name:      "cancel_flow",
		Attempt:   1,
	})
	require.ErrorIs(t, err, ErrCompensationNotImplemented)

	saga, getErr := queueStore.Get(context.Background(), "saga-canceling-step")
	require.NoError(t, getErr)
	require.Equal(t, domain.SagaStatusCanceling, saga.Status)
	require.Len(t, saga.Steps, 1)
	require.Equal(t, domain.SagaStepStatusRunning, saga.Steps[0].Status)
}

func TestShouldReturnErrInvalidTTLWhenLockTTLIsNotPositiveInStepRunnerConstructor(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(newMemoryStepQueueStore(), lock.NewNoop(), 0, 1, time.Millisecond, time.Second, nil)

	require.ErrorIs(t, err, lock.ErrInvalidTTL)
}

func TestShouldReturnErrInvalidStepWorkersWhenStepRunnerWorkersAreNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(newMemoryStepQueueStore(), lock.NewNoop(), time.Second, 0, time.Millisecond, time.Second, nil)

	require.ErrorIs(t, err, ErrInvalidStepWorkers)
}

func TestShouldReturnErrInvalidStepPollIntervalWhenStepRunnerPollIntervalIsNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(newMemoryStepQueueStore(), lock.NewNoop(), time.Second, 1, 0, time.Second, nil)

	require.ErrorIs(t, err, ErrInvalidStepPollInterval)
}

func TestShouldReturnErrInvalidStepStaleTimeoutWhenStepRunnerStaleTimeoutIsNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(newMemoryStepQueueStore(), lock.NewNoop(), time.Second, 1, time.Millisecond, 0, nil)

	require.ErrorIs(t, err, ErrInvalidStepStaleTimeout)
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
			if !areAllPreviousStepsCompleted(saga.Steps, index) {
				continue
			}

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

func areAllPreviousStepsCompleted(steps []domain.SagaStep, currentIndex int) bool {
	for index := 0; index < currentIndex; index++ {
		if steps[index].Status != domain.SagaStepStatusCompleted {
			return false
		}
	}

	return true
}

type dispatchCall struct {
	Saga domain.Saga
	Step domain.SagaStep
}

type capturingStepDispatcher struct {
	mu    sync.Mutex
	calls []dispatchCall
	err   error
}

func (d *capturingStepDispatcher) Dispatch(_ context.Context, saga domain.Saga, step domain.SagaStep) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls = append(d.calls, dispatchCall{
		Saga: saga,
		Step: step,
	})

	return d.err
}

func (d *capturingStepDispatcher) callsSnapshot() []dispatchCall {
	d.mu.Lock()
	defer d.mu.Unlock()

	copied := make([]dispatchCall, len(d.calls))
	copy(copied, d.calls)
	return copied
}
