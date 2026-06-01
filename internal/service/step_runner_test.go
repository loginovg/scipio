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
	queueStore := store.NewMemory()
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
	queueStore := store.NewMemory()
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

	queueStore := store.NewMemory()
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

	queueStore := store.NewMemory()
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

func TestShouldTransitionSagaToFailedWhenCompletingStepAndAnotherStepIsFailed(t *testing.T) {
	t.Parallel()

	queueStore := store.NewMemory()
	now := time.Now().UTC()
	createErr := queueStore.Create(context.Background(), domain.Saga{
		ID:       "saga-with-failed-step",
		Workflow: "order_flow",
		Status:   domain.SagaStatusRunning,
		Context:  map[string]any{},
		Steps: []domain.SagaStep{
			{
				Name:       "step-0",
				GRPCTarget: "billing:9000",
				Status:     domain.SagaStepStatusFailed,
				Attempt:    1,
				Error:      "previous failure",
			},
			{
				Name:       "step-1",
				GRPCTarget: "billing:9000",
				Status:     domain.SagaStepStatusPending,
			},
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	})
	require.NoError(t, createErr)

	dispatcher := &capturingStepDispatcher{}
	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, dispatcher)
	require.NoError(t, newRunnerErr)

	processErr := runner.processClaimedStep(context.Background(), domain.ClaimedSagaStep{
		SagaID:    "saga-with-failed-step",
		StepIndex: 1,
		Name:      "step-1",
		Attempt:   1,
	})
	require.NoError(t, processErr)

	saga, getErr := queueStore.Get(context.Background(), "saga-with-failed-step")
	require.NoError(t, getErr)
	require.Equal(t, domain.SagaStatusFailed, saga.Status)
	require.Len(t, saga.Steps, 2)
	require.Equal(t, domain.SagaStepStatusFailed, saga.Steps[0].Status)
	require.Equal(t, domain.SagaStepStatusCompleted, saga.Steps[1].Status)
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

	queueStore := store.NewMemory()
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

func TestShouldReturnErrClaimedStepIndexOutOfBoundsWhenClaimedStepIndexIsOutsideSagaSteps(t *testing.T) {
	t.Parallel()

	queueStore := store.NewMemory()
	now := time.Now().UTC()
	createErr := queueStore.Create(context.Background(), domain.Saga{
		ID:       "saga-out-of-bounds-step",
		Workflow: "order_flow",
		Status:   domain.SagaStatusCreated,
		Context:  map[string]any{},
		Steps: []domain.SagaStep{{
			Name:       "order_flow",
			GRPCTarget: "billing:9000",
			Status:     domain.SagaStepStatusPending,
		}},
		CreatedAt: now,
		UpdatedAt: now,
	})
	require.NoError(t, createErr)

	runner, newRunnerErr := NewStepRunner(queueStore, lock.NewNoop(), time.Second, 1, time.Millisecond, time.Second, nil)
	require.NoError(t, newRunnerErr)

	err := runner.processClaimedStep(context.Background(), domain.ClaimedSagaStep{
		SagaID:    "saga-out-of-bounds-step",
		StepIndex: 5,
		Name:      "order_flow",
		Attempt:   1,
	})
	require.ErrorIs(t, err, ErrClaimedStepIndexOutOfBounds)
}

func TestShouldReturnErrInvalidTTLWhenLockTTLIsNotPositiveInStepRunnerConstructor(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(store.NewMemory(), lock.NewNoop(), 0, 1, time.Millisecond, time.Second, nil)

	require.ErrorIs(t, err, lock.ErrInvalidTTL)
}

func TestShouldReturnErrInvalidStepWorkersWhenStepRunnerWorkersAreNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(store.NewMemory(), lock.NewNoop(), time.Second, 0, time.Millisecond, time.Second, nil)

	require.ErrorIs(t, err, ErrInvalidStepWorkers)
}

func TestShouldReturnErrInvalidStepPollIntervalWhenStepRunnerPollIntervalIsNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(store.NewMemory(), lock.NewNoop(), time.Second, 1, 0, time.Second, nil)

	require.ErrorIs(t, err, ErrInvalidStepPollInterval)
}

func TestShouldReturnErrInvalidStepStaleTimeoutWhenStepRunnerStaleTimeoutIsNotPositive(t *testing.T) {
	t.Parallel()

	_, err := NewStepRunner(store.NewMemory(), lock.NewNoop(), time.Second, 1, time.Millisecond, 0, nil)

	require.ErrorIs(t, err, ErrInvalidStepStaleTimeout)
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
