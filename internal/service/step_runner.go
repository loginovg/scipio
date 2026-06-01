package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"scipio/internal/domain"
	"scipio/internal/lock"
	"scipio/internal/statemachine"
	"scipio/pkg/workerpool"
)

type stepQueueStore interface {
	sagaStore
	ClaimNextStep(ctx context.Context, staleAfter time.Duration) (domain.ClaimedSagaStep, bool, error)
}

var ErrInvalidStepWorkers = errors.New("step workers must be positive")
var ErrInvalidStepPollInterval = errors.New("step poll interval must be positive")
var ErrInvalidStepStaleTimeout = errors.New("step stale timeout must be positive")
var ErrClaimedStepIndexOutOfBounds = errors.New("claimed step index is out of bounds")

type StepRunner struct {
	store        stepQueueStore
	pollInterval time.Duration
	staleAfter   time.Duration
	dispatcher   stepDispatcher
	sagaLocker   sagaLocker
	pool         *workerpool.Pool[domain.ClaimedSagaStep, struct{}]
}

func NewStepRunner(
	store stepQueueStore,
	locker lock.Locker,
	lockTTL time.Duration,
	workers int,
	pollInterval time.Duration,
	staleAfter time.Duration,
	dispatcher stepDispatcher,
) (*StepRunner, error) {
	if lockTTL <= 0 {
		return nil, lock.ErrInvalidTTL
	}
	if workers <= 0 {
		return nil, ErrInvalidStepWorkers
	}
	if pollInterval <= 0 {
		return nil, ErrInvalidStepPollInterval
	}
	if staleAfter <= 0 {
		return nil, ErrInvalidStepStaleTimeout
	}

	if dispatcher == nil {
		dispatcher = newGRPCStepDispatcher()
	}

	runner := &StepRunner{
		store:        store,
		pollInterval: pollInterval,
		staleAfter:   staleAfter,
		dispatcher:   dispatcher,
		sagaLocker:   newSagaLocker(locker, lockTTL),
	}

	runner.pool = workerpool.New[domain.ClaimedSagaStep, struct{}](workers, func(ctx context.Context, step domain.ClaimedSagaStep) (struct{}, error) {
		return struct{}{}, runner.processClaimedStep(ctx, step)
	})

	return runner, nil
}

func (r *StepRunner) Run(ctx context.Context) error {
	if r.store == nil {
		return ErrStoreNotConfigured
	}

	resultsDone := make(chan struct{})
	go func() {
		defer close(resultsDone)
		for res := range r.pool.Results() {
			if res.Err != nil {
				slog.WarnContext(ctx, "step execution failed", "error", res.Err)
			}
		}
	}()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shutdownErr := r.pool.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.WarnContext(ctx, "failed to shutdown step runner pool", "error", shutdownErr)
		}
		<-resultsDone
	}()

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		claimed, found, err := r.store.ClaimNextStep(ctx, r.staleAfter)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			slog.WarnContext(ctx, "failed to claim saga step", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			continue
		}

		if !found {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			continue
		}

		if submitErr := r.pool.Submit(ctx, claimed); submitErr != nil {
			if errors.Is(submitErr, context.Canceled) || errors.Is(submitErr, workerpool.ErrPoolClosed) {
				return nil
			}

			slog.WarnContext(ctx, "failed to submit claimed saga step", "saga_id", claimed.SagaID, "step_index", claimed.StepIndex, "error", submitErr)
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
}

func (r *StepRunner) processClaimedStep(ctx context.Context, claimed domain.ClaimedSagaStep) error {
	return r.sagaLocker.withSagaLock(ctx, claimed.SagaID, func(lockCtx context.Context) error {
		return r.processClaimedStepLocked(lockCtx, claimed)
	})
}

func (r *StepRunner) processClaimedStepLocked(ctx context.Context, claimed domain.ClaimedSagaStep) error {
	saga, step, shouldDispatch, err := r.loadClaimedStepForDispatch(ctx, claimed)
	if err != nil {
		return err
	}
	if !shouldDispatch {
		return nil
	}

	dispatchErr := r.dispatcher.Dispatch(ctx, saga, step)
	updateErr := r.updateClaimedStepAfterDispatch(ctx, claimed, dispatchErr)
	if updateErr != nil {
		if dispatchErr != nil {
			return errors.Join(updateErr, dispatchErr)
		}
		return updateErr
	}
	if dispatchErr != nil {
		return dispatchErr
	}

	return nil
}

func (r *StepRunner) loadClaimedStepForDispatch(ctx context.Context, claimed domain.ClaimedSagaStep) (domain.Saga, domain.SagaStep, bool, error) {
	saga, err := r.store.Get(ctx, claimed.SagaID)
	if err != nil {
		return domain.Saga{}, domain.SagaStep{}, false, err
	}

	if err := r.validateClaimedStepIndex(ctx, claimed, len(saga.Steps), "claimed saga step index out of bounds"); err != nil {
		return domain.Saga{}, domain.SagaStep{}, false, err
	}

	step := saga.Steps[claimed.StepIndex]
	skipDispatch, err := shouldSkipClaimedStepDispatch(saga.Status, step.Status)
	if err != nil {
		return domain.Saga{}, domain.SagaStep{}, false, err
	}
	if skipDispatch {
		return domain.Saga{}, domain.SagaStep{}, false, nil
	}

	return saga, step, true, nil
}

func shouldSkipClaimedStepDispatch(sagaStatus domain.SagaStatus, stepStatus domain.SagaStepStatus) (bool, error) {
	switch sagaStatus {
	case domain.SagaStatusCanceling:
		return false, ErrCompensationNotImplemented
	case domain.SagaStatusCompensated, domain.SagaStatusFailed:
		return true, nil
	}

	if stepStatus != domain.SagaStepStatusPending && stepStatus != domain.SagaStepStatusRunning {
		return true, nil
	}

	return false, nil
}

func (r *StepRunner) updateClaimedStepAfterDispatch(ctx context.Context, claimed domain.ClaimedSagaStep, dispatchErr error) error {
	_, err := r.store.Update(ctx, claimed.SagaID, func(candidate *domain.Saga) error {
		if err := r.validateClaimedStepIndex(ctx, claimed, len(candidate.Steps), "claimed saga step index out of bounds during update"); err != nil {
			return err
		}

		candidateStep := &candidate.Steps[claimed.StepIndex]
		if candidateStep.Status != domain.SagaStepStatusPending && candidateStep.Status != domain.SagaStepStatusRunning {
			return nil
		}

		applyClaimedStepDispatchResult(candidate, candidateStep, dispatchErr)
		return nil
	})

	return err
}

func applyClaimedStepDispatchResult(candidate *domain.Saga, candidateStep *domain.SagaStep, dispatchErr error) {
	now := time.Now().UTC()
	if dispatchErr != nil {
		candidateStep.Status = domain.SagaStepStatusFailed
		candidateStep.FinishedAt = &now
		candidateStep.Error = dispatchErr.Error()
		if statemachine.CanTransition(candidate.Status, domain.SagaStatusFailed) {
			candidate.Status = domain.SagaStatusFailed
		}
		return
	}

	candidateStep.Status = domain.SagaStepStatusCompleted
	candidateStep.FinishedAt = &now
	candidateStep.Error = ""

	if candidate.Status == domain.SagaStatusCreated && statemachine.CanTransition(candidate.Status, domain.SagaStatusRunning) {
		candidate.Status = domain.SagaStatusRunning
	}

	if hasFailedStep(candidate.Steps) && statemachine.CanTransition(candidate.Status, domain.SagaStatusFailed) {
		candidate.Status = domain.SagaStatusFailed
		return
	}

	if allStepsCompleted(candidate.Steps) && statemachine.CanTransition(candidate.Status, domain.SagaStatusCompleted) {
		candidate.Status = domain.SagaStatusCompleted
	}
}

func (r *StepRunner) validateClaimedStepIndex(ctx context.Context, claimed domain.ClaimedSagaStep, stepsCount int, message string) error {
	if claimed.StepIndex >= 0 && claimed.StepIndex < stepsCount {
		return nil
	}

	err := fmt.Errorf("%w: saga_id=%s step_index=%d steps_count=%d", ErrClaimedStepIndexOutOfBounds, claimed.SagaID, claimed.StepIndex, stepsCount)
	slog.WarnContext(ctx, message, "saga_id", claimed.SagaID, "step_index", claimed.StepIndex, "steps_count", stepsCount, "error", err)
	return err
}

func allStepsCompleted(steps []domain.SagaStep) bool {
	if len(steps) == 0 {
		return false
	}

	for _, step := range steps {
		if step.Status != domain.SagaStepStatusCompleted {
			return false
		}
	}

	return true
}

func hasFailedStep(steps []domain.SagaStep) bool {
	for _, step := range steps {
		if step.Status == domain.SagaStepStatusFailed {
			return true
		}
	}

	return false
}
