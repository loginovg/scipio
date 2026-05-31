package service

import (
	"context"
	"errors"
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

type StepRunner struct {
	store        stepQueueStore
	pollInterval time.Duration
	staleAfter   time.Duration
	dispatcher   stepDispatcher
	logger       *slog.Logger
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
	logger *slog.Logger,
) *StepRunner {
	if logger == nil {
		logger = slog.Default()
	}
	if dispatcher == nil {
		dispatcher = newGRPCStepDispatcher()
	}

	runner := &StepRunner{
		store:        store,
		pollInterval: normalizePollInterval(pollInterval),
		staleAfter:   normalizeStaleAfter(staleAfter),
		dispatcher:   dispatcher,
		logger:       logger,
		sagaLocker:   newSagaLocker(locker, lockTTL, logger),
	}

	runner.pool = workerpool.New[domain.ClaimedSagaStep, struct{}](normalizeWorkers(workers), func(ctx context.Context, step domain.ClaimedSagaStep) (struct{}, error) {
		return struct{}{}, runner.processClaimedStep(ctx, step)
	})

	return runner
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
				r.logger.WarnContext(ctx, "step execution failed", "error", res.Err)
			}
		}
	}()

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if shutdownErr := r.pool.Shutdown(shutdownCtx); shutdownErr != nil {
			r.logger.WarnContext(ctx, "failed to shutdown step runner pool", "error", shutdownErr)
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

			r.logger.WarnContext(ctx, "failed to claim saga step", "error", err)
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

			r.logger.WarnContext(ctx, "failed to submit claimed saga step", "saga_id", claimed.SagaID, "step_index", claimed.StepIndex, "error", submitErr)
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
		saga, err := r.store.Get(lockCtx, claimed.SagaID)
		if err != nil {
			return err
		}

		if claimed.StepIndex < 0 || claimed.StepIndex >= len(saga.Steps) {
			return nil
		}

		step := saga.Steps[claimed.StepIndex]
		switch saga.Status {
		case domain.SagaStatusCanceling:
			return ErrCompensationNotImplemented
		case domain.SagaStatusCompensated, domain.SagaStatusFailed:
			return nil
		}

		if step.Status != domain.SagaStepStatusPending && step.Status != domain.SagaStepStatusRunning {
			return nil
		}

		dispatchErr := r.dispatcher.Dispatch(lockCtx, saga, step)
		_, updateErr := r.store.Update(lockCtx, claimed.SagaID, func(candidate *domain.Saga) error {
			if claimed.StepIndex < 0 || claimed.StepIndex >= len(candidate.Steps) {
				return nil
			}

			candidateStep := &candidate.Steps[claimed.StepIndex]
			if candidateStep.Status != domain.SagaStepStatusPending && candidateStep.Status != domain.SagaStepStatusRunning {
				return nil
			}

			now := time.Now().UTC()
			if dispatchErr != nil {
				candidateStep.Status = domain.SagaStepStatusFailed
				candidateStep.FinishedAt = &now
				candidateStep.Error = dispatchErr.Error()
				if statemachine.CanTransition(candidate.Status, domain.SagaStatusFailed) {
					candidate.Status = domain.SagaStatusFailed
				}
				return nil
			}

			candidateStep.Status = domain.SagaStepStatusCompleted
			candidateStep.FinishedAt = &now
			candidateStep.Error = ""

			if candidate.Status == domain.SagaStatusCreated && statemachine.CanTransition(candidate.Status, domain.SagaStatusRunning) {
				candidate.Status = domain.SagaStatusRunning
			}

			if hasFailedStep(candidate.Steps) && statemachine.CanTransition(candidate.Status, domain.SagaStatusFailed) {
				candidate.Status = domain.SagaStatusFailed
				return nil
			}

			if allStepsCompleted(candidate.Steps) && statemachine.CanTransition(candidate.Status, domain.SagaStatusCompleted) {
				candidate.Status = domain.SagaStatusCompleted
			}

			return nil
		})
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
	})
}

func normalizeWorkers(workers int) int {
	if workers <= 0 {
		return 1
	}

	return workers
}

func normalizePollInterval(pollInterval time.Duration) time.Duration {
	if pollInterval <= 0 {
		return 25 * time.Millisecond
	}

	return pollInterval
}

func normalizeStaleAfter(staleAfter time.Duration) time.Duration {
	if staleAfter <= 0 {
		return 5 * time.Second
	}

	return staleAfter
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
