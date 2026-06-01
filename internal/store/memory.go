package store

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"scipio/internal/domain"
)

var ErrNotFound = errors.New("saga not found")
var ErrAlreadyExists = errors.New("saga already exists")
var ErrInvalidPagination = errors.New("invalid pagination")

type Memory struct {
	mu    sync.RWMutex
	sagas map[string]domain.Saga
}

func NewMemory() *Memory {
	return &Memory{
		sagas: make(map[string]domain.Saga),
	}
}

func (m *Memory) Create(ctx context.Context, saga domain.Saga) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sagas[saga.ID]; exists {
		return ErrAlreadyExists
	}

	m.sagas[saga.ID] = saga.Clone()

	return nil
}

func (m *Memory) Get(ctx context.Context, id string) (domain.Saga, error) {
	select {
	case <-ctx.Done():
		return domain.Saga{}, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	saga, exists := m.sagas[id]
	if !exists {
		return domain.Saga{}, ErrNotFound
	}

	return saga.Clone(), nil
}

func (m *Memory) List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if limit < 0 || offset < 0 {
		return nil, ErrInvalidPagination
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	sagas := make([]domain.Saga, 0, len(m.sagas))
	for _, saga := range m.sagas {
		if status != nil && saga.Status != *status {
			continue
		}

		sagas = append(sagas, saga.Clone())
	}

	sort.Slice(sagas, func(i, j int) bool {
		left := sagas[i]
		right := sagas[j]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}

		return left.CreatedAt.Before(right.CreatedAt)
	})

	if offset >= len(sagas) {
		return []domain.Saga{}, nil
	}

	end := len(sagas)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	return sagas[offset:end], nil
}

func (m *Memory) Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error) {
	select {
	case <-ctx.Done():
		return domain.Saga{}, ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	saga, exists := m.sagas[id]
	if !exists {
		return domain.Saga{}, ErrNotFound
	}

	updated := saga.Clone()
	if err := fn(&updated); err != nil {
		return domain.Saga{}, err
	}

	updated.UpdatedAt = time.Now().UTC()
	m.sagas[id] = updated

	return updated.Clone(), nil
}

func (m *Memory) ClaimNextStep(ctx context.Context, staleAfter time.Duration) (domain.ClaimedSagaStep, bool, error) {
	select {
	case <-ctx.Done():
		return domain.ClaimedSagaStep{}, false, ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	orderedSagas := make([]domain.Saga, 0, len(m.sagas))
	for _, saga := range m.sagas {
		orderedSagas = append(orderedSagas, saga.Clone())
	}

	sort.Slice(orderedSagas, func(i, j int) bool {
		left := orderedSagas[i]
		right := orderedSagas[j]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}

		return left.CreatedAt.Before(right.CreatedAt)
	})

	now := time.Now().UTC()
	for _, saga := range orderedSagas {
		switch saga.Status {
		case domain.SagaStatusCreated, domain.SagaStatusRunning:
			claimed, found := claimExecutionStep(&saga, staleAfter, now)
			if found {
				m.sagas[saga.ID] = saga
				return claimed, true, nil
			}
		case domain.SagaStatusCanceling:
			claimed, found := claimCompensationStep(&saga, staleAfter, now)
			if found {
				m.sagas[saga.ID] = saga
				return claimed, true, nil
			}
		}
	}

	return domain.ClaimedSagaStep{}, false, nil
}

func claimExecutionStep(saga *domain.Saga, staleAfter time.Duration, now time.Time) (domain.ClaimedSagaStep, bool) {
	for index := range saga.Steps {
		if !allPreviousStepsAreCompleted(saga.Steps, index) {
			continue
		}

		step := &saga.Steps[index]
		isPending := step.Status == domain.SagaStepStatusPending
		if !isPending && !isStepStale(step, domain.SagaStepStatusRunning, staleAfter, now) {
			continue
		}

		step.Status = domain.SagaStepStatusRunning
		step.Attempt++
		step.StartedAt = &now
		step.FinishedAt = nil
		step.Error = ""
		saga.UpdatedAt = now

		return domain.ClaimedSagaStep{
			SagaID:    saga.ID,
			StepIndex: index,
			Name:      step.Name,
			Attempt:   step.Attempt,
		}, true
	}

	return domain.ClaimedSagaStep{}, false
}

func claimCompensationStep(saga *domain.Saga, staleAfter time.Duration, now time.Time) (domain.ClaimedSagaStep, bool) {
	for index := len(saga.Steps) - 1; index >= 0; index-- {
		if !allNextStepsAreSettledForCompensation(saga.Steps, index) {
			continue
		}

		step := &saga.Steps[index]
		isCompleted := step.Status == domain.SagaStepStatusCompleted
		isRunning := step.Status == domain.SagaStepStatusRunning
		isStaleCompensating := isStepStale(step, domain.SagaStepStatusCompensating, staleAfter, now)
		if !isCompleted && !isRunning && !isStaleCompensating {
			continue
		}

		step.Status = domain.SagaStepStatusCompensating
		step.Attempt++
		step.StartedAt = &now
		step.FinishedAt = nil
		step.Error = ""
		saga.UpdatedAt = now

		return domain.ClaimedSagaStep{
			SagaID:    saga.ID,
			StepIndex: index,
			Name:      step.Name,
			Attempt:   step.Attempt,
		}, true
	}

	return domain.ClaimedSagaStep{}, false
}

func isStepStale(step *domain.SagaStep, expectedStatus domain.SagaStepStatus, staleAfter time.Duration, now time.Time) bool {
	return step.Status == expectedStatus &&
		step.StartedAt != nil &&
		now.Sub(step.StartedAt.UTC()) >= staleAfter
}

func allPreviousStepsAreCompleted(steps []domain.SagaStep, currentIndex int) bool {
	for index := 0; index < currentIndex; index++ {
		if steps[index].Status != domain.SagaStepStatusCompleted {
			return false
		}
	}

	return true
}

func allNextStepsAreSettledForCompensation(steps []domain.SagaStep, currentIndex int) bool {
	for index := currentIndex + 1; index < len(steps); index++ {
		if steps[index].Status == domain.SagaStepStatusCompleted ||
			steps[index].Status == domain.SagaStepStatusRunning ||
			steps[index].Status == domain.SagaStepStatusCompensating {
			return false
		}
	}

	return true
}
