package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"scipio/internal/domain"
	"scipio/internal/lock"
	"scipio/internal/statemachine"
)

var ErrInvalidWorkflow = errors.New("workflow must not be empty")
var ErrInvalidContext = errors.New("context must be a valid JSON object")
var ErrStepsRequired = errors.New("steps must not be empty")
var ErrInvalidStepName = errors.New("step name must not be empty")
var ErrInvalidStepGRPCTarget = errors.New("step grpc target must not be empty")
var ErrInvalidStatusFilter = errors.New("invalid status filter")
var ErrInvalidPagination = errors.New("invalid pagination")
var ErrStoreNotConfigured = errors.New("saga store is not configured")

const (
	defaultPageLimit = 50
	maxPageLimit     = 200
)

type sagaStore interface {
	Create(ctx context.Context, saga domain.Saga) error
	Get(ctx context.Context, id string) (domain.Saga, error)
	List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error)
	Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error)
}

type StartSagaStep struct {
	Name       string
	GRPCTarget string
}

type Service struct {
	store      sagaStore
	sagaLocker sagaLocker
}

func New(store sagaStore, locker lock.Locker, lockTTL time.Duration, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		store:      store,
		sagaLocker: newSagaLocker(locker, lockTTL, logger),
	}
}

func (s *Service) StartSaga(ctx context.Context, workflow string, rawContext []byte, steps []StartSagaStep) (string, error) {
	if s.store == nil {
		return "", ErrStoreNotConfigured
	}

	if workflow == "" {
		return "", ErrInvalidWorkflow
	}

	sagaContext, err := normalizeContext(rawContext)
	if err != nil {
		return "", err
	}

	sagaSteps, err := validateStartSagaSteps(steps)
	if err != nil {
		return "", err
	}

	sagaID, err := generateSagaID()
	if err != nil {
		return "", err
	}

	now := time.Now().UTC()
	saga := domain.Saga{
		ID:        sagaID,
		Workflow:  workflow,
		Status:    domain.SagaStatusCreated,
		Context:   sagaContext,
		Steps:     sagaSteps,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if createErr := s.store.Create(ctx, saga); createErr != nil {
		return "", createErr
	}

	return sagaID, nil
}

func (s *Service) GetSaga(ctx context.Context, sagaID string) (domain.Saga, error) {
	if s.store == nil {
		return domain.Saga{}, ErrStoreNotConfigured
	}

	return s.store.Get(ctx, sagaID)
}

func (s *Service) CancelSaga(ctx context.Context, sagaID string) (domain.Saga, error) {
	if s.store == nil {
		return domain.Saga{}, ErrStoreNotConfigured
	}

	var saga domain.Saga
	if err := s.sagaLocker.withSagaLock(ctx, sagaID, func(lockCtx context.Context) error {
		updatedSaga, updateErr := s.store.Update(lockCtx, sagaID, func(candidate *domain.Saga) error {
			switch candidate.Status {
			case domain.SagaStatusCreated, domain.SagaStatusRunning, domain.SagaStatusCompleted:
				if statemachine.CanTransition(candidate.Status, domain.SagaStatusCanceling) {
					candidate.Status = domain.SagaStatusCanceling
				}
			case domain.SagaStatusCanceling, domain.SagaStatusCompensated, domain.SagaStatusFailed:
				return nil
			default:
				return nil
			}

			return nil
		})
		if updateErr != nil {
			return updateErr
		}

		saga = updatedSaga
		if saga.Status != domain.SagaStatusCanceling {
			return nil
		}

		compensatedSaga, compensateErr := s.store.Update(lockCtx, sagaID, func(candidate *domain.Saga) error {
			compensateSagaSteps(candidate)
			if statemachine.CanTransition(candidate.Status, domain.SagaStatusCompensated) {
				candidate.Status = domain.SagaStatusCompensated
			}

			return nil
		})
		if compensateErr != nil {
			s.sagaLocker.logger.ErrorContext(lockCtx, "failed to compensate canceled saga", "saga_id", sagaID, "error", compensateErr)
			return compensateErr
		}

		saga = compensatedSaga
		return nil
	}); err != nil {
		return domain.Saga{}, err
	}

	return saga, nil
}

func (s *Service) ListSagas(ctx context.Context, status string, limit int, offset int) ([]domain.Saga, error) {
	if s.store == nil {
		return nil, ErrStoreNotConfigured
	}

	normalizedLimit, normalizedOffset, err := normalizePage(limit, offset)
	if err != nil {
		return nil, err
	}

	normalizedStatus := strings.TrimSpace(status)
	if normalizedStatus == "" {
		return s.store.List(ctx, nil, normalizedLimit, normalizedOffset)
	}

	parsedStatus, statusErr := domain.ParseSagaStatus(normalizedStatus)
	if statusErr != nil {
		return nil, ErrInvalidStatusFilter
	}

	return s.store.List(ctx, &parsedStatus, normalizedLimit, normalizedOffset)
}

func normalizeContext(rawContext []byte) (map[string]any, error) {
	if len(rawContext) == 0 {
		return map[string]any{}, nil
	}

	var parsed map[string]any
	if err := json.Unmarshal(rawContext, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidContext, err)
	}

	if parsed == nil {
		return map[string]any{}, nil
	}

	return parsed, nil
}

func validateStartSagaSteps(requested []StartSagaStep) ([]domain.SagaStep, error) {
	if len(requested) == 0 {
		return nil, ErrStepsRequired
	}

	steps := make([]domain.SagaStep, 0, len(requested))
	for _, requestedStep := range requested {
		if requestedStep.Name == "" {
			return nil, ErrInvalidStepName
		}

		if requestedStep.GRPCTarget == "" {
			return nil, ErrInvalidStepGRPCTarget
		}

		steps = append(steps, domain.SagaStep{
			Name:       requestedStep.Name,
			GRPCTarget: requestedStep.GRPCTarget,
			Status:     domain.SagaStepStatusPending,
		})
	}

	return steps, nil
}

func generateSagaID() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}

	return hex.EncodeToString(buffer), nil
}

func normalizePage(limit int, offset int) (int, int, error) {
	if offset < 0 {
		return 0, 0, ErrInvalidPagination
	}

	if limit < 0 {
		return 0, 0, ErrInvalidPagination
	}

	if limit == 0 {
		limit = defaultPageLimit
	}

	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	return limit, offset, nil
}

func compensateSagaSteps(saga *domain.Saga) {
	now := time.Now().UTC()
	for index := range saga.Steps {
		step := &saga.Steps[index]
		switch step.Status {
		case domain.SagaStepStatusCompleted, domain.SagaStepStatusCompensated, domain.SagaStepStatusFailed:
			continue
		default:
			step.Status = domain.SagaStepStatusCompensated
			step.FinishedAt = &now
			step.Error = ""
		}
	}
}
