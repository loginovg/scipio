package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	sagav1 "scipio/gen/proto"
	"scipio/internal/domain"
	"scipio/internal/service"
	"scipio/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	sagav1.UnimplementedSagaServiceServer
	svc *service.Service
}

func New(svc *service.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) StartSaga(ctx context.Context, req *sagav1.StartSagaRequest) (*sagav1.StartSagaResponse, error) {
	id, err := s.svc.StartSaga(ctx, req.GetWorkflow(), req.GetContext())
	if err != nil {
		return nil, mapError(err)
	}

	return &sagav1.StartSagaResponse{Id: id}, nil
}

func (s *Server) GetSaga(ctx context.Context, req *sagav1.GetSagaRequest) (*sagav1.GetSagaResponse, error) {
	saga, err := s.svc.GetSaga(ctx, req.GetId())
	if err != nil {
		return nil, mapError(err)
	}

	mappedSaga, mapErr := mapSaga(saga)
	if mapErr != nil {
		return nil, mapError(mapErr)
	}

	return &sagav1.GetSagaResponse{Saga: mappedSaga}, nil
}

func (s *Server) CancelSaga(ctx context.Context, req *sagav1.CancelSagaRequest) (*sagav1.CancelSagaResponse, error) {
	saga, err := s.svc.CancelSaga(ctx, req.GetId())
	if err != nil {
		return nil, mapError(err)
	}

	mappedSaga, mapErr := mapSaga(saga)
	if mapErr != nil {
		return nil, mapError(mapErr)
	}

	return &sagav1.CancelSagaResponse{Saga: mappedSaga}, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrInvalidWorkflow), errors.Is(err, service.ErrInvalidContext), errors.Is(err, service.ErrInvalidStatusFilter):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func mapSaga(saga domain.Saga) (*sagav1.Saga, error) {
	serializedContext, err := json.Marshal(saga.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal saga context: %w", err)
	}

	steps := make([]*sagav1.SagaStep, 0, len(saga.Steps))
	for _, step := range saga.Steps {
		steps = append(steps, mapStep(step))
	}

	return &sagav1.Saga{
		Id:        saga.ID,
		Workflow:  saga.Workflow,
		Status:    mapSagaStatus(saga.Status),
		Context:   serializedContext,
		Steps:     steps,
		CreatedAt: saga.CreatedAt.UTC().Format(timeLayout),
		UpdatedAt: saga.UpdatedAt.UTC().Format(timeLayout),
	}, nil
}

func mapStep(step domain.SagaStep) *sagav1.SagaStep {
	startedAt := ""
	if step.StartedAt != nil {
		startedAt = step.StartedAt.UTC().Format(timeLayout)
	}

	finishedAt := ""
	if step.FinishedAt != nil {
		finishedAt = step.FinishedAt.UTC().Format(timeLayout)
	}

	return &sagav1.SagaStep{
		Name:       step.Name,
		Status:     mapStepStatus(step.Status),
		Attempt:    step.Attempt,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		Error:      step.Error,
	}
}

const timeLayout = "2006-01-02T15:04:05.999999999Z07:00"

func mapSagaStatus(statusValue domain.SagaStatus) sagav1.SagaStatus {
	switch statusValue {
	case domain.SagaStatusCreated:
		return sagav1.SagaStatus_SAGA_STATUS_CREATED
	case domain.SagaStatusRunning:
		return sagav1.SagaStatus_SAGA_STATUS_RUNNING
	case domain.SagaStatusCompleted:
		return sagav1.SagaStatus_SAGA_STATUS_COMPLETED
	case domain.SagaStatusCanceling:
		return sagav1.SagaStatus_SAGA_STATUS_CANCELING
	case domain.SagaStatusCompensated:
		return sagav1.SagaStatus_SAGA_STATUS_COMPENSATED
	case domain.SagaStatusFailed:
		return sagav1.SagaStatus_SAGA_STATUS_FAILED
	default:
		return sagav1.SagaStatus_SAGA_STATUS_UNSPECIFIED
	}
}

func mapStepStatus(statusValue domain.SagaStepStatus) sagav1.SagaStepStatus {
	switch statusValue {
	case domain.SagaStepStatusPending:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_PENDING
	case domain.SagaStepStatusRunning:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_RUNNING
	case domain.SagaStepStatusCompleted:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_COMPLETED
	case domain.SagaStepStatusCompensating:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_COMPENSATING
	case domain.SagaStepStatusCompensated:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_COMPENSATED
	case domain.SagaStepStatusFailed:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_FAILED
	default:
		return sagav1.SagaStepStatus_SAGA_STEP_STATUS_UNSPECIFIED
	}
}
