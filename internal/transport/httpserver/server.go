package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"scipio/gen/openapi"
	"scipio/internal/domain"
	"scipio/internal/service"
	"scipio/internal/store"

	middleware "github.com/oapi-codegen/nethttp-middleware"
)

type sagaService interface {
	StartSaga(ctx context.Context, workflow string, rawContext []byte) (string, error)
	GetSaga(ctx context.Context, sagaID string) (domain.Saga, error)
	CancelSaga(ctx context.Context, sagaID string) (domain.Saga, error)
	ListSagas(ctx context.Context, status string, limit int, offset int) ([]domain.Saga, error)
}

type Server struct {
	svc sagaService
}

func New(svc sagaService) *Server {
	return &Server{svc: svc}
}

func (s *Server) Handler() (http.Handler, error) {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		return nil, err
	}

	swagger.Servers = nil
	strictHandler := openapi.NewStrictHandler(s, nil)
	handler := openapi.Handler(strictHandler)
	validator := middleware.OapiRequestValidator(swagger)

	return validator(handler), nil
}

func (s *Server) Healthz(_ context.Context, _ openapi.HealthzRequestObject) (openapi.HealthzResponseObject, error) {
	return openapi.Healthz200JSONResponse{Status: "ok"}, nil
}

func (s *Server) StartSaga(ctx context.Context, request openapi.StartSagaRequestObject) (openapi.StartSagaResponseObject, error) {
	if request.Body == nil {
		return openapi.StartSaga400JSONResponse{Error: "invalid request body"}, nil
	}

	sagaContext := map[string]any{}
	if request.Body.Context != nil {
		sagaContext = *request.Body.Context
	}

	payload, err := json.Marshal(sagaContext)
	if err != nil {
		return openapi.StartSaga400JSONResponse{Error: "context must be valid JSON"}, nil
	}

	sagaID, startErr := s.svc.StartSaga(ctx, request.Body.Workflow, payload)
	if startErr != nil {
		switch {
		case errors.Is(startErr, service.ErrInvalidWorkflow), errors.Is(startErr, service.ErrInvalidContext):
			return openapi.StartSaga400JSONResponse{Error: startErr.Error()}, nil
		default:
			return openapi.StartSaga500JSONResponse{Error: "internal error"}, nil
		}
	}

	return openapi.StartSaga202JSONResponse{Id: sagaID}, nil
}

func (s *Server) ListSagas(ctx context.Context, request openapi.ListSagasRequestObject) (openapi.ListSagasResponseObject, error) {
	status := ""
	if request.Params.Status != nil {
		status = string(*request.Params.Status)
	}

	limit := 0
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}

	offset := 0
	if request.Params.Offset != nil {
		offset = *request.Params.Offset
	}

	sagas, err := s.svc.ListSagas(ctx, status, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidStatusFilter), errors.Is(err, service.ErrInvalidPagination):
			return openapi.ListSagas400JSONResponse{Error: err.Error()}, nil
		default:
			return openapi.ListSagas500JSONResponse{Error: "internal error"}, nil
		}
	}

	response := openapi.ListSagasResponse{Sagas: make([]openapi.Saga, 0, len(sagas))}
	for _, saga := range sagas {
		response.Sagas = append(response.Sagas, mapSaga(saga))
	}

	return openapi.ListSagas200JSONResponse(response), nil
}

func (s *Server) GetSaga(ctx context.Context, request openapi.GetSagaRequestObject) (openapi.GetSagaResponseObject, error) {
	saga, err := s.svc.GetSaga(ctx, request.Id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return openapi.GetSaga404JSONResponse{Error: err.Error()}, nil
		default:
			return openapi.GetSaga500JSONResponse{Error: "internal error"}, nil
		}
	}

	return openapi.GetSaga200JSONResponse(mapSaga(saga)), nil
}

func (s *Server) CancelSaga(ctx context.Context, request openapi.CancelSagaRequestObject) (openapi.CancelSagaResponseObject, error) {
	saga, err := s.svc.CancelSaga(ctx, request.Id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return openapi.CancelSaga404JSONResponse{Error: err.Error()}, nil
		default:
			return openapi.CancelSaga500JSONResponse{Error: "internal error"}, nil
		}
	}

	return openapi.CancelSaga202JSONResponse{Saga: mapSaga(saga)}, nil
}

func mapSaga(saga domain.Saga) openapi.Saga {
	mappedSteps := make([]openapi.SagaStep, 0, len(saga.Steps))
	for _, step := range saga.Steps {
		mappedSteps = append(mappedSteps, mapStep(step))
	}

	createdAt := saga.CreatedAt.UTC()
	updatedAt := saga.UpdatedAt.UTC()

	return openapi.Saga{
		Id:        saga.ID,
		Workflow:  saga.Workflow,
		Status:    openapi.SagaStatus(saga.Status),
		Context:   saga.Context,
		Steps:     mappedSteps,
		CreatedAt: &createdAt,
		UpdatedAt: &updatedAt,
	}
}

func mapStep(step domain.SagaStep) openapi.SagaStep {
	return openapi.SagaStep{
		Name:       step.Name,
		Status:     openapi.SagaStepStatus(step.Status),
		Attempt:    int(step.Attempt),
		StartedAt:  copyTime(step.StartedAt),
		FinishedAt: copyTime(step.FinishedAt),
		Error:      copyString(step.Error),
	}
}

func copyTime(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}

	copied := ts.UTC()
	return &copied
}

func copyString(value string) *string {
	if value == "" {
		return nil
	}

	copied := value
	return &copied
}
