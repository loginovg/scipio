package httpserver

import (
	"context"
	"encoding/json"
	"testing"

	"scipio/gen/openapi"
	"scipio/internal/domain"
	"scipio/internal/service"

	"github.com/stretchr/testify/require"
)

func TestShouldReturn404WhenGetSagaReturnsNotFound(t *testing.T) {
	t.Parallel()

	srv := New(&stubSagaService{
		getSagaFn: func(context.Context, string) (domain.Saga, error) {
			return domain.Saga{}, service.ErrSagaNotFound
		},
	})

	response, err := srv.GetSaga(context.Background(), openapi.GetSagaRequestObject{Id: "missing"})

	require.NoError(t, err)
	notFoundResponse, ok := response.(openapi.GetSaga404JSONResponse)
	require.True(t, ok)
	require.Equal(t, service.ErrSagaNotFound.Error(), notFoundResponse.Error)
}

func TestShouldReturn400WhenListSagasReturnsInvalidPagination(t *testing.T) {
	t.Parallel()

	srv := New(&stubSagaService{
		listSagasFn: func(context.Context, string, int, int) ([]domain.Saga, error) {
			return nil, service.ErrInvalidPagination
		},
	})

	response, err := srv.ListSagas(context.Background(), openapi.ListSagasRequestObject{})

	require.NoError(t, err)
	badRequestResponse, ok := response.(openapi.ListSagas400JSONResponse)
	require.True(t, ok)
	require.Equal(t, service.ErrInvalidPagination.Error(), badRequestResponse.Error)
}

func TestShouldReturn409WhenCancelSagaReturnsLockContended(t *testing.T) {
	t.Parallel()

	srv := New(&stubSagaService{
		cancelSagaFn: func(context.Context, string) (domain.Saga, error) {
			return domain.Saga{}, service.ErrSagaLockContended
		},
	})

	response, err := srv.CancelSaga(context.Background(), openapi.CancelSagaRequestObject{Id: "busy"})

	require.NoError(t, err)
	conflictResponse, ok := response.(openapi.CancelSaga409JSONResponse)
	require.True(t, ok)
	require.Equal(t, service.ErrSagaLockContended.Error(), conflictResponse.Error)
}

func TestShouldReturn409WhenCancelSagaReturnsCancelNotAllowed(t *testing.T) {
	t.Parallel()

	srv := New(&stubSagaService{
		cancelSagaFn: func(context.Context, string) (domain.Saga, error) {
			return domain.Saga{}, service.ErrSagaCancelNotAllowed
		},
	})

	response, err := srv.CancelSaga(context.Background(), openapi.CancelSagaRequestObject{Id: "failed"})

	require.NoError(t, err)
	conflictResponse, ok := response.(openapi.CancelSaga409JSONResponse)
	require.True(t, ok)
	require.Equal(t, service.ErrSagaCancelNotAllowed.Error(), conflictResponse.Error)
}

func TestShouldPassIdempotencyKeyWhenStartSagaRequestContainsIdempotencyKey(t *testing.T) {
	t.Parallel()

	capturedKey := ""
	capturedContext := []byte(nil)
	srv := New(&stubSagaService{
		startSagaFn: func(_ context.Context, _ string, idempotencyKey string, rawContext []byte, _ []service.StartSagaStep) (string, error) {
			capturedKey = idempotencyKey
			capturedContext = rawContext
			return "saga-id-1", nil
		},
	})

	idempotencyKey := "request-key-1"
	request := openapi.StartSagaRequestObject{
		Body: &openapi.StartSagaRequest{
			Workflow:       "order_flow",
			IdempotencyKey: &idempotencyKey,
			Context:        &map[string]any{"amount": 42},
			Steps:          []openapi.StartSagaStep{{Name: "charge", GrpcTarget: "billing:9000"}},
		},
	}

	response, err := srv.StartSaga(context.Background(), request)

	require.NoError(t, err)
	startedResponse, ok := response.(openapi.StartSaga202JSONResponse)
	require.True(t, ok)
	require.Equal(t, "saga-id-1", startedResponse.Id)
	require.Equal(t, idempotencyKey, capturedKey)

	parsedContext := map[string]any{}
	require.NoError(t, json.Unmarshal(capturedContext, &parsedContext))
	require.Equal(t, map[string]any{"amount": float64(42)}, parsedContext)
}

type stubSagaService struct {
	startSagaFn  func(ctx context.Context, workflow string, idempotencyKey string, rawContext []byte, steps []service.StartSagaStep) (string, error)
	getSagaFn    func(ctx context.Context, sagaID string) (domain.Saga, error)
	cancelSagaFn func(ctx context.Context, sagaID string) (domain.Saga, error)
	listSagasFn  func(ctx context.Context, status string, limit int, offset int) ([]domain.Saga, error)
}

func (s *stubSagaService) StartSagaWithIdempotencyKey(ctx context.Context, workflow string, idempotencyKey string, rawContext []byte, steps []service.StartSagaStep) (string, error) {
	if s.startSagaFn == nil {
		return "", nil
	}

	return s.startSagaFn(ctx, workflow, idempotencyKey, rawContext, steps)
}

func (s *stubSagaService) GetSaga(ctx context.Context, sagaID string) (domain.Saga, error) {
	if s.getSagaFn == nil {
		return domain.Saga{}, nil
	}

	return s.getSagaFn(ctx, sagaID)
}

func (s *stubSagaService) CancelSaga(ctx context.Context, sagaID string) (domain.Saga, error) {
	if s.cancelSagaFn == nil {
		return domain.Saga{}, nil
	}

	return s.cancelSagaFn(ctx, sagaID)
}

func (s *stubSagaService) ListSagas(ctx context.Context, status string, limit int, offset int) ([]domain.Saga, error) {
	if s.listSagasFn == nil {
		return nil, nil
	}

	return s.listSagasFn(ctx, status, limit, offset)
}
