package httpserver

import (
	"context"
	"testing"

	"scipio/gen/openapi"
	"scipio/internal/domain"
	"scipio/internal/service"
	"scipio/internal/store"

	"github.com/stretchr/testify/require"
)

func TestShouldReturn404WhenGetSagaReturnsNotFound(t *testing.T) {
	t.Parallel()

	srv := New(&stubSagaService{
		getSagaFn: func(context.Context, string) (domain.Saga, error) {
			return domain.Saga{}, store.ErrNotFound
		},
	})

	response, err := srv.GetSaga(context.Background(), openapi.GetSagaRequestObject{Id: "missing"})

	require.NoError(t, err)
	notFoundResponse, ok := response.(openapi.GetSaga404JSONResponse)
	require.True(t, ok)
	require.Equal(t, store.ErrNotFound.Error(), notFoundResponse.Error)
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

type stubSagaService struct {
	startSagaFn  func(ctx context.Context, workflow string, rawContext []byte, steps []service.StartSagaStep) (string, error)
	getSagaFn    func(ctx context.Context, sagaID string) (domain.Saga, error)
	cancelSagaFn func(ctx context.Context, sagaID string) (domain.Saga, error)
	listSagasFn  func(ctx context.Context, status string, limit int, offset int) ([]domain.Saga, error)
}

func (s *stubSagaService) StartSaga(ctx context.Context, workflow string, rawContext []byte, steps []service.StartSagaStep) (string, error) {
	if s.startSagaFn == nil {
		return "", nil
	}

	return s.startSagaFn(ctx, workflow, rawContext, steps)
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
