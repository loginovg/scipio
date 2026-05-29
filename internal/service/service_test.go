package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"scipio/internal/domain"
	"scipio/internal/lock"
	"scipio/internal/store"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnSagaIDWhenSagaStartsWithValidInput(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount": 42}`))

	// then
	require.NoError(t, err)
	require.NotEmpty(t, sagaID)

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		return saga.Status == domain.SagaStatusCreated &&
			len(saga.Steps) == 1 &&
			saga.Steps[0].Status == domain.SagaStepStatusPending
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldCreatePendingStepWhenSagaStarts(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount": 42}`))

	// then
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		if saga.Status != domain.SagaStatusCreated {
			return false
		}

		if len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		return step.Name == "order_flow" && step.Status == domain.SagaStepStatusPending
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldReturnErrInvalidWorkflowWhenWorkflowIsBlank(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	_, err := svc.StartSaga(context.Background(), "   ", []byte(`{}`))

	// then
	require.ErrorIs(t, err, ErrInvalidWorkflow)
}

func TestShouldReturnErrInvalidContextWhenContextIsInvalidJSON(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	_, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount":`))

	// then
	require.ErrorIs(t, err, ErrInvalidContext)
}

func TestShouldReturnCompensatedSagaWhenCancelRequested(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`))
	require.NoError(t, err)

	// when
	saga, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.NoError(t, cancelErr)
	require.Equal(t, domain.SagaStatusCompensated, saga.Status)
}

func TestShouldFilterSagasByStatusWhenStatusFilterProvided(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	firstID, firstErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 1}`))
	require.NoError(t, firstErr)

	secondID, secondErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 2}`))
	require.NoError(t, secondErr)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), secondID)
	require.NoError(t, cancelErr)

	require.Eventually(t, func() bool {
		sagas, listErr := svc.ListSagas(context.Background(), string(domain.SagaStatusCompensated), 50, 0)
		if listErr != nil {
			return false
		}

		if len(sagas) != 1 {
			return false
		}

		return sagas[0].ID == secondID
	}, 2*time.Second, 20*time.Millisecond)

	// then
	sagas, listErr := svc.ListSagas(context.Background(), "", 50, 0)
	require.NoError(t, listErr)
	require.Len(t, sagas, 2)
	require.NotEqual(t, firstID, secondID)
}

func TestShouldReturnErrInvalidStatusFilterWhenFilterIsUnknown(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	_, err := svc.ListSagas(context.Background(), "UNKNOWN", 50, 0)

	// then
	require.ErrorIs(t, err, ErrInvalidStatusFilter)
}

func TestShouldListSagasWhenStatusFilterContainsOnlyWhitespace(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	_, firstErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 1}`))
	require.NoError(t, firstErr)

	_, secondErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 2}`))
	require.NoError(t, secondErr)

	// when
	sagas, listErr := svc.ListSagas(context.Background(), "   \t", 50, 0)

	// then
	require.NoError(t, listErr)
	require.Len(t, sagas, 2)
}

func TestShouldReturnErrInvalidPaginationWhenPaginationIsNegative(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	_, err := svc.ListSagas(context.Background(), "", -1, 0)

	// then
	require.ErrorIs(t, err, ErrInvalidPagination)

	// when
	_, err = svc.ListSagas(context.Background(), "", 10, -1)

	// then
	require.ErrorIs(t, err, ErrInvalidPagination)
}

func TestShouldReturnErrNotFoundWhenSagaDoesNotExist(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService()

	// when
	_, err := svc.GetSaga(context.Background(), "missing")

	// then
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestShouldReturnLockErrorWhenSagaLockAcquisitionFailsDuringCancel(t *testing.T) {
	t.Parallel()

	// given
	lockErr := errors.New("lock unavailable")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(store.NewMemory(), failingLocker{err: lockErr}, time.Second, logger)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`))
	require.NoError(t, err)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, lockErr)
}

func TestShouldReturnCompensationErrorWhenCompensationUpdateFailsDuringCancel(t *testing.T) {
	t.Parallel()

	// given
	compensationErr := errors.New("compensation failed")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	storeWithFailedSecondUpdate := &failingUpdateStore{
		inner:        store.NewMemory(),
		failAtUpdate: 2,
		err:          compensationErr,
	}
	svc := New(storeWithFailedSecondUpdate, lock.NewNoop(), time.Second, logger)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`))
	require.NoError(t, err)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, compensationErr)
}

func TestShouldReturnErrStoreNotConfiguredWhenStoreIsNil(t *testing.T) {
	t.Parallel()

	// given
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(nil, lock.NewNoop(), time.Second, logger)

	// when
	_, startErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`))

	// then
	require.ErrorIs(t, startErr, ErrStoreNotConfigured)

	// when
	_, getErr := svc.GetSaga(context.Background(), "saga-id")

	// then
	require.ErrorIs(t, getErr, ErrStoreNotConfigured)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), "saga-id")

	// then
	require.ErrorIs(t, cancelErr, ErrStoreNotConfigured)

	// when
	_, listErr := svc.ListSagas(context.Background(), "", 50, 0)

	// then
	require.ErrorIs(t, listErr, ErrStoreNotConfigured)
}

func newTestService() *Service {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(store.NewMemory(), lock.NewNoop(), 5*time.Second, logger)
}

type failingLocker struct {
	err error
}

func (l failingLocker) Acquire(_ context.Context, _ string, _ time.Duration) (lock.Handle, error) {
	return nil, l.err
}

type failingUpdateStore struct {
	inner        *store.Memory
	failAtUpdate int
	err          error
	updates      int
}

func (s *failingUpdateStore) Create(ctx context.Context, saga domain.Saga) error {
	return s.inner.Create(ctx, saga)
}

func (s *failingUpdateStore) Get(ctx context.Context, id string) (domain.Saga, error) {
	return s.inner.Get(ctx, id)
}

func (s *failingUpdateStore) List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error) {
	return s.inner.List(ctx, status, limit, offset)
}

func (s *failingUpdateStore) Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error) {
	s.updates++
	if s.updates == s.failAtUpdate {
		return domain.Saga{}, s.err
	}

	return s.inner.Update(ctx, id, fn)
}
